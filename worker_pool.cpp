#include "worker_pool.h"

#include <cerrno>
#include <utility>
#include <unistd.h>

#include "commands.h"

WorkerPool::WorkerPool(std::size_t worker_count, Cache& cache, int notification_fd)
    : cache_(cache), notification_fd_(notification_fd) {
    if (worker_count == 0) worker_count = 1;

    workers_.reserve(worker_count);
    for (std::size_t i = 0; i < worker_count; ++i) {
        workers_.emplace_back(&WorkerPool::worker_loop, this);
    }
}

WorkerPool::~WorkerPool() {
    shutdown();
}

bool WorkerPool::submit(WorkerTask task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stopping_) return false;
        tasks_.push(std::move(task));
    }
    work_available_.notify_one();
    return true;
}

std::vector<WorkerResult> WorkerPool::take_completed() {
    std::vector<WorkerResult> results;
    std::lock_guard<std::mutex> lock(queue_mutex_);
    results.reserve(completed_.size());
    while (!completed_.empty()) {
        results.push_back(std::move(completed_.front()));
        completed_.pop();
    }
    return results;
}

void WorkerPool::shutdown() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stopping_) return;
        stopping_ = true;
        while (!tasks_.empty()) tasks_.pop();
    }
    work_available_.notify_all();

    for (std::thread& worker : workers_) {
        if (worker.joinable()) worker.join();
    }
}

void WorkerPool::worker_loop() {
    while (true) {
        WorkerTask task;
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            work_available_.wait(lock, [this] {
                return stopping_ || !tasks_.empty();
            });
            if (stopping_) return;

            task = std::move(tasks_.front());
            tasks_.pop();
        }

        std::string response = dispatch(task.args, cache_);
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            completed_.push({task.connection_id, std::move(response)});
        }
        notify_main();
    }
}

void WorkerPool::notify_main() const {
    const char byte = 1;
    while (true) {
        const ssize_t written = write(notification_fd_, &byte, sizeof(byte));
        if (written == static_cast<ssize_t>(sizeof(byte))) return;
        if (written < 0 && errno == EINTR) continue;
        // A full nonblocking pipe is already readable, so no wakeup is lost.
        return;
    }
}
