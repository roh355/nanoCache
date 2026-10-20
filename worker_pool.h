#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

#include "cache.h"

struct WorkerTask {
    std::uint64_t connection_id;
    std::vector<std::string> args;
};

struct WorkerResult {
    std::uint64_t connection_id;
    std::string response;
};

class WorkerPool {
public:
    WorkerPool(std::size_t worker_count, Cache& cache, int notification_fd);
    ~WorkerPool();

    WorkerPool(const WorkerPool&) = delete;
    WorkerPool& operator=(const WorkerPool&) = delete;

    bool submit(WorkerTask task);
    std::vector<WorkerResult> take_completed();
    void shutdown();

private:
    void worker_loop();
    void notify_main() const;

    Cache& cache_;
    int notification_fd_;
    std::mutex queue_mutex_;
    std::condition_variable work_available_;
    std::queue<WorkerTask> tasks_;
    std::queue<WorkerResult> completed_;
    bool stopping_ = false;
    std::vector<std::thread> workers_;
};
