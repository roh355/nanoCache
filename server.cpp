#include <arpa/inet.h>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <string>
#include <sys/socket.h>
#include <unordered_map>
#include <unistd.h>
#include <utility>
#include <vector>

#include "cache.h"
#include "resp_parser.h"
#include "worker_pool.h"

namespace {

constexpr int kPort = 9000;
constexpr std::size_t kWorkerThreadCount = 4;
constexpr std::size_t kMaxInputBufferBytes = 4 * 1024 * 1024;
constexpr std::size_t kMaxPendingOutputBytes = 4 * 1024 * 1024;
constexpr std::size_t kMaxQueuedRequestsPerClient = 256;
constexpr char kSnapshotPath[] = "nanocache.snapshot";

volatile sig_atomic_t shutdown_requested = 0;
volatile sig_atomic_t signal_wakeup_fd = -1;

void handle_shutdown_signal(int) {
    shutdown_requested = 1;
    const int saved_errno = errno;
    const int fd = static_cast<int>(signal_wakeup_fd);
    if (fd >= 0) {
        const char byte = 1;
        (void)write(fd, &byte, sizeof(byte));
    }
    errno = saved_errno;
}

struct PendingRequest {
    std::vector<std::string> args;
    std::string immediate_response;
    bool is_immediate_response = false;
};

struct ClientState {
    std::uint64_t connection_id = 0;
    std::string input;
    std::string output;
    std::size_t output_offset = 0;
    std::deque<PendingRequest> pending_requests;
    bool request_in_flight = false;
    bool close_when_flushed = false;
    bool socket_dead = false;
};

bool set_nonblocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

bool queue_response(ClientState& client, const std::string& response) {
    if (client.socket_dead) return false;

    const std::size_t pending = client.output.size() - client.output_offset;
    if (response.size() > kMaxPendingOutputBytes - pending) {
        client.close_when_flushed = true;
        client.input.clear();
        client.pending_requests.clear();
        return false;
    }

    if (client.output_offset != 0) {
        client.output.erase(0, client.output_offset);
        client.output_offset = 0;
    }
    client.output.append(response);
    return true;
}

void queue_protocol_error(ClientState& client, const std::string& message) {
    PendingRequest error;
    error.immediate_response = encode_error(message);
    error.is_immediate_response = true;
    client.pending_requests.push_back(std::move(error));
    client.close_when_flushed = true;
    client.input.clear();
}

void process_input(ClientState& client) {
    while (!client.input.empty() && !client.close_when_flushed &&
           client.pending_requests.size() < kMaxQueuedRequestsPerClient) {
        ParseResult parsed = try_parse_command(client.input);
        if (parsed.status == ParseStatus::incomplete) return;
        if (parsed.status == ParseStatus::error || parsed.consumed == 0 ||
            parsed.consumed > client.input.size()) {
            queue_protocol_error(client, "ERR protocol error");
            return;
        }

        client.input.erase(0, parsed.consumed);
        if (!parsed.args.empty()) {
            PendingRequest request;
            request.args = std::move(parsed.args);
            client.pending_requests.push_back(std::move(request));
        }
    }

    if (client.input.size() > kMaxInputBufferBytes) {
        queue_protocol_error(client, "ERR input buffer limit exceeded");
    }
}

void schedule_next(ClientState& client, WorkerPool& workers) {
    if (client.request_in_flight || client.socket_dead) return;

    while (!client.pending_requests.empty()) {
        PendingRequest request = std::move(client.pending_requests.front());
        client.pending_requests.pop_front();

        if (request.is_immediate_response) {
            if (!queue_response(client, request.immediate_response)) {
                client.pending_requests.clear();
                return;
            }
            continue;
        }

        client.request_in_flight = true;
        if (!workers.submit({client.connection_id, std::move(request.args)})) {
            client.request_in_flight = false;
            client.pending_requests.clear();
            queue_response(client, encode_error("ERR server is shutting down"));
            client.close_when_flushed = true;
        }
        return;
    }
}

void mark_client_dead(ClientState& client) {
    client.socket_dead = true;
    client.close_when_flushed = true;
    client.input.clear();
    client.output.clear();
    client.output_offset = 0;
    client.pending_requests.clear();
}

bool read_client(int fd, ClientState& client) {
    char buffer[4096];
    while (!client.close_when_flushed &&
           client.pending_requests.size() < kMaxQueuedRequestsPerClient) {
        const ssize_t count = recv(fd, buffer, sizeof(buffer), 0);
        if (count > 0) {
            client.input.append(buffer, static_cast<std::size_t>(count));
            process_input(client);
            if (client.pending_requests.size() >= kMaxQueuedRequestsPerClient) break;
            continue;
        }
        if (count == 0) {
            client.close_when_flushed = true;
            client.input.clear();
            break;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        return false;
    }
    return true;
}

bool flush_output(int fd, ClientState& client) {
    while (client.output_offset < client.output.size()) {
        const char* data = client.output.data() + client.output_offset;
        const std::size_t remaining = client.output.size() - client.output_offset;
        const ssize_t sent = send(fd, data, remaining, 0);
        if (sent > 0) {
            client.output_offset += static_cast<std::size_t>(sent);
            continue;
        }
        if (sent < 0 && errno == EINTR) continue;
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) return true;
        return false;
    }

    client.output.clear();
    client.output_offset = 0;
    return true;
}

void accept_clients(int server_fd,
                    std::unordered_map<int, ClientState>& clients,
                    std::unordered_map<std::uint64_t, int>& fd_by_connection_id,
                    std::uint64_t& next_connection_id) {
    while (true) {
        sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        const int client_fd = accept(server_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (errno != EAGAIN && errno != EWOULDBLOCK) perror("accept");
            return;
        }

        if (!set_nonblocking(client_fd)) {
            perror("fcntl client");
            close(client_fd);
            continue;
        }

        ClientState client;
        client.connection_id = next_connection_id++;
        if (client.connection_id == 0) client.connection_id = next_connection_id++;
        clients.emplace(client_fd, std::move(client));
        fd_by_connection_id.emplace(clients.at(client_fd).connection_id, client_fd);

        char address[INET_ADDRSTRLEN] = {};
        if (inet_ntop(AF_INET, &client_addr.sin_addr, address, sizeof(address)) == nullptr) {
            std::strncpy(address, "unknown", sizeof(address) - 1);
        }
        std::printf("client connected: %s (fd %d)\n", address, client_fd);
        std::fflush(stdout);
    }
}

void close_client(int fd,
                  std::unordered_map<int, ClientState>& clients,
                  std::unordered_map<std::uint64_t, int>& fd_by_connection_id) {
    const auto found = clients.find(fd);
    if (found == clients.end()) return;

    fd_by_connection_id.erase(found->second.connection_id);
    close(fd);
    clients.erase(found);
    std::printf("client disconnected (fd %d)\n", fd);
    std::fflush(stdout);
}

bool ready_to_close(const ClientState& client) {
    return client.close_when_flushed && !client.request_in_flight &&
           client.pending_requests.empty() &&
           client.output_offset == client.output.size();
}

void drain_notifications(int fd) {
    char buffer[256];
    while (true) {
        const ssize_t count = read(fd, buffer, sizeof(buffer));
        if (count > 0) continue;
        if (count < 0 && errno == EINTR) continue;
        break;
    }
}

void handle_worker_results(
    WorkerPool& workers,
    std::unordered_map<int, ClientState>& clients,
    std::unordered_map<std::uint64_t, int>& fd_by_connection_id) {
    std::vector<WorkerResult> results = workers.take_completed();
    for (WorkerResult& result : results) {
        const auto fd_found = fd_by_connection_id.find(result.connection_id);
        if (fd_found == fd_by_connection_id.end()) continue;

        const int fd = fd_found->second;
        auto client_found = clients.find(fd);
        if (client_found == clients.end() ||
            client_found->second.connection_id != result.connection_id) {
            continue;
        }

        ClientState& client = client_found->second;
        client.request_in_flight = false;
        if (!client.socket_dead && queue_response(client, result.response)) {
            process_input(client);
        }

        if (client.socket_dead) client.pending_requests.clear();
        schedule_next(client, workers);
        if (ready_to_close(client)) {
            close_client(fd, clients, fd_by_connection_id);
        }
    }
}

}  // namespace

int main() {
    signal(SIGPIPE, SIG_IGN);

    const int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    int reuse_address = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address)) < 0) {
        perror("setsockopt");
        close(server_fd);
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(kPort);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        perror("bind");
        close(server_fd);
        return 1;
    }
    if (listen(server_fd, 128) < 0) {
        perror("listen");
        close(server_fd);
        return 1;
    }
    if (!set_nonblocking(server_fd)) {
        perror("fcntl server");
        close(server_fd);
        return 1;
    }

    int notification_pipe[2] = {-1, -1};
    if (pipe(notification_pipe) < 0) {
        perror("worker notification pipe");
        close(server_fd);
        return 1;
    }
    if (!set_nonblocking(notification_pipe[0]) ||
        !set_nonblocking(notification_pipe[1])) {
        perror("fcntl worker notification pipe");
        close(notification_pipe[0]);
        close(notification_pipe[1]);
        close(server_fd);
        return 1;
    }

    signal_wakeup_fd = notification_pipe[1];
    struct sigaction shutdown_action {};
    shutdown_action.sa_handler = handle_shutdown_signal;
    sigemptyset(&shutdown_action.sa_mask);
    shutdown_action.sa_flags = 0;
    if (sigaction(SIGINT, &shutdown_action, nullptr) < 0 ||
        sigaction(SIGTERM, &shutdown_action, nullptr) < 0) {
        perror("sigaction");
        signal_wakeup_fd = -1;
        close(notification_pipe[0]);
        close(notification_pipe[1]);
        close(server_fd);
        return 1;
    }

    std::printf("listening on port %d\n", kPort);
    std::fflush(stdout);

    Cache cache;
    if (!cache.load_snapshot(kSnapshotPath)) {
        std::fprintf(stderr, "snapshot is invalid or could not be read: %s\n",
                     kSnapshotPath);
        signal_wakeup_fd = -1;
        close(notification_pipe[0]);
        close(notification_pipe[1]);
        close(server_fd);
        return 1;
    }

    std::unordered_map<int, ClientState> clients;
    std::unordered_map<std::uint64_t, int> fd_by_connection_id;
    std::uint64_t next_connection_id = 1;
    int exit_status = 0;
    WorkerPool workers(kWorkerThreadCount, cache, notification_pipe[1]);
    std::printf("started %zu worker threads\n", kWorkerThreadCount);
    std::fflush(stdout);

    while (!shutdown_requested) {
        std::vector<pollfd> poll_fds;
        std::vector<std::uint64_t> event_connection_ids;
        poll_fds.reserve(clients.size() + 2);
        event_connection_ids.reserve(clients.size() + 2);
        poll_fds.push_back({notification_pipe[0], POLLIN, 0});
        event_connection_ids.push_back(0);
        poll_fds.push_back({server_fd, POLLIN, 0});
        event_connection_ids.push_back(0);

        for (const auto& entry : clients) {
            const ClientState& client = entry.second;
            short events = 0;
            if (!client.close_when_flushed &&
                client.pending_requests.size() < kMaxQueuedRequestsPerClient) {
                events |= POLLIN;
            }
            if (client.output_offset < client.output.size()) events |= POLLOUT;
            if (events == 0) continue;

            poll_fds.push_back({entry.first, events, 0});
            event_connection_ids.push_back(client.connection_id);
        }

        const int ready = poll(poll_fds.data(), poll_fds.size(), -1);
        if (ready < 0) {
            if (errno == EINTR) {
                if (shutdown_requested) break;
                continue;
            }
            perror("poll");
            exit_status = 1;
            break;
        }
        if (shutdown_requested) break;

        for (std::size_t i = 0; i < poll_fds.size(); ++i) {
            const pollfd& event = poll_fds[i];
            if (event.revents == 0) continue;

            if (event.fd == notification_pipe[0]) {
                drain_notifications(notification_pipe[0]);
                handle_worker_results(workers, clients, fd_by_connection_id);
                continue;
            }
            if (event.fd == server_fd) {
                if ((event.revents & POLLIN) && !shutdown_requested) {
                    accept_clients(server_fd, clients, fd_by_connection_id,
                                   next_connection_id);
                }
                continue;
            }

            auto found = clients.find(event.fd);
            if (found == clients.end() ||
                found->second.connection_id != event_connection_ids[i]) {
                continue;
            }

            ClientState& client = found->second;
            if (event.revents & (POLLNVAL | POLLERR)) {
                mark_client_dead(client);
            } else {
                const bool can_read = !client.close_when_flushed &&
                    client.pending_requests.size() < kMaxQueuedRequestsPerClient;
                if ((event.revents & (POLLIN | POLLHUP)) && can_read) {
                    if (!read_client(event.fd, client)) {
                        mark_client_dead(client);
                    } else {
                        schedule_next(client, workers);
                    }
                }
                if (!client.socket_dead && (event.revents & POLLOUT) &&
                    !flush_output(event.fd, client)) {
                    mark_client_dead(client);
                }
            }

            if (ready_to_close(client)) {
                close_client(event.fd, clients, fd_by_connection_id);
            }
        }
    }

    close(server_fd);
    workers.shutdown();
    (void)workers.take_completed();

    for (const auto& entry : clients) close(entry.first);
    clients.clear();
    fd_by_connection_id.clear();

    if (!cache.save_snapshot(kSnapshotPath)) {
        std::fprintf(stderr, "could not save snapshot: %s\n", kSnapshotPath);
        exit_status = 1;
    }

    signal_wakeup_fd = -1;
    close(notification_pipe[0]);
    close(notification_pipe[1]);
    return exit_status;
}
