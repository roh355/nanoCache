#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <string>
#include <sys/socket.h>
#include <unordered_map>
#include <unistd.h>
#include <vector>

#include "cache.h"
#include "commands.h"
#include "resp_parser.h"

namespace {

constexpr int kPort = 9000;
constexpr std::size_t kMaxInputBufferBytes = 4 * 1024 * 1024;
constexpr std::size_t kMaxPendingOutputBytes = 4 * 1024 * 1024;
constexpr char kSnapshotPath[]="nanocache.snapshot";

volatile sig_atomic_t shutdown_requested = 0;

void handle_shutdown_signal(int){
    shutdown_requested = 1;
}

struct ClientState {
    std::string input;
    std::string output;
    std::size_t output_offset = 0;
    bool close_when_flushed = false;
};

bool set_nonblocking(int fd) {
    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return false;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

bool queue_response(ClientState& client, const std::string& response) {
    const std::size_t pending = client.output.size() - client.output_offset;
    if (response.size() > kMaxPendingOutputBytes - pending) {
        client.close_when_flushed = true;
        client.input.clear();
        return false;
    }

    if (client.output_offset != 0) {
        client.output.erase(0, client.output_offset);
        client.output_offset = 0;
    }
    client.output.append(response);
    return true;
}

void process_input(ClientState& client, Cache& cache) {
    while (!client.input.empty() && !client.close_when_flushed) {
        ParseResult parsed = try_parse_command(client.input);
        if (parsed.status == ParseStatus::incomplete) return;
        if (parsed.status == ParseStatus::error ||
            parsed.consumed == 0 || parsed.consumed > client.input.size()) {
            queue_response(client, encode_error("ERR protocol error"));
            client.close_when_flushed = true;
            client.input.clear();
            return;
        }

        client.input.erase(0, parsed.consumed);
        if (!parsed.args.empty()) {
            queue_response(client, dispatch(parsed.args, cache));
        }
    }
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

void accept_clients(int server_fd, std::unordered_map<int, ClientState>& clients) {
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

        clients.emplace(client_fd, ClientState{});
        char address[INET_ADDRSTRLEN] = {};
        if (inet_ntop(AF_INET, &client_addr.sin_addr, address, sizeof(address)) == nullptr) {
            std::strncpy(address, "unknown", sizeof(address) - 1);
        }
        std::printf("client connected: %s (fd %d)\n", address, client_fd);
        std::fflush(stdout);
    }
}

bool read_client(int fd, ClientState& client, Cache& cache) {
    char buffer[4096];
    while (!client.close_when_flushed) {
        const ssize_t count = recv(fd, buffer, sizeof(buffer), 0);
        if (count > 0) {
            client.input.append(buffer, static_cast<std::size_t>(count));
            process_input(client, cache);
            if (client.input.size() > kMaxInputBufferBytes) {
                queue_response(client, encode_error("ERR input buffer limit exceeded"));
                client.close_when_flushed = true;
                client.input.clear();
                break;
            }
            continue;
        }
        if (count == 0) {
            client.close_when_flushed = true;
            break;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        return false;
    }
    return true;
}

void close_client(int fd, std::unordered_map<int, ClientState>& clients) {
    close(fd);
    clients.erase(fd);
    std::printf("client disconnected (fd %d)\n", fd);
    std::fflush(stdout);
}

}  // namespace

int main() {
    signal(SIGPIPE, SIG_IGN);
    struct sigaction shutdown_action {};
    shutdown_action.sa_handler = handle_shutdown_signal;
    sigemptyset(&shutdown_action.sa_mask);
    shutdown_action.sa_flags = 0;

    if (sigaction(SIGINT, &shutdown_action, nullptr) < 0 ||
        sigaction(SIGTERM, &shutdown_action, nullptr) < 0) {
        perror("sigaction");
        return 1;
    }

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

    std::printf("listening on port %d\n", kPort);
    std::fflush(stdout);

    Cache cache;
    if (!cache.load_snapshot(kSnapshotPath)) {
        std::fprintf(stderr, "snapshot is invalid or could not be read: %s\n",
                     kSnapshotPath);
        close(server_fd);
        return 1;
    }
    std::unordered_map<int, ClientState> clients;
    int exit_status=0;
    while (!shutdown_requested) {
        std::vector<pollfd> poll_fds;
        poll_fds.reserve(clients.size() + 1);
        poll_fds.push_back({server_fd, POLLIN, 0});
        for (const auto& entry : clients) {
            const ClientState& client = entry.second;
            short events = client.close_when_flushed ? 0 : POLLIN;
            if (client.output_offset < client.output.size()) events |= POLLOUT;
            poll_fds.push_back({entry.first, events, 0});
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
        for (const pollfd& event : poll_fds) {
            if (event.revents == 0) continue;
            if (event.fd == server_fd) {
                if (event.revents & POLLIN) accept_clients(server_fd, clients);
                continue;
            }

            auto found = clients.find(event.fd);
            if (found == clients.end()) continue;
            ClientState& client = found->second;
            bool keep_client = true;

            if (event.revents & POLLNVAL) {
                keep_client = false;
            } else {
                if (event.revents & POLLIN) keep_client = read_client(event.fd, client, cache);
                if (keep_client && (event.revents & (POLLERR | POLLHUP))) {
                    client.close_when_flushed = true;
                }
                if (keep_client && (event.revents & POLLOUT)) {
                    keep_client = flush_output(event.fd, client);
                }
                if (keep_client && client.close_when_flushed &&
                    client.output_offset == client.output.size()) {
                    keep_client = false;
                }
            }

            if (!keep_client) close_client(event.fd, clients);
        }
    }

    if (!cache.save_snapshot(kSnapshotPath)) {
        std::fprintf(stderr, "could not save snapshot: %s\n", kSnapshotPath);
        exit_status = 1;
    }
    
    for (const auto& entry : clients) {
        close(entry.first);
    }
    
    close(server_fd);
    return exit_status;
}
