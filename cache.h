#pragma once

#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

class Cache {
public:
    void set(const std::string& key, const std::string& value);
    void set(const std::string& key,
             const std::string& value,
             std::chrono::seconds ttl);

    std::optional<std::string> get(const std::string& key);
    bool increment(const std::string& key, long long& value);
    bool erase(const std::string& key);
    long long erase_many(const std::vector<std::string>& keys);
    bool expire(const std::string& key, std::chrono::seconds ttl);
    long long ttl(const std::string& key);

    bool save_snapshot(const std::string& path) const;
    bool load_snapshot(const std::string& path);

private:
    using Clock = std::chrono::steady_clock;

    struct Entry {
        std::string value;
        std::optional<Clock::time_point> expires_at;
    };

    using Entries = std::unordered_map<std::string, Entry>;

    Entries::iterator find_live(const std::string& key);

    mutable std::mutex mutex_;
    Entries data_;
};
