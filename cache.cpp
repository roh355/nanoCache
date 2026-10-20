#include "cache.h"

#include <charconv>
#include <chrono>
#include <limits>
#include <system_error>

void Cache::set(const std::string& key, const std::string& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] = Entry{value, std::nullopt};
}

void Cache::set(const std::string& key,
                const std::string& value,
                std::chrono::seconds ttl_value) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_[key] = Entry{value, Clock::now() + ttl_value};
}

Cache::Entries::iterator Cache::find_live(const std::string& key) {
    auto it = data_.find(key);
    if (it == data_.end()) return it;

    if (it->second.expires_at && Clock::now() >= *it->second.expires_at) {
        data_.erase(it);
        return data_.end();
    }

    return it;
}

std::optional<std::string> Cache::get(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = find_live(key);
    if (it == data_.end()) return std::nullopt;
    return it->second.value;
}

bool Cache::increment(const std::string& key, long long& value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = find_live(key);
    long long current = 0;

    if (it != data_.end()) {
        const std::string& stored_value = it->second.value;
        if (stored_value.empty()) return false;

        const char* begin = stored_value.data();
        const char* end = begin + stored_value.size();
        const auto parsed = std::from_chars(begin, end, current);
        if (parsed.ec != std::errc{} || parsed.ptr != end) return false;
    }

    if (current == std::numeric_limits<long long>::max()) return false;
    ++current;

    if (it == data_.end()) {
        data_.emplace(key, Entry{std::to_string(current), std::nullopt});
    } else {
        // Updating only the value preserves any existing expiration time.
        it->second.value = std::to_string(current);
    }

    value = current;
    return true;
}

bool Cache::erase(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = find_live(key);
    if (it == data_.end()) return false;

    data_.erase(it);
    return true;
}

long long Cache::erase_many(const std::vector<std::string>& keys) {
    std::lock_guard<std::mutex> lock(mutex_);
    long long removed = 0;
    for (const std::string& key : keys) {
        auto it = find_live(key);
        if (it == data_.end()) continue;
        data_.erase(it);
        ++removed;
    }
    return removed;
}

bool Cache::expire(const std::string& key, std::chrono::seconds ttl_value) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = find_live(key);
    if (it == data_.end()) return false;

    if (ttl_value.count() <= 0) {
        data_.erase(it);
        return true;
    }

    it->second.expires_at = Clock::now() + ttl_value;
    return true;
}

long long Cache::ttl(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = find_live(key);
    if (it == data_.end()) return -2;
    if (!it->second.expires_at) return -1;

    const auto now = Clock::now();
    if (now >= *it->second.expires_at) {
        data_.erase(it);
        return -2;
    }

    return std::chrono::duration_cast<std::chrono::seconds>(
        *it->second.expires_at - now
    ).count();
}
