#include "cache.h"

#include <chrono>

void Cache::set(const std::string& key, const std::string& value) {
    data_[key] = Entry{value, std::nullopt};
}

void Cache::set(const std::string& key,
                const std::string& value,
                std::chrono::seconds ttl_value) {
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
    auto it = find_live(key);
    if (it == data_.end()) return std::nullopt;
    return it->second.value;
}

bool Cache::erase(const std::string& key) {
    auto it = find_live(key);
    if (it == data_.end()) return false;

    data_.erase(it);
    return true;
}

bool Cache::expire(const std::string& key, std::chrono::seconds ttl_value) {
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
