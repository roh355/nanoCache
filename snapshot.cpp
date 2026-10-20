#include "cache.h"

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <limits>
#include <string>
#include <unordered_set>
#include <utility>

namespace {

// Version 2 records include an expiration timestamp.
constexpr char kSnapshotMagic[] = "NACACHE2";
constexpr std::uint64_t kMaxEntries = 1'000'000;
constexpr std::uint64_t kMaxFieldBytes = 1ULL * 1024 * 1024;
constexpr std::uint64_t kMaxSnapshotBytes = 512ULL * 1024 * 1024;
constexpr std::int64_t kMaxTtlMilliseconds =
    10LL * 365 * 24 * 60 * 60 * 1000;

bool write_u64(std::ostream& out, std::uint64_t value) {
    unsigned char bytes[8];
    for (int i = 0; i < 8; ++i) {
        bytes[i] = static_cast<unsigned char>((value >> (i * 8)) & 0xff);
    }
    out.write(reinterpret_cast<const char*>(bytes), sizeof(bytes));
    return static_cast<bool>(out);
}

bool read_u64(std::istream& in, std::uint64_t& value) {
    unsigned char bytes[8];
    if (!in.read(reinterpret_cast<char*>(bytes), sizeof(bytes))) return false;

    value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<std::uint64_t>(bytes[i]) << (i * 8);
    }
    return true;
}

}  // namespace

bool Cache::save_snapshot(const std::string& path) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto steady_now = Clock::now();
    const auto wall_now = std::chrono::system_clock::now();

    std::uint64_t live_entries = 0;
    for (const auto& entry : data_) {
        if (!entry.second.expires_at || *entry.second.expires_at > steady_now) {
            ++live_entries;
        }
    }
    if (live_entries > kMaxEntries) return false;

    const std::string temporary_path = path + ".tmp";
    std::ofstream out(temporary_path, std::ios::binary | std::ios::trunc);
    if (!out) return false;

    auto fail = [&]() {
        out.close();
        std::remove(temporary_path.c_str());
        return false;
    };

    std::uint64_t bytes_written = 16;  // Magic marker and entry count
    out.write(kSnapshotMagic, sizeof(kSnapshotMagic) - 1);
    if (!out || !write_u64(out, live_entries)) return fail();

    for (const auto& entry : data_) {
        const std::string& key = entry.first;
        const Entry& value_entry = entry.second;

        if (value_entry.expires_at && *value_entry.expires_at <= steady_now) {
            continue;
        }

        const auto key_size = static_cast<std::uint64_t>(key.size());
        const auto value_size = static_cast<std::uint64_t>(value_entry.value.size());
        if (key_size > kMaxFieldBytes || value_size > kMaxFieldBytes) {
            return fail();
        }

        constexpr std::uint64_t kRecordHeaderBytes = 24;
        const std::uint64_t record_size =
            kRecordHeaderBytes + key_size + value_size;
        if (record_size > kMaxSnapshotBytes - bytes_written) return fail();
        bytes_written += record_size;

        std::uint64_t expiry_unix_ms = 0;
        if (value_entry.expires_at) {
            const auto remaining = *value_entry.expires_at - steady_now;
            const auto wall_deadline = wall_now +
                std::chrono::duration_cast<std::chrono::system_clock::duration>(remaining);
            const auto expiry_count = std::chrono::duration_cast<std::chrono::milliseconds>(
                wall_deadline.time_since_epoch()
            ).count();

            if (expiry_count <= 0) return fail();
            expiry_unix_ms = static_cast<std::uint64_t>(expiry_count);
        }

        if (!write_u64(out, key_size) ||
            !write_u64(out, value_size) ||
            !write_u64(out, expiry_unix_ms)) {
            return fail();
        }

        out.write(key.data(), static_cast<std::streamsize>(key.size()));
        out.write(value_entry.value.data(),
                  static_cast<std::streamsize>(value_entry.value.size()));
        if (!out) return fail();
    }

    out.flush();
    if (!out) return fail();

    out.close();
    if (!out) {
        std::remove(temporary_path.c_str());
        return false;
    }

    // The temporary file is beside the snapshot, so rename stays on one filesystem.
    if (std::rename(temporary_path.c_str(), path.c_str()) != 0) {
        std::remove(temporary_path.c_str());
        return false;
    }

    return true;
}

bool Cache::load_snapshot(const std::string& path) {
    std::lock_guard<std::mutex> lock(mutex_);
    errno = 0;
    std::ifstream in(path, std::ios::binary);
    if (!in) return errno == ENOENT;  // No snapshot is normal on the first run.

    char magic[sizeof(kSnapshotMagic) - 1];
    if (!in.read(magic, sizeof(magic))) return false;
    if (std::memcmp(magic, kSnapshotMagic, sizeof(magic)) != 0) return false;

    std::uint64_t entry_count = 0;
    if (!read_u64(in, entry_count) || entry_count > kMaxEntries) return false;

    Entries loaded;
    std::unordered_set<std::string> seen_keys;
    std::uint64_t bytes_read = 16;
    const auto wall_now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()
    ).count();
    const auto steady_now = Clock::now();

    for (std::uint64_t i = 0; i < entry_count; ++i) {
        constexpr std::uint64_t kRecordHeaderBytes = 24;
        if (bytes_read > kMaxSnapshotBytes - kRecordHeaderBytes) return false;

        std::uint64_t key_size = 0;
        std::uint64_t value_size = 0;
        std::uint64_t expiry_unix_ms = 0;
        if (!read_u64(in, key_size) ||
            !read_u64(in, value_size) ||
            !read_u64(in, expiry_unix_ms)) {
            return false;
        }
        bytes_read += kRecordHeaderBytes;

        if (key_size > kMaxFieldBytes || value_size > kMaxFieldBytes) return false;
        if (key_size > kMaxSnapshotBytes - bytes_read) return false;
        bytes_read += key_size;
        if (value_size > kMaxSnapshotBytes - bytes_read) return false;
        bytes_read += value_size;

        if (expiry_unix_ms > static_cast<std::uint64_t>(
                std::numeric_limits<std::int64_t>::max())) {
            return false;
        }

        std::string key(static_cast<std::size_t>(key_size), '\0');
        std::string value(static_cast<std::size_t>(value_size), '\0');
        if (key_size != 0 &&
            !in.read(&key[0], static_cast<std::streamsize>(key_size))) {
            return false;
        }
        if (value_size != 0 &&
            !in.read(&value[0], static_cast<std::streamsize>(value_size))) {
            return false;
        }

        if (!seen_keys.emplace(key).second) return false;

        Entry loaded_entry{std::move(value), std::nullopt};
        if (expiry_unix_ms != 0) {
            const auto expiry_ms = static_cast<std::int64_t>(expiry_unix_ms);
            if (expiry_ms <= wall_now_ms) continue;  // Already expired.

            std::int64_t remaining_ms = expiry_ms - wall_now_ms;
            if (remaining_ms > kMaxTtlMilliseconds) {
                remaining_ms = kMaxTtlMilliseconds;
            }
            loaded_entry.expires_at = steady_now +
                std::chrono::milliseconds(remaining_ms);
        }

        loaded.emplace(std::move(key), std::move(loaded_entry));
    }

    char extra;
    if (in.get(extra)) return false;
    if (!in.eof()) return false;

    data_.swap(loaded);
    return true;
}
