#include "commands.h"

#include <charconv>
#include <chrono>
#include <cctype>
#include <system_error>

#include "resp_parser.h"

namespace {

constexpr long long kMaxTtlSeconds = 10LL * 365 * 24 * 60 * 60;

std::string to_upper(std::string value) {
    for (char& c : value) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return value;
}

bool parse_integer(const std::string& text, long long& value) {
    if (text.empty()) return false;

    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);

    return result.ec == std::errc{} && result.ptr == end;
}

}  // namespace

std::string dispatch(const std::vector<std::string>& args, Cache& cache) {
    if (args.empty()) return "";

    const std::string cmd = to_upper(args[0]);

    if (cmd == "PING") {
        if (args.size() == 1) return encode_simple("PONG");
        if (args.size() == 2) return encode_bulk(args[1]);
        return encode_error("ERR wrong number of arguments for 'ping' command");
    }

    if (cmd == "ECHO") {
        if (args.size() != 2) {
            return encode_error("ERR wrong number of arguments for 'echo' command");
        }
        return encode_bulk(args[1]);
    }

    if (cmd == "SET") {
        if (args.size() == 3) {
            cache.set(args[1], args[2]);
            return encode_simple("OK");
        }

        if (args.size() != 5) {
            return encode_error("ERR wrong number of arguments for 'set' command");
        }
        if (to_upper(args[3]) != "EX") {
            return encode_error("ERR syntax error");
        }

        long long seconds = 0;
        if (!parse_integer(args[4], seconds) ||
            seconds <= 0 || seconds > kMaxTtlSeconds) {
            return encode_error("ERR invalid expire time in 'set' command");
        }

        cache.set(args[1], args[2], std::chrono::seconds(seconds));
        return encode_simple("OK");
    }

    if (cmd == "GET") {
        if (args.size() != 2) {
            return encode_error("ERR wrong number of arguments for 'get' command");
        }

        auto value = cache.get(args[1]);
        if (!value) return encode_null();
        return encode_bulk(*value);
    }

    if (cmd == "INCR") {
        if (args.size() != 2) {
            return encode_error("ERR wrong number of arguments for 'incr' command");
        }

        long long value = 0;
        if (!cache.increment(args[1], value)) {
            return encode_error("ERR value is not an integer or out of range");
        }
        return encode_integer(value);
    }

    if (cmd == "DEL") {
        if (args.size() < 2) {
            return encode_error("ERR wrong number of arguments for 'del' command");
        }

        const std::vector<std::string> keys(args.begin() + 1, args.end());
        return encode_integer(cache.erase_many(keys));
    }

    if (cmd == "EXPIRE") {
        if (args.size() != 3) {
            return encode_error("ERR wrong number of arguments for 'expire' command");
        }

        long long seconds = 0;
        if (!parse_integer(args[2], seconds) || seconds > kMaxTtlSeconds) {
            return encode_error("ERR value is not an integer or out of range");
        }

        const bool changed = cache.expire(args[1], std::chrono::seconds(seconds));
        return encode_integer(changed ? 1 : 0);
    }

    if (cmd == "TTL") {
        if (args.size() != 2) {
            return encode_error("ERR wrong number of arguments for 'ttl' command");
        }
        return encode_integer(cache.ttl(args[1]));
    }

    return encode_error("ERR unknown command '" + args[0] + "'");
}
