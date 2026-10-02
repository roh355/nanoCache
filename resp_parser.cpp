#include "resp_parser.h"

#include <charconv>
#include <cstddef>
#include <sstream>
#include <string_view>
#include <utility>

namespace {

constexpr std::size_t kMaxFrameBytes = 1024 * 1024;
constexpr long long kMaxArrayItems = 1024;

ParseResult incomplete() {
    return {ParseStatus::incomplete, {}, 0, {}};
}

ParseResult protocol_error(const char* message) {
    return {ParseStatus::error, {}, 0, message};
}

bool parse_integer(std::string_view text, long long& value) {
    if (text.empty()) return false;
    const char* begin = text.data();
    const char* end = begin + text.size();
    const auto result = std::from_chars(begin, end, value);
    return result.ec == std::errc{} && result.ptr == end;
}

ParseResult parse_inline_command(const std::string& buffer) {
    const std::size_t line_end = buffer.find("\r\n");
    if (line_end == std::string::npos) {
        if (buffer.size() > kMaxFrameBytes) {
            return protocol_error("inline command is too large");
        }
        return incomplete();
    }
    if (line_end + 2 > kMaxFrameBytes) {
        return protocol_error("inline command is too large");
    }

    std::istringstream input(buffer.substr(0, line_end));
    std::vector<std::string> args;
    std::string word;
    while (input >> word) {
        args.push_back(word);
        if (args.size() > static_cast<std::size_t>(kMaxArrayItems)) {
            return protocol_error("too many command arguments");
        }
    }
    return {ParseStatus::complete, std::move(args), line_end + 2, {}};
}

}  // namespace

ParseResult try_parse_command(const std::string& buffer) {
    if (buffer.empty()) return incomplete();
    if (buffer[0] != '*') return parse_inline_command(buffer);

    const std::size_t line_end = buffer.find("\r\n", 1);
    if (line_end == std::string::npos) {
        if (buffer.size() > kMaxFrameBytes) {
            return protocol_error("command frame is too large");
        }
        return incomplete();
    }
    if (line_end + 2 > kMaxFrameBytes) {
        return protocol_error("command frame is too large");
    }

    long long count = 0;
    if (!parse_integer(std::string_view(buffer).substr(1, line_end - 1), count) ||
        count < 0 || count > kMaxArrayItems) {
        return protocol_error("invalid array length");
    }

    std::size_t pos = line_end + 2;
    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(count));
    for (long long i = 0; i < count; ++i) {
        if (pos >= buffer.size()) return incomplete();
        if (buffer[pos] != '$') return protocol_error("expected a bulk string");

        const std::size_t length_end = buffer.find("\r\n", pos + 1);
        if (length_end == std::string::npos) {
            if (buffer.size() > kMaxFrameBytes) {
                return protocol_error("command frame is too large");
            }
            return incomplete();
        }

        long long length = 0;
        if (!parse_integer(std::string_view(buffer).substr(pos + 1, length_end - pos - 1), length) ||
            length < 0 || length > static_cast<long long>(kMaxFrameBytes)) {
            return protocol_error("invalid bulk string length");
        }

        const std::size_t data_start = length_end + 2;
        const std::size_t data_length = static_cast<std::size_t>(length);
        if (data_start > kMaxFrameBytes - 2 ||
            data_length > kMaxFrameBytes - data_start - 2) {
            return protocol_error("command frame is too large");
        }
        if (data_length > buffer.size() - data_start) return incomplete();
        const std::size_t data_end = data_start + data_length;
        if (buffer.size() - data_end < 2) return incomplete();
        if (buffer[data_end] != '\r' || buffer[data_end + 1] != '\n') {
            return protocol_error("bulk string is missing its terminator");
        }

        args.push_back(buffer.substr(data_start, data_length));
        pos = data_end + 2;
        if (pos > kMaxFrameBytes) return protocol_error("command frame is too large");
    }

    return {ParseStatus::complete, std::move(args), pos, {}};
}

std::string encode_simple(const std::string& value) {
    return "+" + value + "\r\n";
}

std::string encode_error(const std::string& value) {
    return "-" + value + "\r\n";
}

std::string encode_bulk(const std::string& value) {
    return "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
}

std::string encode_integer(long long value) {
    return ":" + std::to_string(value) + "\r\n";
}

std::string encode_null() {
    return "$-1\r\n";
}
