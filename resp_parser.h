#pragma once

#include <cstddef>
#include <string>
#include <vector>

enum class ParseStatus {
    incomplete,
    complete,
    error,
};

struct ParseResult {
    ParseStatus status;
    std::vector<std::string> args;
    std::size_t consumed = 0;
    std::string error;
};

ParseResult try_parse_command(const std::string& buf);

std::string encode_simple(const std::string& s);
std::string encode_error(const std::string& s);
std::string encode_bulk(const std::string& s);
std::string encode_integer(long long value);
std::string encode_null();
