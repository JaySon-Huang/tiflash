// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <fmt/compile.h>
#include <fmt/format.h>

#include <string>


/// Displays the passed size in bytes as 123.45 GiB.
std::string formatReadableSizeWithBinarySuffix(double value, int precision = 2);

/// Displays the passed size in bytes as 132.55 GB.
std::string formatReadableSizeWithDecimalSuffix(double value, int precision = 2);

/// Prints the number as 123.45 billion.
std::string formatReadableQuantity(double value, int precision = 2);

/// Wrapper around value. If used with fmt library (e.g. for log messages),
///  value is automatically formatted as size with binary suffix.
struct ReadableSize
{
    const uint64_t value;
    constexpr explicit ReadableSize(uint64_t value_)
        : value(value_)
    {}

    static constexpr uint64_t BINARY_DATA_MAGNITUDE = 1024;

    static constexpr ReadableSize KiB(uint64_t val) // NOLINT(readability-identifier-naming)
    {
        return ReadableSize(val * BINARY_DATA_MAGNITUDE);
    }

    static constexpr ReadableSize MiB(uint64_t val) // NOLINT(readability-identifier-naming)
    {
        return ReadableSize(val * BINARY_DATA_MAGNITUDE * BINARY_DATA_MAGNITUDE);
    }

    static constexpr ReadableSize GiB(uint64_t val) // NOLINT(readability-identifier-naming)
    {
        return ReadableSize(val * BINARY_DATA_MAGNITUDE * BINARY_DATA_MAGNITUDE * BINARY_DATA_MAGNITUDE);
    }


    // TODO: parse from string
};

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<ReadableSize>
{
    // Presentation format: 'd' - 132.55 GB, 'b' - 123.45 GiB
    char presentation = 'b';
    uint8_t precision = 2;

    constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        // Parse the presentation format and store it in the formatter:
        // '.2' -> set precision
        if (it != end && *it == '.')
        {
            it++;
            if (it != end && (*it >= '0' && *it <= '9'))
                precision = *it++ - '0';
            if (it != end && (*it >= '0' && *it <= '9'))
                throw format_error("invalid format, only support precision between 0~9");
        }
        // 'b'/'d' -> binary or decimal
        if (it != end && (*it == 'b' || *it == 'd'))
            presentation = *it++;
        if (it != end && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const ReadableSize & size, FormatContext & ctx)
    {
        if (presentation == 'b')
            return format_to(ctx.out(), "{}", formatReadableSizeWithBinarySuffix(size.value, precision));
        else if (presentation == 'd')
            return format_to(ctx.out(), "{}", formatReadableSizeWithDecimalSuffix(size.value, precision));
        throw format_error("invalid format");
    }
};
