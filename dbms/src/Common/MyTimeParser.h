#pragma once

#include <Common/MyTime.h>

#include <optional>
#include <vector>

struct StringRef;
namespace DB
{
struct MyDateTimeParser
{
    explicit MyDateTimeParser(String format_);

    std::optional<UInt64> parseAsPackedUInt(const StringRef & str_view) const;

    struct Context;

private:
    const String format;

    // Parsing method. Parse from ctx.view[ctx.pos].
    // If success, update `datetime`, `ctx` and return true.
    // If fail, return false.
    using ParserCallback = std::function<bool(MyDateTimeParser::Context & ctx, MyTimeBase & datetime)>;
    std::vector<ParserCallback> parsers;
};

} // namespace DB
