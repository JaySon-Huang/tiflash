#include <Common/MyTimeParser.h>
#include <Common/StringUtils/StringRefUtils.h>
#include <Common/StringUtils/StringUtils.h>
#include <IO/WriteHelpers.h>
#include <common/StringRef.h>
#include <common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int NOT_IMPLEMENTED;
} // namespace ErrorCodes

// Defined in "MyTime.cpp"
extern int32_t adjustYear(int32_t year);
extern const String abbrev_month_names[];
extern const String month_names[];


struct MyDateTimeParser::Context
{
    // Some state for `mysqlTimeFix`
    uint32_t state = 0;
    static constexpr uint32_t ST_DAY_OF_YEAR = 0x01;
    static constexpr uint32_t ST_MERIDIEM = 0x02;
    static constexpr uint32_t ST_HOUR_0_23 = 0x04;
    static constexpr uint32_t ST_HOUR_1_12 = 0x08;

    int32_t day_of_year = 0;
    // 0 - invalid, 1 - am, 2 - pm
    int32_t meridiem = 0;

    // The input string view
    const StringRef view;
    // The pos we are parsing from
    size_t pos = 0;

    Context(StringRef view_) : view(std::move(view_)) {}
};

// Try to parse digits with number of `limit` starting from view[pos]
// Return <n chars to step forward, number> if success.
// Return <0, _> if fail.
static std::tuple<size_t, int32_t> parseNDigits(const StringRef & view, const size_t pos, const size_t limit)
{
    size_t step = 0;
    int32_t num = 0;
    while (step < limit && (pos + step) < view.size && isNumericASCII(view.data[pos + step]))
    {
        num = num * 10 + (view.data[pos + step] - '0');
        step += 1;
    }
    return std::make_tuple(step, num);
}

static std::tuple<size_t, int32_t> parseYearNDigits(const StringRef & view, const size_t pos, const size_t limit)
{
    // Try to parse a "year" within `limit` digits
    size_t step = 0;
    int32_t year = 0;
    std::tie(step, year) = parseNDigits(view, pos, limit);
    if (step == 0)
        return std::make_tuple(step, 0);
    else if (step <= 2)
        year = adjustYear(year);
    return std::make_tuple(step, year);
}

enum class ParseState
{
    NORMAL = 0,      // Parsing
    FAIL = 1,        // Fail to parse
    END_OF_FILE = 2, // The end of input
};

//"%r": Time, 12-hour (hh:mm:ss followed by AM or PM)
static bool parseTime12Hour(MyDateTimeParser::Context & ctx, MyTimeBase & time)
{
    // Use temp_pos instead of changing `ctx.pos` directly in case of parsing failure
    size_t temp_pos = ctx.pos;
    auto checkIfEnd = [&temp_pos, &ctx]() -> ParseState {
        // To the end
        if (temp_pos == ctx.view.size)
            return ParseState::END_OF_FILE;
        return ParseState::NORMAL;
    };
    auto skipWhitespaces = [&temp_pos, &ctx, &checkIfEnd]() -> ParseState {
        while (temp_pos < ctx.view.size && isWhitespaceASCII(ctx.view.data[temp_pos]))
            ++temp_pos;
        return checkIfEnd();
    };
    auto parseSep = [&temp_pos, &ctx, &checkIfEnd, &skipWhitespaces]() -> ParseState {
        if (skipWhitespaces() == ParseState::END_OF_FILE)
            return ParseState::END_OF_FILE;
        // parse ":"
        if (ctx.view.data[temp_pos] != ':')
            return ParseState::FAIL;
        temp_pos += 1; // move forward
        return ParseState::NORMAL;
    };
    auto tryParse = [&]() -> ParseState {
        ParseState state = ParseState::NORMAL;
        /// Note that we should update `time` as soon as possible, or we
        /// can not get correct result for incomplete input like "12:13"
        /// that is less than "hh:mm:ssAM"

        // hh
        size_t step = 0;
        int32_t hour = 0;
        if (state = skipWhitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, hour) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || hour > 12 || hour == 0)
            return ParseState::FAIL;
        // Handle special case: 12:34:56 AM -> 00:34:56
        // For PM, we will add 12 it later
        if (hour == 12)
            hour = 0;
        time.hour = hour;
        temp_pos += step; // move forward

        if (state = parseSep(); state != ParseState::NORMAL)
            return state;

        int32_t minute = 0;
        if (state = skipWhitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, minute) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || minute > 59)
            return ParseState::FAIL;
        time.minute = minute;
        temp_pos += step; // move forward

        if (state = parseSep(); state != ParseState::NORMAL)
            return state;

        int32_t second = 0;
        if (state = skipWhitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, second) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || second > 59)
            return ParseState::FAIL;
        time.second = second;
        temp_pos += step; // move forward

        int meridiem = 0; // 0 - invalid, 1 - am, 2 - pm
        if (state = skipWhitespaces(); state != ParseState::NORMAL)
            return state;
        // "AM"/"PM" must be parsed as a single element
        // "11:13:56a" is an invalid input for "%r".
        if (auto size_to_end = ctx.view.size - temp_pos; size_to_end < 2)
            return ParseState::FAIL;
        if (toLowerIfAlphaASCII(ctx.view.data[temp_pos]) == 'a')
            meridiem = 1;
        else if (toLowerIfAlphaASCII(ctx.view.data[temp_pos]) == 'p')
            meridiem = 2;

        if (toLowerIfAlphaASCII(ctx.view.data[temp_pos + 1]) != 'm')
            meridiem = 0;
        switch (meridiem)
        {
            case 0:
                return ParseState::FAIL;
            case 1:
                break;
            case 2:
                time.hour += 12;
                break;
        }
        temp_pos += 2; // move forward
        return ParseState::NORMAL;
    };
    if (auto state = tryParse(); state == ParseState::FAIL)
        return false;
    // Other state, forward the `ctx.pos` and return true
    ctx.pos = temp_pos;
    return true;
}

//"%T": Time, 24-hour (hh:mm:ss)
static bool parseTime24Hour(MyDateTimeParser::Context & ctx, MyTimeBase & time)
{
    // Use temp_pos instead of changing `ctx.pos` directly in case of parsing failure
    size_t temp_pos = ctx.pos;
    auto checkIfEnd = [&temp_pos, &ctx]() -> ParseState {
        // To the end
        if (temp_pos == ctx.view.size)
            return ParseState::END_OF_FILE;
        return ParseState::NORMAL;
    };
    auto skipWhitespaces = [&temp_pos, &ctx, &checkIfEnd]() -> ParseState {
        while (temp_pos < ctx.view.size && isWhitespaceASCII(ctx.view.data[temp_pos]))
            ++temp_pos;
        return checkIfEnd();
    };
    auto parseSep = [&temp_pos, &ctx, &checkIfEnd, &skipWhitespaces]() -> ParseState {
        if (skipWhitespaces() == ParseState::END_OF_FILE)
            return ParseState::END_OF_FILE;
        // parse ":"
        if (ctx.view.data[temp_pos] != ':')
            return ParseState::FAIL;
        temp_pos += 1; // move forward
        return ParseState::NORMAL;
    };
    auto tryParse = [&]() -> ParseState {
        ParseState state = ParseState::NORMAL;
        /// Note that we should update `time` as soon as possible, or we
        /// can not get correct result for incomplete input like "12:13"
        /// that is less than "hh:mm:ss"

        // hh
        size_t step = 0;
        int32_t hour = 0;
        if (state = skipWhitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, hour) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || hour > 23)
            return ParseState::FAIL;
        time.hour = hour;
        temp_pos += step; // move forward

        if (state = parseSep(); state != ParseState::NORMAL)
            return state;

        int32_t minute = 0;
        if (state = skipWhitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, minute) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || minute > 59)
            return ParseState::FAIL;
        time.minute = minute;
        temp_pos += step; // move forward

        if (state = parseSep(); state != ParseState::NORMAL)
            return state;

        int32_t second = 0;
        if (state = skipWhitespaces(); state != ParseState::NORMAL)
            return state;
        std::tie(step, second) = parseNDigits(ctx.view, temp_pos, 2);
        if (step == 0 || second > 59)
            return ParseState::FAIL;
        time.second = second;
        temp_pos += step; // move forward

        return ParseState::NORMAL;
    };
    if (auto state = tryParse(); state == ParseState::FAIL)
        return false;
    // Other state, forward the `ctx.pos` and return true
    ctx.pos = temp_pos;
    return true;
}

// Refer: https://github.com/pingcap/tidb/blob/v5.0.1/types/time.go#L2946
MyDateTimeParser::MyDateTimeParser(String format_) : format(std::move(format_))
{
    // Ignore all prefix white spaces (TODO: handle unicode space?)
    size_t format_pos = 0;
    while (format_pos < format.size() && isWhitespaceASCII(format[format_pos]))
        format_pos++;

    bool in_pattern_match = false;
    while (format_pos < format.size())
    {
        char x = format[format_pos];
        if (in_pattern_match)
        {
            switch (x)
            {
                case 'b':
                {
                    //"%b": Abbreviated month name (Jan..Dec)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        size_t step = 0;
                        auto v = removePrefix(ctx.view, ctx.pos);
                        for (size_t p = 0; p < 12; p++)
                        {
                            if (startsWithCI(v, abbrev_month_names[p]))
                            {
                                time.month = p + 1;
                                step = abbrev_month_names[p].size();
                                break;
                            }
                        }
                        if (step == 0)
                            return false;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'c':
                {
                    //"%c": Month, numeric (0..12)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        // To be compatible with TiDB & MySQL, first try to take two digit and parse it as `num`
                        auto [step, month] = parseNDigits(ctx.view, ctx.pos, 2);
                        // Then check whether num is valid month
                        // Note that 0 is valid when sql_mode does not contain NO_ZERO_IN_DATE,NO_ZERO_DATE
                        if (step == 0 || month > 12)
                            return false;
                        time.month = month;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'd': //"%d": Day of the month, numeric (00..31)
                    [[fallthrough]];
                case 'e': //"%e": Day of the month, numeric (0..31)
                {
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, day] = parseNDigits(ctx.view, ctx.pos, 2);
                        if (step == 0 || day > 31)
                            return false;
                        time.day = day;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'f':
                {
                    //"%f": Microseconds (000000..999999)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, ms] = parseNDigits(ctx.view, ctx.pos, 6);
                        // Empty string is a valid input
                        if (step == 0)
                        {
                            time.micro_second = 0;
                            return true;
                        }
                        // The siffix '0' can be ignored.
                        // "9" means 900000
                        while (ms > 0 && ms * 10 < 1000000)
                        {
                            ms *= 10;
                        }
                        time.micro_second = ms;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'k':
                    //"%k": Hour (0..23)
                    [[fallthrough]];
                case 'H':
                {
                    //"%H": Hour (00..23)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, hour] = parseNDigits(ctx.view, ctx.pos, 2);
                        if (step == 0 || hour > 23)
                            return false;
                        ctx.state |= MyDateTimeParser::Context::ST_HOUR_0_23;
                        time.hour = hour;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'l':
                    //"%l": Hour (1..12)
                    [[fallthrough]];
                case 'I':
                    //"%I": Hour (01..12)
                    [[fallthrough]];
                case 'h':
                {
                    //"%h": Hour (01..12)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, hour] = parseNDigits(ctx.view, ctx.pos, 2);
                        if (step == 0 || hour <= 0 || hour > 12)
                            return false;
                        ctx.state |= MyDateTimeParser::Context::ST_HOUR_1_12;
                        time.hour = hour;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'i':
                {
                    //"%i": Minutes, numeric (00..59)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, num] = parseNDigits(ctx.view, ctx.pos, 2);
                        if (step == 0 || num > 59)
                            return false;
                        time.minute = num;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'j':
                {
                    //"%j": Day of year (001..366)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                        auto [step, num] = parseNDigits(ctx.view, ctx.pos, 3);
                        if (step == 0 || num == 0 || num > 366)
                            return false;
                        ctx.state |= MyDateTimeParser::Context::ST_DAY_OF_YEAR;
                        ctx.day_of_year = num;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'M':
                {
                    //"%M": Month name (January..December)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto v = removePrefix(ctx.view, ctx.pos);
                        size_t step = 0;
                        for (size_t p = 0; p < 12; p++)
                        {
                            if (startsWithCI(v, month_names[p]))
                            {
                                time.month = p + 1;
                                step = month_names[p].size();
                                break;
                            }
                        }
                        if (step == 0)
                            return false;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'm':
                {
                    //"%m": Month, numeric (00..12)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, month] = parseNDigits(ctx.view, ctx.pos, 2);
                        if (step == 0 || month > 12)
                            return false;
                        time.month = month;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'S':
                    //"%S": Seconds (00..59)
                    [[fallthrough]];
                case 's':
                {
                    //"%s": Seconds (00..59)
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, second] = parseNDigits(ctx.view, ctx.pos, 2);
                        if (step == 0 || second > 59)
                            return false;
                        time.second = second;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'p':
                {
                    //"%p": AM or PM
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                        // Check the offset that will visit
                        if (ctx.view.size - ctx.pos < 2)
                            return false;

                        int meridiem = 0; // 0 - invalid, 1 - am, 2 - pm
                        if (toLowerIfAlphaASCII(ctx.view.data[ctx.pos]) == 'a')
                            meridiem = 1;
                        else if (toLowerIfAlphaASCII(ctx.view.data[ctx.pos]) == 'p')
                            meridiem = 2;

                        if (toLowerIfAlphaASCII(ctx.view.data[ctx.pos + 1]) != 'm')
                            meridiem = 0;

                        if (meridiem == 0)
                            return false;

                        ctx.state |= MyDateTimeParser::Context::ST_MERIDIEM;
                        ctx.meridiem = meridiem;
                        ctx.pos += 2;
                        return true;
                    });
                    break;
                }
                case 'r':
                {
                    //"%r": Time, 12-hour (hh:mm:ss followed by AM or PM)
                    parsers.emplace_back(parseTime12Hour);
                    break;
                }
                case 'T':
                {
                    //"%T": Time, 24-hour (hh:mm:ss)
                    parsers.emplace_back(parseTime24Hour);
                    break;
                }
                case 'Y':
                {
                    //"%Y": Year, numeric, four digits
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, year] = parseYearNDigits(ctx.view, ctx.pos, 4);
                        if (step == 0)
                            return false;
                        time.year = year;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case 'y':
                {
                    //"%y": Year, numeric, two digits. Deprecated since MySQL 5.7.5
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase & time) -> bool {
                        auto [step, year] = parseYearNDigits(ctx.view, ctx.pos, 2);
                        if (step == 0)
                            return false;
                        time.year = year;
                        ctx.pos += step;
                        return true;
                    });
                    break;
                }
                case '#':
                {
                    //"%#": Skip all numbers
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                        // TODO: Does ASCII numeric the same with unicode numeric?
                        size_t temp_pos = ctx.pos;
                        while (temp_pos < ctx.view.size && isNumericASCII(ctx.view.data[temp_pos]))
                            temp_pos++;
                        ctx.pos = temp_pos;
                        return true;
                    });
                    break;
                }
                case '.':
                {
                    //"%.": Skip all punctation characters
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                        // TODO: Does ASCII punctuation the same with unicode punctuation?
                        size_t temp_pos = ctx.pos;
                        while (temp_pos < ctx.view.size && isPunctuation(ctx.view.data[temp_pos]))
                            temp_pos++;
                        ctx.pos = temp_pos;
                        return true;
                    });
                    break;
                }
                case '@':
                {
                    //"%@": Skip all alpha characters
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
                        // TODO: Does ASCII alpha the same with unicode alpha?
                        size_t temp_pos = ctx.pos;
                        while (temp_pos < ctx.view.size && isAlphaASCII(ctx.view.data[temp_pos]))
                            temp_pos++;
                        ctx.pos = temp_pos;
                        return true;
                    });
                    break;
                }
                case '%':
                {
                    //"%%": A literal % character
                    parsers.emplace_back([](MyDateTimeParser::Context & ctx, MyTimeBase &) -> bool {
#if 0
                        if (ctx.view.data[ctx.pos] != '%')
                            return false;
                        ctx.pos++;
                        return true;
#else
                        // FIXME: Ignored by now, both tidb 5.0.0 and mariadb 10.3.14 can not handle it
                        std::ignore = ctx;
                        return false;
#endif
                    });
                    break;
                }
                default:
                    throw Exception(
                        "Unknown date format pattern, [format=" + format + "] [pattern=" + x + "] [pos=" + DB::toString(format_pos) + "]",
                        ErrorCodes::BAD_ARGUMENTS);
            }
            // end the state of pattern match
            in_pattern_match = false;
            // move format_pos forward
            format_pos++;
            continue;
        }

        if (x == '%')
        {
            in_pattern_match = true;
            // move format_pos forward
            format_pos++;
        }
        else
        {
            // Ignore whitespace for literal forwarding (TODO: handle unicode space?)
            while (format_pos < format.size() && isWhitespaceASCII(format[format_pos]))
                format_pos++;
            // Move forward ctx.view with a sequence of literal `format[format_pos:span_end]`
            size_t span_end = format_pos;
            while (span_end < format.size() && format[span_end] != '%' && !isWhitespaceASCII(format[span_end]))
                ++span_end;
            const size_t span_size = span_end - format_pos;
            if (span_size > 0)
            {
                StringRef format_view{format.data() + format_pos, span_size};
                parsers.emplace_back([format_view](MyDateTimeParser::Context & ctx, MyTimeBase &) {
                    assert(format_view.size > 0);
                    if (format_view.size == 1)
                    {
                        // Shortcut for only 1 char
                        if (ctx.view.data[ctx.pos] != format_view.data[0])
                            return false;
                        ctx.pos += 1;
                        return true;
                    }
                    // Try best to match input as most literal as possible
                    auto v = removePrefix(ctx.view, ctx.pos);
                    size_t v_step = 0;
                    for (size_t format_step = 0; format_step < format_view.size; ++format_step)
                    {
                        // Ignore prefix whitespace for input
                        while (v_step < v.size && isWhitespaceASCII(v.data[v_step]))
                            ++v_step;
                        if (v_step == v.size) // To the end
                            break;
                        // Try to match literal
                        if (v.data[v_step] != format_view.data[format_step])
                            return false;
                        ++v_step;
                    }
                    ctx.pos += v_step;
                    return true;
                });
            }
            // move format_pos forward
            format_pos = span_end;
        }
    }
}

bool mysqlTimeFix(const MyDateTimeParser::Context & ctx, MyTimeBase & my_time)
{
    // TODO: Implement the function that converts day of year to yy:mm:dd
    if (ctx.state & MyDateTimeParser::Context::ST_DAY_OF_YEAR)
    {
        // %j Day of year (001..366) set
        throw Exception("%j set, parsing day of year is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    if (ctx.state & MyDateTimeParser::Context::ST_MERIDIEM)
    {
        // %H (00..23) set, should not set AM/PM
        if (ctx.state & MyDateTimeParser::Context::ST_HOUR_0_23)
            return false;
        if (my_time.hour == 0)
            return false;
        if (my_time.hour == 12)
        {
            // 12 is a special hour.
            if (ctx.meridiem == 1) // AM
                my_time.hour = 0;
            else if (ctx.meridiem == 2) // PM
                my_time.hour = 12;
            return true;
        }
        if (ctx.meridiem == 2) // PM
            my_time.hour += 12;
    }
    else
    {
        // %h (01..12) set
        if ((ctx.state & MyDateTimeParser::Context::ST_HOUR_1_12) && my_time.hour == 12)
            my_time.hour = 0; // why?
    }
    return true;
}

std::optional<UInt64> MyDateTimeParser::parseAsPackedUInt(const StringRef & str_view) const
{
    MyTimeBase my_time{0, 0, 0, 0, 0, 0, 0};
    MyDateTimeParser::Context ctx(str_view);

    // TODO: can we return warnings to TiDB?
    for (auto & f : parsers)
    {
        // Ignore all prefix white spaces before each pattern match (TODO: handle unicode space?)
        while (ctx.pos < str_view.size && isWhitespaceASCII(str_view.data[ctx.pos]))
            ctx.pos++;
        // To the end of input, exit (successfully) even if there is more patterns to match
        if (ctx.pos == ctx.view.size)
            break;

        if (f(ctx, my_time) != true)
        {
#ifndef NDEBUG
            LOG_TRACE(&Logger::get("MyDateTimeParser"),
                "parse error, [str=" << ctx.view.toString() << "] [format=" << format << "] [parse_pos=" << ctx.pos << "]");
#endif
            return std::nullopt;
        }

        // `ctx.pos` > `ctx.view.size` after callback, must be something wrong
        if (unlikely(ctx.pos > ctx.view.size))
        {
            throw Exception(String(__PRETTY_FUNCTION__) + ": parse error, pos overflow. [str=" + ctx.view.toString() + "] [format=" + format
                + "] [parse_pos=" + DB::toString(ctx.pos) + "] [size=" + DB::toString(ctx.view.size) + "]");
        }
    }
    // Extra characters at the end of date are ignored, but a warning should be reported at this case
    // if (ctx.pos < ctx.view.size) {}

    // Handle the var in `ctx`
    if (!mysqlTimeFix(ctx, my_time))
        return std::nullopt;

    return my_time.toPackedUInt();
}

} // namespace DB
