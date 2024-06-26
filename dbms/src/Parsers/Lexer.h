// Copyright 2023 PingCAP, Inc.
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

#include <stddef.h>


namespace DB
{

#define APPLY_FOR_TOKENS(M)                                                                                              \
    M(Whitespace)                                                                                                        \
    M(Comment)                                                                                                           \
                                                                                                                         \
    M(BareWord) /** Either keyword (SELECT) or identifier (column) */                                                    \
                                                                                                                         \
    M(Number) /** Always non-negative. No leading plus. 123 or something like 123.456e12, 0x123p12 */                    \
    M(StringLiteral) /** 'hello word', 'hello''word', 'hello\'word\\' */                                                 \
                                                                                                                         \
    M(QuotedIdentifier) /** "x", `x` */                                                                                  \
                                                                                                                         \
    M(OpeningRoundBracket)                                                                                               \
    M(ClosingRoundBracket)                                                                                               \
                                                                                                                         \
    M(OpeningSquareBracket)                                                                                              \
    M(ClosingSquareBracket)                                                                                              \
                                                                                                                         \
    M(Comma)                                                                                                             \
    M(Semicolon)                                                                                                         \
    M(Dot) /** Compound identifiers, like a.b or tuple access operator a.1, (x, y).2. */                                 \
    /** Need to be distinguished from floating point number with omitted integer part: .1 */                             \
                                                                                                                         \
    M(Asterisk) /** Could be used as multiplication operator or on it's own: "SELECT *" */                               \
                                                                                                                         \
    M(Plus)                                                                                                              \
    M(Minus)                                                                                                             \
    M(Slash)                                                                                                             \
    M(Percent)                                                                                                           \
    M(Arrow) /** ->. Should be distinguished from minus operator. */                                                     \
    M(QuestionMark)                                                                                                      \
    M(Colon)                                                                                                             \
    M(Equals)                                                                                                            \
    M(NotEquals)                                                                                                         \
    M(Less)                                                                                                              \
    M(Greater)                                                                                                           \
    M(LessOrEquals)                                                                                                      \
    M(GreaterOrEquals)                                                                                                   \
    M(Concatenation) /** String concatenation operator: || */                                                            \
                                                                                                                         \
    /** Order is important. EndOfStream goes after all usual tokens, and special error tokens goes after EndOfStream. */ \
                                                                                                                         \
    M(EndOfStream)                                                                                                       \
                                                                                                                         \
    /** Something unrecognized. */                                                                                       \
    M(Error)                                                                                                             \
    /** Something is wrong and we have more information. */                                                              \
    M(ErrorMultilineCommentIsNotClosed)                                                                                  \
    M(ErrorSingleQuoteIsNotClosed)                                                                                       \
    M(ErrorDoubleQuoteIsNotClosed)                                                                                       \
    M(ErrorBackQuoteIsNotClosed)                                                                                         \
    M(ErrorSingleExclamationMark)                                                                                        \
    M(ErrorSinglePipeMark)                                                                                               \
    M(ErrorWrongNumber)                                                                                                  \
    M(ErrorMaxQuerySizeExceeded)


enum class TokenType
{
#define M(TOKEN) TOKEN,
    APPLY_FOR_TOKENS(M)
#undef M
};

const char * getTokenName(TokenType type);
const char * getErrorTokenDescription(TokenType type);


struct Token
{
    TokenType type;
    const char * begin;
    const char * end;

    size_t size() const { return end - begin; }

    Token() = default;
    Token(TokenType type, const char * begin, const char * end)
        : type(type)
        , begin(begin)
        , end(end)
    {}

    bool isSignificant() const { return type != TokenType::Whitespace && type != TokenType::Comment; }
    bool isError() const { return type > TokenType::EndOfStream; }
    bool isEnd() const { return type == TokenType::EndOfStream; }
};


class Lexer
{
public:
    Lexer(const char * begin, const char * end, size_t max_query_size = 0)
        : begin(begin)
        , pos(begin)
        , end(end)
        , max_query_size(max_query_size)
    {}
    Token nextToken();

private:
    const char * const begin;
    const char * pos;
    const char * const end;

    const size_t max_query_size;

    Token nextTokenImpl();

    /// This is needed to disambiguate tuple access operator from floating point number (.1).
    TokenType prev_significant_token_type = TokenType::Whitespace; /// No previous token.
};

} // namespace DB
