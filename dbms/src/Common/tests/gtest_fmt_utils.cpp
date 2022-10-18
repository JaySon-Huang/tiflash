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

#include <Common/FmtUtils.h>
#include <Common/formatReadable.h>
#include <gtest/gtest.h>

namespace DB::tests
{

TEST(FmtTest, ReadableSize)
{
    EXPECT_EQ(fmt::format("{}", ReadableSize(123)), "123.00 B");
    EXPECT_EQ(fmt::format("{:.2b}", ReadableSize(123)), "123.00 B");
    EXPECT_EQ(fmt::format("{:.2d}", ReadableSize(123)), "123.00 B");

    EXPECT_EQ(fmt::format("{}", ReadableSize(1'234)), "1.21 KiB");
    EXPECT_EQ(fmt::format("{:.2b}", ReadableSize(1'234)), "1.21 KiB");
    EXPECT_EQ(fmt::format("{:.2d}", ReadableSize(1'234)), "1.23 KB");

    EXPECT_EQ(fmt::format("{}", ReadableSize(123'456)), "120.56 KiB");
    EXPECT_EQ(fmt::format("{:.2b}", ReadableSize(123'456)), "120.56 KiB");
    EXPECT_EQ(fmt::format("{:.2d}", ReadableSize(123'456)), "123.46 KB");

    EXPECT_EQ(fmt::format("{}", ReadableSize(1'234'567)), "1.18 MiB");
    EXPECT_EQ(fmt::format("{:.2b}", ReadableSize(1'234'567)), "1.18 MiB");
    EXPECT_EQ(fmt::format("{:.2d}", ReadableSize(1'234'567)), "1.23 MB");

    EXPECT_EQ(fmt::format("{}", ReadableSize(1'234'567'890'123'456ULL)), "1.10 PiB");
    EXPECT_EQ(fmt::format("{:.2b}", ReadableSize(1'234'567'890'123'456ULL)), "1.10 PiB");
    EXPECT_EQ(fmt::format("{:.2d}", ReadableSize(1'234'567'890'123'456ULL)), "1.23 PB");
    EXPECT_EQ(fmt::format("{:.1b}", ReadableSize(1'234'567'890'123'456ULL)), "1.1 PiB");
    EXPECT_EQ(fmt::format("{:.1d}", ReadableSize(1'234'567'890'123'456ULL)), "1.2 PB");
    EXPECT_EQ(fmt::format("{:.9b}", ReadableSize(1'234'567'890'123'456ULL)), "1.096516558 PiB");
    EXPECT_EQ(fmt::format("{:.9d}", ReadableSize(1'234'567'890'123'456ULL)), "1.234567890 PB");
}

TEST(FmtUtilsTest, TestFmtBuffer)
{
    FmtBuffer buffer;
    buffer.append("{").append("test").append("}");
    ASSERT_EQ(buffer.toString(), "{test}");

    buffer.fmtAppend(" fmt append {}", "test");
    ASSERT_EQ(buffer.toString(), "{test} fmt append test");
}

TEST(FmtUtilsTest, TestJoinStr)
{
    FmtBuffer buffer;
    std::vector<std::string> v{"a", "b", "c"};
    buffer.joinStr(v.cbegin(), v.cend());
    ASSERT_EQ(buffer.toString(), "a, b, c");

    buffer.clear();
    v.clear();
    buffer.joinStr(v.cbegin(), v.cend());
    ASSERT_EQ(buffer.toString(), "");

    buffer.clear();
    v.push_back("a");
    buffer.joinStr(v.cbegin(), v.cend())
        .joinStr(
            v.cbegin(),
            v.cend(),
            [](const auto & s, FmtBuffer & fb) { fb.append(s); fb.append("t"); },
            ", ");
    ASSERT_EQ(buffer.toString(), "aat");

    buffer.clear();
    v.clear();
    v.push_back("a");
    v.push_back(("b"));
    buffer.joinStr(v.cbegin(), v.cend(), "+");
    ASSERT_EQ(buffer.toString(), "a+b");

    buffer.clear();
    v.clear();
    v.push_back("a");
    v.push_back(("b"));
    buffer.joinStr(v.cbegin(), v.cend(), "+").joinStr(v.cbegin(), v.cend(), "-");
    ASSERT_EQ(buffer.toString(), "a+ba-b");
}
} // namespace DB::tests
