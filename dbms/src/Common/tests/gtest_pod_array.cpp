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

#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <gtest/gtest.h>

#include <limits>

using namespace DB;

namespace
{
struct AllocationAttempt
{
};

struct ThrowingAllocator : Allocator<false>
{
    void * alloc(size_t /*bytes*/) { throw AllocationAttempt{}; }
};

template <typename Array>
void checkAllocationLimits()
{
    // Probe the actual allocation path without requesting the enormous boundary allocations.
    EXPECT_THROW((Array(Array::maxAllocSize())), AllocationAttempt);
    EXPECT_THROW((Array(Array::maxAllocSize() + 1)), Exception);

    Array array;
    EXPECT_THROW(array.reserve(Array::maxReserveSize()), AllocationAttempt);
    EXPECT_THROW(array.reserve(Array::maxReserveSize() + 1), Exception);
}
} // namespace

TEST(Common, PODArrayAllocationLimitsMatchPrechecks)
{
    checkAllocationLimits<PaddedPODArray<UInt8, 4096, ThrowingAllocator>>();
    checkAllocationLimits<PaddedPODArray<UInt32, 4096, ThrowingAllocator>>();
    checkAllocationLimits<PaddedPODArray<UInt64, 4096, ThrowingAllocator>>();
    checkAllocationLimits<PODArray<UInt32, 4096, ThrowingAllocator>>();
}

TEST(Common, PODArrayInsert)
{
    std::string str = "test_string_abacaba";
    PODArray<char> chars;
    chars.insert(chars.end(), str.begin(), str.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));

    std::string insert_in_the_middle = "insert_in_the_middle";
    auto pos = str.size() / 2;
    str.insert(str.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
    chars.insert(chars.begin() + pos, insert_in_the_middle.begin(), insert_in_the_middle.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));

    std::string insert_with_resize;
    insert_with_resize.reserve(chars.capacity() * 2);
    char cur_char = 'a';
    while (insert_with_resize.size() < insert_with_resize.capacity())
    {
        insert_with_resize += cur_char;
        if (cur_char == 'z')
            cur_char = 'a';
        else
            ++cur_char;
    }
    str.insert(str.begin(), insert_with_resize.begin(), insert_with_resize.end());
    chars.insert(chars.begin(), insert_with_resize.begin(), insert_with_resize.end());
    EXPECT_EQ(str, std::string(chars.data(), chars.size()));
}

TEST(Common, PODPushBackRawMany)
{
    PODArray<char> chars;
    chars.push_back_raw_many(5, "first");
    EXPECT_EQ(std::string("first"), std::string(chars.data(), chars.size()));
    EXPECT_EQ(5UL, chars.size());
    EXPECT_LE(chars.capacity() - chars.size(), 10UL);
    chars.push_back_raw_many(10, "0123456789");
    EXPECT_EQ(15UL, chars.size());
    EXPECT_EQ(std::string("first0123456789"), std::string(chars.data(), chars.size()));
}

TEST(Common, PODNoOverallocation)
{
    /// Check that PaddedPODArray allocates for smaller number of elements than the power of two due to padding.
    /// NOTE: It's Ok to change these numbers if you will modify initial size or padding.

    PaddedPODArray<char> chars;
    std::vector<size_t> capacities;

    size_t prev_capacity = 0;
    for (size_t i = 0; i < 1000000; ++i)
    {
        chars.emplace_back();
        if (chars.capacity() != prev_capacity)
        {
            prev_capacity = chars.capacity();
            capacities.emplace_back(prev_capacity);
        }
    }

    EXPECT_EQ(capacities, (std::vector<size_t>{4017, 8113, 16305, 32689, 65457, 130993, 262065, 524209, 1048497}));
}

TEST(Common, RoundUpToPowerOfTwoOrZeroHugeValues)
{
    /// The rounding is implemented by smearing the bits and adding one, which wraps around to
    /// zero once the most significant bit is set. Zero is not a "nothing to allocate" answer:
    /// PODArray::reserve() forwards it to the allocator as a byte count.
    constexpr size_t sign_bit = size_t{1} << (sizeof(size_t) * 8 - 1);
    constexpr size_t max_size = std::numeric_limits<size_t>::max();

    /// Small values keep the previous behaviour.
    EXPECT_EQ(0UL, roundUpToPowerOfTwoOrZero(0));
    EXPECT_EQ(1UL, roundUpToPowerOfTwoOrZero(1));
    EXPECT_EQ(2UL, roundUpToPowerOfTwoOrZero(2));
    EXPECT_EQ(4UL, roundUpToPowerOfTwoOrZero(3));
    EXPECT_EQ(1024UL, roundUpToPowerOfTwoOrZero(1023));
    EXPECT_EQ(sign_bit / 2, roundUpToPowerOfTwoOrZero(sign_bit / 2));
    EXPECT_EQ(sign_bit, roundUpToPowerOfTwoOrZero(sign_bit / 2 + 1));

    /// Regression: values at or above 2^63 used to round down to 0.
    EXPECT_EQ(sign_bit, roundUpToPowerOfTwoOrZero(sign_bit));
    EXPECT_EQ(sign_bit + 1, roundUpToPowerOfTwoOrZero(sign_bit + 1));
    EXPECT_EQ(max_size, roundUpToPowerOfTwoOrZero(max_size));
}

TEST(Common, PODArrayReserveHugeSizeKeepsBufferOwned)
{
    /// A reserve() whose padded byte count reaches 2^63 must fail the allocation while the
    /// array still owns its buffer. Before the fix the count was rounded down to 0, realloc(0)
    /// freed the buffer and returned nullptr, the exception left c_start dangling and the
    /// destructor freed the very same block again (double free).
    PaddedPODArray<char> chars;
    chars.push_back('a');
    chars.push_back('b');

    const size_t huge_elements = size_t{1} << (sizeof(size_t) * 8 - 1);
    EXPECT_THROW(chars.reserve(huge_elements), Exception);

    /// The array must be untouched and still usable after the failed allocation.
    ASSERT_EQ(2UL, chars.size());
    EXPECT_EQ('a', chars[0]);
    EXPECT_EQ('b', chars[1]);
    chars.push_back('c');
    EXPECT_EQ(3UL, chars.size());
    EXPECT_EQ(std::string("abc"), std::string(chars.data(), chars.size()));
}
