// Modified from: https://github.com/ClickHouse/ClickHouse/blob/30fcaeb2a3fff1bf894aae9c776bed7fd83f783f/dbms/src/Common/BitHelpers.h
//
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

#include <common/likely.h>

#include <cstddef>


/** Returns log2 of number, rounded down.
  * Compiles to single 'bsr' instruction on x86.
  * For zero argument, result is unspecified.
  */
inline unsigned int bitScanReverse(unsigned int x)
{
    return sizeof(unsigned int) * 8 - 1 - __builtin_clz(x);
}


/** For zero argument, result is zero.
  * For arguments with most significand bit set, result is n.
  * For other arguments, returns value, rounded up to power of two.
  */
inline size_t roundUpToPowerOfTwoOrZero(size_t n)
{
    // if MSB is set, return n to avoid return zero
    if (unlikely(n >= 0x8000000000000000ULL))
        return n;
    else if (n <= 1)
        return n;

    --n;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    n |= n >> 8;
    n |= n >> 16;
    n |= n >> 32;
    ++n;

    return n;
}

inline bool isPowerOfTwo(size_t val)
{
    return (val & (val - 1)) == 0;
}
