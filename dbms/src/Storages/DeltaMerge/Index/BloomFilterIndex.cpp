// Copyright 2024 PingCAP, Inc.
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
#include <Storages/DeltaMerge/Index/BloomFilterIndex.h>

#include "AggregateFunctions/Helpers.h"
#include "Columns/ColumnNullable.h"
#include "DataTypes/DataTypeDate.h"
#include "DataTypes/DataTypeDateTime.h"
#include "DataTypes/DataTypeMyDate.h"
#include "DataTypes/DataTypeMyDateTime.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypeString.h"
#include "IO/ReadHelpers.h"
#include "IO/WriteHelpers.h"
#include "Storages/DeltaMerge/Index/RSResult.h"
#include "common/types.h"
namespace DB
{
namespace DM
{
void BloomFilterIndex::write(WriteBuffer & buf)
{
    auto size = bloom_filter_vec.size();
    DB::writeIntBinary(size, buf);
    for (auto & bloom_filter : bloom_filter_vec)
    {
        bloom_filter->write(buf);
    }
}

BloomFilterIndexPtr BloomFilterIndex::read(ReadBuffer & buf, size_t bytes_limit)
{
    UInt64 size = 0;
    if (bytes_limit != 0)
    {
        DB::readIntBinary(size, buf);
    }
    std::vector<BloomFilterPtr> bloom_filter_vec;
    for (size_t index = 0; index < size; index++)
    {
        auto bloom_filter = BloomFilter::read(buf);
        bloom_filter_vec.push_back(bloom_filter);
    }
    return std::make_shared<BloomFilterIndex>(bloom_filter_vec);
}

void BloomFilterIndex::updateBloomFilter(
    BloomFilterPtr & bloom_filter,
    const IColumn & column,
    size_t size,
    const IDataType * type)
{
    for (size_t i = 0; i < size; ++i)
    {
        Field value;
        column.get(i, value);

#define DISPATCH(TYPE)                             \
    if (typeid_cast<const DataType##TYPE *>(type)) \
    {                                              \
        bloom_filter->insert(value.get<TYPE>());   \
    }
        FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
        if (typeid_cast<const DataTypeDate *>(type))
        {
            bloom_filter->insert(value.get<UInt16>());
        }
        if (typeid_cast<const DataTypeDateTime *>(type))
        {
            bloom_filter->insert(value.get<UInt32>());
        }
        if (typeid_cast<const DataTypeMyDateTime *>(type))
        {
            bloom_filter->insert(value.get<UInt64>());
        }
        if (typeid_cast<const DataTypeMyDate *>(type))
        {
            bloom_filter->insert(value.get<UInt64>());
        }
        if (typeid_cast<const DataTypeString *>(type))
        {
            bloom_filter->insert(value.get<String>());
        }
    }
}


void BloomFilterIndex::addPack(const IColumn & column, const IDataType & type)
{
    auto size = column.size();

    bloom_parameters parameters;
    parameters.projected_element_count = size;
    parameters.false_positive_probability = false_positive_probability;
    parameters.compute_optimal_parameters();

    auto bloom_filter = std::make_shared<BloomFilter>(parameters);
    if (column.isColumnNullable())
    {
        auto nest_column = static_cast<const ColumnNullable &>(column).getNestedColumnPtr();
        for (size_t i = 0; i < size; i++) {}
        updateBloomFilter(
            bloom_filter,
            *nest_column,
            size,
            static_cast<const DataTypeNullable &>(type).getNestedType().get());
    }
    else
    {
        for (size_t i = 0; i < size; i++) {}
        updateBloomFilter(bloom_filter, column, size, &type);
    }
    bloom_filter_vec.push_back(bloom_filter);
}

RSResults BloomFilterIndex::checkEqual(
    size_t start_pack,
    size_t pack_count,
    const Field & value,
    const DataTypePtr & type) const
{
    const auto * raw_type = type.get();
    if (typeid_cast<const DataTypeNullable *>(raw_type))
    {
        // FIXME: make it work for nullable
        return RSResults(pack_count, RSResult::Some);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        const auto & v_raw = value.get<String>();
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (bloom_filter_vec[i]->contains(v_raw))
            {
                results[i - start_pack] = results[i - start_pack] || RSResult::Some;
            }
        }
        return results;
    }
#define DISPATCH(TYPE)                                                               \
    if (typeid_cast<const DataType##TYPE *>(raw_type))                               \
    {                                                                                \
        RSResults results(pack_count, RSResult::None);                               \
        auto v_raw = value.get<TYPE>();                                              \
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)                \
        {                                                                            \
            if (bloom_filter_vec[i]->contains(v_raw))                                \
            {                                                                        \
                results[i - start_pack] = results[i - start_pack] || RSResult::Some; \
            }                                                                        \
        }                                                                            \
        return results;                                                              \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        auto v_raw = value.get<UInt64>();
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (bloom_filter_vec[i]->contains(v_raw))
            {
                results[i - start_pack] = results[i - start_pack] || RSResult::Some;
            }
        }
        return results;
    }
    // Should not happen, because TiDB use DataTypeMyDateTime and DataTypeMyDate
    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (bloom_filter_vec[i]->contains(value.get<UInt16>()))
            {
                results[i - start_pack] = results[i - start_pack] || RSResult::Some;
            }
        }
        return results;
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        for (size_t i = start_pack; i < start_pack + pack_count; ++i)
        {
            if (bloom_filter_vec[i]->contains(value.get<UInt32>()))
            {
                results[i - start_pack] = results[i - start_pack] || RSResult::Some;
            }
        }
        return results;
    }
    return RSResults(pack_count, RSResult::Some);
}

RSResults BloomFilterIndex::checkIn(
    size_t start_pack,
    size_t pack_count,
    const std::vector<Field> & values,
    const DataTypePtr & type)
{
    const auto * raw_type = type.get();
    if (typeid_cast<const DataTypeNullable *>(raw_type))
    {
        // FIXME: make it work for nullable
        return RSResults(pack_count, RSResult::Some);
    }
    if (typeid_cast<const DataTypeString *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        for (const auto & v : values)
        {
            for (size_t i = start_pack; i < start_pack + pack_count; ++i)
            {
                if (bloom_filter_vec[i]->contains(v.get<String>()))
                {
                    results[i - start_pack] = results[i - start_pack] || RSResult::Some;
                }
            }
        }
        return results;
    }
#define DISPATCH(TYPE)                                                                   \
    if (typeid_cast<const DataType##TYPE *>(raw_type))                                   \
    {                                                                                    \
        RSResults results(pack_count, RSResult::None);                                   \
        for (const auto & v : values)                                                    \
        {                                                                                \
            for (size_t i = start_pack; i < start_pack + pack_count; ++i)                \
            {                                                                            \
                if (bloom_filter_vec[i]->contains(v.get<TYPE>()))                        \
                {                                                                        \
                    results[i - start_pack] = results[i - start_pack] || RSResult::Some; \
                }                                                                        \
            }                                                                            \
        }                                                                                \
        return results;                                                                  \
    }
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH
    if (typeid_cast<const DataTypeMyDateTime *>(raw_type) || typeid_cast<const DataTypeMyDate *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        for (const auto & v : values)
        {
            for (size_t i = start_pack; i < start_pack + pack_count; ++i)
            {
                if (bloom_filter_vec[i]->contains(v.get<UInt64>()))
                {
                    results[i - start_pack] = results[i - start_pack] || RSResult::Some;
                }
            }
        }
        return results;
    }
    // Should not happen, because TiDB use DataTypeMyDateTime and DataTypeMyDate
    if (typeid_cast<const DataTypeDate *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        for (const auto & v : values)
        {
            for (size_t i = start_pack; i < start_pack + pack_count; ++i)
            {
                if (bloom_filter_vec[i]->contains(v.get<UInt16>()))
                {
                    results[i - start_pack] = results[i - start_pack] || RSResult::Some;
                }
            }
        }
        return results;
    }
    if (typeid_cast<const DataTypeDateTime *>(raw_type))
    {
        RSResults results(pack_count, RSResult::None);
        for (const auto & v : values)
        {
            for (size_t i = start_pack; i < start_pack + pack_count; ++i)
            {
                if (bloom_filter_vec[i]->contains(v.get<UInt32>()))
                {
                    results[i - start_pack] = results[i - start_pack] || RSResult::Some;
                }
            }
        }
        return results;
    }
    return RSResults(pack_count, RSResult::Some);
}

} // namespace DM
} // namespace DB
