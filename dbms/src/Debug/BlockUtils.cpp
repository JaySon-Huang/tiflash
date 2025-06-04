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

#include <Common/Exception.h>
#include <Common/FmtUtils.h>
#include <Debug/BlockUtils.h>

#include <vector>

namespace DB::tests
{

String getColumnsContent(const ColumnsWithTypeAndName & cols)
{
    if (cols.empty())
        return "";
    return getColumnsContent(cols, 0, cols[0].column->size());
}

String getColumnsContent(const ColumnsWithTypeAndName & cols, size_t begin, size_t end)
{
    const size_t col_num = cols.size();
    if (col_num <= 0)
        return "";

    const size_t col_size = cols[0].column->size();
    if (col_size <= 0)
        return "";
    assert(begin <= end);
    assert(col_size >= end);
    assert(col_size > begin);

    /// Ensure the sizes of columns in cols have the same number of rows
    for (size_t i = 1; i < col_num; ++i)
    {
        RUNTIME_CHECK_MSG(
            cols[i].column->size() == col_size,
            "col_size={} actual_col_size={} col_name={} col_id={}",
            col_size,
            cols[i].column->size(),
            cols[i].name,
            cols[i].column_id);
    }

    std::vector<std::pair<size_t, String>> col_content;
    FmtBuffer fmt_buf;
    for (size_t i = 0; i < col_num; ++i)
    {
        /// Push the column name
        fmt_buf.append(fmt::format("{}: (", cols[i].name));
        for (size_t j = begin; j < end; ++j)
            col_content.push_back(std::make_pair(j, (*cols[i].column)[j].toString()));

        /// Add content
        fmt_buf.joinStr(
            col_content.begin(),
            col_content.end(),
            [](const auto & content, FmtBuffer & fmt_buf) {
                fmt_buf.append(fmt::format("{}: {}", content.first, content.second));
            },
            ", ");

        fmt_buf.append(")\n");
        col_content.clear();
    }

    return fmt_buf.toString();
}

} // namespace DB::tests
