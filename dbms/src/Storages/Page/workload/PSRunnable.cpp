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

#include <Common/MemoryTracker.h>
#include <Common/formatReadable.h>
#include <Encryption/MockKeyManager.h>
#include <IO/ReadBufferFromMemory.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Storages/Page/Snapshot.h>
#include <Storages/Page/UniversalWriteBatch.h>
#include <Storages/Page/universal/UniversalPageStorage.h>
#include <Storages/Page/workload/PSRunnable.h>
#include <Storages/Page/workload/PSStressEnv.h>
#include <TestUtils/MockDiskDelegator.h>
#include <fmt/format.h>
#include <google/protobuf/stubs/common.h>
#include <unistd.h>

#include <random>

#include "Storages/Page/PageDefines.h"
#include "Storages/Page/UniversalPage.h"

namespace DB::PS::tests
{

using UniReader = KVStoreReader;

void PSRunnable::run()
try
{
    auto tracker = MemoryTracker::create();
    tracker->setDescription(nullptr);
    current_memory_tracker = tracker.get();
    // If runImpl() return false, means it need break itself
    while (StressEnvStatus::getInstance().isRunning() && runImpl())
    {
        /*Just for no warning*/
    }
    auto peak = current_memory_tracker->getPeak();
    current_memory_tracker = nullptr;
    LOG_FMT_INFO(StressEnv::logger, "{} exit with peak memory usage: {}", description(), formatReadableSizeWithBinarySuffix(peak));
}
catch (...)
{
    DB::tryLogCurrentException(StressEnv::logger);
}

size_t PSRunnable::getBytesUsed() const
{
    return bytes_used;
}

size_t PSRunnable::getPagesUsed() const
{
    return pages_used;
}

/// PSWriter ///

size_t PSWriter::approx_page_mb = 2;
void PSWriter::setApproxPageSize(size_t size_mb)
{
    LOG_FMT_INFO(StressEnv::logger, "Page approx size is set to {} MB", size_mb);
    approx_page_mb = size_mb;
}

DB::ReadBufferPtr PSWriter::genRandomData(const DB::PageId pageId, DB::MemHolder & holder)
{
    // fill page with random bytes
    std::mt19937 size_gen;
    size_gen.seed(time(nullptr));
    std::uniform_int_distribution<> dist(0, 3000);

    const size_t buff_sz = approx_page_mb * DB::MB + dist(size_gen);
    char * buff = static_cast<char *>(malloc(buff_sz)); // NOLINT

    const char buff_ch = pageId % 0xFF;
    memset(buff, buff_ch, buff_sz);

    holder = DB::createMemHolder(buff, [&](char * p) { free(p); }); // NOLINT

    return std::make_shared<DB::ReadBufferFromMemory>(const_cast<char *>(buff), buff_sz);
}

void PSWriter::updatedRandomData()
{
    size_t memory_size = approx_page_mb * DB::MB * 2;
    if (memory == nullptr)
    {
        memory.reset(new char[memory_size]);
        for (size_t i = 0; i < memory_size; i++)
        {
            memory[i] = i % 0xFF;
        }
    }

    std::uniform_int_distribution<> dist(0, memory_size / 2 - 1);
    size_t gen_size = dist(gen);
    buff_ptr = std::make_shared<DB::ReadBufferFromMemory>(memory.get() + gen_size, memory_size - gen_size);
}

void PSWriter::fillAllPages(const PSPtr & ps, const UniPSPtr & uni_ps)
{
    for (DB::PageId page_id = 0; page_id <= MAX_PAGE_ID_DEFAULT; ++page_id)
    {
        DB::MemHolder holder;
        DB::ReadBufferPtr buff = genRandomData(page_id, holder);

        if (ps)
        {
            DB::WriteBatch wb{DB::TEST_NAMESPACE_ID};
            wb.putPage(page_id, 0, buff, buff->buffer().size());
            ps->write(std::move(wb));
        }
        else
        {
            DB::UniversalWriteBatch wb;
            wb.putPage(UniReader::toFullPageId(page_id), 0, buff, buff->buffer().size());
            uni_ps->write(std::move(wb));
        }
        if (page_id % 100 == 0)
            LOG_FMT_INFO(StressEnv::logger, "writer wrote page {}", page_id);
    }
}

bool PSWriter::runImpl()
{
    const DB::PageId page_id = genRandomPageId();
    updatedRandomData();

    if (ps)
    {
        DB::WriteBatch wb{DB::TEST_NAMESPACE_ID};
        wb.putPage(page_id, 0, buff_ptr, buff_ptr->buffer().size());
        ps->write(std::move(wb));
    }
    else
    {
        DB::UniversalWriteBatch wb;
        wb.putPage(UniReader::toFullPageId(page_id), 0, buff_ptr, buff_ptr->buffer().size());
        uni_ps->write(std::move(wb));
    }
    ++pages_used;
    bytes_used += buff_ptr->buffer().size();
    return true;
}

DB::PageId PSWriter::genRandomPageId()
{
    std::normal_distribution<> distribution{static_cast<double>(max_page_id) / 2, 150};
    return static_cast<DB::PageId>(std::round(distribution(gen))) % max_page_id;
}

/// PSCommonWriter ///

void PSCommonWriter::updatedRandomData()
{
    // Calculate the fixed memory size
    size_t single_buff_size = ((buffer_size_min <= buffer_size_max && buffer_size_max > 0) ? buffer_size_max
                                                                                           : batch_buffer_size);
    size_t memory_size = single_buff_size * batch_buffer_nums;

    if (memory == nullptr)
    {
        memory.reset(new char[memory_size]);
        for (size_t i = 0; i < memory_size; i++)
        {
            memory[i] = i % 0xFF;
        }
    }

    buff_ptrs.clear();

    size_t gen_size = genBufferSize();
    for (size_t i = 0; i < batch_buffer_nums; ++i)
    {
        buff_ptrs.emplace_back(std::make_shared<DB::ReadBufferFromMemory>(memory.get() + i * single_buff_size, gen_size));
    }
}

DB::PageId writing_page[1000];

bool PSCommonWriter::runImpl()
{
    const DB::PageId page_id = genRandomPageId();

    updatedRandomData();
    if (ps)
    {
        DB::WriteBatch wb{DB::TEST_NAMESPACE_ID};
        for (auto & buffptr : buff_ptrs)
        {
            wb.putPage(page_id, 0, buffptr, buffptr->buffer().size());
            ++pages_used;
            bytes_used += buffptr->buffer().size();
        }

        ps->write(std::move(wb));
    }
    else
    {
        DB::UniversalWriteBatch wb;
        for (auto & buffptr : buff_ptrs)
        {
            wb.putPage(UniReader::toFullPageId(page_id), 0, buffptr, buffptr->buffer().size());
            ++pages_used;
            bytes_used += buffptr->buffer().size();
        }

        uni_ps->write(std::move(wb));
    }
    return (batch_buffer_limit == 0 || bytes_used < batch_buffer_limit);
}

void PSCommonWriter::setBatchBufferNums(size_t numbers)
{
    batch_buffer_nums = numbers;
}

void PSCommonWriter::setBatchBufferSize(size_t size)
{
    batch_buffer_size = size;
}

void PSCommonWriter::setBatchBufferLimit(size_t size_limit)
{
    batch_buffer_limit = size_limit;
}

void PSCommonWriter::setBatchBufferPageRange(size_t max_page_id_)
{
    max_page_id = max_page_id_;
}

DB::PageId PSCommonWriter::genRandomPageId()
{
    std::uniform_int_distribution<> dist(0, max_page_id);
    return static_cast<DB::PageId>(dist(gen));
}

void PSCommonWriter::setBatchBufferRange(size_t min, size_t max)
{
    if (max > min && min > 0)
    {
        buffer_size_min = min;
        buffer_size_max = max;
    }
}

void PSCommonWriter::setFieldSize(const DB::PageFieldSizes & data_sizes_)
{
    data_sizes = data_sizes_;
}

size_t PSCommonWriter::genBufferSize()
{
    // If set min/max size set, use the range. Otherwise, use batch_buffer_size.
    if (buffer_size_min <= buffer_size_max && buffer_size_max > 0)
    {
        std::uniform_int_distribution<> dist(buffer_size_min, buffer_size_max);
        return dist(gen);
    }
    return batch_buffer_size;
}

/// PSReader ///

DB::PageIds PSReader::genRandomPageIds()
{
    DB::PageIds page_ids;
    for (size_t i = 0; i < page_read_once; ++i)
    {
        std::uniform_int_distribution<> dist(0, max_page_id);
        page_ids.emplace_back(static_cast<DB::PageId>(dist(gen)));
    }
    return page_ids;
}

bool PSReader::runImpl()
{
    DB::PageIds page_ids = genRandomPageIds();

    if (ps)
    {
        auto page_map = ps->read(DB::TEST_NAMESPACE_ID, page_ids);
        for (const auto & page : page_map)
        {
            if (heavy_read_delay_ms > 0)
            {
                usleep(heavy_read_delay_ms * 1000);
            }
            ++pages_used;
            bytes_used += page.second.data.size();
        }
    }
    else
    {
        UniReader reader(*uni_ps);
        auto page_map = reader.read(page_ids);
        for (const auto & page : page_map)
        {
            if (heavy_read_delay_ms > 0)
            {
                usleep(heavy_read_delay_ms * 1000);
            }
            ++pages_used;
            bytes_used += page.second.data.size();
        }
    }
    return true;
}

void PSReader::setPageReadOnce(size_t page_read_once_)
{
    page_read_once = page_read_once_;
}

void PSReader::setReadDelay(size_t delay_ms)
{
    heavy_read_delay_ms = delay_ms;
}

void PSReader::setReadPageRange(size_t max_page_id_)
{
    max_page_id = max_page_id_;
}

void PSReader::setReadPageNums(size_t page_read_once_)
{
    page_read_once = page_read_once_;
}

/// PSWindowWriter ///

void PSWindowWriter::setWindowSize(size_t window_size_)
{
    window_size = window_size_;
}

void PSWindowWriter::setNormalDistributionSigma(size_t sigma_)
{
    sigma = sigma_;
}

UInt64 pageid_boundary = 0;
std::mutex page_id_mutex;

DB::PageId PSWindowWriter::genRandomPageId()
{
    std::lock_guard page_id_lock(page_id_mutex);
    if (pageid_boundary < (window_size / 2))
    {
        writing_page[index] = pageid_boundary++;
        return static_cast<DB::PageId>(writing_page[index]);
    }

    // Generate a random number in the window
    std::normal_distribution<> distribution{static_cast<double>(window_size), static_cast<double>(sigma)};
    auto random = std::round(distribution(gen));
    // Move this "random" near the pageid_boundary, If "random" is still negative, then make it positive
    random = std::abs(random + pageid_boundary);

    auto page_id = static_cast<DB::PageId>(random > pageid_boundary ? pageid_boundary++ : random);
    writing_page[index] = page_id;
    return page_id;
}

void PSWindowReader::setWindowSize(size_t window_size_)
{
    window_size = window_size_;
}

void PSWindowReader::setNormalDistributionSigma(size_t sigma_)
{
    sigma = sigma_;
}

void PSWindowReader::setWriterNums(size_t writer_nums_)
{
    writer_nums = writer_nums_;
}

DB::PageIds PSWindowReader::genRandomPageIds()
{
    std::vector<DB::PageId> page_ids;

    if (pageid_boundary <= (writer_nums + page_read_once))
    {
        // Nothing to read
        return page_ids;
    }

    size_t read_boundary = pageid_boundary - writer_nums - page_read_once;
    if (read_boundary < window_size)
    {
        return page_ids;
    }

    std::normal_distribution<> distribution{static_cast<double>(window_size),
                                            static_cast<double>(sigma)};
    auto rand_id = std::round(distribution(gen));

    rand_id = read_boundary - window_size + rand_id;

    // Bigger than window right boundary
    if (rand_id > read_boundary)
    {
        rand_id = read_boundary;
    }

    // Smaller than window left boundary
    if (rand_id < 0)
    {
        rand_id = std::abs(rand_id);
    }

    for (size_t i = rand_id; i < page_read_once + rand_id; ++i)
    {
        bool writing = false;
        for (size_t j = 0; j < writer_nums; j++)
        {
            if (i == writing_page[j])
            {
                writing = true;
            }
        }
        if (!writing)
            page_ids.emplace_back(i);
    }

    return page_ids;
}

/// PSSnapshotReader ///

bool PSSnapshotReader::runImpl()
{
    PageStorageSnapshotPtr snap;
    if (ps)
        snap = ps->getSnapshot("");
    else
        snap = uni_ps->getSnapshot("");
    snapshots.emplace_back(snap);
    usleep(snapshot_get_interval_ms * 1000);
    return true;
}

void PSSnapshotReader::setSnapshotGetIntervalMs(size_t snapshot_get_interval_ms_)
{
    snapshot_get_interval_ms = snapshot_get_interval_ms_;
}

/// PSIncreaseWriter ///

bool PSIncreaseWriter::runImpl()
{
    return PSCommonWriter::runImpl() && begin_page_id < end_page_id;
}

void PSIncreaseWriter::setPageRange(size_t page_range)
{
    begin_page_id = index * page_range + 1;
    end_page_id = (index + 1) * page_range + 1;
}

DB::PageId PSIncreaseWriter::genRandomPageId()
{
    return static_cast<DB::PageId>(begin_page_id++);
}
} // namespace DB::PS::tests
