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

#include <Common/Checksum.h>
#include <Common/Logger.h>
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/PathPool.h>
#include <TestUtils/TiFlashStorageTestBasic.h>
#include <gtest/gtest.h>

#include <ctime>
#include <fstream>
#include <random>
namespace DTTool::Bench
{
using namespace DB::DM;
using namespace DB;

ColumnDefinesPtr getDefaultColumns();
Context getContext(const DB::Settings & settings, const String & tmp_path);
ColumnDefinesPtr createColumnDefines(size_t column_number);
Block createBlock(
    size_t column_number,
    size_t start,
    size_t row_number,
    std::size_t limit,
    double sparse_ratio,
    std::mt19937_64 & eng,
    size_t & acc,
    const LoggerPtr & logger);
} // namespace DTTool::Bench

namespace DTTool::Inspect
{
int inspectServiceMain(DB::Context & context, const InspectArgs & args);
} // namespace DTTool::Inspect

struct DTToolTest : public DB::base::TiFlashStorageTestBasic
{
    DB::DM::DMFilePtr dmfile = nullptr;
    DB::DM::DMFilePtr dmfile_v3 = nullptr;
    static constexpr size_t column = 64;
    static constexpr size_t size = 128;
    static constexpr size_t field = 512;

    void SetUp() override
    {
        TiFlashStorageTestBasic::SetUp();
        using namespace DTTool::Bench;

        auto dev = std::random_device{};
        auto seed = dev();
        auto engine = std::mt19937_64{seed};
        auto defines = DTTool::Bench::createColumnDefines(column);
        std::vector<DB::Block> blocks;
        std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty> properties;
        size_t effective_size = 0;
        for (size_t i = 0, count = 1; i < size; count++)
        {
            auto block_size = engine() % (size - i) + 1;
            blocks.push_back(
                DTTool::Bench::createBlock(column, i, block_size, field, 0.0, engine, effective_size, Logger::get()));
            i += block_size;
            DB::DM::DMFileBlockOutputStream::BlockProperty property{};
            property.gc_hint_version = count;
            property.effective_num_rows = block_size;
            properties.push_back(property);
        }
        auto path_pool
            = std::make_shared<DB::StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        auto storage_pool
            = std::make_shared<DB::DM::StoragePool>(*db_context, NullspaceID, /*ns_id*/ 1, *path_pool, "test.t1");
        auto dm_settings = DB::DM::DeltaMergeStore::Settings{};
        auto dm_context = DB::DM::DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            /*physical_table_id*/ 1,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
        // Write
        {
            dmfile = DB::DM::DMFile::create(1, getTemporaryPath(), std::nullopt, 0, 0, NullspaceID, DMFileFormat::V0);
            {
                auto stream = DB::DM::DMFileBlockOutputStream(*db_context, dmfile, *defines);
                stream.writePrefix();
                for (size_t j = 0; j < blocks.size(); ++j)
                {
                    stream.write(blocks[j], properties[j]);
                }
                stream.writeSuffix();
            }
        }

        // Write DMFile::V3
        {
            dmfile_v3 = DB::DM::DMFile::create(
                2,
                getTemporaryPath(),
                std::make_optional<DMChecksumConfig>(),
                128 * 1024,
                16 * 1024 * 1024,
                NullspaceID,
                DMFileFormat::V3);
            {
                auto stream = DB::DM::DMFileBlockOutputStream(*db_context, dmfile_v3, *defines);
                stream.writePrefix();
                for (size_t j = 0; j < blocks.size(); ++j)
                {
                    stream.write(blocks[j], properties[j]);
                }
                stream.writeSuffix();
            }
        }
    }
};


TEST_F(DTToolTest, MigrationAllFileRecognizableOnDefault)
{
    std::vector<std::string> sub_files;
    Poco::File(dmfile->path()).list(sub_files);
    for (auto & i : sub_files)
    {
        EXPECT_TRUE(DTTool::Migrate::isRecognizable(*dmfile, i)) << " file: " << i;
    }

    Poco::File(dmfile_v3->path()).list(sub_files);
    for (auto & i : sub_files)
    {
        EXPECT_TRUE(DTTool::Migrate::isRecognizable(*dmfile_v3, i)) << " file: " << i;
    }
}

TEST_F(DTToolTest, MigrationSuccess)
{
    {
        auto args = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 1,
            .version = 2,
            .frame = DBMS_DEFAULT_BUFFER_SIZE,
            .algorithm = DB::ChecksumAlgo::XXH3,
            .workdir = getTemporaryPath(),
            .compression_method = DB::CompressionMethod::LZ4,
            .compression_level = DB::CompressionSetting::getDefaultLevel(DB::CompressionMethod::LZ4),
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, args), 0);
    }
    {
        auto args = DTTool::Inspect::InspectArgs{.check = true, .file_id = 1, .workdir = getTemporaryPath()};
        EXPECT_EQ(DTTool::Inspect::inspectServiceMain(*db_context, args), 0);
    }
}


TEST_F(DTToolTest, MigrationV3toV2Success)
{
    {
        auto args = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 2,
            .version = 2,
            .frame = DBMS_DEFAULT_BUFFER_SIZE,
            .algorithm = DB::ChecksumAlgo::XXH3,
            .workdir = getTemporaryPath(),
            .compression_method = DB::CompressionMethod::LZ4,
            .compression_level = DB::CompressionSetting::getDefaultLevel(DB::CompressionMethod::LZ4),
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, args), 0);
    }
    {
        auto args = DTTool::Inspect::InspectArgs{.check = true, .file_id = 2, .workdir = getTemporaryPath()};
        EXPECT_EQ(DTTool::Inspect::inspectServiceMain(*db_context, args), 0);
    }
}

void getHash(std::unordered_map<std::string, std::string> & records, const std::string & path)
{
    std::fstream file{path};
    auto digest = DB::UnifiedDigest<DB::Digest::CRC64>{};
    std::vector<char> buffer(DBMS_DEFAULT_BUFFER_SIZE);
    while (auto length = file.readsome(buffer.data(), buffer.size()))
    {
        digest.update(buffer.data(), length);
    }
    records[path] = digest.raw();
}

void compareHash(std::unordered_map<std::string, std::string> & records)
{
    for (const auto & i : records)
    {
        std::fstream file{i.first};
        auto digest = DB::UnifiedDigest<DB::Digest::CRC64>{};
        std::vector<char> buffer(DBMS_DEFAULT_BUFFER_SIZE);
        while (auto length = file.readsome(buffer.data(), buffer.size()))
        {
            digest.update(buffer.data(), length);
        }
        EXPECT_TRUE(digest.compareRaw(i.second)) << "file: " << i.first;
    }
}

TEST_F(DTToolTest, ConsecutiveMigration)
{
    auto args = DTTool::Migrate::MigrateArgs{
        .no_keep = false,
        .dry_mode = false,
        .file_id = 1,
        .version = 1,
        .frame = DBMS_DEFAULT_BUFFER_SIZE,
        .algorithm = DB::ChecksumAlgo::XXH3,
        .workdir = getTemporaryPath(),
        .compression_method = DB::CompressionMethod::LZ4,
        .compression_level = DB::CompressionSetting::getDefaultLevel(DB::CompressionMethod::LZ4),
    };

    EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, args), 0);
    auto logger = DB::Logger::get("DTToolTest");
    std::unordered_map<std::string, std::string> records;
    {
        Poco::File file{dmfile->path()};
        std::vector<std::string> subfiles;
        file.list(subfiles);
        for (const auto & i : subfiles)
        {
            if (!DTTool::Migrate::needFrameMigration(*dmfile, i))
                continue;
            LOG_INFO(logger, "record file: {}", i);
            getHash(records, i);
        }
    }
    std::vector<std::tuple<size_t, DB::ChecksumAlgo, DB::CompressionMethod, int>> test_cases{
        {2, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::LZ4, -1},
        {1, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::ZSTD, 1},
        {2, DB::ChecksumAlgo::City128, DB::CompressionMethod::LZ4HC, 0},
        {2, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {args.version, args.algorithm, args.compression_method, args.compression_level}};
    for (auto [version, algo, comp, level] : test_cases)
    {
        auto a = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 1,
            .version = version,
            .frame = DBMS_DEFAULT_BUFFER_SIZE,
            .algorithm = algo,
            .workdir = getTemporaryPath(),
            .compression_method = comp,
            .compression_level = level,
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, a), 0);
    }

    compareHash(records);
}

TEST_F(DTToolTest, BlockwiseInvariant)
{
    std::vector<size_t> size_info{};
    {
        auto stream = DB::DM::createSimpleBlockInputStream(*db_context, dmfile);
        stream->readPrefix();
        while (auto block = stream->read())
        {
            size_info.push_back(block.bytes());
        }
        stream->readSuffix();
    }

    std::vector<std::tuple<size_t, size_t, DB::ChecksumAlgo, DB::CompressionMethod, int>> test_cases{
        {2, DBMS_DEFAULT_BUFFER_SIZE, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::LZ4, -1},
        {1, 64, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::ZSTD, 1},
        {2, DBMS_DEFAULT_BUFFER_SIZE * 2, DB::ChecksumAlgo::City128, DB::CompressionMethod::LZ4HC, 0},
        {2, DBMS_DEFAULT_BUFFER_SIZE * 4, DB::ChecksumAlgo::City128, DB::CompressionMethod::LZ4HC, 0},
        {2, 4, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {2, 13, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {2, 5261, DB::ChecksumAlgo::CRC64, DB::CompressionMethod::ZSTD, 22},
        {1, DBMS_DEFAULT_BUFFER_SIZE, DB::ChecksumAlgo::XXH3, DB::CompressionMethod::NONE, -1}};
    for (auto [version, frame_size, algo, comp, level] : test_cases)
    {
        auto a = DTTool::Migrate::MigrateArgs{
            .no_keep = false,
            .dry_mode = false,
            .file_id = 1,
            .version = version,
            .frame = frame_size,
            .algorithm = algo,
            .workdir = getTemporaryPath(),
            .compression_method = comp,
            .compression_level = level,
        };

        EXPECT_EQ(DTTool::Migrate::migrateServiceMain(*db_context, a), 0);
        auto refreshed_file = DB::DM::DMFile::restore(
            db_context->getFileProvider(),
            1,
            0,
            getTemporaryPath(),
            DB::DM::DMFileMeta::ReadMode::all());
        if (version == 2)
        {
            EXPECT_EQ(refreshed_file->getConfiguration()->getChecksumFrameLength(), frame_size);
        }
        auto stream = DB::DM::createSimpleBlockInputStream(*db_context, refreshed_file);
        auto size_iter = size_info.begin();
        auto prop_iter = dmfile->getPackProperties().property().begin();
        auto new_prop_iter = refreshed_file->getPackProperties().property().begin();
        auto stat_iter = dmfile->getPackStats().begin();
        auto new_stat_iter = refreshed_file->getPackStats().begin();
        stream->readPrefix();
        while (auto block = stream->read())
        {
            EXPECT_EQ(*size_iter++, block.bytes());
            EXPECT_EQ(prop_iter->gc_hint_version(), new_prop_iter->gc_hint_version());
            EXPECT_EQ(prop_iter->num_rows(), new_prop_iter->num_rows());
            EXPECT_EQ(stat_iter->rows, new_stat_iter->rows);
            EXPECT_EQ(stat_iter->not_clean, new_stat_iter->not_clean);
            EXPECT_EQ(stat_iter->first_version, new_stat_iter->first_version);
            EXPECT_EQ(stat_iter->bytes, new_stat_iter->bytes);
            EXPECT_EQ(stat_iter->first_tag, new_stat_iter->first_tag);
            prop_iter++;
            new_prop_iter++;
            stat_iter++;
            new_stat_iter++;
        }
        EXPECT_EQ(stat_iter, dmfile->getPackStats().end());
        EXPECT_EQ(new_stat_iter, refreshed_file->getPackStats().end());
        EXPECT_EQ(prop_iter, dmfile->getPackProperties().property().end());
        EXPECT_EQ(new_prop_iter, refreshed_file->getPackProperties().property().end());
        stream->readSuffix();
    }
}

namespace DTTool::Generate
{
int generateEntry(const std::vector<std::string> & opts);
} // namespace DTTool::Generate

struct DTToolGenerateTest : public ::testing::Test
{
    std::string tmp_dir;

    void SetUp() override
    {
        static int counter = 0;
        tmp_dir = ::testing::TempDir() + "dttool_generate_test_" + std::to_string(++counter);
        Poco::File(tmp_dir).createDirectory();
    }

    void TearDown() override
    {
        if (Poco::File tmp(tmp_dir); tmp.exists())
            tmp.remove(true);
    }

    std::string tsvPath() const { return tmp_dir + "/data.tsv"; }
    std::string schemaPath() const { return tmp_dir + "/schema.json"; }
};

TEST_F(DTToolGenerateTest, HelpText)
{
    std::vector<std::string> opts = {"--help"};
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), 0);
}

TEST_F(DTToolGenerateTest, ValidRowCountMultipleOf8192)
{
    std::vector<std::string> opts = {
        "--rows", "8192",
        "--columns", "4",
        "--random", "42",
        "--output", tsvPath(),
        "--schema", schemaPath(),
    };
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), 0);

    // Verify TSV file exists and has exactly 8192 rows (no header).
    std::ifstream tsv(tsvPath());
    ASSERT_TRUE(tsv.is_open());
    std::string line;
    size_t line_count = 0;
    while (std::getline(tsv, line))
        ++line_count;
    EXPECT_EQ(line_count, 8192);

    // Verify schema JSON exists.
    EXPECT_TRUE(Poco::File(schemaPath()).exists());
}

TEST_F(DTToolGenerateTest, LargeRowCount)
{
    std::vector<std::string> opts = {
        "--rows", "65536", // 8 * 8192
        "--columns", "2",
        "--random", "12345",
        "--output", tsvPath(),
        "--schema", schemaPath(),
    };
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), 0);

    std::ifstream tsv(tsvPath());
    ASSERT_TRUE(tsv.is_open());
    size_t line_count = 0;
    std::string line;
    while (std::getline(tsv, line))
        ++line_count;
    EXPECT_EQ(line_count, 65536);
}

TEST_F(DTToolGenerateTest, InvalidRowCountNotMultipleOf8192)
{
    // Ensure the output file does not exist before the call.
    ASSERT_FALSE(Poco::File(tsvPath()).exists());

    std::vector<std::string> opts = {
        "--rows", "100", // Not a multiple of 8192
        "--columns", "4",
        "--output", tsvPath(),
        "--schema", schemaPath(),
    };
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), -EINVAL);

    // Verify no partial TSV output was written.
    EXPECT_FALSE(Poco::File(tsvPath()).exists());
}

TEST_F(DTToolGenerateTest, NoHeaderRow)
{
    std::vector<std::string> opts = {
        "--rows", "8192",
        "--columns", "4",
        "--random", "42",
        "--output", tsvPath(),
        "--schema", schemaPath(),
    };
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), 0);

    // The first line of the TSV must not contain known column names.
    std::ifstream tsv(tsvPath());
    ASSERT_TRUE(tsv.is_open());
    std::string first_line;
    ASSERT_TRUE(std::getline(tsv, first_line));

    // Column names that would appear in a header row if one were mistakenly written.
    EXPECT_FALSE(first_line.starts_with("_tidb_rowid"));
    EXPECT_FALSE(first_line.starts_with("_INTERNAL_VERSION"));
    EXPECT_FALSE(first_line.starts_with("_INTERNAL_DELMARK"));
    EXPECT_FALSE(first_line.starts_with("int_"));
    EXPECT_FALSE(first_line.starts_with("str_"));
    // The first field should be a numeric handle value.
    // Check that the first character is a digit or minus sign, not a letter.
    EXPECT_TRUE(std::isdigit(first_line[0]) || first_line[0] == '-')
        << "First line starts with unexpected character: " << first_line[0];
}

TEST_F(DTToolGenerateTest, NullEncoding)
{
    // With sparse_ratio = 1.0, all 3 string columns should be null → encoded as \N.
    // Column layout: handle, version, delmark, int_0, int_1, int_2, str_0, str_1, str_2
    // The last 3 fields on each line should be \N.
    std::vector<std::string> opts = {
        "--rows", "8192",
        "--columns", "6", // 3 int + 3 string columns
        "--sparse-ratio", "1.0",
        "--random", "42",
        "--output", tsvPath(),
        "--schema", schemaPath(),
    };
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), 0);

    std::ifstream tsv(tsvPath());
    ASSERT_TRUE(tsv.is_open());
    std::string line;
    for (size_t row = 0; row < 10 && std::getline(tsv, line); ++row)
    {
        // Each line should end with \t\N\t\N\t\N (three null string fields).
        const std::string expected_suffix = "\t\\N\t\\N\t\\N";
        EXPECT_TRUE(line.ends_with(expected_suffix))
            << "Row " << row << " should end with three null-encoded string fields";
    }
}

TEST_F(DTToolGenerateTest, SchemaJsonStructure)
{
    std::vector<std::string> opts = {
        "--rows", "8192",
        "--columns", "2",
        "--random", "42",
        "--output", tsvPath(),
        "--schema", schemaPath(),
    };
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), 0);

    // Read and validate the schema JSON structure.
    std::ifstream schema_file(schemaPath(), std::ios::binary);
    ASSERT_TRUE(schema_file.is_open());
    schema_file.seekg(0, std::ios::end);
    std::string schema_content;
    schema_content.resize(static_cast<size_t>(schema_file.tellg()));
    schema_file.seekg(0, std::ios::beg);
    schema_file.read(&schema_content[0], static_cast<std::streamsize>(schema_content.size()));

    // Basic structural checks: should contain "columns" and known column names.
    EXPECT_TRUE(schema_content.find("\"columns\"") != std::string::npos);
    EXPECT_TRUE(schema_content.find("\"name\"") != std::string::npos);
    EXPECT_TRUE(schema_content.find("\"type\"") != std::string::npos);
    // Should contain the known hidden column names.
    EXPECT_TRUE(schema_content.find("_tidb_rowid") != std::string::npos);
    EXPECT_TRUE(schema_content.find("_INTERNAL_VERSION") != std::string::npos);
    EXPECT_TRUE(schema_content.find("_INTERNAL_DELMARK") != std::string::npos);
    // User columns for 2 columns: 1 int + 1 string.
    EXPECT_TRUE(schema_content.find("int_0") != std::string::npos);
    EXPECT_TRUE(schema_content.find("str_0") != std::string::npos);

    // Schema JSON must not contain internal metadata fields.
    EXPECT_FALSE(schema_content.find("\"rows\"") != std::string::npos);
    EXPECT_FALSE(schema_content.find("\"random\"") != std::string::npos);
    EXPECT_FALSE(schema_content.find("\"field\"") != std::string::npos);
    EXPECT_FALSE(schema_content.find("\"col_id\"") != std::string::npos);
}

TEST_F(DTToolGenerateTest, DeterministicOutputWithFixedSeed)
{
    std::vector<std::string> opts1 = {
        "--rows", "8192",
        "--columns", "4",
        "--random", "12345",
        "--output", tmp_dir + "/run1.tsv",
        "--schema", tmp_dir + "/run1.json",
    };
    std::vector<std::string> opts2 = {
        "--rows", "8192",
        "--columns", "4",
        "--random", "12345",
        "--output", tmp_dir + "/run2.tsv",
        "--schema", tmp_dir + "/run2.json",
    };

    EXPECT_EQ(DTTool::Generate::generateEntry(opts1), 0);
    EXPECT_EQ(DTTool::Generate::generateEntry(opts2), 0);

    // Both runs with the same seed should produce identical TSV output.
    auto readFile = [](const std::string & path) -> std::string {
        std::ifstream f(path, std::ios::binary);
        std::string content;
        f.seekg(0, std::ios::end);
        content.resize(static_cast<size_t>(f.tellg()));
        f.seekg(0, std::ios::beg);
        f.read(&content[0], static_cast<std::streamsize>(content.size()));
        return content;
    };

    EXPECT_EQ(readFile(tmp_dir + "/run1.tsv"), readFile(tmp_dir + "/run2.tsv"));
    EXPECT_EQ(readFile(tmp_dir + "/run1.json"), readFile(tmp_dir + "/run2.json"));
}

TEST_F(DTToolGenerateTest, DifferentSeedsProduceDifferentOutput)
{
    std::vector<std::string> opts1 = {
        "--rows", "8192",
        "--columns", "4",
        "--random", "12345",
        "--output", tmp_dir + "/seed1.tsv",
        "--schema", tmp_dir + "/seed1.json",
    };
    std::vector<std::string> opts2 = {
        "--rows", "8192",
        "--columns", "4",
        "--random", "54321",
        "--output", tmp_dir + "/seed2.tsv",
        "--schema", tmp_dir + "/seed2.json",
    };

    EXPECT_EQ(DTTool::Generate::generateEntry(opts1), 0);
    EXPECT_EQ(DTTool::Generate::generateEntry(opts2), 0);

    auto readFile = [](const std::string & path) -> std::string {
        std::ifstream f(path, std::ios::binary);
        std::string content;
        f.seekg(0, std::ios::end);
        content.resize(static_cast<size_t>(f.tellg()));
        f.seekg(0, std::ios::beg);
        f.read(&content[0], static_cast<std::streamsize>(content.size()));
        return content;
    };

    // Different seeds should produce different TSV content.
    EXPECT_NE(readFile(tmp_dir + "/seed1.tsv"), readFile(tmp_dir + "/seed2.tsv"));
    // But the schema JSON should still match (same columns, same types).
    EXPECT_EQ(readFile(tmp_dir + "/seed1.json"), readFile(tmp_dir + "/seed2.json"));
}

TEST_F(DTToolGenerateTest, MissingRequiredOptions)
{
    // Missing --rows
    {
        std::vector<std::string> opts = {
            "--columns", "4",
            "--output", tsvPath(),
            "--schema", schemaPath(),
        };
        EXPECT_EQ(DTTool::Generate::generateEntry(opts), -EINVAL);
    }
    // Missing --columns
    {
        std::vector<std::string> opts = {
            "--rows", "8192",
            "--output", tsvPath(),
            "--schema", schemaPath(),
        };
        EXPECT_EQ(DTTool::Generate::generateEntry(opts), -EINVAL);
    }
    // Missing --output
    {
        std::vector<std::string> opts = {
            "--rows", "8192",
            "--columns", "4",
            "--schema", schemaPath(),
        };
        EXPECT_EQ(DTTool::Generate::generateEntry(opts), -EINVAL);
    }
    // Missing --schema
    {
        std::vector<std::string> opts = {
            "--rows", "8192",
            "--columns", "4",
            "--output", tsvPath(),
        };
        EXPECT_EQ(DTTool::Generate::generateEntry(opts), -EINVAL);
    }
}

TEST_F(DTToolGenerateTest, ZeroColumns)
{
    // --columns 0 means no user data columns, only the 3 hidden columns.
    std::vector<std::string> opts = {
        "--rows", "8192",
        "--columns", "0",
        "--random", "42",
        "--output", tsvPath(),
        "--schema", schemaPath(),
    };
    EXPECT_EQ(DTTool::Generate::generateEntry(opts), 0);

    // Verify TSV has 8192 rows.
    std::ifstream tsv(tsvPath());
    ASSERT_TRUE(tsv.is_open());
    size_t line_count = 0;
    std::string first_line;
    while (std::getline(tsv, first_line))
    {
        if (line_count == 0)
        {
            // First line should have exactly 3 tab-separated fields (handle, version, delmark).
            size_t tab_count = 0;
            for (char c : first_line)
            {
                if (c == '\t')
                    ++tab_count;
            }
            EXPECT_EQ(tab_count, 2) << "Expected 3 columns (2 tabs), got " << tab_count << " tabs";
        }
        ++line_count;
    }
    EXPECT_EQ(line_count, 8192);

    // Schema should have exactly 3 columns (the hidden columns only).
    std::ifstream schema_file(schemaPath(), std::ios::binary);
    ASSERT_TRUE(schema_file.is_open());
    schema_file.seekg(0, std::ios::end);
    std::string schema_content;
    schema_content.resize(static_cast<size_t>(schema_file.tellg()));
    schema_file.seekg(0, std::ios::beg);
    schema_file.read(&schema_content[0], static_cast<std::streamsize>(schema_content.size()));
    EXPECT_TRUE(schema_content.find("_tidb_rowid") != std::string::npos);
    EXPECT_TRUE(schema_content.find("_INTERNAL_VERSION") != std::string::npos);
    EXPECT_TRUE(schema_content.find("_INTERNAL_DELMARK") != std::string::npos);
    EXPECT_FALSE(schema_content.find("int_") != std::string::npos);
    EXPECT_FALSE(schema_content.find("str_") != std::string::npos);
}
