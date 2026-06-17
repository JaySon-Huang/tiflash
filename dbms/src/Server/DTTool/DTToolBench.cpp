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
#include <Common/Logger.h>
#include <Common/RandomData.h>
#include <Common/TiFlashMetrics.h>
#include <DataStreams/materializeBlock.h>
#include <IO/Buffer/ReadBufferFromFile.h>
#include <IO/Checksum/ChecksumBuffer.h>
#include <IO/Encryption/MockKeyManager.h>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Parser.h>
#pragma clang diagnostic pop
#include <Poco/Path.h>
#include <Server/DTTool/DTTool.h>
#include <Server/RaftConfigParser.h>
#include <Storages/DeltaMerge/DMChecksumConfig.h>
#include <Storages/DeltaMerge/DMContext.h>
#include <Storages/DeltaMerge/DeltaMergeStore.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <Storages/DeltaMerge/StoragePool/StoragePool.h>
#include <Storages/FormatVersion.h>
#include <Storages/KVStore/TMTContext.h>
#include <Storages/PathPool.h>
#include <boost_wrapper/program_options.h>
#include <common/defines.h>
#include <pingcap/Config.h>

#include <boost/program_options/errors.hpp>
#include <boost/throw_exception.hpp>
#include <algorithm>
#include <chrono>
#include <iostream>
#include <random>
#include <utility>
namespace bpo = boost::program_options;

namespace DTTool::Bench
{

using namespace DB::DM;
using namespace DB;
std::unique_ptr<Context> global_context = nullptr;

ColumnDefinesPtr getDefaultColumns()
{
    // Return [handle, ver, del] column defines
    ColumnDefinesPtr columns = std::make_shared<ColumnDefines>(ColumnDefines{
        getExtraHandleColumnDefine(/*is_common_handle=*/false),
        getVersionColumnDefine(),
        getTagColumnDefine(),
    });
    return columns;
}

ColumnDefinesPtr createColumnDefines(size_t column_number)
{
    auto primitive = getDefaultColumns();
    auto int_num = column_number / 2;
    auto str_num = column_number - int_num;
    for (size_t i = 0; i < int_num; ++i)
    {
        primitive->emplace_back(ColumnDefine{
            static_cast<ColId>(3 + i),
            fmt::format("int_{}", i),
            DB::DataTypeFactory::instance().get("Int64")});
    }
    for (size_t i = 0; i < str_num; ++i)
    {
        primitive->emplace_back(ColumnDefine{
            static_cast<ColId>(3 + int_num + i),
            fmt::format("str_{}", i),
            DB::DataTypeFactory::instance().get(DataTypeString::getNullableDefaultName())});
    }
    return primitive;
}

DB::Block createBlock(
    size_t column_number,
    size_t start,
    size_t row_number,
    std::size_t str_len_limit,
    double sparse_ratio,
    std::mt19937_64 & eng,
    size_t & acc,
    const LoggerPtr & logger)
{
    using namespace DB;
    auto int_num = column_number / 2;
    auto str_num = column_number - int_num;
    Block block;
    //PK
    {
        ColumnWithTypeAndName pk_col(nullptr, MutSup::getExtraHandleColumnIntType(), "id", MutSup::extra_handle_id);
        IColumn::MutablePtr m_col = pk_col.type->createColumn();
        for (size_t i = 0; i < row_number; i++)
        {
            Field field = static_cast<DB::Int64>(start + i);
            m_col->insert(field);
            acc += 8;
        }
        pk_col.column = std::move(m_col);
        block.insert(std::move(pk_col));
    }
    // Version
    {
        ColumnWithTypeAndName version_col(
            {},
            MutSup::getVersionColumnType(),
            MutSup::version_column_name,
            MutSup::version_col_id);
        IColumn::MutablePtr m_col = version_col.type->createColumn();
        for (size_t i = 0; i < row_number; ++i)
        {
            Field field = static_cast<DB::UInt64>((start + i) * 10);
            m_col->insert(field);
            acc += 8;
        }
        version_col.column = std::move(m_col);
        block.insert(std::move(version_col));
    }

    //Tag
    {
        ColumnWithTypeAndName tag_col(
            nullptr,
            MutSup::getDelmarkColumnType(),
            MutSup::delmark_column_name,
            MutSup::delmark_col_id);
        IColumn::MutablePtr m_col = tag_col.type->createColumn();
        auto & column_data = typeid_cast<ColumnVector<UInt8> &>(*m_col).getData();
        column_data.resize(row_number);
        for (size_t i = 0; i < row_number; ++i)
        {
            column_data[i] = eng() & 1;
            acc += 1;
        }
        tag_col.column = std::move(m_col);
        block.insert(std::move(tag_col));
    }

    std::uniform_int_distribution<Int64> int_dist;
    for (size_t i = 0; i < int_num; ++i)
    {
        ColumnWithTypeAndName int_col(
            nullptr,
            DB::DataTypeFactory::instance().get("Int64"),
            fmt::format("int_{}", i),
            static_cast<ColId>(3 + i));
        IColumn::MutablePtr m_col = int_col.type->createColumn();
        auto & column_data = typeid_cast<ColumnVector<Int64> &>(*m_col).getData();
        column_data.resize(row_number);
        for (size_t j = 0; j < row_number; ++j)
        {
            column_data[j] = int_dist(eng);
            acc += 8;
        }
        int_col.column = std::move(m_col);
        block.insert(std::move(int_col));
    }

    std::uniform_real_distribution<> real_dist(0.0, 1.0);
    for (size_t i = 0; i < str_num; ++i)
    {
        String col_name = fmt::format("str_{}", i);
        ColumnWithTypeAndName str_col(
            nullptr,
            DB::DataTypeFactory::instance().get(DataTypeString::getNullableDefaultName()),
            col_name,
            static_cast<ColId>(3 + int_num + i));
        IColumn::MutablePtr m_col = str_col.type->createColumn();
        size_t num_null = 0;
        for (size_t j = 0; j < row_number; j++)
        {
            bool is_null = false;
            if (sparse_ratio > 0.0 && real_dist(eng) < sparse_ratio)
                is_null = true;
            if (is_null)
            {
                m_col->insertDefault();
                num_null++;
            }
            else
            {
                // Use eng for random string generation so that the output is
                // deterministic when a fixed --random seed is provided.
                static const std::string charset{
                    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*()|[]{}:;',<.>`~"};
                std::uniform_int_distribution<size_t> char_dist(0, charset.size() - 1);
                String str(str_len_limit, '\x00');
                std::generate_n(str.begin(), str.size(), [&]() { return charset[char_dist(eng)]; });
                Field field = str;
                m_col->insert(field);
            }
        }
        str_col.column = std::move(m_col);
        block.insert(std::move(str_col));
        if (sparse_ratio > 0.0)
        {
            LOG_TRACE(
                logger,
                "Sparse_ratio={} column_name={} num_null={} num_rows={}",
                sparse_ratio,
                col_name,
                num_null,
                row_number);
        }
    }

    return block;
}

std::tuple<std::vector<DB::Block>, std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty>, size_t> //
genBlocks(
    size_t random,
    const size_t num_rows,
    const size_t num_column,
    size_t str_len,
    double sparse_ratio,
    const LoggerPtr & logger)
{
    std::vector<DB::Block> blocks;
    std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty> properties;
    size_t effective_size = 0;

    auto engine = std::mt19937_64{random};
    auto num_blocks = static_cast<size_t>(std::round(1.0 * num_rows / DEFAULT_MERGE_BLOCK_SIZE));
    for (size_t i = 0, count = 1, start_handle = 0; i < num_blocks; ++i)
    {
        auto block_size = DEFAULT_MERGE_BLOCK_SIZE;
        LOG_INFO(logger, "generating block with size: {}", block_size);
        blocks.push_back(DTTool::Bench::createBlock(
            num_column,
            start_handle,
            block_size,
            str_len,
            sparse_ratio,
            engine,
            effective_size,
            logger));
        start_handle += block_size;
        DB::DM::DMFileBlockOutputStream::BlockProperty property{};
        property.gc_hint_version = count;
        property.effective_num_rows = block_size;
        properties.push_back(property);
    }
    LOG_INFO(
        logger,
        "Blocks generated, num_rows={} num_blocks={} num_column={} effective_size={}",
        num_rows,
        num_blocks,
        num_column,
        effective_size);
    return {std::move(blocks), std::move(properties), effective_size};
}


/// Load column definitions from the schema JSON file produced by `dttool generate`.
/// Returns nullptr on any parse error, missing fields, or unsupported type.
ColumnDefinesPtr loadSchemaFromJson(const std::string & schema_path)
{
    std::ifstream file(schema_path);
    if (!file.is_open())
    {
        std::cerr << "Failed to open schema file: " << schema_path << std::endl;
        return nullptr;
    }

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result;
    try
    {
        result = parser.parse(file);
    }
    catch (const Poco::Exception & e)
    {
        std::cerr << "Failed to parse schema JSON: " << e.displayText() << std::endl;
        return nullptr;
    }

    auto root = result.extract<Poco::JSON::Object::Ptr>();
    auto columns_array = root->getArray("columns");
    if (!columns_array || columns_array->size() == 0)
    {
        std::cerr << "Schema JSON missing or empty 'columns' array" << std::endl;
        return nullptr;
    }

    auto defines = std::make_shared<ColumnDefines>();
    ColId user_col_id = 3; // User columns start from ID 3, matching createColumnDefines.

    for (size_t i = 0; i < columns_array->size(); ++i)
    {
        auto col_obj = columns_array->getObject(static_cast<unsigned int>(i));
        auto name = col_obj->getValue<std::string>("name");
        auto type_name = col_obj->getValue<std::string>("type");

        ColId col_id;
        if (name == MutSup::extra_handle_column_name)
            col_id = MutSup::extra_handle_id;
        else if (name == MutSup::version_column_name)
            col_id = MutSup::version_col_id;
        else if (name == MutSup::delmark_column_name)
            col_id = MutSup::delmark_col_id;
        else
            col_id = user_col_id++;

        DataTypePtr type;
        try
        {
            type = DataTypeFactory::instance().get(type_name);
        }
        catch (const Exception & e)
        {
            std::cerr << "Unsupported type in schema: " << type_name << " (" << e.message() << ")" << std::endl;
            return nullptr;
        }

        defines->emplace_back(col_id, name, std::move(type));
    }

    // Validate that the three hidden columns are present at positions 0–2 with
    // the expected names.
    if (defines->size() < 3 || (*defines)[0].name != MutSup::extra_handle_column_name
        || (*defines)[1].name != MutSup::version_column_name
        || (*defines)[2].name != MutSup::delmark_column_name)
    {
        std::cerr << "Schema must contain _tidb_rowid, _INTERNAL_VERSION, and _INTERNAL_DELMARK "
                     "as the first three columns"
                  << std::endl;
        return nullptr;
    }

    return defines;
}


/// Load blocks from the TSV data file produced by `dttool generate`.
/// Uses the existing TabSeparated input format to parse rows.
/// Returns empty blocks vector on any load error.
std::tuple<std::vector<Block>, std::vector<DMFileBlockOutputStream::BlockProperty>, size_t>
loadBlocksFromTSV(const std::string & tsv_path, const ColumnDefinesPtr & defines, Context & context)
{
    // Build a sample block from the column definitions so that the input format
    // knows the expected schema. Each column must carry a valid (non-null) empty
    // column pointer — materializeBlock and assertBlocksHaveEqualStructure
    // dereference the column pointer unconditionally.
    Block sample;
    for (const auto & cd : *defines)
    {
        auto empty_col = cd.type->createColumn();
        ColumnWithTypeAndName col(std::move(empty_col), cd.type, cd.name, cd.id);
        sample.insert(std::move(col));
    }

    // Materialize the sample block so that columns have valid (empty) data
    // pointers, which are required by assertBlocksHaveEqualStructure during read.
    Block sample_materialized = materializeBlock(sample);

    ReadBufferFromFile buffer(tsv_path);

    BlockInputStreamPtr stream;
    try
    {
        stream = context.getInputFormat("TabSeparated", buffer, sample_materialized, DEFAULT_MERGE_BLOCK_SIZE);
        stream->readPrefix();
    }
    catch (const Exception & e)
    {
        std::cerr << "Failed to create TSV input stream: " << e.message() << std::endl;
        return {};
    }

    std::vector<Block> blocks;
    size_t effective_size = 0;
    size_t total_rows = 0;

    try
    {
        while (true)
        {
            auto block = stream->read();
            if (!block)
                break;

            size_t rows = block.rows();
            if (rows != DEFAULT_MERGE_BLOCK_SIZE)
            {
                std::cerr << fmt::format(
                    "Each block must have exactly {} rows (DEFAULT_MERGE_BLOCK_SIZE), got block with {} rows",
                    DEFAULT_MERGE_BLOCK_SIZE,
                    rows)
                          << std::endl;
                return {};
            }

            // Verify column count matches schema.
            if (block.columns() != sample.columns())
            {
                std::cerr << fmt::format(
                    "TSV column count ({}) does not match schema column count ({})",
                    block.columns(),
                    sample.columns())
                          << std::endl;
                return {};
            }

            total_rows += rows;
            effective_size += block.bytes();
            blocks.push_back(std::move(block));
        }
        stream->readSuffix();
    }
    catch (const Exception & e)
    {
        std::cerr << "Error reading TSV data: " << e.message() << std::endl;
        return {};
    }

    if (total_rows == 0)
    {
        std::cerr << "TSV file contains no data rows" << std::endl;
        return {};
    }

    if (total_rows % DEFAULT_MERGE_BLOCK_SIZE != 0)
    {
        std::cerr << fmt::format(
            "Loaded row count ({}) is not a multiple of {} (DEFAULT_MERGE_BLOCK_SIZE)",
            total_rows,
            DEFAULT_MERGE_BLOCK_SIZE)
                  << std::endl;
        return {};
    }

    // Build matching block properties for each loaded block.
    std::vector<DMFileBlockOutputStream::BlockProperty> properties;
    properties.reserve(blocks.size());
    for (size_t i = 0; i < blocks.size(); ++i)
    {
        DMFileBlockOutputStream::BlockProperty prop{};
        prop.gc_hint_version = static_cast<UInt64>(i + 1);
        prop.effective_num_rows = blocks[i].rows();
        properties.push_back(prop);
    }

    return {std::move(blocks), std::move(properties), effective_size};
}

std::optional<DB::ChecksumAlgo> parseChecksumAlgo(const std::string & algo_str)
{
    if (algo_str == "xxh3")
        return DB::ChecksumAlgo::XXH3;
    else if (algo_str == "crc32")
        return DB::ChecksumAlgo::CRC32;
    else if (algo_str == "crc64")
        return DB::ChecksumAlgo::CRC64;
    else if (algo_str == "city128")
        return DB::ChecksumAlgo::City128;
    else if (algo_str == "none")
        return DB::ChecksumAlgo::None;
    else
        return std::nullopt;
}

int benchEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge IO Bench"};
    bpo::variables_map vm;
    bpo::positional_options_description positional;
    bool encryption;
    // clang-format off
    options.add_options()
        ("help", "Print help message and exit.")
        // File-backed mode parameters
        ("input", bpo::value<std::string>(), "Path to TSV data file generated by dttool generate (enables file-backed mode).")
        ("schema", bpo::value<std::string>(), "Path to schema JSON file generated by dttool generate (enables file-backed mode).")
        // Random data generation parameters for non file-backed mode (i.e. when --input / --schema is not provided).
        ("rows", bpo::value<size_t>()->default_value(131072), "Row number.")
        ("columns", bpo::value<size_t>()->default_value(100), "Column number.")
        ("sparse-ratio", bpo::value<double>()->default_value(0.0), "Sparse ratio. Null ratio for string columns.")
        ("str-len", bpo::value<size_t>()->default_value(1024), "Maximum length of generated random string values.")
        ("random", bpo::value<size_t>(), "Random seed. If not set, a random seed will be generated.")
        // Other parameters
        ("version", bpo::value<size_t>()->default_value(2), "DTFile version. [available: 1, 2, 3]")
        ("algorithm", bpo::value<std::string>()->default_value("xxh3"), "Checksum algorithm. [available: xxh3, city128, crc32, crc64, none]")
        ("frame", bpo::value<size_t>()->default_value(TIFLASH_DEFAULT_CHECKSUM_FRAME_SIZE), "Checksum frame length.")
        ("repeat", bpo::value<size_t>()->default_value(5), "Repeat times.")
        ("write-repeat", bpo::value<size_t>()->default_value(5), "Write repeat times, 0 means no write operation.")
        ("encryption", bpo::bool_switch(&encryption), "Enable encryption.")
        ("workdir", bpo::value<String>()->default_value("/tmp/test"), "Directory to create temporary data storage.")
        ("clean", bpo::bool_switch(), "Clean up the workdir after the bench is done. If false, the workdir will not be cleaned up, please clean it manually if needed.");
    ;
    // clang-format on

    try
    {
        bpo::store(
            bpo::command_line_parser(opts)
                .options(options)
                .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                .run(),
            vm);

        bpo::notify(vm);
    }
    catch (const boost::wrapexcept<boost::program_options::unknown_option> & e)
    {
        std::cerr << e.what() << std::endl;
        options.print(std::cerr);
        return -EINVAL;
    }

    if (vm.count("help") != 0)
    {
        options.print(std::cerr);
        return 0;
    }

    try
    {
        auto version = vm["version"].as<size_t>();
        auto algorithm_str = vm["algorithm"].as<std::string>();
        auto algorithm_config = parseChecksumAlgo(algorithm_str);
        if (!algorithm_config.has_value())
        {
            std::cerr << "invalid algorithm: " << algorithm_str << std::endl;
            return -EINVAL;
        }
        DB::ChecksumAlgo algorithm = algorithm_config.value();
        auto frame = vm["frame"].as<size_t>();
        auto num_rows = vm["rows"].as<size_t>();
        auto num_cols = vm["columns"].as<size_t>();
        auto sparse_ratio = vm["sparse-ratio"].as<double>();
        auto str_len = vm["str-len"].as<size_t>();
        auto repeat = vm["repeat"].as<size_t>();
        auto write_repeat = vm["write-repeat"].as<size_t>();
        size_t random_seed = 0;
        if (vm.count("random"))
        {
            random_seed = vm["random"].as<size_t>();
        }
        else
        {
            random_seed = std::random_device{}();
        }

        // File-backed mode: read data from TSV and schema JSON produced by
        // `dttool generate` instead of generating random data in memory.
        bool has_input = vm.count("input") != 0;
        bool has_schema = vm.count("schema") != 0;
        if (has_input != has_schema)
        {
            std::cerr << "Both --input and --schema must be provided together for file-backed mode"
                      << std::endl;
            return -EINVAL;
        }
        const bool file_backed_mode = has_input && has_schema;
        std::string input_path;
        std::string schema_path;
        if (file_backed_mode)
        {
            input_path = vm["input"].as<std::string>();
            schema_path = vm["schema"].as<std::string>();
        }
        auto workdir = vm["workdir"].as<std::string>() + "/.tmp";
        bool clean = vm["clean"].as<bool>();
        if (write_repeat == 0)
            clean = false;
        auto env = detail::ImitativeEnv{workdir, encryption};

        // env is up, use logger from now on
        auto logger = Logger::get();
        SCOPE_EXIT({
            // Cleanup the workdir after the bench is done
            if (clean)
            {
                if (Poco::File file(workdir); file.exists())
                {
                    file.remove(true);
                }
            }
            else
            {
                LOG_INFO(logger, "Workdir {} is not cleaned up, please clean it manually if needed", workdir);
            }
        });

        static constexpr char SUMMARY_TEMPLATE_V2[] = "version: {} "
                                                      "column: {} "
                                                      "num_rows: {} "
                                                      "str_len: {} "
                                                      "random: {} "
                                                      "encryption: {} "
                                                      "workdir: {} "
                                                      "frame: {} "
                                                      "algorithm: {} ";
        DB::DM::DMConfigurationOpt opt = std::nullopt;
        if (version == 1)
        {
            LOG_INFO(
                logger,
                SUMMARY_TEMPLATE_V2,
                version,
                num_cols,
                num_rows,
                str_len,
                random_seed,
                encryption,
                workdir,
                "none",
                "none");
            DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V2;
        }
        else
        {
            LOG_INFO(
                logger,
                SUMMARY_TEMPLATE_V2,
                version,
                num_cols,
                num_rows,
                str_len,
                random_seed,
                encryption,
                workdir,
                frame,
                algorithm_str);
            opt.emplace(std::map<std::string, std::string>{}, frame, algorithm);
            if (version == 2)
            {
                // frame checksum
                DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V3;
            }
            else if (version == 3)
            {
                // DMFileMetaV2
                DB::STORAGE_FORMAT_CURRENT = DB::STORAGE_FORMAT_V5;
            }
            else
            {
                std::cerr << "invalid dtfile version: " << version << std::endl;
                return -EINVAL;
            }
        }

        // start initialization
        size_t effective_size = 0;
        ColumnDefinesPtr defines;
        std::vector<DB::Block> blocks;
        std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty> properties;

        TableID table_id = 1;
        auto settings = DB::Settings();
        auto db_context = env.getContext();

        if (file_backed_mode)
        {
            // --- File-backed mode ---
            defines = loadSchemaFromJson(schema_path);
            if (!defines)
            {
                std::cerr << "Failed to load schema from: " << schema_path << std::endl;
                return -EINVAL;
            }

            if (write_repeat > 0)
            {
                std::tie(blocks, properties, effective_size)
                    = loadBlocksFromTSV(input_path, defines, *db_context);
                if (blocks.empty())
                {
                    std::cerr << "Failed to load TSV data from: " << input_path << std::endl;
                    return -EINVAL;
                }
            }

            // Update num_rows / num_cols from the loaded data for logging.
            num_cols = defines->size();
            num_rows = 0;
            for (const auto & b : blocks)
                num_rows += b.rows();

            LOG_INFO(
                logger,
                "File-backed mode: input={} schema={} loaded_rows={} loaded_blocks={} num_columns={} effective_size={}",
                input_path,
                schema_path,
                num_rows,
                blocks.size(),
                num_cols,
                effective_size);
        }
        else
        {
            // --- Random generation mode (existing behaviour) ---
            defines = DTTool::Bench::createColumnDefines(num_cols);
            if (write_repeat > 0)
            {
                std::tie(blocks, properties, effective_size)
                    = genBlocks(random_seed, num_rows, num_cols, str_len, sparse_ratio, logger);
            }
        }
        auto path_pool
            = std::make_shared<DB::StoragePathPool>(db_context->getPathPool().withTable("test", "t1", false));
        auto storage_pool
            = std::make_shared<DB::DM::StoragePool>(*db_context, NullspaceID, table_id, *path_pool, "test.t1");
        auto dm_settings = DB::DM::DeltaMergeStore::Settings{};
        auto dm_context = DB::DM::DMContext::createUnique(
            *db_context,
            path_pool,
            storage_pool,
            /*min_version_*/ 0,
            NullspaceID,
            table_id,
            /*pk_col_id*/ 0,
            false,
            1,
            db_context->getSettingsRef());
        DB::DM::DMFilePtr dmfile = nullptr;

        UInt64 file_id = 1;

        // Write
        if (write_repeat > 0)
        {
            size_t write_cost_ms = 0;
            LOG_INFO(logger, "start writing");
            for (size_t i = 0; i < write_repeat; ++i)
            {
                using namespace std::chrono;
                dmfile = DB::DM::DMFile::create(file_id, workdir, opt);
                auto start = high_resolution_clock::now();
                {
                    auto stream = DB::DM::DMFileBlockOutputStream(*db_context, dmfile, *defines);
                    stream.writePrefix();
                    for (size_t j = 0; j < blocks.size(); ++j)
                    {
                        stream.write(blocks[j], properties[j]);
                    }
                    stream.writeSuffix();
                }
                auto end = high_resolution_clock::now();
                auto duration = duration_cast<milliseconds>(end - start).count();
                write_cost_ms += duration;
                LOG_INFO(logger, "attempt {} finished in {} ms", i, duration);
            }
            size_t effective_size_on_disk = dmfile->getBytesOnDisk();
            LOG_INFO(
                logger,
                "average write time: {} ms",
                (static_cast<double>(write_cost_ms) / static_cast<double>(repeat)));
            LOG_INFO(
                logger,
                "write throughput by uncompressed size: {:.3f}MiB/s;"
                " write throughput by compressed size: {:.3f}MiB/s",
                (effective_size * 1'000.0 * repeat / write_cost_ms / 1024 / 1024),
                (effective_size_on_disk * 1'000.0 * repeat / write_cost_ms / 1024 / 1024));
        }

        // Read
        dmfile
            = DB::DM::DMFile::restore(db_context->getFileProvider(), file_id, 0, workdir, DMFileMeta::ReadMode::all());
        if (!dmfile)
        {
            LOG_ERROR(logger, "Failed to restore DMFile with file_id={}", file_id);
            return -ENOENT;
        }

        size_t effective_size_read = dmfile->getBytes();
        size_t effective_size_on_disk = dmfile->getBytesOnDisk();
        LOG_INFO(
            logger,
            "start reading, effective_size={}, effective_size_on_disk={}",
            effective_size_read,
            effective_size_on_disk);
        size_t read_cost_ms = 0;
        for (size_t i = 0; i < repeat; ++i)
        {
            using namespace std::chrono;

            auto start = high_resolution_clock::now();
            {
                auto builder = DB::DM::DMFileBlockInputStreamBuilder(*db_context);
                auto stream = builder.setColumnCache(std::make_shared<DB::DM::ColumnCache>())
                                  .build(
                                      dmfile,
                                      *defines,
                                      {DB::DM::RowKeyRange::newAll(false, 1)},
                                      std::make_shared<ScanContext>());
                while (true)
                {
                    auto block = stream->read();
                    if (!block)
                        break;
                    TIFLASH_NO_OPTIMIZE(block);
                }
                stream->readSuffix();
            }
            auto end = high_resolution_clock::now();
            auto duration = duration_cast<milliseconds>(end - start).count();
            read_cost_ms += duration;
            LOG_INFO(logger, "attempt {} finished in {} ms", i, duration);
        }

        LOG_INFO(logger, "average read time: {} ms", (static_cast<double>(read_cost_ms) / static_cast<double>(repeat)));
        LOG_INFO(
            logger,
            "read throughput by uncompressed bytes: {:.3f}MiB/s;"
            " read throughput by compressed bytes: {:.3f}MiB/s",
            (effective_size_read * 1'000.0 * repeat / read_cost_ms / 1024 / 1024),
            (effective_size_on_disk * 1'000.0 * repeat / read_cost_ms / 1024 / 1024));
    }
    catch (const boost::wrapexcept<boost::bad_any_cast> & e)
    {
        std::cerr << "invalid argument: " << e.what() << std::endl;
        options.print(std::cerr); // no env available here
        return -EINVAL;
    }
    catch (...)
    {
        tryLogCurrentException(Logger::get(), "DTToolBench");
    }

    return 0;
}

} // namespace DTTool::Bench
