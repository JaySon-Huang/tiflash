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

#include <Common/FieldVisitors.h>
#include <Common/Logger.h>
#include <Core/Block.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#pragma clang diagnostic pop
#include <Server/DTTool/DTTool.h>
#include <Storages/DeltaMerge/DeltaMergeDefines.h>
#include <Storages/DeltaMerge/File/DMFileBlockOutputStream.h>
#include <boost_wrapper/program_options.h>

#include <boost/program_options/errors.hpp>
#include <fstream>
#include <iostream>
#include <random>

namespace bpo = boost::program_options;

// Forward-declare generation helpers from DTToolBench.cpp so we can reuse the
// same column definitions and data generation logic that `dttool bench` uses.
namespace DTTool::Bench
{
using namespace DB::DM;
using namespace DB;

ColumnDefinesPtr createColumnDefines(size_t column_number);

std::tuple<std::vector<DB::Block>, std::vector<DB::DM::DMFileBlockOutputStream::BlockProperty>, size_t>
genBlocks(
    size_t random,
    size_t num_rows,
    size_t num_column,
    size_t str_len,
    double sparse_ratio,
    const LoggerPtr & logger);
} // namespace DTTool::Bench

namespace DTTool::Generate
{
using namespace DB;

// clang-format off
static constexpr char GENERATE_HELP[] =
    "Usage: generate [args]\n"
    "Available Arguments:\n"
    "  --help          Print help message and exit.\n"
    "  --rows          Number of rows to generate. Must be a multiple of 8192.\n"
    "  --columns       Number of user data columns to generate.\n"
    "  --sparse-ratio  Null ratio for generated nullable string columns. [default: 0.0]\n"
    "  --str-len       Maximum length of generated random string values. [default: 1024]\n"
    "  --random        Random seed. If not set, a random seed will be generated.\n"
    "  --output        Path to the generated TSV data file.\n"
    "  --schema        Path to the generated schema JSON file.";
// clang-format on

namespace
{
void writeSchemaJson(const DB::DM::ColumnDefinesPtr & defines, const std::string & schema_path)
{
    Poco::JSON::Array::Ptr columns = new Poco::JSON::Array();
    for (const auto & cd : *defines)
    {
        Poco::JSON::Object::Ptr col = new Poco::JSON::Object();
        col->set("name", cd.name);
        col->set("type", cd.type->getName());
        columns->add(col);
    }

    Poco::JSON::Object::Ptr root = new Poco::JSON::Object();
    root->set("columns", columns);

    std::ofstream schema_file(schema_path);
    if (!schema_file.is_open())
    {
        std::cerr << "Failed to open schema file: " << schema_path << std::endl;
        return;
    }
    root->stringify(schema_file, /*indent=*/2);
}

/// Write a single cell value to the TSV output stream.
/// Null values are encoded as \N.
void writeTsvCell(std::ostream & out, const DB::ColumnPtr & column, size_t row)
{
    // For nullable columns, check the null bitmap first.
    if (column->isColumnNullable())
    {
        if (column->isNullAt(row))
        {
            out << "\\N";
            return;
        }
    }

    // Convert the cell to a Field and serialize it as text.
    auto field = (*column)[row];
    out << applyVisitor(FieldVisitorToString(), field);
}
} // namespace

int generateEntry(const std::vector<std::string> & opts)
{
    bpo::options_description options{"Delta Merge Generate"};
    bpo::variables_map vm;

    // clang-format off
    options.add_options()
        ("help", "Print help message and exit.")
        ("rows", bpo::value<size_t>(), "Number of rows to generate. Must be a multiple of 8192.")
        ("columns", bpo::value<size_t>(), "Number of user data columns to generate.")
        ("sparse-ratio", bpo::value<double>()->default_value(0.0), "Null ratio for generated nullable string columns.")
        ("str-len", bpo::value<size_t>()->default_value(1024), "Maximum length of generated random string values.")
        ("random", bpo::value<size_t>(), "Random seed. If not set, a random seed will be generated.")
        ("output", bpo::value<std::string>(), "Path to the generated TSV data file.")
        ("schema", bpo::value<std::string>(), "Path to the generated schema JSON file.");
    // clang-format on

    try
    {
        bpo::store(
            bpo::command_line_parser(opts)
                .options(options)
                .style(bpo::command_line_style::unix_style | bpo::command_line_style::allow_long_disguise)
                .run(),
            vm);

        if (vm.count("help") != 0)
        {
            std::cout << GENERATE_HELP << std::endl;
            return 0;
        }

        bpo::notify(vm);

        // Validate required options
        bool has_error = false;
        if (vm.count("rows") == 0)
        {
            std::cerr << "missing required option: --rows" << std::endl;
            has_error = true;
        }
        if (vm.count("columns") == 0)
        {
            std::cerr << "missing required option: --columns" << std::endl;
            has_error = true;
        }
        if (vm.count("output") == 0)
        {
            std::cerr << "missing required option: --output" << std::endl;
            has_error = true;
        }
        if (vm.count("schema") == 0)
        {
            std::cerr << "missing required option: --schema" << std::endl;
            has_error = true;
        }
        if (has_error)
        {
            std::cerr << GENERATE_HELP << std::endl;
            return -EINVAL;
        }

        auto num_rows = vm["rows"].as<size_t>();
        auto num_cols = vm["columns"].as<size_t>();
        auto sparse_ratio = vm["sparse-ratio"].as<double>();
        auto str_len = vm["str-len"].as<size_t>();
        auto output_path = vm["output"].as<std::string>();
        auto schema_path = vm["schema"].as<std::string>();

        size_t random_seed;
        if (vm.count("random") != 0)
        {
            random_seed = vm["random"].as<size_t>();
        }
        else
        {
            random_seed = std::random_device{}();
        }

        // Validate that --rows is a multiple of DEFAULT_MERGE_BLOCK_SIZE (8192).
        // This check must happen before any output is written.
        if (num_rows % DEFAULT_MERGE_BLOCK_SIZE != 0)
        {
            std::cerr << fmt::format(
                "rows must be a multiple of {} (DEFAULT_MERGE_BLOCK_SIZE), got {}",
                DEFAULT_MERGE_BLOCK_SIZE,
                num_rows)
                      << std::endl;
            return -EINVAL;
        }

        // Generate column definitions (used for both block generation and schema JSON).
        auto defines = DTTool::Bench::createColumnDefines(num_cols);

        // Write schema JSON first — if this fails, we haven't written TSV data yet.
        writeSchemaJson(defines, schema_path);

        // Generate blocks using the same logic as `dttool bench`.
        auto logger = Logger::get("DTToolGenerate");
        LOG_INFO(
            logger,
            "Generating TSV data: rows={} columns={} sparse_ratio={} str_len={} random_seed={} output={} schema={}",
            num_rows,
            num_cols,
            sparse_ratio,
            str_len,
            random_seed,
            output_path,
            schema_path);

        auto [blocks, properties, effective_size] = DTTool::Bench::genBlocks(
            random_seed,
            num_rows,
            num_cols,
            str_len,
            sparse_ratio,
            logger);

        // Write TSV data.
        {
            std::ofstream tsv_file(output_path);
            if (!tsv_file.is_open())
            {
                std::cerr << "Failed to open output file: " << output_path << std::endl;
                return -EINVAL;
            }

            size_t rows_written = 0;
            for (const auto & block : blocks)
            {
                size_t rows_in_block = block.rows();
                for (size_t row = 0; row < rows_in_block; ++row)
                {
                    for (size_t col = 0; col < block.columns(); ++col)
                    {
                        if (col > 0)
                            tsv_file << '\t';
                        writeTsvCell(tsv_file, block.getByPosition(col).column, row);
                    }
                    tsv_file << '\n';
                    ++rows_written;
                }
            }

            if (!tsv_file)
            {
                std::cerr << "Error writing TSV output to: " << output_path << std::endl;
                return -EINVAL;
            }

            LOG_INFO(logger, "TSV data written: {} rows, effective_size={}", rows_written, effective_size);
        }

        LOG_INFO(logger, "Generation complete");
    }
    catch (const boost::wrapexcept<boost::bad_any_cast> & e)
    {
        std::cerr << "invalid argument: " << e.what() << std::endl;
        std::cerr << GENERATE_HELP << std::endl;
        return -EINVAL;
    }
    catch (const boost::wrapexcept<boost::program_options::unknown_option> & e)
    {
        std::cerr << e.what() << std::endl;
        std::cerr << GENERATE_HELP << std::endl;
        return -EINVAL;
    }
    catch (...)
    {
        tryLogCurrentException(Logger::get("DTToolGenerate"), "DTToolGenerate");
        return -EINVAL;
    }

    return 0;
}

} // namespace DTTool::Generate
