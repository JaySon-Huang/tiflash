# Add `dttool generate` for TSV Performance Data

## Introduction

This document describes the requirements for adding a new `tiflash dttool generate`
subcommand. The command generates deterministic TSV data for DeltaTree and
DMFile performance tests, and writes a companion JSON file that describes the
generated columns.

## Motivation or Background

`tiflash dttool bench` already contains data generation helpers in
`dbms/src/Server/DTTool/DTToolBench.cpp`, including column definition creation
and random block generation. These helpers are currently used to write DMFiles
for IO benchmarking.

Performance tests also need a file-based input format that can be consumed by
external tools or later test flows. The new `generate` command should reuse the
existing benchmark data generator, but write plain TSV files instead of DMFiles.
The command should also emit a small schema JSON file so consumers can map each
TSV field to its logical column.

## Goals

- Add a `tiflash dttool generate` subcommand.
- Reuse the existing column and block generation logic from
  `DTToolBench.cpp`.
- Generate a TSV data file for performance tests.
- Generate a schema JSON file that describes each generated column by name and
  type.
- Keep output deterministic when a random seed is provided.
- Require the requested row count to be a multiple of `8192`
  (`DEFAULT_MERGE_BLOCK_SIZE`).

## Non-Goals

- Do not generate CSV output.
- Do not write a header row into the TSV file.
- Do not add or expose a `--with-header` option.
- Do not support arbitrary TiDB table schemas.
- Do not add a generic CSV or TSV extension to `FormatFactory`.
- Do not change existing `bench`, `migrate`, or `inspect` behavior.

## Command Requirements

The new command should follow the existing `dttool` subcommand style:

```text
tiflash dttool generate [args]
```

Required or expected options:

| Option | Description |
| --- | --- |
| `--rows` | Number of rows to generate. Must be a multiple of `8192`. |
| `--columns` | Number of user data columns to generate, using the same meaning as `dttool bench`. |
| `--sparse-ratio` | Null ratio for generated nullable string columns. |
| `--field` | String field length limit, matching the existing benchmark generator. |
| `--random` | Optional random seed. When omitted, the command may generate one. |
| `--output` | Path to the generated TSV data file. |
| `--schema` | Path to the generated schema JSON file. |
| `--help` | Print command help and exit. |

The command should reject invalid options consistently with other `dttool`
subcommands and return `-EINVAL` for invalid arguments.

## TSV Output Requirements

- The generated data file uses TSV format.
- Fields are separated by `\t`.
- Rows are terminated by `\n`.
- The TSV file must not contain a header row.
- Column order must match the generated block column order.
- Nullable string null values must be encoded as `\N`.
- The generated row count must exactly equal `--rows`.
- `--rows` must be validated before generation. If it is not divisible by
  `8192`, the command should fail without writing partial output.

The current benchmark generator creates the hidden handle, version, delete mark,
integer, and nullable string columns. `generate` should keep the same generated
data model unless a later design explicitly extends it.

## Schema JSON Requirements

The schema JSON should be intentionally small and only describe column names and
types. It should not include column IDs, row counts, random seeds, field length,
or other generation metadata.

Expected shape:

```json
{
  "columns": [
    {
      "name": "_tidb_rowid",
      "type": "Int64"
    },
    {
      "name": "_INTERNAL_VERSION",
      "type": "UInt64"
    }
  ]
}
```

The full column list should be derived from the same column definitions used to
generate the TSV data.

## Implementation Notes

- Update `dbms/src/Server/DTTool/DTTool.cpp` to include `generate` in the main
  help text and dispatch table.
- Update `dbms/src/Server/DTTool/DTTool.h` with the `DTTool::Generate`
  declaration and any shared generator declarations needed by tests.
- Add `dbms/src/Server/DTTool/DTToolGenerate.cpp` for option parsing, row count
  validation, TSV writing, and schema JSON writing.
- Reuse or lightly refactor the generation helpers in `DTToolBench.cpp` so the
  benchmark command keeps its current behavior.
- Update `dbms/src/Server/CMakeLists.txt` so `DTToolGenerate.cpp` is linked into
  `tiflash-dttool-lib`.

## Test Design

Functional tests should cover:

- `generate` appears in the top-level `dttool` help and dispatches correctly.
- A valid row count that is a multiple of `8192` produces exactly that many TSV
  rows.
- An invalid row count returns `-EINVAL` and does not leave partial output.
- The generated TSV has no header row.
- Nullable string nulls are encoded as `\N`.
- The schema JSON contains only column `name` and `type` fields.
- Fixed `--random` seed produces stable output.

Suggested validation command after implementation:

```bash
cmake --build --preset unit-tests
cmake-build-debug/dbms/gtests_dbms --gtest_filter=DTToolTest.*
```

## Impacts and Risks

- Reusing `DTToolBench.cpp` generation helpers reduces drift between benchmark
  DMFile data and file-based TSV data.
- The row-count restriction simplifies block generation because `8192` matches
  `DEFAULT_MERGE_BLOCK_SIZE`.
- TSV output is not a full general-purpose table export format. It is scoped to
  the current benchmark-generated data types.
- If the block column names and column definitions diverge, the TSV data and
  schema JSON can become inconsistent. Tests should compare the generated block
  header with the schema output.

## Unresolved Questions

None.
