# Project Changelog

### Current Status
SDK v0.5.1 - Column tracking optimization with input schema awareness and hidden column support.

---

## v0.5.1 (2026-02-26)

### Optimized
- **Column Tracking (`ColumnTracker`)**: Track explicit column schemas across the DAG, enabling precise `SELECT` column lists instead of `SELECT *`
- **Input Node Schema Awareness**: Extract `fields` array from input nodes to establish KNOWN column state from the source
- **Hidden Column Support**: Parse `hiddenColumns` from `displaySettings` to exclude hidden fields from intermediate CTEs
- **`translate_tfl_file` Enhancement**: Now reads `displaySettings` for hidden column optimization

---

## v0.5.0 (2026-02-26)

### Added
- **SQL Translator** `SQLTranslator`: Translate TFL flow definitions to equivalent ANSI SQL (CTE format) with flow summary header and step-by-step comments.
  - `translate_flow()`: From flow JSON dict (builder output)
  - `translate_tfl_file()`: From `.tfl` ZIP file
  - Configurable comments (`include_comments`, `include_summary`)
- **Expression Translator** `ExpressionTranslator`: Regex-based Tableau Prep formula → ANSI SQL expression translator. Covers ~30 function mappings including:
  - Logic: `IF/THEN/ELSE/END` → `CASE WHEN`, `IIF` → `CASE WHEN`, `ISNULL` → `IS NULL`, `IFNULL` → `COALESCE`, `ZN` → `COALESCE(x, 0)`
  - String: `CONTAINS` → `LIKE`, `PROPER` → `INITCAP`, `LEN` → `LENGTH`, `MID` → `SUBSTRING`, `FIND` → `POSITION`
  - Date: `DATEPART` → `EXTRACT`, `YEAR/MONTH/DAY` → `EXTRACT`, `NOW()` → `CURRENT_TIMESTAMP`
  - Type: `INT/FLOAT/STR/DATE` → `CAST(... AS ...)`
  - Aggregate: `COUNTD` → `COUNT(DISTINCT ...)`
  - Unsupported functions (REGEXP_*, SPLIT, analytics) marked with `/* UNSUPPORTED */` comments
- **MCP Tool** `translate_to_sql`: Single tool supporting both declarative definition and `.tfl` file input.
- **Tests**: 40 new test cases (27 expression + 10 SQL + 3 MCP integration).

### Fixed
- **test_mcp_server.py**: Fixed `_build_flow` return value unpacking (4 → 5 tuple), added missing file input types to `test_all_types_present`.
- **test_builder.py**: Updated version assertion.

---

## v0.4.1 (2026-02-26)

### Added
- **Excel File Connection** `add_file_connection()`: Auto-detects `.xlsx`/`.xls` → `excel-direct`, `.csv` → `textscan`. Handles path/directory attributes for TFL vs basename-only for TFLX.
- **Excel Input** `add_input_excel()`: Read from Excel worksheets (`.v1.LoadExcel` node type).
- **CSV Input** `add_input_csv()`: Read from CSV files (`.v1.LoadCsv` node type) with configurable separator, locale, charset.
- **CSV Union** `add_input_csv_union()`: Merge multiple CSV files (`.v1.LoadCsvInputUnion` node type).
- **TFLX Packaging**: `build(is_packaged=True)` + `TFLPackager.pack_tflx()` — generate `.tflx` files with embedded data in `Data/{connection_id}/` structure.
- **Multi-column Join**: `add_join()` now accepts `List[str]` for `left_col`/`right_col` to generate multiple join conditions.
- **MCP Server**: Full integration — `generate_tfl` supports `.tflx` output with `data_files` parameter; file connection types in `_NODE_TYPES`, `list_supported_operations`, and `validate_flow_definition`.
- **Tests**: 10 new test cases (file connections, CSV nodes, multi-column join, packaged/non-packaged paths).

### Fixed
- **Excel connection** no longer includes invalid `cleaning`/`interpretationMode` attributes.
- **CSV input node** `connectionAttributes` changed to empty `{}` (matches Tableau Prep actual format).
- **TFLX packaging** correctly stores basename-only filenames in packaged connections.

---

## v0.4.0 (2026-02-25)

### Added
- **Multi-database Connection Support**: Introduced `_DB_PROFILES` architecture in `builder.py` — extensible dictionary-based connection attribute profiles for each database type.
- **SQL Server Connection**: Full support for SQL Server 2022 with two authentication modes:
  - `authentication="sspi"` — Windows Authentication (no username needed)
  - `authentication="sqlserver"` — Username/password login
- **PostgreSQL Connection Profile**: Pre-configured profile with port 5432 defaults.
- **Schema Support**: `add_input_table(schema="dbo")` generates `[dbo].[table_name]` format for SQL Server.
- **New Config Fields**: `DatabaseConfig` now supports `authentication` and `schema` fields.
- **Database Loaders**: Unified `load_superstore_sqlserver.py`, `load_superstore_postgresql.py`, and MySQL loader into a single `load_superstore.py` with `--db` argument.
- **Tests**: 9 new test cases for SQL Server connection (builder + MCP server).

### Changed
- **`add_connection()` API**: `username` and `dbname` are now optional (default `""`), enabling SQL Server SSPI connections without credentials.
- **MCP Validation**: `validate_flow_definition` dynamically validates required fields based on `db_class` and `authentication` mode.
- **Config Defaults**: `DatabaseConfig.port` default changed from `"3306"` to `""` (determined by profile).

---

## v0.3.1 (2026-02-24)

### Improved
- **MCP Resources**: Extracted hardcoded `_API_REFERENCE` and `_CALC_SYNTAX` strings to external `.md` files under `src/cwprep/references/` for easier maintenance and extensibility.
- **New Resource**: Added `cwprep://docs/best-practices` — common Tableau Prep vs SQL syntax pitfalls and flow design rules.
- **Enhanced Instructions**: `FastMCP` instructions now list all available resources and recommended workflow.
- **Enhanced Prompt**: `design_data_flow` prompt guides AI to read resources before designing flows.

### Changed
- **Simplified AI Skills**: Reduced `.agents/skills/tfl-generator/SKILL.md` from 226 to 42 lines. Skills now serve as an MCP index with SDK fallback, instead of duplicating API and syntax documentation.
- **Removed duplicate references**: Deleted `references/api.md` and `references/calculations.md` from Skills (content available via MCP Resources).

---

## v0.3.0 (2026-02-20)

### Added
- **Quick Calc** `add_quick_calc()`: 8 quick clean operations — `lowercase`, `uppercase`, `titlecase`, `trim_spaces`, `remove_extra_spaces`, `remove_all_spaces`, `remove_letters`, `remove_punctuation`. Uses `.v2024_2_0.QuickCalcColumn` node type.
- **Change Column Type** `add_change_type()`: Change column data types (string/integer/real/date/datetime/boolean). Uses `.v1.ChangeColumnType` node type.
- **Duplicate Column** `add_duplicate_column()`: Copy an existing column with configurable new name. Uses `.v2019_2_3.DuplicateColumn` node type.
- **MCP Server**: All 3 new operations exposed via MCP (`quick_calc`, `change_type`, `duplicate_column` node types).
- **Tests**: 7 new test cases for builder and MCP server.
- **Examples**: Added `demo_field_operations.py` demonstrating `add_quick_calc`, `add_change_type`, and `add_duplicate_column`.
- **MCP Prompts**: Added `examples/prompts.md` with 8 business scenario prompt templates for AI-driven TFL generation.

---

## v0.2.2 (2026-02-17)

### Documentation
- **MCP Configuration**: Updated README to recommend `python -m cwprep.mcp_server` for Claude Desktop config (fixes PATH issues on Windows).

## v0.2.1 (2026-02-17)

### Fixed
- **Dependency Conflict**: Pinned `cffi<2.0.0` to resolve incompatibility with `tableauhyperapi` when installing `cwprep[mcp]`.

## v0.2.0 (2026-02-17)

### Added
- **MCP Server Support**: Integrated Model Context Protocol (MCP) server for AI clients (Claude Desktop, Cursor, Gemini CLI).
  - **Tools**: `generate_tfl`, `list_supported_operations`, `validate_flow_definition`.
  - **Resources**: API reference and calculation syntax documentation accessible via MCP.
  - **Prompts**: `design_data_flow` and `explain_tfl_structure` for interactive assistance.
  - **Transports**: Supports both `stdio` (local) and `streamable-http` (remote) transports.
- **CLI Command**: Added `cwprep-mcp` entry point.
- **Optional Dependency**: Added `mcp` extra (install via `pip install cwprep[mcp]`).

---

---

## v0.1.3 (2026-02-16 10:00)

### Refactored
- **TFL Generator Skill Optimized**: Refined `SKILL.md` with structured agent-centric workflow and YAML metadata.
- **Reference Extraction**: Moved detailed API and Calculation syntax to separate files under `references/` to reduce context bloat.
- **Language Update**: Translated techncial reference files (`api.md`, `calculations.md`) to English for better AI comprehension.

### Changed
- **Directory Migration**: Moved AI skills from root `skills/` to standard `.agents/skills/` directory.
- **Import Paths**: Fixed outdated code examples to use correct `cwprep` package imports instead of `core`.

## v0.1.2 (2026-02-10 09:15)

### Added
- **Excel-to-Database Loader** `load_superstore.py`: Automates importing `Sample - Superstore.xls` into a 5-table normalized schema.
- Support for `pymysql` driver as a more stable alternative for SQLAlchemy connections.

### Improved
- **Robust Database Connection**: Handling for empty passwords and user-specific local environments.
- **Flexible Column Mapping**: Case-insensitive and alias-aware column identification for Excel imports.

## v0.1.1 (2026-02-09 14:20)

### Added
- `add_input_table()`: Direct table connection without custom SQL
- Proper `dbname` resolution from connection object

### Fixed
- **Value Filter quotes**: `add_value_filter()` now uses `.v1.FilterOperation` format with single quotes for strings (e.g., `'Same Day'`)
- **Connection dbname bug**: Input nodes no longer use hardcoded `voxadmin`, correctly reads from `add_connection()`

### Changed
- All example scripts converted from Chinese to English
- `add_rename()` internal node name: "重命名" → "rename"

---

## v1.5 (2026-02-09 10:24)

### Added
- **Comprehensive test script** `test_comprehensive.py`: Covers all 14 SDK methods
- **Tableau Prep calculation syntax rules**: Added to SKILL.md

### Fixed
- Fixed employee table SQL field names (username/filialename/filialeid/job_status)
- Fixed filter expression syntax (added quotes for strings, changed IN to OR)

### Documentation
- Added `docs/tableau_prep_calculation.md`: Complete function reference

---

## v1.4 (2026-02-08 16:10)

### Added
- **Union** `add_union`: Merges multiple data sources with the same structure
- **Pivot** `add_pivot`: Expands dimension fields into columns
- **Unpivot** `add_unpivot`: Converts multiple columns into rows

### Testing
- Added `test_union_pivot.py`: Verifies union and pivot/unpivot functionality

---

## v1.3 (2026-02-08 12:05)

### Added
- **Calculated Field** `add_calculation`: Supports Tableau formulas (IF/THEN/ELSE/ELSEIF)
- **Remove Columns** `add_remove_columns`: Batch removal of unwanted fields
- **Value Filter** `add_value_filter`: Keeps or excludes records based on values

### Fixed
- Corrected calculated field node type: `.v1.CreateCalculatedColumn` → `.v1.AddColumn`

### Testing
- Added `test_calculation.py`: Verifies calculated field functionality
- Added `test_new_features_v2.py`: Verifies column removal and value filtering

---

## v1.2 (2026-02-08 11:37)

### Added
- **Clean Step Container** `add_clean_step`: Supports combining multiple cleaning operations in a container
- **Keep Only Columns** `add_keep_only`: Quickly filters required fields
- **Rename Columns** `add_rename`: Batch renaming of fields
- **Filter** `add_filter`: Data filtering using Tableau calculation expressions
- **Aggregate Step** `add_aggregate`: GROUP BY + aggregation functions (SUM/AVG/COUNT, etc.)

### Documentation Updates
- Updated `skills/tfl-generator/SKILL.md`: Added new API reference and usage examples
- Added `docs/tableau_agent_reference.md`: Tableau Agent official feature reference and project plan comparison

### Testing
- Added `test_new_features.py`: Verifies .tfl files generated with new features

---

## v1.1 (2026-02-08)

### Added
- **YAML Configuration System**: Supports configuring Tableau Server and database connections via `config.yaml`
- **Environment Variable Support**: Sensitive information (passwords) configured via `.env` file, automatically excluded by gitignore
- **Config Loading Function**: `load_config()` automatically merges YAML and environment variables
- **Connection Helper Method**: `add_connection_from_config()` uses default configuration directly

### Improved
- `TFLBuilder` supports `config` parameter injection; all hardcoded values removed
- `add_connection()` supports custom `port` and `db_class` parameters
- `add_output_server()` supports custom `server_url` parameters
- Added type hints to improve code readability

### New Files
- `config.yaml` - Non-sensitive configuration
- `.env` / `.env.example` - Environment variable configuration
- `requirements.txt` - Project dependencies
- `.gitignore` - Git ignore rules

---

### Modification History

#### 1. Structural Analysis & Initial Modifications
- Analyzed that .tfl files are essentially ZIP packages containing three core files: flow, displaySettings, and maestroMetadata.
- Wrote scripts to modify existing flows: removed geographic info table joins and reconnected data paths.
- Wrote initial TFL file JSON specification documentation.

#### 2. SDK Core Framework Construction
- Created project directory structure: `core` (engine), `skills` (AI skill), `docs` (documentation), `workspace` (workspace).
- Wrote `core/builder.py`: Responsible for generating flow nodes, calculating coordinates, and managing UUIDs.
- Wrote `core/packager.py`: Responsible for packaging the generated JSON folder into a standard .tfl format.

#### 3. AI Skill Integration
- Created the AI skill definition file `SKILL.md` under `skills/tfl-generator` following Gemini CLI specifications.
- Defined instructions for how the AI should call the SDK to generate flows based on business logic.

#### 4. Fixing Corrupt Files & Compatibility Issues
- Fixed the "File is corrupt" error when opening generated files in Tableau Prep.
- Modified `maestroMetadata`: Set error messages to an empty list `[]` instead of deleting them, and completed the four-digit version info.
- Modified `displaySettings`: Completed version numbers and layout metadata, and removed incomplete field sort mappings to allow the software to auto-refresh fields.
- Updated `builder.py`: Ensured the generated JSON structure perfectly aligns with manually created flows.

#### 5. Documentation Organization
- Generated `TFL_Engineering_Log.md`: Summarized technical breakthroughs and tips for avoiding pitfalls during development.
- Organized database schema documentation to help the AI understand business logic.

#### 6. SDK 1.0 Release & Project Engineering
- Completed refactoring of the `TFLBuilder` class, supporting automatic coordinate calculation, UUID management, and PrimaryKey attribute registration.
- Implemented the `TFLPackager` class, standardizing the packaging and unpackaging process for .tfl files.
- Resolved deep compatibility issues with `displaySettings` and `maestroMetadata`, ensuring generated flows can be correctly read by the software.
- Established the `tfl-generator` skill according to Gemini CLI specifications, achieving a fully automated path from business description to .tfl file.

