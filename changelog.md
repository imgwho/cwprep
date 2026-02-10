# Project Changelog

### Current Status
SDK v0.1.2 - Added Excel-to-Database loader, improved database configuration robustness.

---

---

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

