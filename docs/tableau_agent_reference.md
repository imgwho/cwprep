# Tableau Agent Reference & Project Comparison

> Based on [Tableau Prep Einstein Official Docs](https://help.tableau.com/current/prep/en-us/prep_einstein.htm)
> 
> Last updated: 2026-02-09 14:20

---

## Tableau Agent vs cwprep Feature Comparison

### Tableau Agent Supported Operations

| Tableau Agent Feature | cwprep Method | Status |
|----------------------|---------------|--------|
| **Inspect Data** | | |
| Filter data | `add_filter()` | ✅ Implemented |
| Filter null values | `add_filter("ISNULL([field])")` | ✅ Implemented |
| Filter by date range | `add_filter("[date] >= ...")` | ✅ Implemented |
| Filter by relative date | `add_filter()` + expression | ✅ Implemented |
| Remove fields | `add_remove_columns()` | ✅ Implemented |
| Change data type | - | 🔲 Planned |
| **Clean and Shape** | | |
| Set case (upper/lower/title) | - | 🔲 Planned |
| Remove characters | - | 🔲 Planned |
| Trim whitespace | - | 🔲 Planned |
| Create calculation | `add_calculation()` | ✅ Implemented |
| Rename field | `add_rename()` | ✅ Implemented |
| Convert date format | `add_calculation()` | ✅ Implemented |
| Split values | - | 🔲 Planned |
| Identify duplicate rows | - | 🔲 Planned |
| Fill gaps in sequence | - | 🔲 Planned |
| **Pivot Data** | | |
| Columns to rows | `add_unpivot()` | ✅ Implemented |
| Rows to columns | `add_pivot()` | ✅ Implemented |
| **Aggregate Data** | | |
| Create aggregate step | `add_aggregate()` | ✅ Implemented |
| Group and aggregate | `add_aggregate()` | ✅ Implemented |

**Coverage**: 12/18 (67%)

---

### cwprep Exclusive Features (Not in Tableau Agent)

| Feature | cwprep Method | Description |
|---------|---------------|-------------|
| Select data source | `add_input_sql()` | ✅ Read from database |
| Direct table input | `add_input_table()` | ✅ Connect to table directly |
| Join operation | `add_join()` | ✅ left/right/inner/full |
| Union operation | `add_union()` | ✅ Merge multiple tables |
| Output step | `add_output_server()` | ✅ Publish to Server |
| Flow branching | Multiple nextNodes | ✅ Non-linear flows |
| Value filter | `add_value_filter()` | ✅ Keep/exclude values |
| Keep only columns | `add_keep_only()` | ✅ Select fields |

---

### Planned Features 🔲

| Feature | Priority | Notes |
|---------|----------|-------|
| Change data type | 🟡 Medium | ChangeDataType node |
| Quick clean operations | 🟢 Low | Case, trim, etc. |
| Split values | 🟢 Low | SplitValues node |
| Identify duplicates | 🟢 Low | Deduplication |
| File input/output | 🟢 Low | CSV/Excel/Hyper |

---

## Project Differentiation

| Comparison | Tableau Agent | cwprep |
|------------|---------------|--------|
| Join/Union | ❌ Not supported | ✅ Supported |
| Pivot/Unpivot | ✅ Supported | ✅ Supported |
| Flow branching | ❌ Linear only | ✅ Supported |
| Data source selection | ❌ Not supported | ✅ Supported |
| Output step | ❌ Not supported | ✅ Supported |
| Offline usage | ❌ Requires connection | ✅ Fully local |
| Automation | ❌ Interactive | ✅ CI/CD integration |
| Version control | ❌ Not supported | ✅ Git-friendly |
| Cost | Requires Tableau+ | Open source |

---

## References

- [Tableau Prep Einstein Docs](https://help.tableau.com/current/prep/en-us/prep_einstein.htm)
- [Tableau Agent Operations](https://help.tableau.com/current/prep/en-us/prep_einstein.htm#Tableau)
