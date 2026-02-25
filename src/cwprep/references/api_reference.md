# cwprep SDK API Reference

## TFLBuilder

Core class for building Tableau Prep data flows.

### Constructor
```python
TFLBuilder(flow_name="Untitled Flow", config=None)
```

### Connection Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_connection(host, username?, dbname?, port?, db_class?, authentication?)` | Connection info | Connection ID |
| `add_connection_from_config()` | None | Connection ID |

**Supported `db_class` values**: `"mysql"` (default), `"sqlserver"`, `"postgres"`

**SQL Server authentication modes** (`authentication` parameter):
- `"sspi"` — Windows Authentication (no username needed)
- `"sqlserver"` — SQL Server username/password login

### Input Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_input_sql(name, sql, connection_id)` | Node name, SQL query, conn ID | Node ID |
| `add_input_table(name, table_name, connection_id, schema?)` | Node name, table name, conn ID, schema prefix | Node ID |

When `schema` is provided (e.g. `"dbo"`), the table reference becomes `[dbo].[table_name]`.

### Transform Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_join(name, left_id, right_id, left_col, right_col, join_type?)` | Join params | Node ID |
| `add_union(name, parent_ids)` | Name, list of parent IDs | Node ID |
| `add_filter(name, parent_id, expression)` | Filter with Tableau syntax | Node ID |
| `add_value_filter(name, parent_id, field, values, exclude?)` | Keep/exclude by values | Node ID |
| `add_calculation(name, parent_id, column_name, formula)` | Calculated field | Node ID |
| `add_aggregate(name, parent_id, group_by, aggregations?)` | GROUP BY + aggregation | Node ID |
| `add_keep_only(name, parent_id, columns)` | Keep specified columns | Node ID |
| `add_remove_columns(name, parent_id, columns)` | Remove specified columns | Node ID |
| `add_rename(parent_id, renames)` | Rename {old: new} | Node ID |
| `add_pivot(name, parent_id, pivot_column, aggregate_column, new_columns, ...)` | Rows to columns | Node ID |
| `add_unpivot(name, parent_id, columns_to_unpivot, ...)` | Columns to rows | Node ID |
| `add_quick_calc(name, parent_id, column_name, calc_type)` | Quick clean (lowercase, uppercase, trim, etc.) | Node ID |
| `add_change_type(name, parent_id, fields)` | Change column data types | Node ID |
| `add_duplicate_column(name, parent_id, source_column, new_column_name?)` | Duplicate a column | Node ID |

### Output Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_output_server(name, parent_id, datasource_name, project_name?, server_url?)` | Publish output | Node ID |

### Build
| Method | Parameters | Returns |
|--------|-----------|---------|
| `build()` | None | (flow, displaySettings, maestroMetadata) |

## TFLPackager

| Method | Parameters | Description |
|--------|-----------|-------------|
| `save_to_folder(folder, flow, display, meta)` | Path + JSON objects | Write exploded folder |
| `pack_zip(folder, output_tfl)` | Folder + output path | Pack as .tfl (ZIP) |

## Connection Examples

### MySQL (default)
```python
conn_id = builder.add_connection("localhost", "root", "my_db")
```

### SQL Server — Windows Authentication (SSPI)
```python
conn_id = builder.add_connection("localhost", db_class="sqlserver", authentication="sspi")
input_id = builder.add_input_table("orders", "orders", conn_id, schema="dbo")
```

### SQL Server — Username/Password
```python
conn_id = builder.add_connection("localhost", username="sa", db_class="sqlserver", authentication="sqlserver")
input_id = builder.add_input_table("orders", "orders", conn_id, schema="dbo")
```

## Join Types
- `"left"` — Left Join (default)
- `"right"` — Right Join
- `"inner"` — Inner Join
- `"full"` — Full Outer Join

## Aggregate Functions
SUM, AVG, COUNT, COUNTD, MIN, MAX, MEDIAN, STDEV, VAR
