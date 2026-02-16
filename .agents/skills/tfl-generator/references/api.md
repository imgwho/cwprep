# TFL Generator SDK API Reference

## `TFLBuilder` Class

The core class for creating and managing flow nodes.

| Method | Parameters | Description |
|------|------|------|
| `__init__(flow_name, config)` | flow_name, config (optional) | Initialize the builder |
| `add_connection(host, username, dbname, port, db_class)` | connection info | Manually add a connection |
| `add_connection_from_config()` | None | Add connection using default config |
| `add_input_sql(name, sql, conn_id)` | node_name, SQL, connection_id | Add a SQL input node |
| `add_join(name, left_id, right_id, left_col, right_col, join_type)` | join parameters | Add a join node (type: "left", "right", "inner", "full") |
| `add_union(name, parent_ids)` | name, parent_id list | Add a union node |
| `add_pivot(name, parent_id, pivot_column, aggregate_column, ...)` | pivot parameters | Rows to columns |
| `add_unpivot(name, parent_id, columns_to_unpivot, ...)` | unpivot parameters | Columns to rows |
| `add_clean_step(name, parent_id, actions)` | name, parent_id, action list | Add a cleaning step |
| `add_keep_only(name, parent_id, columns)` | name, parent_id, column list | Keep only specific columns |
| `add_remove_columns(name, parent_id, columns)` | name, parent_id, column list | Remove specific columns |
| `add_rename(parent_id, renames)` | parent_id, rename mapping | Rename columns |
| `add_filter(name, parent_id, expression)` | name, parent_id, expression | Filter using Tableau Prep calculation syntax |
| `add_value_filter(name, parent_id, field, values, exclude)` | filter parameters | Filter by specific values |
| `add_calculation(name, parent_id, column_name, formula)` | calculation parameters | Add a calculated field |
| `add_aggregate(name, parent_id, group_by, aggregations)` | aggregate parameters | Add an aggregation step |
| `add_output_server(name, parent_id, datasource_name, project_name, server_url)` | output parameters | Add a Tableau Server output |
| `build()` | None | Returns (flow, display, meta) |

### Aggregation Functions
Supported functions: `SUM`, `AVG`, `COUNT`, `COUNTD`, `MIN`, `MAX`, `MEDIAN`, `STDEV`, `VAR`.

---

## `TFLPackager` Class

Used to package build objects into `.tfl` files.

| Method | Parameters | Description |
|------|------|------| 
| `save_to_folder(folder, flow, display, meta)` | folder_path, JSON objects | Save as a folder structure |
| `pack_zip(folder, output_tfl)` | folder_path, output_tfl_path | Zip the folder into a .tfl file |

---

## Configuration System

Configuration file is located at `src/cwprep/config.py`.

### Default Configurations
- `DEFAULT_CONFIG`: Internal corporate environment (default)
- `LOCAL_CONFIG`: Local testing environment

### Custom Configuration Example

```python
from cwprep.config import TFLConfig, TableauServerConfig, DatabaseConfig

my_config = TFLConfig(
    server=TableauServerConfig(
        server_url="https://my-tableau-server.com",
        default_project="My Project",
        project_luid="..."
    ),
    database=DatabaseConfig(
        host="my-database.com",
        username="user",
        dbname="mydb",
        port="3306",
        db_class="mysql"
    )
)

builder = TFLBuilder(flow_name="Flow Name", config=my_config)
```
