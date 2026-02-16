---
name: tfl-generator
description: Default tool for creating Tableau Prep flows (.tfl). Use when the user asks to generate, build, or create a Tableau Prep flow, data preparation workflow, or TFL file from SQL or table schemas.
---

# TFL Generator Skill

This skill allows AI Agents to automatically generate Tableau Prep data flow files (`.tfl`) based on database schemas and business logic.

## Quick Start

```python
from cwprep import TFLBuilder, TFLPackager, load_config, DEFAULT_CONFIG

# Option 1: Use default configuration
builder = TFLBuilder(flow_name="My Flow")
conn_id = builder.add_connection_from_config()

# Option 2: Custom connection
conn_id = builder.add_connection(
    host="your-host.com",
    username="user",
    dbname="mydb",
    port="3306",       # Optional, defaults to 3306
    db_class="mysql"   # Optional, supports mysql/postgres/oracle
)

# Add inputs and processing
input1 = builder.add_input_sql("Orders", "SELECT * FROM orders", conn_id)
input2 = builder.add_input_sql("Customers", "SELECT * FROM customers", conn_id)
join = builder.add_join("Orders-Customers Join", input1, input2, "customer_id", "id")
builder.add_output_server("Output", join, "Datasource Name")

# Build and pack
flow, display, meta = builder.build()
TFLPackager.save_to_folder("workspace/output/my_flow", flow, display, meta)
TFLPackager.pack_zip("workspace/output/my_flow", "workspace/output/my_flow.tfl")
```

---

## Configuration System

### Configuration Source
`cwprep` supports loading configurations from `config.yaml` and `.env` files using `load_config()`.

### Preset Configurations

| Name | Description |
|--------|------|
| `DEFAULT_CONFIG` | Default configuration loaded from environment |

### Custom Configuration

```python
from cwprep import TFLConfig, TableauServerConfig, DatabaseConfig, TFLBuilder

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

builder = TFLBuilder(flow_name="MyFlow", config=my_config)
```

---

## SDK API Reference

### `TFLBuilder` class

| Method | Parameters | Description |
|------|------|------|
| `__init__(flow_name, config)` | flow_name, config(opt) | Initialize builder |
| `add_connection(host, username, dbname, port, db_class)` | connection info | Manually add a connection |
| `add_connection_from_config()` | None | Connect using default config |
| `add_input_sql(name, sql, conn_id)` | node_name, SQL, conn_id | Add SQL input node |
| `add_input_table(name, table, conn_id)` | node_name, table, conn_id | Add table input node |
| `add_join(name, left_id, right_id, left_col, right_col, join_type)` | join params | Add join node |
| `add_union(name, parent_ids)` | name, parent IDs | Add union node |
| `add_pivot(name, parent_id, pivot_column, aggregate_column, ...)` | pivot params | Rows to Columns |
| `add_unpivot(name, parent_id, columns_to_unpivot, ...)` | unpivot params | Columns to Rows |
| `add_clean_step(name, parent_id, actions)` | name, parent_id, actions | Add clean step (Container) |
| `add_keep_only(name, parent_id, columns)` | name, parent_id, columns | Keep specified columns only |
| `add_remove_columns(name, parent_id, columns)` | name, parent_id, columns | Remove specified columns |
| `add_rename(parent_id, renames)` | parent_id, renames map | Rename columns |
| `add_filter(name, parent_id, expression)` | name, parent_id, expr | Filter by expression |
| `add_value_filter(name, parent_id, field, values, exclude)` | filter params | Filter by specific values |
| `add_calculation(name, parent_id, column_name, formula)` | calc params | Add calculated field |
| `add_aggregate(name, parent_id, group_by, aggregations)` | agg params | Add aggregate step |
| `add_output_server(name, parent_id, datasource_name, ...)` | output params | Add server output node |
| `build()` | None | Returns (flow, display, meta) |

### `TFLPackager` class

| Method | Parameters | Description |
|------|------|------| 
| `save_to_folder(folder, flow, display, meta)` | folder, JSON objects | Save as exploded folder |
| `pack_zip(folder, output_tfl)` | folder, output path | Pack into .tfl (ZIP) |

---

## Data Cleaning Operations

### Keep Columns Only
```python
builder.add_keep_only(
    name="Keep Key Columns",
    parent_id=input_id,
    columns=["Order ID", "Customer ID", "Amount"]
)
```

### Rename Columns
```python
builder.add_rename(
    parent_id=input_id,
    renames={
        "OldName1": "NewName1",
        "OldName2": "NewName2"
    }
)
```

### Filter Data
```python
# Filter expression uses Tableau calculation syntax
builder.add_filter(
    name="Filter Valid Orders",
    parent_id=input_id,
    expression='[Amount] > 100 AND NOT ISNULL([Customer ID])'
)
```

---

## Aggregation Operations

```python
builder.add_aggregate(
    name="Summary by Company",
    parent_id=input_id,
    group_by=["Company ID", "Month"],
    aggregations=[
        {"field": "Amount", "function": "SUM", "output_name": "Total Amount"},
        {"field": "Order ID", "function": "COUNT", "output_name": "Order Count"},
        {"field": "Amount", "function": "AVG", "output_name": "Average Amount"}
    ]
)
```

**Supported Aggregate Functions**: `SUM`, `AVG`, `COUNT`, `COUNTD`, `MIN`, `MAX`, `MEDIAN`, `STDEV`, `VAR`

---

## Join Types

| Type | Description |
|------|------|
| `"left"` | Left Join (Default) |
| `"right"` | Right Join |
| `"inner"` | Inner Join |
| `"full"` | Full Outer Join |

---

## Tableau Prep Calculation Syntax

> ⚠️ **IMPORTANT**: Tableau Prep calculation syntax differs from SQL. Follow these rules:

### Unsupported Syntax
| Unsupported | Alternative |
|--------|----------|
| `IN (1, 2, 3)` | Use `OR` to chain conditions |
| `BETWEEN a AND b` | Use `>= a AND <= b` |
| `!=` | Use `<>` |

### String Comparison
- Strings must use **single quotes**: `[Field] = 'Value'`
- Incorrect: `[name] == Headquarter` ❌
- Correct: `[name] = 'Headquarter'` ✅

### Logical Expression Examples
```
# Multiple value check (Alternative to IN)
[status] = 2 OR [status] = 3 OR [status] = 4

# Exclude multiple values
NOT ([branch] = 'Main' OR [branch] = 'Sales')

# Regex match
REGEXP_MATCH(STR([status]), '^[2-8]$')
```

### Supported Function Classes
- **Numeric**: ABS, ROUND, CEILING, FLOOR, POWER, SQRT
- **String**: CONTAINS, LEFT, RIGHT, LEN, TRIM, UPPER, LOWER, SPLIT
- **Date**: DATEADD, DATEDIFF, DATEPART, YEAR, MONTH, DAY, NOW, TODAY
- **Logic**: IF/THEN/ELSE/ELSEIF/END, CASE/WHEN, IIF, IFNULL, ISNULL, ZN
- **Type Conversion**: INT, FLOAT, STR, DATE, DATETIME

---

## Build Guide

1. **Parse Schema**: Read database documentation to understand table structures and relationships.
2. **Create Flow**: Construct the flow in order: Input → Clean → Join → Aggregate → Output.
3. **Join Keys**: Ensure `left_col` and `right_col` match correctly.
4. **Verify**: Open generated files in Tableau Prep to validate.

## Best Practices

- Save outputs to `workspace/output/`.
- Use descriptive flow names (e.g., "Order_Analysis_v1").
- Layout is automatically calculated by the SDK.
