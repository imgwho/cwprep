# cwprep - Tableau Prep Flow SDK

A Python SDK for programmatically generating Tableau Prep data flow (.tfl) files. Built through reverse-engineering the TFL JSON structure, enabling flow creation and modification via code without opening the GUI.

## Installation

```bash
pip install cwprep
```

## Quick Start

```python
from cwprep import TFLBuilder, TFLPackager

# Create builder
builder = TFLBuilder(flow_name="My Flow")

# Add database connection
conn_id = builder.add_connection(
    host="localhost",
    username="root",
    dbname="mydb"
)

# Add input tables
orders = builder.add_input_table("orders", "orders", conn_id)
customers = builder.add_input_table("customers", "customers", conn_id)

# Join tables
joined = builder.add_join(
    name="Orders + Customers",
    left_id=orders,
    right_id=customers,
    left_col="customer_id",
    right_col="customer_id",
    join_type="left"
)

# Add output
builder.add_output_server("Output", joined, "My_Datasource")

# Build and save
flow, display, meta = builder.build()
TFLPackager.save_to_folder("./output", flow, display, meta)
TFLPackager.pack_zip("./output", "./my_flow.tfl")
```

## Features

| Feature | Method | Description |
|---------|--------|-------------|
| Database Connection | `add_connection()` | Connect to MySQL/PostgreSQL/Oracle |
| SQL Input | `add_input_sql()` | Custom SQL query input |
| Table Input | `add_input_table()` | Direct table connection |
| Join | `add_join()` | left/right/inner/full joins |
| Union | `add_union()` | Merge multiple tables |
| Filter | `add_filter()` | Expression-based filter |
| Value Filter | `add_value_filter()` | Keep/exclude by values |
| Keep Only | `add_keep_only()` | Select columns |
| Remove Columns | `add_remove_columns()` | Drop columns |
| Rename | `add_rename()` | Rename columns |
| Calculation | `add_calculation()` | Tableau formula fields |
| Aggregate | `add_aggregate()` | GROUP BY with SUM/AVG/COUNT |
| Pivot | `add_pivot()` | Rows to columns |
| Unpivot | `add_unpivot()` | Columns to rows |
| Output | `add_output_server()` | Publish to Tableau Server |

## Examples

See the `examples/` directory for complete demos:
- `demo_basic.py` - Input, Join, Output
- `demo_cleaning.py` - Filter, Calculate, Rename
- `demo_aggregation.py` - Union, Aggregate, Pivot
- `demo_comprehensive.py` - All features combined

## Directory Structure

```
cwprep/
├── src/cwprep/          # SDK source code
│   ├── builder.py       # TFLBuilder class
│   ├── packager.py      # TFLPackager class
│   └── config.py        # Configuration utilities
├── examples/            # Demo scripts
│   └── demo_data/       # Sample database SQL
├── docs/                # Documentation
└── tests/               # Unit tests
```

## Configuration

Create `config.yaml` for default settings:

```yaml
database:
  host: localhost
  username: root
  dbname: mydb
  port: "3306"
  db_class: mysql

tableau_server:
  url: http://your-server
  project_name: Default
```

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history.

## License

MIT License
