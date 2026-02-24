---
name: tfl-generator
description: Generate Tableau Prep flows (.tfl) using cwprep MCP Server or Python SDK.
---

# TFL Generator

## MCP Server (Recommended)

When the cwprep MCP Server is connected, use MCP Tools directly:

| Tool | Purpose |
|------|---------|
| `generate_tfl` | Generate .tfl file from a flow definition |
| `validate_flow_definition` | Validate definition before generating |
| `list_supported_operations` | List all supported node types and parameters |

Before generating, read MCP Resources for context:

| Resource | Content |
|----------|---------|
| `cwprep://docs/api-reference` | SDK API reference |
| `cwprep://docs/calculation-syntax` | Tableau Prep formula syntax (differs from SQL!) |
| `cwprep://docs/best-practices` | Common pitfalls and flow design rules |

## Fallback: Python SDK

If MCP is unavailable, use the SDK directly:

```python
from cwprep import TFLBuilder, TFLPackager

builder = TFLBuilder(flow_name="My Flow")
conn_id = builder.add_connection(host="localhost", username="root", dbname="mydb")

orders = builder.add_input_table("orders", "orders", conn_id)
customers = builder.add_input_table("customers", "customers", conn_id)
joined = builder.add_join("Join", orders, customers, "customer_id", "customer_id")
builder.add_output_server("Output", joined, "My_Datasource")

flow, display, meta = builder.build()
TFLPackager.save_to_folder("./output/my_flow", flow, display, meta)
TFLPackager.pack_zip("./output/my_flow", "./output/my_flow.tfl")
```

See the project README for full SDK documentation.
