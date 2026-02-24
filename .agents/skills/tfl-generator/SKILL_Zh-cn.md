---
name: tfl-generator
description: 使用 cwprep MCP Server 或 Python SDK 生成 Tableau Prep 流程文件 (.tfl)。
---

# TFL Generator

## MCP Server（推荐）

cwprep MCP Server 连接后，直接使用 MCP Tools：

| Tool | 用途 |
|------|------|
| `generate_tfl` | 从 flow 定义生成 .tfl 文件 |
| `validate_flow_definition` | 生成前验证定义 |
| `list_supported_operations` | 查看所有支持的节点类型和参数 |

生成前请阅读 MCP Resources：

| Resource | 内容 |
|----------|------|
| `cwprep://docs/api-reference` | SDK API 参考 |
| `cwprep://docs/calculation-syntax` | Tableau Prep 计算语法（与 SQL 不同！） |
| `cwprep://docs/best-practices` | 常见错误与流程设计规则 |

## 回退：Python SDK

MCP 不可用时，直接调用 SDK：

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

详细 API 文档见项目 README。