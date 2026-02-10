# TFL Generator Skill

此技能允许 AI Agent 根据数据库表结构和业务逻辑，自动生成 Tableau Prep 数据流程文件 (`.tfl`)。

## 快速开始

```python
from core.builder import TFLBuilder
from core.packager import TFLPackager
from core.config import DEFAULT_CONFIG

# 方式1: 使用默认配置
builder = TFLBuilder(flow_name="我的流程")
conn_id = builder.add_connection_from_config()

# 方式2: 自定义连接
conn_id = builder.add_connection(
    host="your-host.com",
    username="user",
    dbname="mydb",
    port="3306",       # 可选，默认 3306
    db_class="mysql"   # 可选，支持 mysql/postgres/oracle
)

# 添加输入和处理
input1 = builder.add_input_sql("订单表", "SELECT * FROM orders", conn_id)
input2 = builder.add_input_sql("客户表", "SELECT * FROM customers", conn_id)
join = builder.add_join("订单-客户联接", input1, input2, "customer_id", "id")
builder.add_output_server("输出", join, "数据源名称")

# 构建并打包
flow, display, meta = builder.build()
TFLPackager.save_to_folder("workspace/output/my_flow", flow, display, meta)
TFLPackager.pack_zip("workspace/output/my_flow", "workspace/output/my_flow.tfl")
```

---

## 配置系统

### 配置文件位置
`core/config.py`

### 预设配置

| 配置名 | 说明 |
|--------|------|
| `DEFAULT_CONFIG` | 公司内部环境（默认） |
| `LOCAL_CONFIG` | 本地测试环境 |

### 自定义配置

```python
from core.config import TFLConfig, TableauServerConfig, DatabaseConfig

my_config = TFLConfig(
    server=TableauServerConfig(
        server_url="https://my-tableau-server.com",
        default_project="我的项目",
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

builder = TFLBuilder(flow_name="流程名", config=my_config)
```

---

## SDK API 参考

### `TFLBuilder` 类

| 方法 | 参数 | 说明 |
|------|------|------|
| `__init__(flow_name, config)` | 流程名, 配置对象(可选) | 初始化 |
| `add_connection(host, username, dbname, port, db_class)` | 连接信息 | 手动添加连接 |
| `add_connection_from_config()` | 无 | 使用默认配置连接 |
| `add_input_sql(name, sql, conn_id)` | 节点名, SQL, 连接ID | 添加 SQL 输入 |
| `add_join(name, left_id, right_id, left_col, right_col, join_type)` | 联接参数 | 添加联接 |
| `add_output_server(name, parent_id, datasource_name, project_name, server_url)` | 输出参数 | 添加服务器输出 |
| `build()` | 无 | 返回 (flow, display, meta) |

### `TFLPackager` 类

| 方法 | 参数 | 说明 |
|------|------|------|
| `save_to_folder(folder, flow, display, meta)` | 保存路径, JSON对象 | 保存为文件夹 |
| `pack_zip(folder, output_tfl)` | 文件夹, 输出路径 | 打包为 .tfl |

---

## 联接类型

| 类型 | 说明 |
|------|------|
| `"left"` | 左联接（默认） |
| `"right"` | 右联接 |
| `"inner"` | 内联接 |
| `"full"` | 全联接 |

---

## 构建指南

1. **解析 Schema**: 读取 `docs/database.md` 了解表结构和关联关系
2. **创建流程**: 按 输入→联接→输出 顺序构建
3. **关联键**: 确保 left_col 和 right_col 正确匹配
4. **验证**: 生成后用 Tableau Prep 打开验证

## 最佳实践

- 输出统一放在 `workspace/output/`
- 流程名使用业务含义（如 "Order_Analysis_v1"）
- 布局由 SDK 自动计算