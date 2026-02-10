# TFL Generator Skill

此技能允许 AI Agent 根据数据库表结构和业务逻辑，自动生成或修改 Tableau Prep 数据流程文件 (`.tfl`)。

## 快速开始

```python
from core.builder import TFLBuilder
from core.packager import TFLPackager

# 1. 创建构建器
builder = TFLBuilder(flow_name="我的数据流程")

# 2. 添加数据库连接
conn_id = builder.add_connection(
    host="your-mysql-host.com",
    username="your_user", 
    dbname="your_db"
)

# 3. 添加输入节点
input1 = builder.add_input_sql("订单表", "SELECT * FROM orders", conn_id)
input2 = builder.add_input_sql("客户表", "SELECT * FROM customers", conn_id)

# 4. 添加联接
join = builder.add_join("订单-客户联接", input1, input2, "customer_id", "id")

# 5. 添加输出
builder.add_output_server("输出到Server", join, "订单分析数据源")

# 6. 构建并打包
flow, display, meta = builder.build()
TFLPackager.save_to_folder("workspace/output/my_flow", flow, display, meta)
TFLPackager.pack_zip("workspace/output/my_flow", "workspace/output/my_flow.tfl")
```

---

## 核心 SDK 接口

### `TFLBuilder` 类

| 方法 | 参数 | 返回值 | 说明 |
|------|------|--------|------|
| `__init__(flow_name)` | `flow_name`: 流程名称 | - | 初始化构建器 |
| `add_connection(host, username, dbname)` | 数据库连接信息 | `conn_id` | 添加 MySQL 数据库连接 |
| `add_input_sql(name, sql, conn_id)` | 节点名, SQL查询, 连接ID | `node_id` | 添加自定义 SQL 输入节点 |
| `add_join(name, left_id, right_id, left_col, right_col, join_type="left")` | 节点名, 左右表ID, 关联列 | `node_id` | 添加联接节点 |
| `add_output_server(name, parent_id, datasource_name, project_name="数据源")` | 节点名, 上游节点ID, 数据源名 | `node_id` | 添加服务器发布输出 |
| `build()` | - | `(flow, display, meta)` | 构建 TFL 组件 |

### `TFLPackager` 类

| 方法 | 参数 | 说明 |
|------|------|------|
| `save_to_folder(folder, flow, display, meta)` | 输出文件夹, 三个JSON对象 | 保存为解压后的文件夹结构 |
| `pack_zip(folder, output_tfl)` | 文件夹路径, 输出tfl路径 | 打包为 .tfl 文件 |

---

## 构建指南

1. **解析 Schema**: 优先读取 `docs/` 下的数据库结构database.md，识别每个表的主键字段。

2. **构建流式逻辑**:
   - **Step 1**: 创建 `TFLBuilder(flow_name="...")` 实例
   - **Step 2**: 添加数据库连接
   - **Step 3**: 为每个业务表添加 `add_input_sql`
   - **Step 4**: 使用 `add_join` 处理表关系（左表和右表的关联键必须准确）
   - **Step 5**: 添加 `add_output_server` 定义输出

3. **遵循金标准**: 
   - SDK 自动注册 `PrimaryKey` 元数据以防止文件损坏
   - SDK 使用 `maestroMetadata` 的"空列表规则"保留版本校验结构
   - SDK 自动添加所有必需的顶级字段

4. **物理输出**: 调用 `TFLPackager` 将生成的 JSON 序列化并打包为 `.tfl`

---

## 联接类型

`add_join` 方法支持以下联接类型:

| 类型 | 说明 |
|------|------|
| `"left"` | 左联接（默认） - 保留左表所有记录 |
| `"right"` | 右联接 - 保留右表所有记录 |
| `"inner"` | 内联接 - 只保留匹配记录 |
| `"full"` | 全联接 - 保留两表所有记录 |

---

## 最佳实践

- 流程命名应具有业务含义（如 "Order_Analysis_v1"）
- 坐标计算由 SDK 自动完成，AI 无需手动指定 X/Y
- 输入节点自动垂直排列，联接和输出节点水平排列
- 所有输出文件统一放置在 `workspace/output/` 目录下
- 生成后建议用 Tableau Prep 打开验证

---

## 文件结构

生成的 `.tfl` 文件是一个 ZIP 包，包含:

| 文件 | 说明 |
|------|------|
| `flow` | 核心逻辑 JSON（节点、连接、转换） |
| `displaySettings` | UI 布局（节点坐标、颜色） |
| `maestroMetadata` | 版本特性声明 |