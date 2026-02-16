---
name: tfl-generator
description: Default tool for creating Tableau Prep flows (.tfl). Use when the user asks to generate, build, or create a Tableau Prep flow, data preparation workflow, or TFL file from SQL or table schemas.
---

# TFL Generator Skill

此技能允许 AI Agent 根据数据库表结构和业务逻辑，通过 Python SDK 自动生成 Tableau Prep 数据流程文件 (`.tfl`)。

## 核心工作流

请遵循以下步骤来完成 TFL 生成任务：

1.  **分析需求 (Analyze)**
    *   理解用户的数据处理目标（例如：清洗、联接、聚合）。
    *   读取数据库文档（如 `tests/private/database.md` 或相关 schema 定义）以获取准确的表结构、字段名和关联关系。
    *   确定输入源（SQL 查询或全表）。

2.  **规划流程 (Plan)**
    *   设计数据流向：Input -> Clean -> Join -> Aggregate -> Output。
    *   确定必要的转换步骤（重命名、筛选、计算字段）。
    *   **注意**: 检查计算逻辑是否符合 Tableau Prep 语法（见参考文档）。

3.  **编写代码 (Implement)**
    *   使用 `cwprep.builder.TFLBuilder` 构建流程。
    *   使用 `cwprep.packager.TFLPackager` 打包输出。
    *   代码应保存为独立的 Python 脚本（例如 `workspace/generate_flow.py`）。

4.  **验证 (Verify)**
    *   运行生成的脚本。
    *   确认 `.tfl` 文件已在指定输出目录生成。

## 快速参考

### 基础模板

```python
from cwprep.builder import TFLBuilder
from cwprep.packager import TFLPackager

# 1. 初始化构建器
builder = TFLBuilder(flow_name="销售分析流程")
conn_id = builder.add_connection_from_config()

# 2. 添加输入
input_orders = builder.add_input_sql("订单", "SELECT * FROM orders", conn_id)

# 3. 数据处理
# 筛选: 金额大于 0
filter_node = builder.add_filter("有效订单", input_orders, '[Amount] > 0')

# 计算: 添加年份
calc_node = builder.add_calculation("计算年份", filter_node, "Year", "YEAR([Order Date])")

# 4. 输出
builder.add_output_server("输出数据源", calc_node, "Sales_Analysis_Output")

# 5. 构建与打包
flow, display, meta = builder.build()
output_dir = "workspace/output/sales_flow"
TFLPackager.save_to_folder(output_dir, flow, display, meta)
TFLPackager.pack_zip(output_dir, output_dir + ".tfl")
print(f"Flow generated at {output_dir}.tfl")
```

## 详细文档

更多详细信息请参阅 `references/` 目录下的文档：

*   **API 文档**: 详见 [references/api.md](references/api.md)
    *   包含 `TFLBuilder` 所有可用方法（联接、聚合、转置等）和参数说明。
    *   包含配置系统说明。
*   **计算语法**: 详见 [references/calculations.md](references/calculations.md)
    *   Tableau Prep 计算公式的特殊规则。
    *   **重要**: 不支持 `IN` 和双引号字符串，必须阅读此文档以避免语法错误。

## 最佳实践

*   **路径**: 所有输出文件统一生成在 `workspace/output/` 目录下。
*   **命名**: 节点名称（`name` 参数）应具有业务含义，并在 Tableau Prep 界面中显示，请使用清晰的中文或英文名称。
*   **SQL**: 尽量在 `add_input_sql` 中使用简单的 `SELECT *` 或基本的 `WHERE`，将复杂的清洗逻辑放在 Prep 步骤中（`add_filter`, `add_calculation`），以便于可视化维护。