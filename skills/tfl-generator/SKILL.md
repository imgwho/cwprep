# TFL Generator Skill (Refined)

此技能允许 AI Agent 根据数据库表结构和业务逻辑，自动生成或修改 Tableau Prep 数据流程文件 (`.tfl`)。

## 指令 (Instructions)

1.  **解析 Schema**: 优先读取 `docs/` 下的数据库结构。识别每个表的主键字段。
2.  **构建流式逻辑**:
    - **Step 1**: 创建 `TFLBuilder(flow_name="...")` 实例。
    - **Step 2**: 添加数据库连接。
    - **Step 3**: 为每个业务表添加 `add_input_sql`。如果字段不明确，`fields` 传 `None`，但必须指定 `pk_fields`（主键）。
    - **Step 4**: 使用 `add_join` 处理表关系。左表和右表的关联键必须准确。
    - **Step 5**: 添加 `add_output_server` 定义输出。
3.  **遵循金标准**: 
    - 始终注册 `PrimaryKey` 元数据以防止文件损坏。
    - 使用 `maestroMetadata` 的“空列表规则”保留版本校验结构。
4.  **物理输出**: 调用 `TFLPackager` 将生成的 JSON 序列化并打包为 `.tfl`。

## 核心接口 (SDK Methods)
- `add_input_sql(name, sql, conn_id, pk_fields=["id"])`
- `add_join(name, left_id, right_id, left_col, right_col, pk_fields=["id"])`
- `add_output_server(name, parent_id, datasource_name)`

---

## 最佳实践
- 流程命名应具有业务含义（如 "Order_Analysis_v1"）。
- 坐标计算由 SDK 自动完成，AI 无需手动指定 X/Y。
- 所有输出文件统一放置在 `workspace/output/` 目录下。