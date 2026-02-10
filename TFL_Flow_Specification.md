# Tableau Prep Flow (.tfl) 结构与生成指南

本文档旨在为 AI Agent 或开发者提供通过编程方式生成、修改 Tableau Prep 流程文件（`.tfl`）的完整规范。`.tfl` 本质是一个 ZIP 压缩包，核心逻辑存储在解压后的 `flow` JSON 文件中。

---

## 1. 文件系统结构

一个标准的 `.tfl` 文件解压后包含以下核心组件：

| 文件/文件夹 | 说明 | 关键程度 |
| :--- | :--- | :--- |
| **`flow`** | **核心逻辑文件**。无后缀的 JSON，定义了所有节点、连接、转换逻辑。 | ⭐⭐⭐⭐⭐ (最重要) |
| `connections` | (可选) 有时连接信息会单独存储，但在 `flow` 文件中通常也包含。 | ⭐⭐ |
| `displaySettings` | 定义节点在 UI 画布上的 X,Y 坐标和颜色。若缺失，流程可运行但 UI 会重叠。 | ⭐⭐⭐ |
| `maestroMetadata` | 版本和元数据信息。 | ⭐ |

---

## 2. `flow` JSON 顶级结构

`flow` 文件是一个巨大的 JSON 对象，核心由以下四个部分组成：

```json
{
  "initialNodes": ["uuid-1", "uuid-2"],  // 流程入口节点的 ID 列表
  "nodes": {                             // 所有节点的具体定义（字典结构，Key 为 NodeID）
    "uuid-1": { ... },
    "uuid-2": { ... }
  },
  "connections": {                       // 数据库/文件连接配置
    "conn-uuid": { ... }
  },
  "nodeProperties": { ... }              // 节点元数据（如主键定义，可为空）
}
```

---

## 3. 核心节点类型详解

每个节点都有共同的基础属性：
*   **`id`**: 唯一标识符（UUID v4）。
*   **`name`**: 在 UI 上显示的节点名称。
*   **`nodeType`**: 决定节点功能的类型字符串。
*   **`nextNodes`**: 定义数据流向（指向下一个节点）。

### 3.1 输入节点 (Input Node) - 自定义 SQL 或表

用于加载数据。

*   **NodeType**: `.v1.LoadSql` (SQL) 或 `.v1.LoadExcel` (Excel) 等。
*   **关键属性**:
    *   `connectionId`: 指向 `connections` 对象中的 ID。
    *   `connectionAttributes`: `{"dbname": "voxadmin"}`。
    *   **`relation`**:
        *   对于表：`{"type": "table", "table": "[table_name]"}`
        *   **对于自定义 SQL**:
            ```json
            "relation": {
              "type": "query",
              "query": "SELECT * FROM orders WHERE status = 1"
            }
            ```
    *   **`fields`**: **必须严格定义**。即使是 SQL，也需要显式列出输出字段的 `name` (字段名), `type` (类型: integer, string, real, datetime), `ordinal` (顺序)。

### 3.2 联接节点 (SuperJoin) - 表关系

Tableau Prep 的 Join 是一个“超级节点”，包含外壳和内部动作。

*   **NodeType**: `.v2018_2_3.SuperJoin` (版本号可能变动)。
*   **结构**:
    *   外层 `nextNodes`: 定义联接后的数据去向。
    *   **`actionNode`**: 定义实际的 Join 逻辑。
        *   `joinType`: `"inner"`, `"left"`, `"right"`, `"full"`.
        *   **`conditions`**: 联接条件数组。
            ```json
            "conditions": [
              {
                "leftExpression": "[OrderID]",   // 左表字段
                "rightExpression": "[ID]",       // 右表字段
                "comparator": "=="               // 操作符
              }
            ]
            ```
*   **流向控制 (Namespace)**:
    Join 节点有两个输入端口。上游节点连接到 Join 时，必须指定 `nextNamespace`：
    *   **`Left`**: 作为左表进入。
    *   **`Right`**: 作为右表进入。

### 3.3 清洗步骤 (Container / Clean Step)

清洗步骤（重命名、删除列、计算字段）被封装在一个 **Container** 中。这是一个**子图（Sub-graph）**。

*   **NodeType**: `.v1.Container`
*   **结构**:
    *   `loomContainer`: 内部包含 `nodes` 和 `initialNodes`。
    *   **内部节点类型**:
        *   `.v1.RenameColumn`: 重命名 (`columnName` -> `rename`)。
        *   `.v2019_2_2.KeepOnlyColumns`: 只保留指定列 (`columnNames` 数组)。
        *   `.v1.RemoveColumns`: 删除列。
        *   `.v1.CalculatedColumn`: 计算字段（支持 Tableau 公式）。

**注意**: 清洗步骤是线性的，内部节点需要像链表一样通过 `nextNodes` 串联。

### 3.4 输出节点 (Output Node)

定义数据的导出位置。

*   **NodeType**: `.v1.PublishExtract` (发布到 Server) 或 `.v1.WriteToFile` (本地文件)。
*   **关键属性**:
    *   `projectName`: Tableau Server 上的项目名。
    *   `datasourceName`: 发布后的数据源名称。
    *   `serverUrl`: 服务器地址。
    *   或者 `filePath`: 本地输出路径（如果是写文件）。

---

## 4. 连接 (Connections) 定义

在 `connections` 对象中定义数据源凭证。

```json
"b688...": {
  "connectionType": ".v1.SqlConnection",
  "name": "mysql.aliyun.com",
  "connectionAttributes": {
    "server": "rm-uf6...mysql.rds.aliyuncs.com",
    "class": "mysql",
    "port": "3306",
    "username": "link_BI",
    "dbname": "voxadmin",
    "authentication": "sql-password" // 密码通常不在此处明文存储，需用户在打开时输入或配置
  }
}
```

---

## 5. 生成/修改逻辑的核心规则 (AI Implementation Rules)

如果要编写代码生成此流程，请遵循以下规则：

1.  **UUID 生成**: 所有 `id` 必须是唯一的 UUID v4。
2.  **链式连接 (Wiring)**:
    *   每个节点（除了 Output）必须在 `nextNodes` 数组中定义下游节点。
    *   **格式**:
        ```json
        "nextNodes": [{
          "namespace": "Default",
          "nextNodeId": "target-uuid",
          "nextNamespace": "Default" // 如果目标是 Join，这里必须是 "Left" 或 "Right"
        }]
        ```
3.  **Schema 校验**: Input 节点中的 `fields` 列表必须与 SQL 查询结果或数据库表结构严格匹配，否则流程会报错。
4.  **删除节点的逻辑**:
    *   若删除节点 B (A -> B -> C)，必须将 A 的 `nextNodeId` 更新为 C 的 UUID。
    *   若 C 是 Join 节点，必须保留 B 原来连接到 C 时使用的 `nextNamespace` (Left/Right)。
5.  **Clean Step 的特殊性**: 不要试图直接在主 `nodes` 列表里加 `RenameColumn`。必须先创建一个 `Container` 节点，然后在 `loomContainer.nodes` 内部添加具体的清洗操作节点。

---

## 6. 示例：Python 伪代码构建流程

```python
flow = { "nodes": {}, "connections": {} }

# 1. 创建连接
conn_id = create_connection(flow, host="127.0.0.1", user="root")

# 2. 创建输入节点 (自定义 SQL)
input_id = create_node(flow, type="LoadSql", sql="SELECT * FROM Orders")
link_connection(flow, input_id, conn_id)

# 3. 创建清洗节点 (重命名 ID 为 OrderID)
clean_id = create_clean_step(flow, parent=input_id)
add_rename_operation(flow, clean_id, col="ID", new_name="OrderID")

# 4. 创建输出
output_id = create_output(flow, parent=clean_id, path="output.hyper")
```
