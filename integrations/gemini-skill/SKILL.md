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
| `add_union(name, parent_ids)` | 名称, ID列表 | 添加并集 |
| `add_pivot(name, parent_id, pivot_column, aggregate_column, ...)` | 转置参数 | 行转列 |
| `add_unpivot(name, parent_id, columns_to_unpivot, ...)` | 转置参数 | 列转行 |
| `add_clean_step(name, parent_id, actions)` | 步骤名, 上游ID, 操作列表 | 添加清理步骤 |
| `add_keep_only(name, parent_id, columns)` | 步骤名, 上游ID, 列名列表 | 只保留指定列 |
| `add_remove_columns(name, parent_id, columns)` | 步骤名, 上游ID, 列名列表 | 移除指定列 |
| `add_rename(parent_id, renames)` | 上游ID, 重命名映射 | 重命名列 |
| `add_filter(name, parent_id, expression)` | 步骤名, 上游ID, 筛选表达式 | 表达式筛选 |
| `add_value_filter(name, parent_id, field, values, exclude)` | 筛选参数 | 按值筛选 |
| `add_calculation(name, parent_id, column_name, formula)` | 计算参数 | 添加计算字段 |
| `add_aggregate(name, parent_id, group_by, aggregations)` | 聚合参数 | 添加聚合步骤 |
| `add_output_server(name, parent_id, datasource_name, project_name, server_url)` | 输出参数 | 添加服务器输出 |
| `build()` | 无 | 返回 (flow, display, meta) |

### `TFLPackager` 类

| 方法 | 参数 | 说明 |
|------|------|------| 
| `save_to_folder(folder, flow, display, meta)` | 保存路径, JSON对象 | 保存为文件夹 |
| `pack_zip(folder, output_tfl)` | 文件夹, 输出路径 | 打包为 .tfl |

---

## 数据清理操作

### 只保留列
```python
builder.add_keep_only(
    name="只保留关键列",
    parent_id=input_id,
    columns=["订单ID", "客户ID", "金额"]
)
```

### 重命名列
```python
builder.add_rename(
    parent_id=input_id,
    renames={
        "旧列名1": "新列名1",
        "旧列名2": "新列名2"
    }
)
```

### 筛选数据
```python
# 筛选表达式使用 Tableau 计算语法
builder.add_filter(
    name="筛选有效订单",
    parent_id=input_id,
    expression='[金额] > 100 AND NOT ISNULL([客户ID])'
)
```

---

## 聚合操作

```python
builder.add_aggregate(
    name="按公司汇总",
    parent_id=input_id,
    group_by=["公司ID", "月份"],
    aggregations=[
        {"field": "金额", "function": "SUM", "output_name": "总金额"},
        {"field": "订单ID", "function": "COUNT", "output_name": "订单数"},
        {"field": "金额", "function": "AVG", "output_name": "平均金额"}
    ]
)
```

**支持的聚合函数**: `SUM`, `AVG`, `COUNT`, `COUNTD`, `MIN`, `MAX`, `MEDIAN`, `STDEV`, `VAR`

---

## 联接类型

| 类型 | 说明 |
|------|------|
| `"left"` | 左联接（默认） |
| `"right"` | 右联接 |
| `"inner"` | 内联接 |
| `"full"` | 全联接 |

---

## Tableau Prep 计算语法规则

> ⚠️ **重要**: Tableau Prep 的计算语法与 SQL 不同，以下规则必须遵守：

### 不支持的语法
| 不支持 | 替代方案 |
|--------|----------|
| `IN (1, 2, 3)` | 用 `OR` 连接多个条件 |
| `BETWEEN a AND b` | 用 `>= a AND <= b` |
| `!=` | 用 `<>` |

### 字符串比较
- 字符串必须用**单引号**：`[字段] = '值'`
- 错误：`[name] == 总部` ❌
- 正确：`[name] = '总部'` ✅

### 逻辑表达式示例
```
# 多值判断（替代 IN）
[status] = 2 OR [status] = 3 OR [status] = 4

# 排除多个值
NOT ([branch] = '总部' OR [branch] = '招商部')

# 正则匹配
REGEXP_MATCH(STR([status]), '^[2-8]$')
```

### 支持的函数分类
- **数字**: ABS, ROUND, CEILING, FLOOR, POWER, SQRT
- **字符串**: CONTAINS, LEFT, RIGHT, LEN, TRIM, UPPER, LOWER, SPLIT
- **日期**: DATEADD, DATEDIFF, DATEPART, YEAR, MONTH, DAY, NOW, TODAY
- **逻辑**: IF/THEN/ELSE/ELSEIF/END, CASE/WHEN, IIF, IFNULL, ISNULL, ZN
- **类型转换**: INT, FLOAT, STR, DATE, DATETIME

> 完整函数参考：`docs/tableau_prep_calculation.md`

---

## 构建指南

1. **解析 Schema**: 读取 `docs/database.md` 了解表结构和关联关系
2. **创建流程**: 按 输入→清理→联接→聚合→输出 顺序构建
3. **关联键**: 确保 left_col 和 right_col 正确匹配
4. **验证**: 生成后用 Tableau Prep 打开验证

## 最佳实践

- 输出统一放在 `workspace/output/`
- 流程名使用业务含义（如 "Order_Analysis_v1"）
- 布局由 SDK 自动计算