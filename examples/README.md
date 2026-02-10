# cwprep 示例目录

本目录包含 cwprep SDK 的使用示例，基于 **Sample Superstore** 数据集。

## 快速开始

### 1. 初始化示例数据库

在 MySQL 客户端中执行：

```sql
examples/demo_data/init_superstore.sql
```

### 2. 运行示例脚本

```bash
# 基础功能（输入、联接、输出）
python examples/demo_basic.py

# 数据清理（筛选、重命名、计算）
python examples/demo_cleaning.py

# 聚合转置（并集、聚合、Pivot）
python examples/demo_aggregation.py

# 综合演示（覆盖全部 15 个 SDK 方法）
python examples/demo_comprehensive.py
```

### 3. 验证结果

用 Tableau Prep 打开 `demo_output/` 目录下生成的 `.tfl` 文件。

---

## 示例脚本说明

| 脚本 | 功能覆盖 | 业务场景 |
|------|----------|----------|
| `demo_basic.py` | 输入、联接、输出 | 客户订单关联 |
| `demo_cleaning.py` | 筛选、保留、重命名、计算 | 盈利订单分析 |
| `demo_aggregation.py` | 并集、聚合、Pivot/Unpivot | 区域月度对比 |
| `demo_comprehensive.py` | **全部 15 个 SDK 方法** | 完整销售分析 |

---

## 数据模型

```
                    ┌─────────────┐
                    │   regions   │
                    ├─────────────┤
                    │ region_id   │
                    │ region_name │
                    │ manager_name│
                    └──────┬──────┘
                           │
┌─────────────┐    ┌───────┴───────┐    ┌─────────────┐
│  customers  │    │    orders     │    │  products   │
├─────────────┤    ├───────────────┤    ├─────────────┤
│ customer_id │◄───│ customer_id   │───►│ product_id  │
│ customer_name│   │ region_id     │    │ product_name│
│ segment     │    │ product_id    │    │ category    │
└─────────────┘    │ sales/profit  │    │ sub_category│
                   └───────┬───────┘    └─────────────┘
                           │
                    ┌──────┴──────┐
                    │   returns   │
                    ├─────────────┤
                    │ order_id    │
                    │ returned    │
                    └─────────────┘
```

---

## 数据库配置

```python
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}
```
