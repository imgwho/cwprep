# Tableau Agent 功能参考与项目规划

> 基于 [Tableau Prep Einstein 官方文档](https://help.tableau.com/current/prep/zh-cn/prep_einstein.htm) 整理
> 
> 文档生成时间：2026-02-08

---

## Tableau Agent 官方支持的操作

### 检查数据
| 操作 | 说明 |
|------|------|
| 筛选数据 | 创建筛选器计算 |
| 筛选 null 值 | 移除空值 |
| 按日期时间范围筛选 | 日期范围过滤 |
| 通过相对日期筛选 | 动态日期过滤 |
| 移除字段 | 删除不需要的列 |
| 更改数据类型 | 类型转换 |

### 清理和调整数据
| 操作 | 说明 |
|------|------|
| 设为大写/小写/首字母大写 | 快速清理 |
| 移除空格/字母/数字/标点 | 快速清理 |
| 剪裁空格 | 快速清理 |
| 创建计算 | 支持 Tableau 计算语法 |
| 重命名字段 | 字段重命名 |
| 转换日期格式 | 日期处理 |
| 拆分值 | 按分隔符拆分 |
| 识别重复的行 | 数据去重 |
| 填补顺序数据中的空白 | 生成缺失行 |

### 转置数据
| 操作 | 说明 |
|------|------|
| 将列转置为行 | 行列转换 |
| 将行转置为列 | 行列转换 |

### 聚合数据
| 操作 | 说明 |
|------|------|
| 创建聚合步骤 | GROUP BY |
| 对值进行聚合和分组 | SUM/AVG/COUNT 等 |

### Tableau Agent 限制（官方不支持）
- ❌ 选择数据源或添加数据到流程
- ❌ 联接和并集操作
- ❌ 预测、脚本、输出步骤
- ❌ 跨步骤分析（仅支持单步骤）
- ❌ 流程分支（仅支持线性）
- ❌ 优化流程性能
- ❌ 运行或计划流程

---

## TFL Generator 项目功能规划

### 已实现 ✅
| 功能 | API 方法 | 说明 |
|------|----------|------|
| SQL 输入 | `add_input_sql` | 从数据库读取数据 |
| 联接 | `add_join` | 支持 left/right/inner/full |
| 服务器输出 | `add_output_server` | 发布到 Tableau Server |
| 清理步骤 | `add_clean_step` | Container 容器 |
| 只保留列 | `add_keep_only` | KeepOnlyColumns |
| 重命名列 | `add_rename` | RenameColumn |
| 筛选器 | `add_filter` | FilterOperation |
| 聚合 | `add_aggregate` | SuperAggregate |

### 待实现 🔲
| 功能 | 优先级 | 说明 |
|------|--------|------|
| 计算字段 | 🔴 高 | 支持 Tableau 公式语法 |
| 并集 (Union) | 🟡 中 | 合并多个结构相同的表 |
| 转置 (Pivot) | 🟡 中 | 行列转换 |
| 文件输入 | 🟢 低 | CSV/Excel 输入 |
| 文件输出 | 🟢 低 | 导出为 CSV/Hyper |
| 快速清理操作 | 🟢 低 | 大小写、去空格等 |

---

## 项目差异化优势

| 对比项 | Tableau Agent | TFL Generator |
|--------|---------------|---------------|
| 联接/并集 | ❌ 不支持 | ✅ 支持 |
| 流程分支 | ❌ 仅线性 | ✅ 支持 |
| 离线使用 | ❌ 需连接 | ✅ 完全本地 |
| 自动化集成 | ❌ 交互式 | ✅ CI/CD 集成 |
| 版本控制 | ❌ 不支持 | ✅ Git 管理 |
| 成本 | 需 Tableau+ | 开源免费 |

---

## 参考链接

- [Tableau Prep Einstein 官方文档](https://help.tableau.com/current/prep/zh-cn/prep_einstein.htm)
- [Tableau Agent 支持的操作](https://help.tableau.com/current/prep/zh-cn/prep_einstein.htm#Tableau)
