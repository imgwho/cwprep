# 项目更新日志 (Changelog)

### 当前状态
SDK v1.3 - 新增计算字段、移除列、值筛选功能，支持完整的 Tableau 公式语法。

---

## v1.3 (2026-02-08 12:05)

### 新增功能
- **计算字段** `add_calculation`：支持 Tableau 公式（IF/THEN/ELSE/ELSEIF）
- **移除列** `add_remove_columns`：批量移除不需要的字段
- **值筛选** `add_value_filter`：按值保留或排除记录

### 修复
- 修正计算字段节点类型：`.v1.CreateCalculatedColumn` → `.v1.AddColumn`

### 测试
- 新增 `test_calculation.py`：验证计算字段功能
- 新增 `test_new_features_v2.py`：验证移除列和值筛选功能

---

## v1.2 (2026-02-08 11:37)

### 新增功能
- **清理步骤容器** `add_clean_step`：支持在 Container 中组合多个清理操作
- **只保留列** `add_keep_only`：快速筛选需要的字段
- **重命名列** `add_rename`：批量重命名字段
- **筛选器** `add_filter`：支持 Tableau 计算表达式的数据筛选
- **聚合步骤** `add_aggregate`：GROUP BY + 聚合函数（SUM/AVG/COUNT 等）

### 文档更新
- 更新 `skills/tfl-generator/SKILL.md`：添加新 API 参考和使用示例
- 新增 `docs/tableau_agent_reference.md`：Tableau Agent 官方功能参考与项目规划对比

### 测试
- 新增 `test_new_features.py`：验证新功能生成的 .tfl 文件

---

## v1.1 (2026-02-08)

### 新增功能
- **YAML 配置系统**：支持通过 `config.yaml` 配置 Tableau Server 和数据库连接
- **环境变量支持**：敏感信息（密码）通过 `.env` 文件配置，自动被 gitignore 忽略
- **配置加载函数**：`load_config()` 自动合并 YAML 和环境变量
- **连接便捷方法**：`add_connection_from_config()` 直接使用默认配置

### 改进
- `TFLBuilder` 支持 `config` 参数注入，所有硬编码值已移除
- `add_connection()` 支持自定义 `port` 和 `db_class` 参数
- `add_output_server()` 支持自定义 `server_url` 参数
- 添加类型注解，提升代码可读性

### 新增文件
- `config.yaml` - 非敏感配置
- `.env` / `.env.example` - 环境变量配置
- `requirements.txt` - 项目依赖
- `.gitignore` - Git 忽略规则

---

### 修改记录

#### 1. 结构分析与初步修改
- 分析出 .tfl 文件本质是 ZIP 包，包含 flow、displaySettings 和 maestroMetadata 三个核心文件。
- 编写脚本实现了对现有流程的修改：删除了地理信息表连接并重新连接了数据路径。
- 编写了初始的 TFL 文件 JSON 规范文档。

#### 2. SDK 核心框架搭建
- 创建项目目录结构：`core` (引擎), `skills` (AI 技能), `docs` (文档), `workspace` (工作区)。
- 编写 `core/builder.py`：负责生成流程节点、计算坐标和管理 UUID。
- 编写 `core/packager.py`：负责将生成的 JSON 文件夹打包成标准的 .tfl 格式。

#### 3. AI 技能 (Skill) 集成
- 按照 Gemini CLI 规范，在 `skills/tfl-generator` 下创建了 AI 技能定义文件 `SKILL.md`。
- 定义了 AI 如何根据业务逻辑调用 SDK 生成流程的操作指令。

#### 4. 修复文件损坏与兼容性问题
- 修复了生成文件在 Tableau Prep 中报“已损坏”的错误。
- 修改了 `maestroMetadata`：将报错信息设为空列表 `[]` 而非删除，补全了四位版本号信息。
- 修改了 `displaySettings`：补全了版本号和布局元数据，并移除不完整的字段排序映射，让软件自动刷新字段。
- 更新了 `builder.py`：确保生成的 JSON 结构与手动制作成功的流程完全对齐。

#### 5. 文档整理

- 生成 `TFL_Engineering_Log.md`：总结了开发过程中的技术突破和避坑指南。

- 整理了数据库表结构文档，方便 AI 理解业务逻辑。



#### 6. SDK 1.0 发布与项目工程化

- 完成了 `TFLBuilder` 类的重构，支持自动计算坐标、自动管理 UUID 以及注册 PrimaryKey 属性。

- 实现了 `TFLPackager` 类，标准化了 .tfl 文件的打包与解包流程。

- 解决了 `displaySettings` 和 `maestroMetadata` 的深度兼容性问题，确保生成的流程可被软件正常读取。

- 按照 Gemini CLI 规范建立了 `tfl-generator` 技能，实现了从业务描述到 .tfl 文件的全自动化路径。
