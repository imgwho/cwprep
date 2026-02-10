# TFL Generator

这是一个基于 AI 和 Python SDK 的 Tableau Prep 数据流程 (.tfl) 自动生成工具。它通过对 .tfl 文件的底层 JSON 结构进行逆向工程，实现了无需打开软件即可通过代码生成或修改数据准备流程的能力。

## 项目核心功能
- **自动构建流程**：支持通过 Python 代码添加 SQL 输入、多表联接（Join）和输出节点。
- **UI 自动布局**：SDK 会自动计算节点在 Tableau Prep 画布上的坐标，确保生成的流程整洁不重叠。
- **兼容性保障**：遵循 Tableau Prep 的元数据校验规则，生成的流程文件可直接在软件中打开。
- **AI 技能集成**：内置 Gemini CLI 技能规范，允许 AI Agent 理解业务逻辑后直接调用 SDK 产出文件。

## 目录结构说明
- `core/`: SDK 源码。`builder.py` 负责逻辑构造，`packager.py` 负责文件打包。
- `docs/`: 存放 .tfl 文件 JSON 结构规范和开发迭代日志。
- `skills/`: 符合 Gemini CLI 规范的 AI 技能定义。
- `workspace/`: 工作目录。`input/` 存放原始文件，`output/` 存放生成的产物。
- `scripts/`: 存放开发过程中使用的比对和测试工具。

## 使用规则
1. **更新日志**：**每次提交代码修改（Commit）后，必须同步更新 `changelog.md`**，记录本次修改的具体内容。
2. **生成流程**：参考 `test_sdk_ultimate_final_fix.py` 调用 `TFLBuilder` 类构建逻辑，然后使用 `TFLPackager` 进行打包。
3. **打包规范**：生成的 flow, displaySettings, maestroMetadata 三个文件必须放在同级目录下打包为 ZIP，并重命名为 `.tfl`。

## 当前技术要点
- **maestroMetadata**: 必须保留版本详情对象，且报错信息列表需设为 `[]`。
- **displaySettings**: 必须包含主版本号，且若无法预测字段顺序，应保持 `fieldOrder` 对象为空。
- **nodeProperties**: 必须为联接节点注册主键信息，否则软件可能报损坏。
