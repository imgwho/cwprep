# cwprep - Text-to-PrepFlow Engine

**cwprep** is a Python-based engine that enables **Text-to-PrepFlow** generation. 

By reverse-engineering the `.tfl` JSON structure and providing a built-in MCP (Model Context Protocol) server, cwprep acts as a bridge between LLMs (like Claude, Gemini) and Tableau Prep. You can now generate, modify, and build data cleaning flows simply through natural language conversations or Python scripts, without ever opening the GUI!


## Installation

```bash
pip install cwprep
```

## Quick Start

```python
from cwprep import TFLBuilder, TFLPackager

# Create builder
builder = TFLBuilder(flow_name="My Flow")

# Add database connection
conn_id = builder.add_connection(
    host="localhost",
    username="root",
    dbname="mydb"
)

# Add input tables
orders = builder.add_input_table("orders", "orders", conn_id)
customers = builder.add_input_table("customers", "customers", conn_id)

# Join tables
joined = builder.add_join(
    name="Orders + Customers",
    left_id=orders,
    right_id=customers,
    left_col="customer_id",
    right_col="customer_id",
    join_type="left"
)

# Add output
builder.add_output_server("Output", joined, "My_Datasource")

# Build and save
flow, display, meta = builder.build()
TFLPackager.save_to_folder("./output", flow, display, meta)
TFLPackager.pack_zip("./output", "./my_flow.tfl")
```

## Features

| Feature | Method | Description |
|---------|--------|-------------|
| Database Connection | `add_connection()` | Connect to MySQL/PostgreSQL/Oracle |
| SQL Input | `add_input_sql()` | Custom SQL query input |
| Table Input | `add_input_table()` | Direct table connection |
| Join | `add_join()` | left/right/inner/full joins |
| Union | `add_union()` | Merge multiple tables |
| Filter | `add_filter()` | Expression-based filter |
| Value Filter | `add_value_filter()` | Keep/exclude by values |
| Keep Only | `add_keep_only()` | Select columns |
| Remove Columns | `add_remove_columns()` | Drop columns |
| Rename | `add_rename()` | Rename columns |
| Calculation | `add_calculation()` | Tableau formula fields |
| Quick Calc | `add_quick_calc()` | Quick clean (lowercase/uppercase/trim/remove) |
| Change Type | `add_change_type()` | Change column data types |
| Duplicate Column | `add_duplicate_column()` | Duplicate (copy) a column |
| Aggregate | `add_aggregate()` | GROUP BY with SUM/AVG/COUNT |
| Pivot | `add_pivot()` | Rows to columns |
| Unpivot | `add_unpivot()` | Columns to rows |
| Output | `add_output_server()` | Publish to Tableau Server |

## Examples

See the `examples/` directory for complete demos:
- `demo_basic.py` - Input, Join, Output
- `demo_cleaning.py` - Filter, Calculate, Rename
- `demo_aggregation.py` - Union, Aggregate, Pivot
- `demo_comprehensive.py` - All features combined

## MCP Server

cwprep includes a built-in [Model Context Protocol](https://modelcontextprotocol.io/) server, enabling AI clients (Claude Desktop, Cursor, Gemini CLI, etc.) to generate TFL files directly.

### Prerequisites

| Method | Requirement |
|--------|-------------|
| `uvx` (recommended) | Install [uv](https://docs.astral.sh/uv/getting-started/installation/) — it auto-downloads `cwprep[mcp]` in an isolated env |
| `pip install` | Python ≥ 3.8 + `pip install cwprep[mcp]` |

### Quick Start

```bash
# Local (stdio)
cwprep-mcp

# Remote (Streamable HTTP)
cwprep-mcp --transport streamable-http --port 8000
```

### Client Configuration

All clients below use the **`uvx` method** (recommended). Replace `uvx` with `cwprep-mcp` if you prefer a local `pip install`.

<details>
<summary><b>Claude Desktop</b></summary>

Edit config file:
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "cwprep": {
      "command": "uvx",
      "args": ["--from", "cwprep[mcp]", "cwprep-mcp"]
    }
  }
}
```
</details>

<details>
<summary><b>Cursor</b></summary>

Settings → MCP → Add new MCP server, or edit `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "cwprep": {
      "command": "uvx",
      "args": ["--from", "cwprep[mcp]", "cwprep-mcp"]
    }
  }
}
```
</details>

<details>
<summary><b>VS Code (Copilot)</b></summary>

Create `.vscode/mcp.json` in project root:

```json
{
  "servers": {
    "cwprep": {
      "command": "uvx",
      "args": ["--from", "cwprep[mcp]", "cwprep-mcp"]
    }
  }
}
```
</details>

<details>
<summary><b>Windsurf (Codeium)</b></summary>

Edit `~/.codeium/windsurf/mcp_config.json`:

```json
{
  "mcpServers": {
    "cwprep": {
      "command": "uvx",
      "args": ["--from", "cwprep[mcp]", "cwprep-mcp"]
    }
  }
}
```
</details>

<details>
<summary><b>Claude Code (CLI)</b></summary>

```bash
claude mcp add cwprep -- uvx --from "cwprep[mcp]" cwprep-mcp
```
</details>

<details>
<summary><b>Gemini CLI</b></summary>

Edit `~/.gemini/settings.json`:

```json
{
  "mcpServers": {
    "cwprep": {
      "command": "uvx",
      "args": ["--from", "cwprep[mcp]", "cwprep-mcp"]
    }
  }
}
```
</details>

<details>
<summary><b>Continue (VS Code / JetBrains)</b></summary>

Edit `~/.continue/config.yaml`:

```yaml
mcpServers:
  - name: cwprep
    command: uvx
    args:
      - --from
      - cwprep[mcp]
      - cwprep-mcp
```
</details>

<details>
<summary><b>Remote HTTP Mode (any client)</b></summary>

Start the server:

```bash
cwprep-mcp --transport streamable-http --port 8000
```

Then configure your client with the endpoint: `http://your-server-ip:8000/mcp`
</details>

### Available MCP Capabilities

| Type | Name | Description |
|------|------|-------------|
| 🔧 Tool | `generate_tfl` | Generate .tfl file from flow definition |
| 🔧 Tool | `list_supported_operations` | List all supported node types |
| 🔧 Tool | `validate_flow_definition` | Validate flow definition before generating |
| 📖 Resource | `cwprep://docs/api-reference` | SDK API reference |
| 📖 Resource | `cwprep://docs/calculation-syntax` | Tableau Prep calculation syntax |
| 💬 Prompt | `design_data_flow` | Interactive flow design assistant |
| 💬 Prompt | `explain_tfl_structure` | TFL file structure explanation |

## AI Skill Support

This project includes a specialized AI Skill for assistants like Claude or Gemini to help you build flows.
- **Location**: `.agents/skills/tfl-generator/`
- **Features**: Procedural guidance for flow construction, API reference, and Tableau Prep calculation syntax rules.

## Directory Structure

```
cwprep/
├── .agents/skills/      # AI Agent skills and technical references
├── src/cwprep/          # SDK source code
│   ├── builder.py       # TFLBuilder class
│   ├── packager.py      # TFLPackager class
│   ├── config.py        # Configuration utilities
│   └── mcp_server.py    # MCP Server (Tools, Resources, Prompts)
├── examples/            # Demo scripts
├── docs/                # Documentation
└── tests/               # Unit tests
```

## Configuration

Create `config.yaml` for default settings:

```yaml
database:
  host: localhost
  username: root
  dbname: mydb
  port: "3306"
  db_class: mysql

tableau_server:
  url: http://your-server
  project_name: Default
```

## Changelog

See [changelog.md](changelog.md) for version history.

## License

MIT License
