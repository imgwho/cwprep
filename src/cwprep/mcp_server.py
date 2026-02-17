"""
cwprep MCP Server

Expose cwprep SDK capabilities through the Model Context Protocol (MCP),
allowing AI clients (Claude Desktop, Cursor, etc.) to generate Tableau Prep
data flow files (.tfl) via standardized tool calls.

Usage:
    # stdio (local)
    cwprep-mcp

    # streamable-http (remote)
    cwprep-mcp --transport streamable-http
"""

import sys
import os
import json
import tempfile
from typing import Any, Dict, List, Optional
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .builder import TFLBuilder
from .packager import TFLPackager
from .config import (
    TFLConfig,
    DatabaseConfig,
    TableauServerConfig,
    load_config,
    DEFAULT_CONFIG,
)


# ---------------------------------------------------------------------------
# Server instance
# ---------------------------------------------------------------------------
mcp = FastMCP(
    "cwprep",
    instructions=(
        "cwprep is a Tableau Prep Flow SDK. Use the provided tools to "
        "generate .tfl files programmatically. Read the resources first to "
        "understand the supported operations and Tableau Prep calculation syntax."
    ),
)


# ============================= MCP Tools ====================================

# Supported node types for generate_tfl
_NODE_TYPES = {
    "input_sql",
    "input_table",
    "join",
    "union",
    "filter",
    "value_filter",
    "calculation",
    "aggregate",
    "keep_only",
    "remove_columns",
    "rename",
    "pivot",
    "unpivot",
    "output_server",
}


def _resolve_output_path(output_path: str) -> str:
    """Resolve the output path, creating parent directories if needed."""
    path = Path(output_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _build_flow(flow_name: str, connection: Dict[str, Any], nodes: List[Dict[str, Any]]) -> tuple:
    """
    Internal helper: build TFL components from a declarative flow definition.

    Returns:
        (flow, display, meta, node_id_map)
    """
    builder = TFLBuilder(flow_name=flow_name)

    # --- connection ---
    conn_params = {
        "host": connection.get("host", "localhost"),
        "username": connection.get("username", ""),
        "dbname": connection.get("dbname", ""),
    }
    if "port" in connection:
        conn_params["port"] = str(connection["port"])
    if "db_class" in connection:
        conn_params["db_class"] = connection["db_class"]

    conn_id = builder.add_connection(**conn_params)

    # Map user-supplied node names → internal IDs
    node_id_map: Dict[str, str] = {}

    for node_def in nodes:
        ntype = node_def.get("type")
        name = node_def.get("name", "")

        if ntype == "input_sql":
            nid = builder.add_input_sql(name, node_def["sql"], conn_id)

        elif ntype == "input_table":
            nid = builder.add_input_table(name, node_def["table"], conn_id)

        elif ntype == "join":
            left = node_id_map[node_def["left"]]
            right = node_id_map[node_def["right"]]
            nid = builder.add_join(
                name,
                left,
                right,
                node_def["left_col"],
                node_def["right_col"],
                node_def.get("join_type", "left"),
            )

        elif ntype == "union":
            parent_ids = [node_id_map[p] for p in node_def["parents"]]
            nid = builder.add_union(name, parent_ids)

        elif ntype == "filter":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_filter(name, parent, node_def["expression"])

        elif ntype == "value_filter":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_value_filter(
                name,
                parent,
                node_def["field"],
                node_def["values"],
                node_def.get("exclude", False),
            )

        elif ntype == "calculation":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_calculation(
                name, parent, node_def["column_name"], node_def["formula"]
            )

        elif ntype == "aggregate":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_aggregate(
                name,
                parent,
                node_def["group_by"],
                node_def.get("aggregations"),
            )

        elif ntype == "keep_only":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_keep_only(name, parent, node_def["columns"])

        elif ntype == "remove_columns":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_remove_columns(name, parent, node_def["columns"])

        elif ntype == "rename":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_rename(parent, node_def["renames"])

        elif ntype == "pivot":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_pivot(
                name,
                parent,
                node_def["pivot_column"],
                node_def["aggregate_column"],
                node_def["new_columns"],
                node_def.get("group_by"),
                node_def.get("aggregation", "COUNT"),
            )

        elif ntype == "unpivot":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_unpivot(
                name,
                parent,
                node_def["columns_to_unpivot"],
                node_def.get("name_column", "Name"),
                node_def.get("value_column", "Value"),
            )

        elif ntype == "output_server":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_output_server(
                name,
                parent,
                node_def["datasource_name"],
                node_def.get("project_name"),
                node_def.get("server_url"),
            )

        else:
            raise ValueError(
                f"Unknown node type: '{ntype}'. "
                f"Supported types: {sorted(_NODE_TYPES)}"
            )

        node_id_map[name] = nid

    flow, display, meta = builder.build()
    return flow, display, meta, node_id_map


@mcp.tool()
def generate_tfl(
    flow_name: str,
    connection: Dict[str, Any],
    nodes: List[Dict[str, Any]],
    output_path: str,
) -> str:
    """Generate a Tableau Prep data flow (.tfl) file from a declarative flow definition.

    Args:
        flow_name: Display name for the flow in Tableau Prep.
        connection: Database connection settings.
            Required keys: host, username, dbname.
            Optional keys: port (default "3306"), db_class (default "mysql").
        nodes: Ordered list of node definitions. Each node is a dict with a
            "type" key and a "name" key (used to reference this node from
            downstream nodes). Additional keys depend on the node type:

            - input_sql:      sql (str)
            - input_table:    table (str)
            - join:           left, right (node names), left_col, right_col, join_type?
            - union:          parents (list of node names)
            - filter:         parent (node name), expression (str)
            - value_filter:   parent, field, values (list), exclude? (bool)
            - calculation:    parent, column_name, formula
            - aggregate:      parent, group_by (list), aggregations? (list of {field, function, output_name?})
            - keep_only:      parent, columns (list)
            - remove_columns: parent, columns (list)
            - rename:         parent, renames ({old: new})
            - pivot:          parent, pivot_column, aggregate_column, new_columns, group_by?, aggregation?
            - unpivot:        parent, columns_to_unpivot, name_column?, value_column?
            - output_server:  parent, datasource_name, project_name?, server_url?
        output_path: File path for the generated .tfl file (e.g. "./output/my_flow.tfl").

    Returns:
        A summary string with the output file path and node count.
    """
    resolved = _resolve_output_path(output_path)
    folder = resolved.rsplit(".", 1)[0]  # strip .tfl extension for temp folder

    flow, display, meta, node_map = _build_flow(flow_name, connection, nodes)
    TFLPackager.save_to_folder(folder, flow, display, meta)
    TFLPackager.pack_zip(folder, resolved)

    return (
        f"Successfully generated TFL file: {resolved}\n"
        f"Flow name: {flow_name}\n"
        f"Total nodes: {len(node_map)}\n"
        f"Nodes: {', '.join(node_map.keys())}"
    )


@mcp.tool()
def list_supported_operations() -> str:
    """List all supported node types and their required/optional parameters.

    Returns:
        A JSON string describing every supported operation type.
    """
    operations = [
        {
            "type": "input_sql",
            "description": "SQL query input node",
            "required": ["name", "sql"],
            "optional": [],
        },
        {
            "type": "input_table",
            "description": "Direct table input node (no custom SQL)",
            "required": ["name", "table"],
            "optional": [],
        },
        {
            "type": "join",
            "description": "Join two data sources",
            "required": ["name", "left", "right", "left_col", "right_col"],
            "optional": [{"join_type": "left|right|inner|full (default: left)"}],
        },
        {
            "type": "union",
            "description": "Merge multiple data sources with the same structure",
            "required": ["name", "parents (list, min 2)"],
            "optional": [],
        },
        {
            "type": "filter",
            "description": "Filter rows by Tableau calculation expression",
            "required": ["name", "parent", "expression"],
            "optional": [],
        },
        {
            "type": "value_filter",
            "description": "Keep or exclude rows by specific field values",
            "required": ["name", "parent", "field", "values (list)"],
            "optional": [{"exclude": "bool (default: false)"}],
        },
        {
            "type": "calculation",
            "description": "Add a calculated field using Tableau formula",
            "required": ["name", "parent", "column_name", "formula"],
            "optional": [],
        },
        {
            "type": "aggregate",
            "description": "Group and aggregate data (GROUP BY)",
            "required": ["name", "parent", "group_by (list)"],
            "optional": [
                {
                    "aggregations": "list of {field, function (SUM|AVG|COUNT|COUNTD|MIN|MAX|MEDIAN|STDEV|VAR), output_name?}"
                }
            ],
        },
        {
            "type": "keep_only",
            "description": "Keep only specified columns",
            "required": ["name", "parent", "columns (list)"],
            "optional": [],
        },
        {
            "type": "remove_columns",
            "description": "Remove specified columns",
            "required": ["name", "parent", "columns (list)"],
            "optional": [],
        },
        {
            "type": "rename",
            "description": "Rename columns",
            "required": ["name", "parent", "renames ({old_name: new_name})"],
            "optional": [],
        },
        {
            "type": "pivot",
            "description": "Rows to columns (Pivot)",
            "required": [
                "name",
                "parent",
                "pivot_column",
                "aggregate_column",
                "new_columns (list)",
            ],
            "optional": [
                {"group_by": "list"},
                {"aggregation": "COUNT|SUM|AVG|MIN|MAX (default: COUNT)"},
            ],
        },
        {
            "type": "unpivot",
            "description": "Columns to rows (Unpivot)",
            "required": ["name", "parent", "columns_to_unpivot (list)"],
            "optional": [
                {"name_column": "str (default: Name)"},
                {"value_column": "str (default: Value)"},
            ],
        },
        {
            "type": "output_server",
            "description": "Publish output to Tableau Server",
            "required": ["name", "parent", "datasource_name"],
            "optional": [{"project_name": "str"}, {"server_url": "str"}],
        },
    ]
    return json.dumps(operations, indent=2, ensure_ascii=False)


@mcp.tool()
def validate_flow_definition(
    flow_name: str,
    connection: Dict[str, Any],
    nodes: List[Dict[str, Any]],
) -> str:
    """Validate a flow definition without generating a file.

    Checks for:
    - Required connection fields
    - Valid node types
    - Node reference integrity (parent/left/right references exist)
    - Required fields for each node type

    Args:
        flow_name: Flow display name.
        connection: Database connection settings (same format as generate_tfl).
        nodes: Ordered list of node definitions (same format as generate_tfl).

    Returns:
        A JSON string with "valid" (bool) and "errors" (list of error strings).
    """
    errors: List[str] = []

    # Validate connection
    if not flow_name:
        errors.append("flow_name is required.")
    for key in ("host", "username", "dbname"):
        if key not in connection or not connection[key]:
            errors.append(f"connection.{key} is required.")

    # Validate nodes
    if not nodes:
        errors.append("At least one node is required.")

    known_names: set = set()
    has_output = False

    _required_fields = {
        "input_sql": ["sql"],
        "input_table": ["table"],
        "join": ["left", "right", "left_col", "right_col"],
        "union": ["parents"],
        "filter": ["parent", "expression"],
        "value_filter": ["parent", "field", "values"],
        "calculation": ["parent", "column_name", "formula"],
        "aggregate": ["parent", "group_by"],
        "keep_only": ["parent", "columns"],
        "remove_columns": ["parent", "columns"],
        "rename": ["parent", "renames"],
        "pivot": ["parent", "pivot_column", "aggregate_column", "new_columns"],
        "unpivot": ["parent", "columns_to_unpivot"],
        "output_server": ["parent", "datasource_name"],
    }

    for i, node in enumerate(nodes):
        ntype = node.get("type")
        name = node.get("name")

        if not name:
            errors.append(f"Node #{i}: 'name' is required.")
            continue

        if name in known_names:
            errors.append(f"Node #{i}: duplicate name '{name}'.")
        known_names.add(name)

        if not ntype:
            errors.append(f"Node '{name}': 'type' is required.")
            continue

        if ntype not in _NODE_TYPES:
            errors.append(
                f"Node '{name}': unknown type '{ntype}'. "
                f"Supported: {sorted(_NODE_TYPES)}"
            )
            continue

        if ntype == "output_server":
            has_output = True

        # Check required fields
        for field in _required_fields.get(ntype, []):
            if field not in node:
                errors.append(f"Node '{name}' (type={ntype}): missing required field '{field}'.")

        # Check parent references
        for ref_field in ("parent", "left", "right"):
            if ref_field in node:
                ref = node[ref_field]
                if ref not in known_names:
                    errors.append(
                        f"Node '{name}': '{ref_field}' references unknown node '{ref}'. "
                        f"It must be defined before this node."
                    )

        # Check parents list (union)
        if "parents" in node:
            parents = node["parents"]
            if not isinstance(parents, list) or len(parents) < 2:
                errors.append(f"Node '{name}' (union): 'parents' must be a list with at least 2 items.")
            else:
                for p in parents:
                    if p not in known_names:
                        errors.append(f"Node '{name}': parent '{p}' not found.")

    if not has_output and not errors:
        errors.append("Warning: No output_server node found. The flow will have no output.")

    return json.dumps(
        {"valid": len(errors) == 0, "errors": errors},
        indent=2,
        ensure_ascii=False,
    )


# ============================= MCP Resources ================================

_API_REFERENCE = """\
# cwprep SDK API Reference

## TFLBuilder

Core class for building Tableau Prep data flows.

### Constructor
```python
TFLBuilder(flow_name="Untitled Flow", config=None)
```

### Connection Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_connection(host, username, dbname, port?, db_class?)` | Connection info | Connection ID |
| `add_connection_from_config()` | None | Connection ID |

### Input Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_input_sql(name, sql, connection_id)` | Node name, SQL query, conn ID | Node ID |
| `add_input_table(name, table_name, connection_id)` | Node name, table name, conn ID | Node ID |

### Transform Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_join(name, left_id, right_id, left_col, right_col, join_type?)` | Join params | Node ID |
| `add_union(name, parent_ids)` | Name, list of parent IDs | Node ID |
| `add_filter(name, parent_id, expression)` | Filter with Tableau syntax | Node ID |
| `add_value_filter(name, parent_id, field, values, exclude?)` | Keep/exclude by values | Node ID |
| `add_calculation(name, parent_id, column_name, formula)` | Calculated field | Node ID |
| `add_aggregate(name, parent_id, group_by, aggregations?)` | GROUP BY + aggregation | Node ID |
| `add_keep_only(name, parent_id, columns)` | Keep specified columns | Node ID |
| `add_remove_columns(name, parent_id, columns)` | Remove specified columns | Node ID |
| `add_rename(parent_id, renames)` | Rename {old: new} | Node ID |
| `add_pivot(name, parent_id, pivot_column, aggregate_column, new_columns, ...)` | Rows to columns | Node ID |
| `add_unpivot(name, parent_id, columns_to_unpivot, ...)` | Columns to rows | Node ID |

### Output Methods
| Method | Parameters | Returns |
|--------|-----------|---------|
| `add_output_server(name, parent_id, datasource_name, project_name?, server_url?)` | Publish output | Node ID |

### Build
| Method | Parameters | Returns |
|--------|-----------|---------|
| `build()` | None | (flow, displaySettings, maestroMetadata) |

## TFLPackager

| Method | Parameters | Description |
|--------|-----------|-------------|
| `save_to_folder(folder, flow, display, meta)` | Path + JSON objects | Write exploded folder |
| `pack_zip(folder, output_tfl)` | Folder + output path | Pack as .tfl (ZIP) |

## Join Types
- `"left"` — Left Join (default)
- `"right"` — Right Join
- `"inner"` — Inner Join
- `"full"` — Full Outer Join

## Aggregate Functions
SUM, AVG, COUNT, COUNTD, MIN, MAX, MEDIAN, STDEV, VAR
"""


_CALC_SYNTAX = """\
# Tableau Prep Calculation Syntax Reference

## Important: Syntax Differences from SQL

| Unsupported | Alternative |
|-------------|------------|
| `IN (1, 2, 3)` | Use `OR` to chain: `[x] = 1 OR [x] = 2 OR [x] = 3` |
| `BETWEEN a AND b` | Use `[x] >= a AND [x] <= b` |
| `!=` | Use `<>` |

## String Rules
- Strings must use **single quotes**: `[Field] = 'Value'`
- Field references use **square brackets**: `[Field Name]`
- Incorrect: `[name] == Headquarter` ❌
- Correct: `[name] = 'Headquarter'` ✅

## Logical Expressions
```
# Multiple value check (alternative to IN)
[status] = 2 OR [status] = 3 OR [status] = 4

# Exclude multiple values
NOT ([branch] = 'Main' OR [branch] = 'Sales')

# Regex match
REGEXP_MATCH(STR([status]), '^[2-8]$')
```

## Function Categories

### Numeric
ABS, ROUND, CEILING, FLOOR, POWER, SQRT, LN, LOG, EXP, SIGN, SQUARE, PI, ACOS, ASIN, ATAN, COS, SIN, TAN

### String
CONTAINS, STARTSWITH, ENDSWITH, FIND, FINDNTH, LEFT, RIGHT, MID, LEN, TRIM, LTRIM, RTRIM,
UPPER, LOWER, PROPER, REPLACE, SPLIT, SPACE, CHAR, ASCII, REGEXP_MATCH, REGEXP_REPLACE, REGEXP_EXTRACT

### Date
DATEADD, DATEDIFF, DATEPART, DATETRUNC, DATENAME, DATEPARSE,
YEAR, MONTH, DAY, WEEK, QUARTER, MAKEDATE, MAKEDATETIME, NOW, TODAY, ISDATE

### Logic
IF / THEN / ELSEIF / ELSE / END
CASE [field] WHEN value THEN result ... ELSE default END
IIF(condition, then, else)
IFNULL(expr, alternate)
ISNULL(expr)
ZN(expr)  — returns 0 if null
ISBLANK(expr)

### Type Conversion
INT(expr), FLOAT(expr), STR(expr), DATE(expr), DATETIME(expr)

### Aggregate (in calculated fields)
SUM, AVG, COUNT, COUNTD, MIN, MAX, MEDIAN, STDEV, VAR, ATTR
"""


@mcp.resource("cwprep://docs/api-reference")
def get_api_reference() -> str:
    """Complete API reference for the cwprep TFLBuilder and TFLPackager classes."""
    return _API_REFERENCE


@mcp.resource("cwprep://docs/calculation-syntax")
def get_calculation_syntax() -> str:
    """Tableau Prep calculation syntax reference — supported functions, operators, and important differences from SQL."""
    return _CALC_SYNTAX


# ============================= MCP Prompts ==================================

@mcp.prompt(title="Design Data Flow")
def design_data_flow(
    data_sources: str,
    business_goal: str,
    output_name: str = "Output",
) -> str:
    """Interactive prompt to help design a Tableau Prep data flow.

    Args:
        data_sources: Description of available tables/data sources and their columns.
        business_goal: What the user wants to achieve (e.g. "join orders with customers and calculate monthly revenue").
        output_name: Desired output datasource name (default: "Output").
    """
    return (
        f"You are a Tableau Prep data flow architect using the cwprep SDK.\n\n"
        f"## Available Data Sources\n{data_sources}\n\n"
        f"## Business Goal\n{business_goal}\n\n"
        f"## Output Name\n{output_name}\n\n"
        f"## Your Task\n"
        f"1. Analyze the data sources and business goal.\n"
        f"2. Design a data flow with appropriate operations "
        f"(input, join, filter, calculation, aggregate, output).\n"
        f"3. Generate the complete flow definition as a JSON object compatible "
        f"with the `generate_tfl` tool.\n"
        f"4. Before generating, read the `cwprep://docs/api-reference` resource "
        f"for supported operations and the `cwprep://docs/calculation-syntax` "
        f"resource for Tableau Prep formula syntax.\n"
        f"5. Call `validate_flow_definition` to check for errors.\n"
        f"6. If valid, call `generate_tfl` to create the .tfl file.\n"
    )


@mcp.prompt(title="Explain TFL Structure")
def explain_tfl_structure() -> str:
    """Explain the structure of Tableau Prep .tfl files and how cwprep works."""
    return (
        "You are a Tableau Prep expert. Explain the following to the user:\n\n"
        "1. **What is a .tfl file?** — A Tableau Prep flow file is actually a ZIP "
        "archive containing three JSON files: `flow`, `displaySettings`, and "
        "`maestroMetadata`.\n\n"
        "2. **flow** — Contains all node definitions (inputs, transforms, outputs), "
        "connections (database configs), and the relationships between nodes.\n\n"
        "3. **displaySettings** — Controls the visual layout of nodes in the "
        "Tableau Prep Builder canvas (positions, colors, sizes).\n\n"
        "4. **maestroMetadata** — Stores version compatibility information.\n\n"
        "5. **How cwprep works** — The `TFLBuilder` class constructs the three "
        "JSON structures in memory by providing high-level methods like "
        "`add_join()` and `add_filter()`. The `TFLPackager` then writes them to "
        "disk and zips them into a `.tfl` file.\n\n"
        "6. **Node types** — Explain input nodes (LoadSql), transform nodes "
        "(SuperJoin, Container, SuperAggregate, etc.), and output nodes "
        "(PublishExtract).\n\n"
        "Be clear, concise, and include a simple example flow."
    )


# ============================= Entry Point ==================================

def main():
    """CLI entry point for the cwprep MCP server."""
    import argparse

    parser = argparse.ArgumentParser(
        prog="cwprep-mcp",
        description="cwprep MCP Server — Tableau Prep Flow SDK over Model Context Protocol",
    )
    parser.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="stdio",
        help="MCP transport type (default: stdio)",
    )
    parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind when using streamable-http (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind when using streamable-http (default: 8000)",
    )

    args = parser.parse_args()

    if args.transport == "streamable-http":
        mcp.settings.host = args.host
        mcp.settings.port = args.port

    mcp.run(transport=args.transport)


if __name__ == "__main__":
    main()
