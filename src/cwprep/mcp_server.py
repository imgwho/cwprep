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
from .translator import SQLTranslator
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
        "cwprep generates Tableau Prep .tfl files.\n\n"
        "BEFORE generating, read these resources:\n"
        "1. cwprep://docs/api-reference\n"
        "2. cwprep://docs/calculation-syntax (differs from SQL!)\n"
        "3. cwprep://docs/best-practices\n\n"
        "WORKFLOW: read resources -> design -> validate_flow_definition -> generate_tfl"
    ),
)


# ============================= MCP Tools ====================================

# Supported node types for generate_tfl
_NODE_TYPES = {
    "input_sql",
    "input_table",
    "input_excel",
    "input_csv",
    "input_csv_union",
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
    "quick_calc",
    "change_type",
    "duplicate_column",
    "output_server",
}


def _resolve_output_path(output_path: str) -> str:
    """Resolve the output path, creating parent directories if needed."""
    path = Path(output_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    return str(path)


def _build_flow(
    flow_name: str,
    connection: Dict[str, Any],
    nodes: List[Dict[str, Any]],
    is_packaged: bool = False,
) -> tuple:
    """
    Internal helper: build TFL components from a declarative flow definition.

    Returns:
        (flow, display, meta, node_id_map, file_conn_ids)
    """
    builder = TFLBuilder(flow_name=flow_name)

    # --- connection ---
    conn_type = connection.get("type", "database")
    file_conn_ids: Dict[str, str] = {}  # filename -> conn_id

    if conn_type == "database":
        conn_params = {
            "host": connection.get("host", "localhost"),
            "username": connection.get("username", ""),
            "dbname": connection.get("dbname", ""),
        }
        if "port" in connection:
            conn_params["port"] = str(connection["port"])
        if "db_class" in connection:
            conn_params["db_class"] = connection["db_class"]
        if "authentication" in connection:
            conn_params["authentication"] = connection["authentication"]
        conn_id = builder.add_connection(**conn_params)
    elif conn_type == "file":
        # File connections are created per-node (each file needs its own)
        conn_id = None
    else:
        raise ValueError(f"Unknown connection type: '{conn_type}'. Use 'database' or 'file'.")

    # Resolve schema for input_table nodes
    schema = connection.get("schema", None)

    # Map user-supplied node names → internal IDs
    node_id_map: Dict[str, str] = {}

    def _get_or_create_file_conn(filename: str) -> str:
        """Get or create a file connection for the given filename."""
        if filename not in file_conn_ids:
            file_conn_ids[filename] = builder.add_file_connection(
                filename, is_packaged=is_packaged
            )
        return file_conn_ids[filename]

    for node_def in nodes:
        ntype = node_def.get("type")
        name = node_def.get("name", "")

        if ntype == "input_sql":
            nid = builder.add_input_sql(name, node_def["sql"], conn_id)

        elif ntype == "input_table":
            nid = builder.add_input_table(
                name, node_def["table"], conn_id,
                schema=node_def.get("schema", schema),
            )

        elif ntype == "input_excel":
            fc = _get_or_create_file_conn(node_def["filename"])
            nid = builder.add_input_excel(
                name, node_def["sheet"], fc,
                fields=node_def.get("fields"),
            )

        elif ntype == "input_csv":
            fc = _get_or_create_file_conn(node_def["filename"])
            nid = builder.add_input_csv(
                name, fc,
                fields=node_def.get("fields"),
                separator=node_def.get("separator", "A"),
                locale=node_def.get("locale", "en_US"),
                charset=node_def.get("charset", "UTF-8"),
                contains_headers=node_def.get("contains_headers", True),
            )

        elif ntype == "input_csv_union":
            fc = _get_or_create_file_conn(node_def["file_names"][0])
            nid = builder.add_input_csv_union(
                name, fc,
                node_def["file_names"],
                fields=node_def.get("fields"),
                separator=node_def.get("separator", "A"),
                locale=node_def.get("locale", "en_US"),
                charset=node_def.get("charset", "UTF-8"),
                contains_headers=node_def.get("contains_headers", True),
            )

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

        elif ntype == "quick_calc":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_quick_calc(
                name, parent, node_def["column_name"], node_def["calc_type"]
            )

        elif ntype == "change_type":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_change_type(name, parent, node_def["fields"])

        elif ntype == "duplicate_column":
            parent = node_id_map[node_def["parent"]]
            nid = builder.add_duplicate_column(
                name,
                parent,
                node_def["source_column"],
                node_def.get("new_column_name"),
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

    flow, display, meta = builder.build(is_packaged=is_packaged)
    return flow, display, meta, node_id_map, file_conn_ids


@mcp.tool()
def generate_tfl(
    flow_name: str,
    connection: Dict[str, Any],
    nodes: List[Dict[str, Any]],
    output_path: str,
    data_files: Optional[Dict[str, List[str]]] = None,
) -> str:
    """Generate a Tableau Prep data flow (.tfl/.tflx) file from a declarative flow definition.

    Args:
        flow_name: Display name for the flow in Tableau Prep.
        connection: Connection settings. Must include a "type" key:
            For type="database":
                Required keys: host.
                Conditionally required: username (required for mysql/postgres,
                    optional for sqlserver with sspi authentication),
                    dbname (required for mysql/postgres, often empty at
                    connection-level for sqlserver).
                Optional keys: port (default depends on db_class),
                    db_class ("mysql"|"sqlserver"|"postgres", default "mysql"),
                    authentication ("sspi"|"sqlserver"|"", default ""),
                    schema (e.g. "dbo" for SQL Server input_table nodes).
            For type="file":
                No additional keys needed at this level. File connections
                are created automatically from input_excel/input_csv nodes.
        nodes: Ordered list of node definitions. Each node is a dict with a
            "type" key and a "name" key (used to reference this node from
            downstream nodes). Additional keys depend on the node type:

            - input_sql:        sql (str)
            - input_table:      table (str)
            - input_excel:      filename, sheet (str), fields? (list)
            - input_csv:        filename, fields? (list), separator?, locale?, charset?, contains_headers?
            - input_csv_union:  file_names (list of str), fields? (list), separator?, locale?, charset?, contains_headers?
            - join:             left, right (node names), left_col, right_col, join_type?
            - union:            parents (list of node names)
            - filter:           parent (node name), expression (str)
            - value_filter:     parent, field, values (list), exclude? (bool)
            - calculation:      parent, column_name, formula
            - aggregate:        parent, group_by (list), aggregations? (list of {field, function, output_name?})
            - keep_only:        parent, columns (list)
            - remove_columns:   parent, columns (list)
            - rename:           parent, renames ({old: new})
            - pivot:            parent, pivot_column, aggregate_column, new_columns, group_by?, aggregation?
            - unpivot:          parent, columns_to_unpivot, name_column?, value_column?
            - quick_calc:       parent, column_name, calc_type (lowercase|uppercase|titlecase|trim_spaces|remove_extra_spaces|remove_all_spaces|remove_letters|remove_punctuation)
            - change_type:      parent, fields ({column: target_type})
            - duplicate_column: parent, source_column, new_column_name?
            - output_server:    parent, datasource_name, project_name?, server_url?
        output_path: File path for the generated flow file.
            Use .tfl extension for standard flows.
            Use .tflx extension for packaged flows with embedded data files.
        data_files: Optional dict mapping filenames to list of absolute source
            file paths. Required when output_path ends with .tflx.
            Example: {"orders.xlsx": ["C:/data/orders.xlsx"]}

    Returns:
        A summary string with the output file path and node count.
    """
    resolved = _resolve_output_path(output_path)
    folder = resolved.rsplit(".", 1)[0]  # strip extension for temp folder
    is_tflx = resolved.lower().endswith(".tflx")

    flow, display, meta, node_map, file_conn_ids = _build_flow(
        flow_name, connection, nodes, is_packaged=is_tflx
    )

    # Build data_files mapping: {connection_id: [file_paths]}
    packager_data = None
    if is_tflx and data_files:
        packager_data = {}
        for filename, file_paths in data_files.items():
            if filename in file_conn_ids:
                packager_data[file_conn_ids[filename]] = file_paths

    TFLPackager.save_to_folder(folder, flow, display, meta, data_files=packager_data)

    if is_tflx:
        TFLPackager.pack_tflx(folder, resolved)
    else:
        TFLPackager.pack_zip(folder, resolved)

    ext_label = "TFLX" if is_tflx else "TFL"
    return (
        f"Successfully generated {ext_label} file: {resolved}\n"
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
            "description": "SQL query input node (database connection)",
            "required": ["name", "sql"],
            "optional": [],
        },
        {
            "type": "input_table",
            "description": "Direct table input node (database connection, no custom SQL)",
            "required": ["name", "table"],
            "optional": [],
        },
        {
            "type": "input_excel",
            "description": "Excel file input node (file connection)",
            "required": ["name", "filename", "sheet"],
            "optional": [{"fields": "list of {name, type}"}],
        },
        {
            "type": "input_csv",
            "description": "CSV file input node (file connection)",
            "required": ["name", "filename"],
            "optional": [
                {"fields": "list of {name, type}"},
                {"separator": "str (default: A=auto)"},
                {"locale": "str (default: en_US)"},
                {"charset": "str (default: UTF-8)"},
                {"contains_headers": "bool (default: true)"},
            ],
        },
        {
            "type": "input_csv_union",
            "description": "CSV union input node (merge multiple CSV files)",
            "required": ["name", "file_names (list)"],
            "optional": [
                {"fields": "list of {name, type}"},
                {"separator": "str (default: A=auto)"},
                {"locale": "str (default: en_US)"},
                {"charset": "str (default: UTF-8)"},
                {"contains_headers": "bool (default: true)"},
            ],
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
            "type": "quick_calc",
            "description": "Quick clean operation (lowercase, uppercase, trim, remove letters/punctuation/spaces)",
            "required": ["name", "parent", "column_name", "calc_type"],
            "optional": [],
            "calc_type_options": "lowercase|uppercase|titlecase|trim_spaces|remove_extra_spaces|remove_all_spaces|remove_letters|remove_punctuation",
        },
        {
            "type": "change_type",
            "description": "Change column data types",
            "required": ["name", "parent", "fields ({column_name: target_type})"],
            "optional": [],
            "target_type_options": "string|integer|real|date|datetime|boolean",
        },
        {
            "type": "duplicate_column",
            "description": "Duplicate (copy) an existing column",
            "required": ["name", "parent", "source_column"],
            "optional": [{"new_column_name": "str (default: {source}-1)"}],
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
    
    conn_type = connection.get("type", "database")
    
    if conn_type == "database":
        db_class = connection.get("db_class", "mysql")
        
        # host is always required
        if "host" not in connection or not connection["host"]:
            errors.append("connection.host is required.")
        
        # username & dbname requirements depend on db_class
        if db_class == "sqlserver":
            auth = connection.get("authentication", "")
            # sqlserver with username/password auth requires username
            if auth == "sqlserver" and not connection.get("username"):
                errors.append("connection.username is required for sqlserver authentication.")
        else:
            # mysql, postgres, etc. always require username and dbname
            for key in ("username", "dbname"):
                if key not in connection or not connection[key]:
                    errors.append(f"connection.{key} is required.")
    elif conn_type != "file":
        errors.append(f"Unknown connection type: '{conn_type}'. Use 'database' or 'file'.")

    # Validate nodes
    if not nodes:
        errors.append("At least one node is required.")

    known_names: set = set()
    has_output = False

    _required_fields = {
        "input_sql": ["sql"],
        "input_table": ["table"],
        "input_excel": ["filename", "sheet"],
        "input_csv": ["filename"],
        "input_csv_union": ["file_names"],
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
        "quick_calc": ["parent", "column_name", "calc_type"],
        "change_type": ["parent", "fields"],
        "duplicate_column": ["parent", "source_column"],
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


@mcp.tool()
def translate_to_sql(
    flow_name: str = "",
    connection: Optional[Dict[str, Any]] = None,
    nodes: Optional[List[Dict[str, Any]]] = None,
    tfl_path: Optional[str] = None,
) -> str:
    """Translate a Tableau Prep flow to equivalent ANSI SQL (CTE format).

    Use this to preview the logical equivalence of a data flow as SQL,
    for review and verification purposes. Supports two input modes:

    Mode 1 — From declarative definition (same format as generate_tfl):
        Provide flow_name, connection, and nodes.

    Mode 2 — From existing .tfl file:
        Provide tfl_path pointing to a .tfl file on disk.

    If both tfl_path and (connection + nodes) are provided, tfl_path takes priority.

    Args:
        flow_name: Display name for the flow (used in Mode 1 and header comments).
        connection: Connection settings (same format as generate_tfl). Required for Mode 1.
        nodes: Ordered list of node definitions (same format as generate_tfl). Required for Mode 1.
        tfl_path: Path to an existing .tfl file. Required for Mode 2.

    Returns:
        ANSI SQL string with CTEs representing the flow logic,
        including a flow summary header and step-by-step comments.
    """
    translator = SQLTranslator()

    # Mode 2: from .tfl file
    if tfl_path:
        resolved = str(Path(tfl_path).resolve())
        return translator.translate_tfl_file(resolved)

    # Mode 1: from declarative definition
    if connection is not None and nodes is not None:
        flow, _display, _meta, _node_map, _file_conns = _build_flow(
            flow_name or "Untitled Flow", connection, nodes
        )
        return translator.translate_flow(flow, flow_name=flow_name)

    return (
        "Error: Please provide either:\n"
        "  - tfl_path: path to a .tfl file, OR\n"
        "  - connection + nodes: declarative flow definition"
    )


# ============================= MCP Resources ================================

_REFERENCES_DIR = Path(__file__).parent / "references"


def _load_reference(filename: str) -> str:
    """Load a bundled reference document from the references/ directory."""
    filepath = _REFERENCES_DIR / filename
    if filepath.is_file():
        return filepath.read_text(encoding="utf-8")
    return f"# Error\n\nReference file not found: {filename}"



@mcp.resource("cwprep://docs/api-reference")
def get_api_reference() -> str:
    """Complete API reference for the cwprep TFLBuilder and TFLPackager classes."""
    return _load_reference("api_reference.md")


@mcp.resource("cwprep://docs/calculation-syntax")
def get_calculation_syntax() -> str:
    """Tableau Prep calculation syntax reference — supported functions, operators, and important differences from SQL."""
    return _load_reference("calculation_syntax.md")


@mcp.resource("cwprep://docs/best-practices")
def get_best_practices() -> str:
    """Common pitfalls, Tableau Prep vs SQL syntax differences, and flow design rules."""
    return _load_reference("best_practices.md")



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
        f"1. Read `cwprep://docs/api-reference` for supported operations.\n"
        f"2. Read `cwprep://docs/calculation-syntax` for Tableau Prep formula syntax.\n"
        f"3. Read `cwprep://docs/best-practices` to avoid common pitfalls.\n"
        f"4. Analyze the data sources and business goal.\n"
        f"5. Design a data flow with appropriate operations "
        f"(input, join, filter, calculation, aggregate, output).\n"
        f"6. Generate the complete flow definition as a JSON object compatible "
        f"with the `generate_tfl` tool.\n"
        f"7. Call `validate_flow_definition` to check for errors.\n"
        f"8. If valid, call `generate_tfl` to create the .tfl file.\n"
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
