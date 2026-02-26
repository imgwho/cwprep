"""
TFL Flow → SQL Translator

Translates Tableau Prep flow definitions (from .tfl files or TFLBuilder output)
to equivalent ANSI SQL using CTE (Common Table Expression) format.

Supports two input modes:
    1. From flow JSON dict (builder.build() output)
    2. From .tfl file path (ZIP archive)

Usage:
    from cwprep.translator import SQLTranslator

    # From builder
    flow, display, meta = builder.build()
    sql = SQLTranslator().translate_flow(flow)

    # From .tfl file
    sql = SQLTranslator().translate_tfl_file("path/to/flow.tfl")
"""

import json
import zipfile
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from .expression_translator import ExpressionTranslator


# Node type icons for human-readable comments
_NODE_ICONS = {
    "input": "📥",
    "join": "🔗",
    "union": "🔀",
    "filter": "🔍",
    "clean": "🧹",
    "aggregate": "📊",
    "output": "📤",
    "pivot": "🔄",
    "unpivot": "🔄",
}


class SQLTranslator:
    """Translate TFL flow JSON to ANSI SQL (CTE format).

    Args:
        include_comments: Whether to include step-by-step comments (default: True)
        include_summary: Whether to include a flow summary header (default: True)
    """

    def __init__(
        self,
        include_comments: bool = True,
        include_summary: bool = True,
    ):
        self.include_comments = include_comments
        self.include_summary = include_summary
        self.expr_translator = ExpressionTranslator()

    # ==================================================================
    # Public API
    # ==================================================================

    def translate_flow(
        self,
        flow: Dict[str, Any],
        flow_name: str = "",
    ) -> str:
        """Translate a flow JSON dict to SQL.

        Args:
            flow: Flow JSON dict (from builder.build() or .tfl archive)
            flow_name: Optional flow name for the header comment

        Returns:
            ANSI SQL string with CTEs
        """
        nodes = flow.get("nodes", {})
        connections = flow.get("connections", {})
        initial_nodes = flow.get("initialNodes", [])

        if not nodes:
            return "-- Empty flow (no nodes)"

        # Build the DAG and get topological order
        ordered_ids = self._topological_sort(nodes, initial_nodes)

        # Generate CTE for each node
        cte_entries: List[Dict[str, Any]] = []
        cte_name_map: Dict[str, str] = {}  # node_id → cte_name
        name_counter: Dict[str, int] = defaultdict(int)

        for node_id in ordered_ids:
            node = nodes[node_id]
            cte_name = self._make_cte_name(node, name_counter)
            cte_name_map[node_id] = cte_name

            entry = self._translate_node(
                node, node_id, connections, cte_name_map, nodes
            )
            entry["cte_name"] = cte_name
            cte_entries.append(entry)

        # Assemble final SQL
        return self._assemble_sql(cte_entries, flow_name)

    def translate_tfl_file(self, tfl_path: str) -> str:
        """Read a .tfl file and translate to SQL.

        Args:
            tfl_path: Path to the .tfl (ZIP) file

        Returns:
            ANSI SQL string with CTEs
        """
        with zipfile.ZipFile(tfl_path, "r") as zf:
            # The flow JSON is typically stored as "flow" in the ZIP
            with zf.open("flow") as f:
                flow = json.loads(f.read().decode("utf-8"))

        # Try to extract flow name from metadata
        flow_name = ""
        try:
            with zipfile.ZipFile(tfl_path, "r") as zf:
                if "maestroMetadata" in zf.namelist():
                    with zf.open("maestroMetadata") as f:
                        meta = json.loads(f.read().decode("utf-8"))
                        flow_name = meta.get("flowEntryName", "")
        except Exception:
            pass

        return self.translate_flow(flow, flow_name=flow_name)

    # ==================================================================
    # DAG traversal
    # ==================================================================

    def _topological_sort(
        self,
        nodes: Dict[str, Any],
        initial_nodes: List[str],
    ) -> List[str]:
        """Topological sort of the node DAG."""
        # Build adjacency list and in-degree count
        in_degree: Dict[str, int] = defaultdict(int)
        children: Dict[str, List[str]] = defaultdict(list)

        for nid, node in nodes.items():
            if nid not in in_degree:
                in_degree[nid] = 0
            for link in node.get("nextNodes", []):
                child_id = link.get("nextNodeId")
                if child_id:
                    children[nid].append(child_id)
                    in_degree[child_id] = in_degree.get(child_id, 0) + 1

        # BFS starting from nodes with in_degree == 0
        queue = [nid for nid in nodes if in_degree.get(nid, 0) == 0]
        # Prefer initial_nodes ordering
        if initial_nodes:
            seen = set()
            ordered_start = []
            for nid in initial_nodes:
                if nid in nodes and nid not in seen:
                    ordered_start.append(nid)
                    seen.add(nid)
            for nid in queue:
                if nid not in seen:
                    ordered_start.append(nid)
                    seen.add(nid)
            queue = ordered_start

        result = []
        visited = set()
        while queue:
            nid = queue.pop(0)
            if nid in visited:
                continue
            visited.add(nid)
            result.append(nid)
            for child_id in children.get(nid, []):
                in_degree[child_id] -= 1
                if in_degree[child_id] <= 0 and child_id not in visited:
                    queue.append(child_id)

        # Append any remaining unvisited nodes
        for nid in nodes:
            if nid not in visited:
                result.append(nid)

        return result

    # ==================================================================
    # CTE name generation
    # ==================================================================

    def _make_cte_name(
        self, node: Dict[str, Any], counter: Dict[str, int]
    ) -> str:
        """Generate a unique, readable CTE name from node name."""
        raw_name = node.get("name", "step")
        # Sanitize: replace spaces/special chars with underscores
        safe = "".join(
            c if c.isalnum() or c == "_" else "_"
            for c in raw_name
        ).strip("_").lower()
        if not safe or safe[0].isdigit():
            safe = "step_" + safe

        counter[safe] += 1
        if counter[safe] > 1:
            safe = f"{safe}_{counter[safe]}"

        return safe

    # ==================================================================
    # Node translation dispatcher
    # ==================================================================

    def _translate_node(
        self,
        node: Dict[str, Any],
        node_id: str,
        connections: Dict[str, Any],
        cte_name_map: Dict[str, str],
        all_nodes: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Translate a single node to a CTE entry dict.

        Returns dict with keys:
            - cte_name: str (filled by caller)
            - sql: str (the CTE body)
            - comment: str (human-readable description)
            - icon: str (emoji icon)
            - node_type: str
            - node_name: str
        """
        node_type = node.get("nodeType", "")
        base_type = node.get("baseType", "")
        node_name = node.get("name", "")

        # Input nodes
        if node_type == ".v1.LoadSql":
            return self._translate_input_sql(node, connections)
        if node_type == ".v1.LoadExcel":
            return self._translate_input_file(node, connections, "Excel")
        if node_type in (".v1.LoadCsv", ".v1.LoadCsvInputUnion"):
            return self._translate_input_file(node, connections, "CSV")

        # Join
        if node_type == ".v2018_2_3.SuperJoin":
            return self._translate_join(node, cte_name_map, all_nodes)

        # Union
        if node_type == ".v2018_2_3.SuperUnion":
            return self._translate_union(node, cte_name_map, all_nodes)

        # Aggregate
        if node_type == ".v2018_2_3.SuperAggregate":
            return self._translate_aggregate(node, cte_name_map, all_nodes)

        # Pivot / Unpivot — unsupported
        if node_type in (".v2018_3_3.SuperPivot", ".v2018_2_3.SuperUnpivot"):
            return self._translate_unsupported_node(node, node_type)

        # Clean step (Container)
        if node_type == ".v1.Container":
            return self._translate_container(node, cte_name_map, all_nodes)

        # Output
        if node_type == ".v1.PublishExtract":
            return self._translate_output(node, cte_name_map, all_nodes)

        # Fallback
        return {
            "sql": "SELECT 1 /* unknown node type */",
            "comment": f"未知节点类型: {node_type}",
            "icon": "❓",
            "node_type": "unknown",
            "node_name": node_name,
        }

    # ==================================================================
    # Input nodes
    # ==================================================================

    def _translate_input_sql(
        self, node: Dict[str, Any], connections: Dict[str, Any]
    ) -> Dict[str, Any]:
        relation = node.get("relation", {})
        conn_id = node.get("connectionId", "")
        node_name = node.get("name", "")

        # Get connection info for comment
        conn_info = self._get_connection_info(conn_id, connections)

        if relation.get("type") == "table":
            table_ref = relation.get("table", "")
            # Remove Tableau's bracket quoting for SQL
            sql = f"SELECT * FROM {table_ref}"
            comment = f"表输入: {table_ref}\n-- 来源: {conn_info}"
        else:
            # Custom SQL query
            query = relation.get("query", "SELECT 1")
            sql = query
            comment = f"SQL 查询\n-- 来源: {conn_info}"

        return {
            "sql": sql,
            "comment": comment,
            "icon": _NODE_ICONS["input"],
            "node_type": "input",
            "node_name": node_name,
        }

    def _translate_input_file(
        self, node: Dict[str, Any], connections: Dict[str, Any], file_type: str
    ) -> Dict[str, Any]:
        conn_id = node.get("connectionId", "")
        node_name = node.get("name", "")
        conn = connections.get(conn_id, {})
        filename = conn.get("connectionAttributes", {}).get("filename", "")

        return {
            "sql": f"SELECT 1 /* [UNSUPPORTED] {file_type} 文件输入: {filename} */",
            "comment": f"{file_type} 文件输入: {filename}\n-- ⚠️ 文件输入不可翻译为 SQL",
            "icon": _NODE_ICONS["input"],
            "node_type": "input",
            "node_name": node_name,
        }

    # ==================================================================
    # Join node
    # ==================================================================

    def _translate_join(
        self,
        node: Dict[str, Any],
        cte_name_map: Dict[str, str],
        all_nodes: Dict[str, Any],
    ) -> Dict[str, Any]:
        node_name = node.get("name", "")
        action_node = node.get("actionNode", {})
        join_type = action_node.get("joinType", "left").upper()
        conditions = action_node.get("conditions", [])

        # Find parent CTEs by scanning all_nodes for nextNodes pointing here
        left_cte, right_cte = self._find_join_parents(
            node.get("id", ""), all_nodes, cte_name_map
        )

        # Build ON clause
        on_parts = []
        for cond in conditions:
            left_expr = self.expr_translator.translate(
                cond.get("leftExpression", "")
            )
            right_expr = self.expr_translator.translate(
                cond.get("rightExpression", "")
            )
            on_parts.append(
                f'{left_cte}.{left_expr} = {right_cte}.{right_expr}'
            )
        on_clause = " AND ".join(on_parts) if on_parts else "1 = 1"

        join_keyword = f"{join_type} JOIN" if join_type != "INNER" else "INNER JOIN"
        if join_type == "FULL":
            join_keyword = "FULL OUTER JOIN"

        sql = (
            f"SELECT *\n"
            f"    FROM {left_cte}\n"
            f"    {join_keyword} {right_cte}\n"
            f"        ON {on_clause}"
        )

        cond_desc = ", ".join(
            f'{c.get("leftExpression", "")} = {c.get("rightExpression", "")}'
            for c in conditions
        )
        comment = (
            f"{join_type} JOIN\n"
            f"-- 左表: {left_cte}, 右表: {right_cte}\n"
            f"-- 条件: {cond_desc}"
        )

        return {
            "sql": sql,
            "comment": comment,
            "icon": _NODE_ICONS["join"],
            "node_type": "join",
            "node_name": node_name,
        }

    def _find_join_parents(
        self,
        join_node_id: str,
        all_nodes: Dict[str, Any],
        cte_name_map: Dict[str, str],
    ) -> Tuple[str, str]:
        """Find the Left and Right parent CTE names for a join node."""
        left_cte = "unknown_left"
        right_cte = "unknown_right"
        for nid, n in all_nodes.items():
            for link in n.get("nextNodes", []):
                if link.get("nextNodeId") == join_node_id:
                    ns = link.get("nextNamespace", "")
                    cte = cte_name_map.get(nid, nid)
                    if ns == "Left":
                        left_cte = cte
                    elif ns == "Right":
                        right_cte = cte
        return left_cte, right_cte

    # ==================================================================
    # Union node
    # ==================================================================

    def _translate_union(
        self,
        node: Dict[str, Any],
        cte_name_map: Dict[str, str],
        all_nodes: Dict[str, Any],
    ) -> Dict[str, Any]:
        node_name = node.get("name", "")

        # Find all parent CTEs pointing to this union node
        parent_ctes = self._find_union_parents(
            node.get("id", ""), all_nodes, cte_name_map
        )

        if len(parent_ctes) < 2:
            parent_ctes = ["unknown_1", "unknown_2"]

        union_parts = [f"SELECT * FROM {cte}" for cte in parent_ctes]
        sql = "\n    UNION ALL\n    ".join(union_parts)

        comment = f"合并: {', '.join(parent_ctes)}"

        return {
            "sql": sql,
            "comment": comment,
            "icon": _NODE_ICONS["union"],
            "node_type": "union",
            "node_name": node_name,
        }

    def _find_union_parents(
        self,
        union_node_id: str,
        all_nodes: Dict[str, Any],
        cte_name_map: Dict[str, str],
    ) -> List[str]:
        """Find all parent CTE names for a union node."""
        parents = []
        for nid, n in all_nodes.items():
            for link in n.get("nextNodes", []):
                if link.get("nextNodeId") == union_node_id:
                    parents.append(cte_name_map.get(nid, nid))
        return parents

    # ==================================================================
    # Aggregate node
    # ==================================================================

    def _translate_aggregate(
        self,
        node: Dict[str, Any],
        cte_name_map: Dict[str, str],
        all_nodes: Dict[str, Any],
    ) -> Dict[str, Any]:
        node_name = node.get("name", "")
        action_node = node.get("actionNode", {})

        parent_cte = self._find_single_parent(
            node.get("id", ""), all_nodes, cte_name_map
        )

        # Group by fields
        group_fields = action_node.get("groupByFields", [])
        group_cols = [f.get("columnName", "") for f in group_fields]

        # Aggregate fields
        agg_fields = action_node.get("aggregateFields", [])

        select_parts = [f'"{col}"' for col in group_cols]
        for agg in agg_fields:
            func = agg.get("function", "COUNT")
            col = agg.get("columnName", "")
            output = agg.get("newColumnName") or f"{func}_{col}"
            # COUNTD → COUNT(DISTINCT ...)
            if func.upper() == "COUNTD":
                select_parts.append(f'COUNT(DISTINCT "{col}") AS "{output}"')
            else:
                select_parts.append(f'{func}("{col}") AS "{output}"')

        group_by = ", ".join(f'"{col}"' for col in group_cols)
        select_clause = ",\n        ".join(select_parts)

        sql = f"SELECT {select_clause}\n    FROM {parent_cte}"
        if group_cols:
            sql += f"\n    GROUP BY {group_by}"

        agg_desc = ", ".join(
            f'{a.get("function", "")}({a.get("columnName", "")})'
            for a in agg_fields
        )
        comment = (
            f"聚合\n"
            f"-- 分组: {', '.join(group_cols)}\n"
            f"-- 聚合: {agg_desc}"
        )

        return {
            "sql": sql,
            "comment": comment,
            "icon": _NODE_ICONS["aggregate"],
            "node_type": "aggregate",
            "node_name": node_name,
        }

    # ==================================================================
    # Container (Clean Step)
    # ==================================================================

    def _translate_container(
        self,
        node: Dict[str, Any],
        cte_name_map: Dict[str, str],
        all_nodes: Dict[str, Any],
    ) -> Dict[str, Any]:
        node_name = node.get("name", "")

        parent_cte = self._find_single_parent(
            node.get("id", ""), all_nodes, cte_name_map
        )

        loom = node.get("loomContainer", {})
        inner_nodes = loom.get("nodes", {})
        initial = loom.get("initialNodes", [])

        # Walk the action chain in order
        actions = self._walk_action_chain(inner_nodes, initial)

        if not actions:
            return {
                "sql": f"SELECT * FROM {parent_cte}",
                "comment": "空的清理步骤",
                "icon": _NODE_ICONS["clean"],
                "node_type": "clean",
                "node_name": node_name,
            }

        # Translate action chain to SQL parts
        select_extras = []    # Additional SELECT expressions
        where_clauses = []    # WHERE conditions
        renames = {}          # old_name → new_name
        removes = []          # columns to note as removed
        keeps = []            # columns to keep only
        comment_parts = []    # Human-readable descriptions

        for action in actions:
            atype = action.get("nodeType", "")

            if atype == ".v1.RenameColumn":
                old = action.get("columnName", "")
                new = action.get("rename", "")
                renames[old] = new
                comment_parts.append(f"重命名: {old} → {new}")

            elif atype == ".v1.RemoveColumns":
                cols = action.get("columnNames", [])
                removes.extend(cols)
                comment_parts.append(f"移除列: {', '.join(cols)}")

            elif atype == ".v2019_2_2.KeepOnlyColumns":
                cols = action.get("columnNames", [])
                keeps = cols
                comment_parts.append(f"仅保留列: {', '.join(cols)}")

            elif atype == ".v1.FilterOperation":
                expr = action.get("filterExpression", "")
                sql_expr = self.expr_translator.translate(expr)
                where_clauses.append(sql_expr)
                comment_parts.append(f"筛选: {expr}")

            elif atype == ".v1.AddColumn":
                col_name = action.get("columnName", "")
                expr = action.get("expression", "")
                sql_expr = self.expr_translator.translate(expr)
                select_extras.append(f'{sql_expr} AS "{col_name}"')
                comment_parts.append(f"计算字段: {col_name} = {expr}")

            elif atype == ".v2024_2_0.QuickCalcColumn":
                col_name = action.get("columnName", "")
                expr = action.get("expression", "")
                calc_type = action.get("calcExpressionType", "")
                sql_expr = self.expr_translator.translate(expr)
                # QuickCalc replaces the column in-place
                renames[col_name] = col_name  # placeholder
                select_extras.append(f'{sql_expr} AS "{col_name}"')
                comment_parts.append(f"快速清理 ({calc_type}): {col_name}")

            elif atype == ".v1.ChangeColumnType":
                fields = action.get("fields", {})
                for col, info in fields.items():
                    target_type = info.get("type", "string")
                    sql_type = self._map_tableau_type(target_type)
                    select_extras.append(
                        f'CAST("{col}" AS {sql_type}) AS "{col}"'
                    )
                    comment_parts.append(f"类型转换: {col} → {target_type}")

            elif atype == ".v2019_2_3.DuplicateColumn":
                col_name = action.get("columnName", "")
                expr = action.get("expression", "")
                sql_expr = self.expr_translator.translate(expr)
                select_extras.append(f'{sql_expr} AS "{col_name}"')
                comment_parts.append(f"复制列: {col_name}")

            else:
                comment_parts.append(f"未知操作: {atype}")

        # Build SQL
        sql = self._build_container_sql(
            parent_cte, select_extras, where_clauses,
            renames, removes, keeps
        )

        return {
            "sql": sql,
            "comment": "\n-- ".join(comment_parts),
            "icon": _NODE_ICONS["clean"],
            "node_type": "clean",
            "node_name": node_name,
        }

    def _walk_action_chain(
        self,
        inner_nodes: Dict[str, Any],
        initial: List[str],
    ) -> List[Dict[str, Any]]:
        """Walk the linked list of action nodes in order."""
        if not initial or not inner_nodes:
            return []

        result = []
        current_id = initial[0] if initial else None
        visited = set()

        while current_id and current_id in inner_nodes and current_id not in visited:
            visited.add(current_id)
            action = inner_nodes[current_id]
            result.append(action)
            # Follow the chain
            next_links = action.get("nextNodes", [])
            current_id = next_links[0].get("nextNodeId") if next_links else None

        return result

    def _build_container_sql(
        self,
        parent_cte: str,
        select_extras: List[str],
        where_clauses: List[str],
        renames: Dict[str, str],
        removes: List[str],
        keeps: List[str],
    ) -> str:
        """Build SQL for a container step."""
        parts = []

        if keeps:
            # SELECT only specified columns
            col_list = ", ".join(f'"{c}"' for c in keeps)
            parts.append(f"SELECT {col_list}")
        elif removes:
            parts.append(
                f"SELECT * /* REMOVE: {', '.join(removes)} */"
            )
        else:
            parts.append("SELECT *")

        # Add rename aliases
        for old, new in renames.items():
            if old != new:  # skip self-rename from QuickCalc
                parts[0] += f',\n        "{old}" AS "{new}"'

        # Add calculated/extra columns
        if select_extras:
            extras = ",\n        ".join(select_extras)
            parts[0] += f",\n        {extras}"

        parts.append(f"    FROM {parent_cte}")

        if where_clauses:
            where = " AND ".join(f"({w})" for w in where_clauses)
            parts.append(f"    WHERE {where}")

        return "\n".join(parts)

    # ==================================================================
    # Output node
    # ==================================================================

    def _translate_output(
        self,
        node: Dict[str, Any],
        cte_name_map: Dict[str, str],
        all_nodes: Dict[str, Any],
    ) -> Dict[str, Any]:
        node_name = node.get("name", "")
        ds_name = node.get("datasourceName", "")

        parent_cte = self._find_single_parent(
            node.get("id", ""), all_nodes, cte_name_map
        )

        return {
            "sql": f"SELECT * FROM {parent_cte}",
            "comment": f"输出到 Tableau Server: {ds_name}",
            "icon": _NODE_ICONS["output"],
            "node_type": "output",
            "node_name": node_name,
        }

    # ==================================================================
    # Unsupported nodes (pivot/unpivot)
    # ==================================================================

    def _translate_unsupported_node(
        self,
        node: Dict[str, Any],
        node_type: str,
    ) -> Dict[str, Any]:
        node_name = node.get("name", "")
        type_label = "透视" if "Pivot" in node_type else "逆透视"
        return {
            "sql": f"SELECT 1 /* [UNSUPPORTED] {type_label}: {node_name} */",
            "comment": f"⚠️ {type_label}节点暂不支持 SQL 翻译",
            "icon": _NODE_ICONS.get("pivot", "🔄"),
            "node_type": "unsupported",
            "node_name": node_name,
        }

    # ==================================================================
    # Assembly
    # ==================================================================

    def _assemble_sql(
        self,
        cte_entries: List[Dict[str, Any]],
        flow_name: str,
    ) -> str:
        """Assemble the final SQL from CTE entries."""
        lines = []

        # --- Flow summary header ---
        if self.include_summary:
            lines.append(self._build_summary(cte_entries, flow_name))

        # --- CTE body ---
        cte_bodies = []
        last_cte_name = ""

        for i, entry in enumerate(cte_entries):
            cte_name = entry["cte_name"]
            sql_body = entry["sql"]
            last_cte_name = cte_name

            # Skip output nodes from CTE (they become the final SELECT)
            if entry["node_type"] == "output":
                continue

            cte_block = ""
            if self.include_comments:
                icon = entry.get("icon", "")
                node_name = entry.get("node_name", "")
                comment = entry.get("comment", "")
                step_num = i + 1
                cte_block += f"-- [Step {step_num}] {icon} {node_name}\n"
                cte_block += f"-- {comment}\n"

            cte_block += f"{cte_name} AS (\n    {sql_body}\n)"
            cte_bodies.append(cte_block)

        if not cte_bodies:
            return "-- 没有可翻译的节点"

        lines.append("WITH")
        lines.append(",\n\n".join(cte_bodies))

        # --- Final SELECT ---
        # Find the output node or use the last CTE
        output_entry = None
        for entry in cte_entries:
            if entry["node_type"] == "output":
                output_entry = entry
                break

        if output_entry:
            ds_name = ""
            for e in cte_entries:
                if e["node_type"] == "output":
                    ds_name = e.get("node_name", "")
            # The output refers to its parent CTE through the SQL body
            output_sql = output_entry["sql"]
            if self.include_comments:
                lines.append(f"\n-- [输出] {_NODE_ICONS['output']} {output_entry.get('node_name', '')}")
                lines.append(f"-- {output_entry.get('comment', '')}")
            lines.append(f"{output_sql};")
        else:
            # No explicit output — select from last CTE
            # Find the last non-output CTE
            for entry in reversed(cte_entries):
                if entry["node_type"] != "output":
                    last_cte_name = entry["cte_name"]
                    break
            lines.append(f"\nSELECT * FROM {last_cte_name};")

        return "\n".join(lines)

    def _build_summary(
        self,
        cte_entries: List[Dict[str, Any]],
        flow_name: str,
    ) -> str:
        """Build the flow summary header."""
        now = datetime.now().strftime("%Y-%m-%d %H:%M")
        title = flow_name or "Tableau Prep Flow"

        lines = [
            f"-- ═══════════════════════════════════════",
            f"-- Flow: {title}",
            f"-- 翻译时间: {now}",
            f"-- ═══════════════════════════════════════",
            f"--",
        ]

        # Build ASCII flow diagram
        step_labels = []
        for entry in cte_entries:
            icon = entry.get("icon", "")
            name = entry.get("node_name", entry["cte_name"])
            step_labels.append(f"{icon} {name}")

        if step_labels:
            flow_line = " ──→ ".join(step_labels)
            lines.append(f"-- {flow_line}")
            lines.append(f"--")

        # Step details
        lines.append(f"-- 步骤明细:")
        for i, entry in enumerate(cte_entries):
            icon = entry.get("icon", "")
            name = entry.get("node_name", entry["cte_name"])
            node_type = entry.get("node_type", "")
            first_comment_line = entry.get("comment", "").split("\n")[0]
            lines.append(f"--   {i+1}. {icon} {name} — {first_comment_line}")

        lines.append(f"-- ═══════════════════════════════════════")
        lines.append("")

        return "\n".join(lines)

    # ==================================================================
    # Helpers
    # ==================================================================

    def _find_single_parent(
        self,
        node_id: str,
        all_nodes: Dict[str, Any],
        cte_name_map: Dict[str, str],
    ) -> str:
        """Find the single parent CTE name for a node."""
        for nid, n in all_nodes.items():
            for link in n.get("nextNodes", []):
                if link.get("nextNodeId") == node_id:
                    return cte_name_map.get(nid, nid)
        return "unknown_parent"

    def _get_connection_info(
        self, conn_id: str, connections: Dict[str, Any]
    ) -> str:
        """Get a human-readable connection description."""
        conn = connections.get(conn_id, {})
        attrs = conn.get("connectionAttributes", {})
        db_class = attrs.get("class", "")
        server = attrs.get("server", "")
        dbname = attrs.get("dbname", "")
        if server and dbname:
            return f"{db_class} {server}.{dbname}"
        elif server:
            return f"{db_class} {server}"
        return conn.get("name", conn_id)

    @staticmethod
    def _map_tableau_type(tableau_type: str) -> str:
        """Map Tableau type names to SQL type names."""
        mapping = {
            "string": "VARCHAR",
            "integer": "INTEGER",
            "real": "REAL",
            "date": "DATE",
            "datetime": "TIMESTAMP",
            "boolean": "BOOLEAN",
        }
        return mapping.get(tableau_type.lower(), "VARCHAR")
