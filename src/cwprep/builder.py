"""
cwprep - Tableau Prep Flow SDK

Programmatically generate Tableau Prep data flow files.

Usage:
    from cwprep import TFLBuilder, TFLPackager
    
    builder = TFLBuilder(flow_name="My Flow")
    conn_id = builder.add_connection("host", "user", "db")
    input1 = builder.add_input_sql("Table1", "SELECT * FROM t1", conn_id)
    input2 = builder.add_input_sql("Table2", "SELECT * FROM t2", conn_id)
    join = builder.add_join("Join", input1, input2, "id", "t1_id")
    builder.add_output_server("Output", join, "Datasource Name")
    flow, display, meta = builder.build()
"""

import uuid
from typing import Optional, List, Dict, Any

from .config import TFLConfig, DEFAULT_CONFIG, DatabaseConfig


class TFLBuilder:
    """
    TFL Builder
    
    Args:
        flow_name: Flow name, displayed in Tableau Prep
        config: TFL config object, defaults to DEFAULT_CONFIG
    """
    
    def __init__(self, flow_name: str = "Untitled Flow", config: Optional[TFLConfig] = None):
        self.flow_name = flow_name
        self.config = config or DEFAULT_CONFIG
        
        self.nodes: Dict[str, Any] = {}
        self.initial_nodes: List[str] = []
        self.connections: Dict[str, Any] = {}
        self.node_properties: Dict[str, Any] = {}
        self.doc_id = str(uuid.uuid4())
        self.obfuscator_id = str(uuid.uuid4())
        self.features = {"document.v2019_1_3.Flow", "nodeProperty.v2019_1_3.PrimaryKey"}
        
        # Layout tracking
        self._node_order: List[Dict] = []
        self._input_count = 0

    def add_connection(
        self, 
        host: str, 
        username: str, 
        dbname: str,
        port: str = None,
        db_class: str = None,
        **kwargs
    ) -> str:
        """
        Add database connection
        
        Args:
            host: Database host address
            username: Username
            dbname: Database name
            port: Port number (defaults to config value)
            db_class: Database type mysql/postgres/oracle (defaults to config value)
            **kwargs: Other connection attributes
            
        Returns:
            str: Connection ID, used by subsequent input nodes
        """
        conn_id = str(uuid.uuid4())
        
        # Use passed parameters or config defaults
        default_db = self.config.database or DatabaseConfig()
        actual_port = port or default_db.port or "3306"
        actual_class = db_class or default_db.db_class or "mysql"
        
        connection_attrs = {
            "sslmode": kwargs.get("sslmode", ""),
            "odbc-connect-string-extras": "",
            "server": host,
            "prep-protocol-role": ":prep-protocol-reader",
            ":flow-name": self.flow_name,
            "odbc-dbms-name": "",
            ":use-allowlist": "false",
            "dbname": dbname,
            "port": actual_port,
            ":protocol-customizations": "",
            "source-charset": kwargs.get("source_charset", ""),
            "sslcert": "",
            "odbc-native-protocol": "",
            "expected-driver-version": "",
            "class": actual_class,
            "one-time-sql": "",
            "authentication": kwargs.get("authentication", ""),
            "username": username
        }
        
        self.connections[conn_id] = {
            "connectionType": ".v1.SqlConnection",
            "id": conn_id,
            "name": host,
            "isPackaged": False,
            "connectionAttributes": connection_attrs
        }
        return conn_id

    def add_connection_from_config(self) -> str:
        """
        Use default database connection from config file
        
        Returns:
            str: Connection ID
            
        Raises:
            ValueError: If no database settings in config
        """
        if not self.config.database:
            raise ValueError("No default database settings in config")
        
        db = self.config.database
        return self.add_connection(
            host=db.host,
            username=db.username,
            dbname=db.dbname,
            port=db.port,
            db_class=db.db_class
        )

    def add_input_sql(self, name: str, sql: str, connection_id: str) -> str:
        """
        Add SQL input node
        
        Args:
            name: Node name (usually table name)
            sql: SQL query statement
            connection_id: Database connection ID
            
        Returns:
            str: Node ID, used by subsequent operations
        """
        node_id = str(uuid.uuid4())
        self._input_count += 1
        self._node_order.append({"id": node_id, "type": "input", "y_hint": self._input_count})
        
        # Get database name from connection object
        dbname = ""
        if connection_id in self.connections:
            conn_attrs = self.connections[connection_id].get("connectionAttributes", {})
            dbname = conn_attrs.get("dbname", dbname)
        
        self.nodes[node_id] = {
            "nodeType": ".v1.LoadSql", "name": name, "id": node_id,
            "baseType": "input", "nextNodes": [], "serialize": False, "description": None,
            "connectionId": connection_id, 
            "connectionAttributes": {"dbname": dbname},
            "fields": None, "actions": [], "debugModeRowLimit": None,
            "originalDataTypes": {}, "randomSampling": None,
            "updateTimestamp": None,
            "restrictedFields": {}, "userRenamedFields": {},
            "selectedFields": None, "samplingType": None,
            "groupByFields": None, "filters": [],
            "relation": {"type": "query", "query": sql}
        }
        self.initial_nodes.append(node_id)
        return node_id

    def add_input_table(self, name: str, table_name: str, connection_id: str) -> str:
        """
        Add table input node (direct table connection, no custom SQL)
        
        Args:
            name: Node name (usually same as table name)
            table_name: Database table name
            connection_id: Database connection ID
            
        Returns:
            str: Node ID, used by subsequent operations
        """
        node_id = str(uuid.uuid4())
        self._input_count += 1
        self._node_order.append({"id": node_id, "type": "input", "y_hint": self._input_count})
        
        # Get database name from connection object
        dbname = ""
        if connection_id in self.connections:
            conn_attrs = self.connections[connection_id].get("connectionAttributes", {})
            dbname = conn_attrs.get("dbname", dbname)
        
        self.nodes[node_id] = {
            "nodeType": ".v1.LoadSql", "name": name, "id": node_id,
            "baseType": "input", "nextNodes": [], "serialize": False, "description": None,
            "connectionId": connection_id, 
            "connectionAttributes": {"dbname": dbname},
            "fields": None, "actions": [], "debugModeRowLimit": None,
            "originalDataTypes": {}, "randomSampling": None,
            "updateTimestamp": None,
            "restrictedFields": {}, "userRenamedFields": {},
            "selectedFields": None, "samplingType": None,
            "groupByFields": None, "filters": [],
            "relation": {"type": "table", "table": f"[{table_name}]"}
        }
        self.initial_nodes.append(node_id)
        return node_id


    def add_join(
        self, 
        name: str, 
        left_id: str, 
        right_id: str, 
        left_col: str, 
        right_col: str, 
        join_type: str = "left"
    ) -> str:
        """
        Add join node
        
        Args:
            name: Join node name
            left_id: Left table node ID
            right_id: Right table node ID
            left_col: Left table join column name
            right_col: Right table join column name
            join_type: Join type ("left", "right", "inner", "full")
            
        Returns:
            str: Join node ID
        """
        node_id = str(uuid.uuid4())
        self.features.add("node.v2018_2_3.SuperJoin")
        self._node_order.append({"id": node_id, "type": "join"})
        
        # Register PrimaryKey here
        self.node_properties[node_id] = {
            "com.tableau.loom.doc.fileformat.v2019_1_3.PrimaryKey": {
                "nodePropertyType": ".v2019_1_3.PrimaryKey",
                "empty": False,
                "fieldNames": ["id"]
            }
        }
        
        self.nodes[node_id] = {
            "nodeType": ".v2018_2_3.SuperJoin", "name": name, "id": node_id,
            "baseType": "superNode", "nextNodes": [], "serialize": False, "description": None,
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleJoin", "name": name, "id": str(uuid.uuid4()),
                "baseType": "transform", "nextNodes": [], "serialize": False, "description": None,
                "conditions": [{"leftExpression": f"[{left_col}]", "rightExpression": f"[{right_col}]", "comparator": "=="}],
                "joinType": join_type
            }
        }
        self.nodes[left_id]["nextNodes"].append({"namespace": "Default", "nextNodeId": node_id, "nextNamespace": "Left"})
        self.nodes[right_id]["nextNodes"].append({"namespace": "Default", "nextNodeId": node_id, "nextNamespace": "Right"})
        return node_id

    def add_union(
        self,
        name: str,
        parent_ids: List[str]
    ) -> str:
        """
        Add union node (merge multiple data sources with same structure)
        
        Args:
            name: Union node name
            parent_ids: List of upstream node IDs (at least 2)
            
        Returns:
            str: Union node ID
        """
        if len(parent_ids) < 2:
            raise ValueError("Union requires at least 2 data sources")
        
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "union"})
        
        # Create unique namespace for each input
        namespace_mappings = []
        for parent_id in parent_ids:
            namespace_id = f"Union-Namespace-{str(uuid.uuid4())}"
            namespace_mappings.append({
                "namespaceName": namespace_id,
                "fieldMappings": {}
            })
            # Connect upstream node to union node
            self.nodes[parent_id]["nextNodes"].append({
                "namespace": "Default",
                "nextNodeId": node_id,
                "nextNamespace": namespace_id
            })
        
        self.nodes[node_id] = {
            "nodeType": ".v2018_2_3.SuperUnion",
            "name": name,
            "id": node_id,
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.SimpleUnion",
                "name": name,
                "id": str(uuid.uuid4()),
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None,
                "namespaceFieldMappings": namespace_mappings
            }
        }
        return node_id

    def add_pivot(
        self,
        name: str,
        parent_id: str,
        pivot_column: str,
        aggregate_column: str,
        new_columns: List[str],
        group_by: List[str] = None,
        aggregation: str = "COUNT"
    ) -> str:
        """
        Add rows to columns (Pivot) node
        
        Args:
            name: Pivot node name
            parent_id: Upstream node ID
            pivot_column: Field name to use as column headers
            aggregate_column: Field name to aggregate
            new_columns: List of new column names (e.g. ["2026-01", "2026-02"])
            group_by: List of grouping fields (optional)
            aggregation: Aggregation function (COUNT, SUM, AVG, MIN, MAX)
            
        Returns:
            str: Pivot node ID
        """
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "pivot"})
        
        self.nodes[node_id] = {
            "nodeType": ".v2018_3_3.SuperPivot",
            "name": name,
            "id": node_id,
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v2018_3_3.Pivot",
                "name": f"{pivot_column} 1",
                "id": str(uuid.uuid4()),
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None,
                "defaultAggregation": aggregation,
                "aggregateColumnName": aggregate_column,
                "pivotColumnName": pivot_column,
                "pivotGroupingColumns": group_by or [],
                "newPivotColumns": [{"newColumnName": col} for col in new_columns]
            }
        }
        
        self.nodes[parent_id]["nextNodes"].append({
            "namespace": "Default",
            "nextNodeId": node_id,
            "nextNamespace": "Default"
        })
        return node_id

    def add_unpivot(
        self,
        name: str,
        parent_id: str,
        columns_to_unpivot: List[str],
        name_column: str = "Name",
        value_column: str = "Value"
    ) -> str:
        """
        Add columns to rows (Unpivot) node
        
        Args:
            name: Unpivot node name
            parent_id: Upstream node ID
            columns_to_unpivot: List of column names to unpivot (e.g. ["staff_wechat_id", "customer_wechat_id"])
            name_column: New field name for column names (default "Name")
            value_column: New field name for values (default "Value")
            
        Returns:
            str: Unpivot node ID
        """
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "unpivot"})
        
        # Build unpivotGroups
        expressions = []
        for col in columns_to_unpivot:
            expressions.append({
                "bindings": [
                    {
                        "bindingType": "literal",
                        "newColumnName": name_column,
                        "groupName": col
                    },
                    {
                        "bindingType": "column",
                        "newColumnName": value_column,
                        "columnName": col
                    }
                ]
            })
        
        self.nodes[node_id] = {
            "nodeType": ".v2018_2_3.SuperUnpivot",
            "name": name,
            "id": node_id,
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.Unpivot",
                "name": name,
                "id": str(uuid.uuid4()),
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None,
                "usesSmartDefaults": True,
                "unpivotGroups": [{"expressions": expressions}]
            }
        }
        
        self.nodes[parent_id]["nextNodes"].append({
            "namespace": "Default",
            "nextNodeId": node_id,
            "nextNamespace": "Default"
        })
        return node_id

    def add_output_server(
        self, 
        name: str, 
        parent_id: str, 
        datasource_name: str, 
        project_name: str = None,
        server_url: str = None
    ) -> str:
        """
        Add server output node
        
        Args:
            name: Output node name
            parent_id: Upstream node ID 
            datasource_name: Published datasource name
            project_name: Project name on Tableau Server (defaults to config value)
            server_url: Tableau Server URL (defaults to config value)
            
        Returns:
            str: Output node ID
        """
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "output"})
        
        # Use passed parameters or config defaults
        actual_project = project_name or self.config.server.default_project
        actual_server = server_url or self.config.server.server_url
        actual_luid = self.config.server.project_luid
        
        self.nodes[node_id] = {
            "nodeType": ".v1.PublishExtract", "name": name, "id": node_id,
            "baseType": "output", "nextNodes": [], "serialize": False, "description": None,
            "projectName": actual_project, 
            "projectLuid": actual_luid,
            "datasourceName": datasource_name, 
            "datasourceDescription": "",
            "serverUrl": actual_server
        }
        self.nodes[parent_id]["nextNodes"].append({"namespace": "Default", "nextNodeId": node_id, "nextNamespace": "Default"})
        return node_id

    def add_clean_step(
        self,
        name: str,
        parent_id: str,
        actions: List[Dict[str, Any]] = None
    ) -> str:
        """
        Add clean step (Container)
        
        Args:
            name: Clean step name
            parent_id: Upstream node ID
            actions: List of clean operations, each is a dict containing:
                - type: Operation type (rename, keep_only, remove, etc.)
                - Other parameters depend on operation type
            
        Returns:
            str: Clean step node ID
        """
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "clean"})
        
        # Build inner nodes
        inner_nodes = {}
        initial_node_id = None
        prev_node_id = None
        
        if actions:
            for i, action in enumerate(actions):
                action_node_id = str(uuid.uuid4())
                if i == 0:
                    initial_node_id = action_node_id
                
                action_node = self._create_action_node(action, action_node_id)
                if prev_node_id:
                    inner_nodes[prev_node_id]["nextNodes"].append({
                        "namespace": "Default",
                        "nextNodeId": action_node_id,
                        "nextNamespace": "Default"
                    })
                inner_nodes[action_node_id] = action_node
                prev_node_id = action_node_id
        
        self.nodes[node_id] = {
            "nodeType": ".v1.Container",
            "name": name,
            "id": node_id,
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [initial_node_id] if initial_node_id else [],
                "nodes": inner_nodes,
                "connections": {},
                "dataConnections": {},
                "connectionIds": [],
                "dataConnectionIds": [],
                "nodeProperties": {},
                "extensibility": None
            },
            "namespacesToInput": {
                "Default": {"nodeId": initial_node_id, "namespace": "Default"}
            } if initial_node_id else {},
            "namespacesToOutput": {
                "Default": {"nodeId": prev_node_id, "namespace": "Default"}
            } if prev_node_id else {},
            "providedParameters": None
        }
        
        self.nodes[parent_id]["nextNodes"].append({
            "namespace": "Default",
            "nextNodeId": node_id,
            "nextNamespace": "Default"
        })
        return node_id
    
    def _create_action_node(self, action: Dict[str, Any], node_id: str) -> Dict[str, Any]:
        """Create single clean action node"""
        action_type = action.get("type")
        
        if action_type == "keep_only":
            return {
                "nodeType": ".v2019_2_2.KeepOnlyColumns",
                "name": f"Keep only {action['columns'][0]} + {len(action['columns'])-1} more",
                "id": node_id,
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None,
                "columnNames": action["columns"]
            }
        elif action_type == "rename":
            return {
                "nodeType": ".v1.RenameColumn",
                "columnName": action["from"],
                "rename": action["to"],
                "name": f"Rename {action['from']} to {action['to']}",
                "id": node_id,
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None
            }
        elif action_type == "remove":
            return {
                "nodeType": ".v1.RemoveColumns",
                "name": f"Remove {action['columns'][0]} + {len(action['columns'])-1} more",
                "id": node_id,
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None,
                "columnNames": action["columns"]
            }
        else:
            raise ValueError(f"Unsupported action type: {action_type}")
    
    def add_remove_columns(self, name: str, parent_id: str, columns: List[str]) -> str:
        """
        Add "remove columns" operation
        
        Args:
            name: Step name
            parent_id: Upstream node ID
            columns: List of column names to remove
            
        Returns:
            str: Node ID
        """
        return self.add_clean_step(name, parent_id, [{"type": "remove", "columns": columns}])
    
    def add_value_filter(
        self,
        name: str,
        parent_id: str,
        field: str,
        values: List[str],
        exclude: bool = False
    ) -> str:
        """
        Add value filter operation (keep or exclude by values)
        
        Args:
            name: Step name
            parent_id: Upstream node ID
            field: Field name to filter
            values: List of values to keep (or exclude)
            exclude: If True, exclude these values; False keeps only these values
            
        Returns:
            str: Node ID
        """
        node_id = str(uuid.uuid4())
        filter_node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "clean"})
        
        # Build filter expression with proper single quotes for string values
        # Format: NOT ((([field] == 'value1') OR ([field] == 'value2')) AND NOT (ISNULL([field])))
        # or for keep: (([field] == 'value1') OR ([field] == 'value2')) AND NOT (ISNULL([field]))
        conditions = [f"([{field}] == '{v}')" for v in values]
        if len(conditions) == 1:
            inner_expr = conditions[0]
        else:
            inner_expr = " OR ".join(conditions)
            inner_expr = f"({inner_expr})"
        
        # Add ISNULL check
        full_expr = f"({inner_expr} AND NOT (ISNULL([{field}])))"
        
        # Wrap with NOT if excluding
        if exclude:
            filter_expr = f"NOT ({full_expr})"
        else:
            filter_expr = full_expr
        
        action_name = f"{'Exclude' if exclude else 'Keep'} {field}:{values[0] if len(values) == 1 else 'multiple'}"
        
        self.nodes[node_id] = {
            "nodeType": ".v1.Container",
            "name": name,
            "id": node_id,
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [filter_node_id],
                "nodes": {
                    filter_node_id: {
                        "nodeType": ".v1.FilterOperation",
                        "name": action_name,
                        "id": filter_node_id,
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                        "filterExpression": filter_expr
                    }
                },
                "connections": {},
                "dataConnections": {},
                "connectionIds": [],
                "dataConnectionIds": [],
                "nodeProperties": {},
                "extensibility": None
            },
            "namespacesToInput": {"Default": {"nodeId": filter_node_id, "namespace": "Default"}},
            "namespacesToOutput": {"Default": {"nodeId": filter_node_id, "namespace": "Default"}},
            "providedParameters": None
        }
        
        self.nodes[parent_id]["nextNodes"].append({
            "namespace": "Default",
            "nextNodeId": node_id,
            "nextNamespace": "Default"
        })
        return node_id
    
    def add_calculation(
        self,
        name: str,
        parent_id: str,
        column_name: str,
        formula: str
    ) -> str:
        """
        Add calculated field
        
        Args:
            name: Clean step name
            parent_id: Upstream node ID
            column_name: New calculated field name
            formula: Tableau calculation formula, supports:
                - Conditional: IF [Amount] > 99 THEN 1 ELSE 0 END
                - String: UPPER([Name])
                - Math: [Price] * [Quantity]
                - Date: DATEPART('year', [Order Date])
            
        Returns:
            str: Clean step node ID
        """
        node_id = str(uuid.uuid4())
        calc_node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "clean"})
        
        self.nodes[node_id] = {
            "nodeType": ".v1.Container",
            "name": name,
            "id": node_id,
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [calc_node_id],
                "nodes": {
                    calc_node_id: {
                        "nodeType": ".v1.AddColumn",
                        "columnName": column_name,
                        "expression": formula,
                        "name": f"Add {column_name}",
                        "id": calc_node_id,
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None
                    }
                },
                "connections": {},
                "dataConnections": {},
                "connectionIds": [],
                "dataConnectionIds": [],
                "nodeProperties": {},
                "extensibility": None
            },
            "namespacesToInput": {"Default": {"nodeId": calc_node_id, "namespace": "Default"}},
            "namespacesToOutput": {"Default": {"nodeId": calc_node_id, "namespace": "Default"}},
            "providedParameters": None
        }
        
        self.nodes[parent_id]["nextNodes"].append({
            "namespace": "Default",
            "nextNodeId": node_id,
            "nextNamespace": "Default"
        })
        return node_id

    def add_keep_only(self, name: str, parent_id: str, columns: List[str]) -> str:
        """
        Add "keep only columns" operation (shortcut method)
        
        Args:
            name: Step name
            parent_id: Upstream node ID
            columns: List of column names to keep
            
        Returns:
            str: Node ID
        """
        return self.add_clean_step(name, parent_id, [{"type": "keep_only", "columns": columns}])
    
    def add_rename(self, parent_id: str, renames: Dict[str, str]) -> str:
        """
        Add rename columns operation (shortcut method)
        
        Args:
            parent_id: Upstream node ID
            renames: Rename mapping {old_column_name: new_column_name}
            
        Returns:
            str: Node ID
        """
        actions = [{"type": "rename", "from": old, "to": new} for old, new in renames.items()]
        return self.add_clean_step("rename", parent_id, actions)
    
    def add_filter(
        self,
        name: str,
        parent_id: str,
        expression: str
    ) -> str:
        """
        Add filter node
        
        Args:
            name: Filter name
            parent_id: Upstream node ID
            expression: Filter expression (Tableau calculation syntax)
                Example: "[Amount] > 100"
                Example: "NOT ISNULL([Customer Name])"
                Example: "[Status] = \"Completed\" OR [Status] = \"In Progress\""
            
        Returns:
            str: Filter node ID
        """
        node_id = str(uuid.uuid4())
        filter_node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "clean"})
        
        self.nodes[node_id] = {
            "nodeType": ".v1.Container",
            "name": name,
            "id": node_id,
            "baseType": "container",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "loomContainer": {
                "parameters": {"parameters": {}},
                "initialNodes": [filter_node_id],
                "nodes": {
                    filter_node_id: {
                        "nodeType": ".v1.FilterOperation",
                        "name": "Filter",
                        "id": filter_node_id,
                        "baseType": "transform",
                        "nextNodes": [],
                        "serialize": False,
                        "description": None,
                        "filterExpression": expression
                    }
                },
                "connections": {},
                "dataConnections": {},
                "connectionIds": [],
                "dataConnectionIds": [],
                "nodeProperties": {},
                "extensibility": None
            },
            "namespacesToInput": {"Default": {"nodeId": filter_node_id, "namespace": "Default"}},
            "namespacesToOutput": {"Default": {"nodeId": filter_node_id, "namespace": "Default"}},
            "providedParameters": None
        }
        
        self.nodes[parent_id]["nextNodes"].append({
            "namespace": "Default",
            "nextNodeId": node_id,
            "nextNamespace": "Default"
        })
        return node_id
    
    def add_aggregate(
        self,
        name: str,
        parent_id: str,
        group_by: List[str],
        aggregations: List[Dict[str, str]] = None
    ) -> str:
        """
        Add aggregate step
        
        Args:
            name: Aggregate step name
            parent_id: Upstream node ID
            group_by: List of grouping fields
            aggregations: List of aggregation fields, each element is a dict:
                - field: Field name
                - function: Aggregation function (SUM, AVG, COUNT, MIN, MAX, MEDIAN, COUNTD, STDEV, VAR)
                - output_name: Output column name (optional)
            
        Returns:
            str: Aggregate step node ID
        """
        node_id = str(uuid.uuid4())
        action_node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "aggregate"})
        self.features.add("node.v2018_2_3.SuperAggregate")
        
        # Build grouping fields
        group_by_fields = [
            {
                "columnName": col,
                "function": "GroupBy",
                "newColumnName": None,
                "specialFieldType": None
            }
            for col in group_by
        ]
        
        # Build aggregation fields
        aggregate_fields = []
        if aggregations:
            for agg in aggregations:
                aggregate_fields.append({
                    "columnName": agg["field"],
                    "function": agg["function"],
                    "newColumnName": agg.get("output_name"),
                    "specialFieldType": None
                })
        
        self.nodes[node_id] = {
            "nodeType": ".v2018_2_3.SuperAggregate",
            "name": name,
            "id": node_id,
            "baseType": "superNode",
            "nextNodes": [],
            "serialize": False,
            "description": None,
            "beforeActionAnnotations": [],
            "afterActionAnnotations": [],
            "actionNode": {
                "nodeType": ".v1.Aggregate",
                "name": name,
                "id": action_node_id,
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None,
                "groupByFields": group_by_fields,
                "aggregateFields": aggregate_fields
            }
        }
        
        self.nodes[parent_id]["nextNodes"].append({
            "namespace": "Default",
            "nextNodeId": node_id,
            "nextNamespace": "Default"
        })
        return node_id

    def _calculate_layout(self) -> Dict[str, Any]:
        """Calculate node layout"""
        layout = {}
        
        # Input nodes arranged vertically
        input_nodes = [n for n in self._node_order if n["type"] == "input"]
        for i, node_info in enumerate(input_nodes):
            layout[node_info["id"]] = {
                "color": {"hexCss": "#EFC637", "rgba": ["239", "198", "55", "1"]},
                "position": {"x": 0, "y": i + 1},
                "size": {"width": 1, "height": 1}
            }
        
        # Other nodes arranged horizontally
        other_nodes = [n for n in self._node_order if n["type"] != "input"]
        mid_y = len(input_nodes) // 2 + 1 if input_nodes else 1
        
        for i, node_info in enumerate(other_nodes):
            if node_info["type"] == "join":
                color = {"hexCss": "#499893", "rgba": ["73", "152", "147", "1"]}
            elif node_info["type"] == "output":
                color = {"hexCss": "#E76E50", "rgba": ["231", "110", "80", "1"]}
            else:
                color = {"hexCss": "#3D7FA6", "rgba": ["61", "127", "166", "1"]}
            
            layout[node_info["id"]] = {
                "color": color,
                "position": {"x": i + 1, "y": mid_y},
                "size": {"width": 1, "height": 1}
            }
        
        return layout

    def build(self) -> tuple:
        """
        Build final TFL file components
        
        Returns:
            tuple: (flow, displaySettings, maestroMetadata) three JSON objects
        """
        connection_ids = list(self.connections.keys())
        
        flow = {
            "parameters": {"parameters": {}},
            "initialNodes": self.initial_nodes,
            "nodes": self.nodes,
            "connections": self.connections,
            "dataConnections": {},
            "connectionIds": connection_ids,
            "dataConnectionIds": [],
            "nodeProperties": self.node_properties,
            "extensibility": {},
            "selection": [],
            "majorVersion": 1,
            "minorVersion": 4,
            "documentId": self.doc_id,
            "obfuscatorId": self.obfuscator_id
        }
        
        display = {
            "majorVersion": 1, "minorVersion": 0,
            "flowDisplaySettings": {
                "flowGroupNodeDisplay": {},
                "flowSelection": {"type": "nothing", "selectedNodePath": None, "selectedType": None},
                "flowNodeDisplaySettings": self._calculate_layout()
            },
            "hiddenColumns": []
        }
        
        # Use version info from config
        v = {
            "year": self.config.prep_year, 
            "quarterOfYear": self.config.prep_quarter, 
            "versionString": self.config.prep_version, 
            "indexOfRelease": self.config.prep_release
        }
        
        meta = {
            "majorVersion": 1, "minorVersion": 0,
            "flowEntryName": "flow", 
            "displaySettingsEntryName": "displaySettings",
            "isPackagedMaestroDocument": False,
            "documentFeaturesUsedInDocument": [
                {
                    "id": f, 
                    "featureName": "",
                    "notSupportedMessages": [], 
                    "firstSoftwareVersionSupportedIn": v, 
                    "minimumCompatibleSoftwareVersion": v
                } for f in sorted(self.features)
            ]
        }
        return flow, display, meta