"""
TFL (Tableau Prep Flow) 构建器 SDK

用于程序化生成 Tableau Prep 数据流程文件。

使用示例:
    from core.builder import TFLBuilder
    from core.config import DEFAULT_CONFIG
    
    builder = TFLBuilder(flow_name="我的流程", config=DEFAULT_CONFIG)
    conn_id = builder.add_connection("host", "user", "db")
    input1 = builder.add_input_sql("表1", "SELECT * FROM t1", conn_id)
    input2 = builder.add_input_sql("表2", "SELECT * FROM t2", conn_id)
    join = builder.add_join("联接", input1, input2, "id", "t1_id")
    builder.add_output_server("输出", join, "数据源名")
    flow, display, meta = builder.build()
"""

import uuid
from typing import Optional, List, Dict, Any

from .config import TFLConfig, DEFAULT_CONFIG, DatabaseConfig


class TFLBuilder:
    """
    TFL 构建器
    
    Args:
        flow_name: 流程名称，将显示在 Tableau Prep 中
        config: TFL 配置对象，默认使用 DEFAULT_CONFIG
    """
    
    def __init__(self, flow_name: str = "订单员工", config: Optional[TFLConfig] = None):
        self.flow_name = flow_name
        self.config = config or DEFAULT_CONFIG
        
        self.nodes: Dict[str, Any] = {}
        self.initial_nodes: List[str] = []
        self.connections: Dict[str, Any] = {}
        self.node_properties: Dict[str, Any] = {}
        self.doc_id = str(uuid.uuid4())
        self.obfuscator_id = str(uuid.uuid4())
        self.features = {"document.v2019_1_3.Flow", "nodeProperty.v2019_1_3.PrimaryKey"}
        
        # 布局跟踪
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
        添加数据库连接
        
        Args:
            host: 数据库主机地址
            username: 用户名
            dbname: 数据库名称
            port: 端口号（默认从配置读取）
            db_class: 数据库类型 mysql/postgres/oracle（默认从配置读取）
            **kwargs: 其他连接属性
            
        Returns:
            str: 连接 ID，供后续输入节点引用
        """
        conn_id = str(uuid.uuid4())
        
        # 使用传入参数或配置默认值
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
        使用配置文件中的默认数据库连接
        
        Returns:
            str: 连接 ID
            
        Raises:
            ValueError: 如果配置中没有数据库设置
        """
        if not self.config.database:
            raise ValueError("配置中没有默认数据库设置")
        
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
        添加 SQL 输入节点
        
        Args:
            name: 节点名称（通常是表名）
            sql: SQL 查询语句
            connection_id: 数据库连接 ID
            
        Returns:
            str: 节点 ID，供后续操作引用
        """
        node_id = str(uuid.uuid4())
        self._input_count += 1
        self._node_order.append({"id": node_id, "type": "input", "y_hint": self._input_count})
        
        # 获取默认数据库名
        default_dbname = "voxadmin"
        if self.config.database:
            default_dbname = self.config.database.dbname
        
        self.nodes[node_id] = {
            "nodeType": ".v1.LoadSql", "name": name, "id": node_id,
            "baseType": "input", "nextNodes": [], "serialize": False, "description": None,
            "connectionId": connection_id, 
            "connectionAttributes": {"dbname": default_dbname},
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
        添加联接节点
        
        Args:
            name: 联接节点名称
            left_id: 左表节点 ID
            right_id: 右表节点 ID
            left_col: 左表关联列名
            right_col: 右表关联列名
            join_type: 联接类型 ("left", "right", "inner", "full")
            
        Returns:
            str: 联接节点 ID
        """
        node_id = str(uuid.uuid4())
        self.features.add("node.v2018_2_3.SuperJoin")
        self._node_order.append({"id": node_id, "type": "join"})
        
        # 必须在这里注册 PrimaryKey
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

    def add_output_server(
        self, 
        name: str, 
        parent_id: str, 
        datasource_name: str, 
        project_name: str = None,
        server_url: str = None
    ) -> str:
        """
        添加服务器输出节点
        
        Args:
            name: 输出节点名称
            parent_id: 上游节点 ID 
            datasource_name: 发布后的数据源名称
            project_name: Tableau Server 上的项目名称（默认从配置读取）
            server_url: Tableau Server URL（默认从配置读取）
            
        Returns:
            str: 输出节点 ID
        """
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "output"})
        
        # 使用传入参数或配置默认值
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
        添加清理步骤（Container）
        
        Args:
            name: 清理步骤名称
            parent_id: 上游节点 ID
            actions: 清理操作列表，每个操作是一个字典，包含：
                - type: 操作类型 (rename, keep_only, remove, 等)
                - 其他参数取决于操作类型
            
        Returns:
            str: 清理步骤节点 ID
        """
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "clean"})
        
        # 构建内部节点
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
        """创建单个清理操作节点"""
        action_type = action.get("type")
        
        if action_type == "keep_only":
            return {
                "nodeType": ".v2019_2_2.KeepOnlyColumns",
                "name": f"只保留 {action['columns'][0]} + 另外 {len(action['columns'])-1} 个",
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
                "name": f"将 {action['from']} 重命名为 {action['to']}",
                "id": node_id,
                "baseType": "transform",
                "nextNodes": [],
                "serialize": False,
                "description": None
            }
        else:
            raise ValueError(f"不支持的操作类型: {action_type}")
    
    def add_keep_only(self, name: str, parent_id: str, columns: List[str]) -> str:
        """
        添加"只保留列"操作（快捷方法）
        
        Args:
            name: 步骤名称
            parent_id: 上游节点 ID
            columns: 要保留的列名列表
            
        Returns:
            str: 节点 ID
        """
        return self.add_clean_step(name, parent_id, [{"type": "keep_only", "columns": columns}])
    
    def add_rename(self, parent_id: str, renames: Dict[str, str]) -> str:
        """
        添加重命名列操作（快捷方法）
        
        Args:
            parent_id: 上游节点 ID
            renames: 重命名映射 {旧列名: 新列名}
            
        Returns:
            str: 节点 ID
        """
        actions = [{"type": "rename", "from": old, "to": new} for old, new in renames.items()]
        return self.add_clean_step("重命名", parent_id, actions)
    
    def add_filter(
        self,
        name: str,
        parent_id: str,
        expression: str
    ) -> str:
        """
        添加筛选器节点
        
        Args:
            name: 筛选器名称
            parent_id: 上游节点 ID
            expression: 筛选表达式（Tableau 计算语法）
                示例: "[金额] > 100"
                示例: "NOT ISNULL([客户名])"
                示例: "[状态] = \"已完成\" OR [状态] = \"进行中\""
            
        Returns:
            str: 筛选器节点 ID
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
                        "name": "筛选器",
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
        添加聚合步骤
        
        Args:
            name: 聚合步骤名称
            parent_id: 上游节点 ID
            group_by: 分组字段列表
            aggregations: 聚合字段列表，每个元素是字典：
                - field: 字段名
                - function: 聚合函数 (SUM, AVG, COUNT, MIN, MAX, MEDIAN, COUNTD, STDEV, VAR)
                - output_name: 输出列名（可选）
            
        Returns:
            str: 聚合步骤节点 ID
        """
        node_id = str(uuid.uuid4())
        action_node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "aggregate"})
        self.features.add("node.v2018_2_3.SuperAggregate")
        
        # 构建分组字段
        group_by_fields = [
            {
                "columnName": col,
                "function": "GroupBy",
                "newColumnName": None,
                "specialFieldType": None
            }
            for col in group_by
        ]
        
        # 构建聚合字段
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
        """计算节点布局"""
        layout = {}
        
        # 输入节点垂直排列
        input_nodes = [n for n in self._node_order if n["type"] == "input"]
        for i, node_info in enumerate(input_nodes):
            layout[node_info["id"]] = {
                "color": {"hexCss": "#EFC637", "rgba": ["239", "198", "55", "1"]},
                "position": {"x": 0, "y": i + 1},
                "size": {"width": 1, "height": 1}
            }
        
        # 其他节点水平排列
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
        构建最终的 TFL 文件组件
        
        Returns:
            tuple: (flow, displaySettings, maestroMetadata) 三个 JSON 对象
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
        
        # 使用配置中的版本信息
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