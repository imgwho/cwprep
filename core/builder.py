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