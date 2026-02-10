import uuid
import json
import time

class TFLBuilder:
    """
    TFL (Tableau Prep Flow) 构建器 SDK
    
    用于程序化生成 Tableau Prep 数据流程文件。
    
    使用示例:
        builder = TFLBuilder(flow_name="我的流程")
        conn_id = builder.add_connection("host", "user", "db")
        input1 = builder.add_input_sql("表1", "SELECT * FROM t1", conn_id)
        input2 = builder.add_input_sql("表2", "SELECT * FROM t2", conn_id)
        join = builder.add_join("联接", input1, input2, "id", "t1_id")
        builder.add_output_server("输出", join, "数据源名")
        flow, display, meta = builder.build()
    """
    
    def __init__(self, flow_name="订单员工"):
        """
        初始化 TFL 构建器
        
        Args:
            flow_name: 流程名称，将显示在 Tableau Prep 中
        """
        self.flow_name = flow_name
        self.nodes = {}
        self.initial_nodes = []
        self.connections = {}
        self.node_properties = {}
        self.doc_id = str(uuid.uuid4())
        self.obfuscator_id = str(uuid.uuid4())
        self.features = {"document.v2019_1_3.Flow", "nodeProperty.v2019_1_3.PrimaryKey"}
        # 布局跟踪
        self._node_order = []  # 记录节点添加顺序，用于布局计算
        self._input_count = 0  # 输入节点计数，用于 Y 坐标分配

    def add_connection(self, host, username, dbname):
        """
        添加数据库连接
        
        Args:
            host: 数据库主机地址
            username: 用户名
            dbname: 数据库名称
            
        Returns:
            str: 连接 ID，供后续输入节点引用
        """
        conn_id = str(uuid.uuid4())
        self.connections[conn_id] = {
            "connectionType": ".v1.SqlConnection",
            "id": conn_id,
            "name": host,
            "isPackaged": False,
            "connectionAttributes": {
                "sslmode": "", "odbc-connect-string-extras": "",
                "server": host, "prep-protocol-role": ":prep-protocol-reader",
                ":flow-name": self.flow_name, "odbc-dbms-name": "",
                ":use-allowlist": "false", "dbname": dbname, "port": "3306",
                ":protocol-customizations": "", "source-charset": "",
                "sslcert": "", "odbc-native-protocol": "",
                "expected-driver-version": "", "class": "mysql",
                "one-time-sql": "", "authentication": "", "username": username
            }
        }
        return conn_id

    def add_input_sql(self, name, sql, connection_id):
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
        
        self.nodes[node_id] = {
            "nodeType": ".v1.LoadSql", "name": name, "id": node_id,
            "baseType": "input", "nextNodes": [], "serialize": False, "description": None,
            "connectionId": connection_id, "connectionAttributes": {"dbname": "voxadmin"},
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

    def add_join(self, name, left_id, right_id, left_col, right_col, join_type="left"):
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
        
        # 必须在这里注册 PrimaryKey，Key 必须是 node_id
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
            "beforeActionAnnotations": [],  # Tableau Prep 必需字段
            "afterActionAnnotations": [],   # Tableau Prep 必需字段
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

    def add_output_server(self, name, parent_id, datasource_name, project_name="数据源"):
        """
        添加服务器输出节点
        
        Args:
            name: 输出节点名称
            parent_id: 上游节点 ID 
            datasource_name: 发布后的数据源名称
            project_name: Tableau Server 上的项目名称
            
        Returns:
            str: 输出节点 ID
        """
        node_id = str(uuid.uuid4())
        self._node_order.append({"id": node_id, "type": "output"})
        
        self.nodes[node_id] = {
            "nodeType": ".v1.PublishExtract", "name": name, "id": node_id,
            "baseType": "output", "nextNodes": [], "serialize": False, "description": None,
            "projectName": project_name, "projectLuid": "7fa01ebc-72f4-4fd0-a27f-f8ac1e30ac20",
            "datasourceName": datasource_name, "datasourceDescription": "",
            "serverUrl": "http://win-picvs30bivi"
        }
        self.nodes[parent_id]["nextNodes"].append({"namespace": "Default", "nextNodeId": node_id, "nextNamespace": "Default"})
        return node_id

    def _calculate_layout(self):
        """
        计算节点布局
        
        输入节点垂直排列在 x=0
        后续节点按添加顺序向右排列
        """
        layout = {}
        x_counter = 0
        
        # 首先处理输入节点 - 垂直排列
        input_nodes = [n for n in self._node_order if n["type"] == "input"]
        for i, node_info in enumerate(input_nodes):
            layout[node_info["id"]] = {
                "color": {"hexCss": "#EFC637", "rgba": ["239", "198", "55", "1"]},  # 黄色 - 输入
                "position": {"x": 0, "y": i + 1},
                "size": {"width": 1, "height": 1}
            }
        
        # 处理非输入节点 - 水平排列
        other_nodes = [n for n in self._node_order if n["type"] != "input"]
        mid_y = len(input_nodes) // 2 + 1 if input_nodes else 1
        
        for i, node_info in enumerate(other_nodes):
            if node_info["type"] == "join":
                color = {"hexCss": "#499893", "rgba": ["73", "152", "147", "1"]}  # 青色 - 联接
            elif node_info["type"] == "output":
                color = {"hexCss": "#E76E50", "rgba": ["231", "110", "80", "1"]}  # 橙色 - 输出
            else:
                color = {"hexCss": "#3D7FA6", "rgba": ["61", "127", "166", "1"]}  # 蓝色 - 默认
            
            layout[node_info["id"]] = {
                "color": color,
                "position": {"x": i + 1, "y": mid_y},
                "size": {"width": 1, "height": 1}
            }
        
        return layout

    def build(self):
        """
        构建最终的 TFL 文件组件
        
        Returns:
            tuple: (flow, displaySettings, maestroMetadata) 三个 JSON 对象
        """
        # 完整的 flow 结构 - 包含所有必需的顶级字段
        connection_ids = list(self.connections.keys())
        flow = {
            "parameters": {"parameters": {}},
            "initialNodes": self.initial_nodes,
            "nodes": self.nodes,
            "connections": self.connections,
            "dataConnections": {},  # 空对象，但必须存在
            "connectionIds": connection_ids,  # 连接 ID 列表
            "dataConnectionIds": [],  # 空列表
            "nodeProperties": self.node_properties,
            "extensibility": {},  # 空对象，但必须存在
            "selection": [],  # 空列表
            "majorVersion": 1,
            "minorVersion": 4,
            "documentId": self.doc_id,
            "obfuscatorId": self.obfuscator_id
        }
        
        # 改进的 displaySettings：使用智能布局
        display = {
            "majorVersion": 1, "minorVersion": 0,
            "flowDisplaySettings": {
                "flowGroupNodeDisplay": {},
                "flowSelection": {"type": "nothing", "selectedNodePath": None, "selectedType": None},
                "flowNodeDisplaySettings": self._calculate_layout()
            },
            "hiddenColumns": []
        }
        
        # maestroMetadata - 特性声明
        v = {"year": 2019, "quarterOfYear": 1, "versionString": "2019.1.3", "indexOfRelease": 3}
        meta = {
            "majorVersion": 1, "minorVersion": 0,
            "flowEntryName": "flow", 
            "displaySettingsEntryName": "displaySettings",
            "isPackagedMaestroDocument": False,
            "documentFeaturesUsedInDocument": [
                {
                    "id": f, 
                    "featureName": "",  # 添加空的 featureName
                    "notSupportedMessages": [], 
                    "firstSoftwareVersionSupportedIn": v, 
                    "minimumCompatibleSoftwareVersion": v
                } for f in sorted(self.features)  # 排序保证顺序一致
            ]
        }
        return flow, display, meta