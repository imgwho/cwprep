import uuid
import json
import time

class TFLBuilder:
    def __init__(self, flow_name="订单员工"):
        self.flow_name = flow_name
        self.nodes = {}
        self.initial_nodes = []
        self.connections = {}
        self.node_properties = {}
        self.doc_id = str(uuid.uuid4())
        self.obfuscator_id = str(uuid.uuid4())
        self.features = {"document.v2019_1_3.Flow", "nodeProperty.v2019_1_3.PrimaryKey"}
        self.current_x = 0

    def add_connection(self, host, username, dbname):
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
        node_id = str(uuid.uuid4())
        self.nodes[node_id] = {
            "nodeType": ".v1.LoadSql", "name": name, "id": node_id,
            "baseType": "input", "nextNodes": [], "serialize": False, "description": None,
            "connectionId": connection_id, "connectionAttributes": {"dbname": "voxadmin"},
            "fields": None, "actions": [], "debugModeRowLimit": None,
            "originalDataTypes": {}, "randomSampling": None,
            "updateTimestamp": None, "relation": {"type": "query", "query": sql}
        }
        self.initial_nodes.append(node_id)
        return node_id

    def add_join(self, name, left_id, right_id, left_col, right_col):
        node_id = str(uuid.uuid4())
        self.features.add("node.v2018_2_3.SuperJoin")
        
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
            "actionNode": {
                "nodeType": ".v1.SimpleJoin", "name": name, "id": str(uuid.uuid4()),
                "baseType": "transform", "nextNodes": [], "serialize": False, "description": None,
                "conditions": [{"leftExpression": f"[{left_col}]", "rightExpression": f"[{right_col}]", "comparator": "=="}],
                "joinType": "left"
            }
        }
        self.nodes[left_id]["nextNodes"].append({"namespace": "Default", "nextNodeId": node_id, "nextNamespace": "Left"})
        self.nodes[right_id]["nextNodes"].append({"namespace": "Default", "nextNodeId": node_id, "nextNamespace": "Right"})
        return node_id

    def add_output_server(self, name, parent_id, datasource_name):
        node_id = str(uuid.uuid4())
        self.nodes[node_id] = {
            "nodeType": ".v1.PublishExtract", "name": name, "id": node_id,
            "baseType": "output", "nextNodes": [], "serialize": False, "description": None,
            "projectName": "数据源", "projectLuid": "7fa01ebc-72f4-4fd0-a27f-f8ac1e30ac20",
            "datasourceName": datasource_name, "serverUrl": "http://win-picvs30bivi"
        }
        self.nodes[parent_id]["nextNodes"].append({"namespace": "Default", "nextNodeId": node_id, "nextNamespace": "Default"})
        return node_id

    def build(self):
        flow = {
            "parameters": {"parameters": {}}, "initialNodes": self.initial_nodes,
            "nodes": self.nodes, "connections": self.connections,
            "nodeProperties": self.node_properties, "majorVersion": 1, "minorVersion": 4,
            "documentId": self.doc_id, "obfuscatorId": self.obfuscator_id
        }
        
        # 极简 displaySettings：只保留节点坐标，删除 fieldOrder
        display = {
            "majorVersion": 1, "minorVersion": 0,
            "flowDisplaySettings": {
                "flowGroupNodeDisplay": {},
                "flowSelection": {"type": "nothing", "selectedNodePath": None, "selectedType": None},
                "flowNodeDisplaySettings": {
                    nid: {
                        "color": {"hexCss": "#3D7FA6", "rgba": ["61", "127", "166", "1"]},
                        "position": {"x": i, "y": 1}, "size": {"width": 1, "height": 1}
                    } for i, nid in enumerate(self.nodes.keys())
                }
            },
            "hiddenColumns": []
        }
        
        v = {"year": 2019, "quarterOfYear": 1, "versionString": "2019.1.3", "indexOfRelease": 3}
        meta = {
            "majorVersion": 1, "minorVersion": 0,
            "flowEntryName": "flow", "displaySettingsEntryName": "displaySettings",
            "isPackagedMaestroDocument": False,
            "documentFeaturesUsedInDocument": [{"id": f, "notSupportedMessages": [], "firstSoftwareVersionSupportedIn": v, "minimumCompatibleSoftwareVersion": v} for f in self.features]
        }
        return flow, display, meta