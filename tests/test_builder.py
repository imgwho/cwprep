"""
cwprep 基础单元测试

这些测试不需要真实数据库，只验证 SDK 生成的 JSON 结构正确。
"""

import pytest
import uuid


def test_import():
    """测试包可以正常导入"""
    from cwprep import TFLBuilder, TFLPackager, TFLConfig
    assert TFLBuilder is not None
    assert TFLPackager is not None
    assert TFLConfig is not None


def test_builder_init():
    """测试 Builder 初始化"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="测试流程")
    assert builder.flow_name == "测试流程"
    assert builder.nodes == {}
    assert builder.connections == {}


def test_add_connection():
    """测试添加数据库连接"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection(
        host="localhost",
        username="root",
        dbname="test_db"
    )
    
    assert conn_id is not None
    assert conn_id in builder.connections
    assert builder.connections[conn_id]["connectionAttributes"]["server"] == "localhost"
    assert builder.connections[conn_id]["connectionAttributes"]["dbname"] == "test_db"


def test_add_input_sql():
    """测试添加 SQL 输入节点"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    
    input_id = builder.add_input_sql(
        name="Users",
        sql="SELECT * FROM users",
        connection_id=conn_id
    )
    
    assert input_id is not None
    assert input_id in builder.nodes
    assert builder.nodes[input_id]["nodeType"] == ".v1.LoadSql"
    assert builder.nodes[input_id]["relation"]["query"] == "SELECT * FROM users"


def test_add_join():
    """测试添加联接节点"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    
    input1 = builder.add_input_sql("Users", "SELECT * FROM users", conn_id)
    input2 = builder.add_input_sql("Orders", "SELECT * FROM orders", conn_id)
    
    join_id = builder.add_join(
        name="User Orders",
        left_id=input1,
        right_id=input2,
        left_col="id",
        right_col="user_id"
    )
    
    assert join_id is not None
    assert join_id in builder.nodes
    assert builder.nodes[join_id]["nodeType"] == ".v2018_2_3.SuperJoin"


def test_add_filter():
    """测试添加筛选器"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    input_id = builder.add_input_sql("Users", "SELECT * FROM users", conn_id)
    
    filter_id = builder.add_filter(
        name="Active Users",
        parent_id=input_id,
        expression="[status] = 'active'"
    )
    
    assert filter_id is not None
    assert filter_id in builder.nodes


def test_add_calculation():
    """测试添加计算字段"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    input_id = builder.add_input_sql("Orders", "SELECT * FROM orders", conn_id)
    
    calc_id = builder.add_calculation(
        name="Calculate Total",
        parent_id=input_id,
        column_name="total",
        formula="[price] * [quantity]"
    )
    
    assert calc_id is not None
    assert calc_id in builder.nodes


def test_add_quick_calc():
    """测试添加快速清理操作"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    input_id = builder.add_input_sql("Orders", "SELECT * FROM orders", conn_id)
    
    calc_id = builder.add_quick_calc(
        name="Lowercase Ship Mode",
        parent_id=input_id,
        column_name="ship_mode",
        calc_type="lowercase"
    )
    
    assert calc_id is not None
    assert calc_id in builder.nodes
    container = builder.nodes[calc_id]
    assert container["nodeType"] == ".v1.Container"
    # Check inner QuickCalcColumn node
    inner_nodes = container["loomContainer"]["nodes"]
    inner_node = list(inner_nodes.values())[0]
    assert inner_node["nodeType"] == ".v2024_2_0.QuickCalcColumn"
    assert inner_node["expression"] == "LOWER([ship_mode])"
    assert inner_node["calcExpressionType"] == "Lowercase"


def test_add_change_type():
    """测试更改列数据类型"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    input_id = builder.add_input_sql("Orders", "SELECT * FROM orders", conn_id)
    
    change_id = builder.add_change_type(
        name="Change Types",
        parent_id=input_id,
        fields={"profit": "integer", "quantity": "string"}
    )
    
    assert change_id is not None
    assert change_id in builder.nodes
    container = builder.nodes[change_id]
    inner_nodes = container["loomContainer"]["nodes"]
    # Should have 2 ChangeColumnType nodes chained
    assert len(inner_nodes) == 2
    types_found = set()
    for n in inner_nodes.values():
        assert n["nodeType"] == ".v1.ChangeColumnType"
        for col, info in n["fields"].items():
            types_found.add((col, info["type"]))
    assert ("profit", "integer") in types_found
    assert ("quantity", "string") in types_found


def test_add_duplicate_column():
    """测试复制列"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    input_id = builder.add_input_sql("Orders", "SELECT * FROM orders", conn_id)
    
    dup_id = builder.add_duplicate_column(
        name="Copy row_id",
        parent_id=input_id,
        source_column="row_id"
    )
    
    assert dup_id is not None
    assert dup_id in builder.nodes
    container = builder.nodes[dup_id]
    inner_nodes = container["loomContainer"]["nodes"]
    inner_node = list(inner_nodes.values())[0]
    assert inner_node["nodeType"] == ".v2019_2_3.DuplicateColumn"
    assert inner_node["columnName"] == "row_id-1"
    assert inner_node["expression"] == "[row_id]"


def test_add_connection_sqlserver_sspi():
    """测试 SQL Server SSPI (Windows 身份验证) 连接"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection(
        host="localhost",
        db_class="sqlserver",
        authentication="sspi"
    )
    
    attrs = builder.connections[conn_id]["connectionAttributes"]
    assert attrs["class"] == "sqlserver"
    assert attrs["authentication"] == "sspi"
    assert attrs["odbc-native-protocol"] == "yes"
    assert "port" not in attrs  # SQL Server has no port field
    assert "username" not in attrs  # SSPI doesn't need username
    assert ":protocol-clone-parent" in attrs
    assert "IsolationLevel" in attrs


def test_add_connection_sqlserver_username():
    """测试 SQL Server 用户名密码登录连接"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection(
        host="localhost",
        username="sa",
        db_class="sqlserver",
        authentication="sqlserver"
    )
    
    attrs = builder.connections[conn_id]["connectionAttributes"]
    assert attrs["class"] == "sqlserver"
    assert attrs["authentication"] == "sqlserver"
    assert attrs["username"] == "sa"  # username present for sqlserver auth
    assert "port" not in attrs
    assert attrs["odbc-native-protocol"] == "yes"


def test_add_input_table_with_schema():
    """测试带 schema 前缀的表名格式"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection(
        host="localhost",
        db_class="sqlserver",
        authentication="sspi"
    )
    
    input_id = builder.add_input_table("orders", "orders", conn_id, schema="dbo")
    assert builder.nodes[input_id]["relation"]["table"] == "[dbo].[orders]"


def test_add_connection_mysql_unchanged():
    """测试 MySQL 连接行为不变（回归测试）"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection(
        host="localhost",
        username="root",
        dbname="test_db"
    )
    
    attrs = builder.connections[conn_id]["connectionAttributes"]
    assert attrs["class"] == "mysql"
    assert attrs["username"] == "root"
    assert attrs["port"] == "3306"  # MySQL default port
    assert "source-charset" in attrs
    assert "sslcert" in attrs
    assert "IsolationLevel" not in attrs  # SQL Server only


def test_add_input_table_without_schema():
    """测试不带 schema 的表名格式保持不变"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection(
        host="localhost",
        username="root",
        dbname="test_db"
    )
    
    input_id = builder.add_input_table("orders", "orders", conn_id)
    assert builder.nodes[input_id]["relation"]["table"] == "[orders]"


def test_build_output():
    """测试构建输出结构"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test Flow")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    input_id = builder.add_input_sql("Users", "SELECT * FROM users", conn_id)
    builder.add_output_server("Output", input_id, "Test_Datasource")
    
    flow, display, meta = builder.build()
    
    # 验证 flow 结构
    assert "nodes" in flow
    assert "connections" in flow
    assert "initialNodes" in flow
    
    # 验证 displaySettings 结构
    assert "majorVersion" in display
    assert "flowDisplaySettings" in display
    
    # 验证 maestroMetadata 结构
    assert "majorVersion" in meta
    assert "flowEntryName" in meta


def test_version():
    """测试版本号"""
    from cwprep import __version__
    assert __version__ == "0.4.0"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
