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
    assert __version__ == "0.1.0"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
