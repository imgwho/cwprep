"""
cwprep 基础单元测试

这些测试不需要真实数据库，只验证 SDK 生成的 JSON 结构正确。
"""

import pytest
import uuid
import zipfile


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
    assert __version__ == "0.5.4"


def test_default_prep_version():
    """???? Prep ????? 2024.2.0"""
    from cwprep.config import TFLConfig

    config = TFLConfig()
    assert config.prep_version == "2024.2.0"
    assert config.prep_year == 2024
    assert config.prep_quarter == 2
    assert config.prep_release == 0


def test_build_uses_default_prep_version():
    """?? build() ???? 2024.2.0 metadata"""
    from cwprep import TFLBuilder

    builder = TFLBuilder(flow_name="Version Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    input_id = builder.add_input_table("orders", "orders", conn_id)
    builder.add_output_server("Output", input_id, "DS")

    _flow, _display, meta = builder.build()
    feature_versions = meta["documentFeaturesUsedInDocument"]
    assert feature_versions
    for feature in feature_versions:
        assert feature["firstSoftwareVersionSupportedIn"]["versionString"] == "2024.2.0"
        assert feature["minimumCompatibleSoftwareVersion"]["versionString"] == "2024.2.0"


def test_save_to_folder_backs_up_existing_directory(workspace_tmp_dir):
    """?? save_to_folder() ????????"""
    from cwprep import TFLPackager

    folder_path = workspace_tmp_dir / "out"
    TFLPackager.save_to_folder(
        str(folder_path),
        {"nodes": {"legacy": True}},
        {"flowDisplaySettings": {"legacy": True}},
        {"flowEntryName": "legacy_flow"},
    )

    TFLPackager.save_to_folder(
        str(folder_path),
        {"nodes": {"fresh": True}},
        {"flowDisplaySettings": {"fresh": True}},
        {"flowEntryName": "flow"},
    )

    backups = [path for path in workspace_tmp_dir.iterdir() if path.name.startswith("out.bak-")]
    assert len(backups) == 1
    assert backups[0].is_dir()
    assert (backups[0] / "flow").exists()
    assert (folder_path / "flow").exists()
    assert (folder_path / "displaySettings").exists()
    assert (folder_path / "maestroMetadata").exists()


def test_save_to_folder_uses_incrementing_backup_suffix(monkeypatch, workspace_tmp_dir):
    """????????????????"""
    from cwprep import TFLPackager

    class FixedDateTime:
        @staticmethod
        def now():
            class FixedNow:
                @staticmethod
                def strftime(_fmt):
                    return "20240101120000"

            return FixedNow()

    monkeypatch.setattr("cwprep.packager.datetime", FixedDateTime)

    folder_path = workspace_tmp_dir / "out"
    for marker in ("first", "second", "third"):
        TFLPackager.save_to_folder(
            str(folder_path),
            {"nodes": {marker: True}},
            {"flowDisplaySettings": {marker: True}},
            {"flowEntryName": marker},
        )

    assert (workspace_tmp_dir / "out.bak-20240101120000").is_dir()
    assert (workspace_tmp_dir / "out.bak-20240101120000-1").is_dir()


def test_pack_zip_removes_source_folder_by_default(workspace_tmp_dir):
    """?? pack_zip() ?????????"""
    from cwprep import TFLPackager

    folder_path = workspace_tmp_dir / "exploded"
    archive_path = workspace_tmp_dir / "flow.tfl"

    TFLPackager.save_to_folder(
        str(folder_path),
        {"nodes": {"fresh": True}},
        {"flowDisplaySettings": {"fresh": True}},
        {"flowEntryName": "flow"},
    )
    TFLPackager.pack_zip(str(folder_path), str(archive_path))

    assert archive_path.exists()
    assert not folder_path.exists()
    with zipfile.ZipFile(archive_path, "r") as zf:
        assert {"flow", "displaySettings", "maestroMetadata"}.issubset(set(zf.namelist()))


def test_pack_zip_can_keep_source_folder(workspace_tmp_dir):
    """?? pack_zip(keep_folder=True) ???????"""
    from cwprep import TFLPackager

    folder_path = workspace_tmp_dir / "exploded"
    archive_path = workspace_tmp_dir / "flow.tfl"

    TFLPackager.save_to_folder(
        str(folder_path),
        {"nodes": {"fresh": True}},
        {"flowDisplaySettings": {"fresh": True}},
        {"flowEntryName": "flow"},
    )
    TFLPackager.pack_zip(str(folder_path), str(archive_path), keep_folder=True)

    assert archive_path.exists()
    assert folder_path.exists()


def test_save_tfl_creates_archive_without_leaving_folder(workspace_tmp_dir):
    """?? save_tfl() ?????????"""
    from cwprep import TFLPackager

    archive_path = workspace_tmp_dir / "flow.tfl"

    TFLPackager.save_tfl(
        str(archive_path),
        {"nodes": {"fresh": True}},
        {"flowDisplaySettings": {"fresh": True}},
        {"flowEntryName": "flow"},
    )

    assert archive_path.exists()
    assert [path for path in workspace_tmp_dir.iterdir() if path.is_dir()] == []


def test_save_tflx_embeds_data_without_leaving_folder(workspace_tmp_dir):
    """?? save_tflx() ??????????????"""
    from cwprep import TFLPackager

    source_file = workspace_tmp_dir / "orders.csv"
    archive_path = workspace_tmp_dir / "flow.tflx"
    source_file.write_text("order_id,amount\n1,120\n", encoding="utf-8")

    TFLPackager.save_tflx(
        str(archive_path),
        {"nodes": {"fresh": True}},
        {"flowDisplaySettings": {"fresh": True}},
        {"flowEntryName": "flow"},
        data_files={"conn-1": [str(source_file)]},
    )

    assert archive_path.exists()
    assert [path for path in workspace_tmp_dir.iterdir() if path.is_dir()] == []
    with zipfile.ZipFile(archive_path, "r") as zf:
        assert "Data/conn-1/orders.csv" in set(zf.namelist())


# ====================== File Connection Tests ======================

def test_add_file_connection_excel():
    """测试添加 Excel 文件连接"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection("orders.xlsx")
    
    assert conn_id in builder.connections
    conn = builder.connections[conn_id]
    assert conn["connectionType"] == ".v1.SqlConnection"
    assert conn["connectionAttributes"]["class"] == "excel-direct"
    assert conn["isPackaged"] == False
    # Non-packaged should have directory attribute
    assert "directory" in conn["connectionAttributes"]
    assert conn["name"] == "orders.xlsx"


def test_add_file_connection_csv():
    """测试添加 CSV 文件连接"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection("data.csv")
    
    conn = builder.connections[conn_id]
    assert conn["connectionAttributes"]["class"] == "textscan"
    assert conn["name"] == "data.csv"


def test_add_file_connection_auto_class():
    """测试自动检测文件类型"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    
    # .xlsx -> excel-direct
    xlsx_conn = builder.add_file_connection("test.xlsx")
    assert builder.connections[xlsx_conn]["connectionAttributes"]["class"] == "excel-direct"
    
    # .xls -> excel-direct
    xls_conn = builder.add_file_connection("test.xls")
    assert builder.connections[xls_conn]["connectionAttributes"]["class"] == "excel-direct"
    
    # .csv -> textscan
    csv_conn = builder.add_file_connection("test.csv")
    assert builder.connections[csv_conn]["connectionAttributes"]["class"] == "textscan"
    
    # Unknown extension -> ValueError
    with pytest.raises(ValueError, match="Cannot auto-detect"):
        builder.add_file_connection("test.json")


def test_add_file_connection_packaged():
    """测试 is_packaged 标记 — 只存 basename"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection(
        "C:\\Users\\test\\data\\orders.xlsx", is_packaged=True
    )
    conn = builder.connections[conn_id]
    assert conn["isPackaged"] == True
    # Packaged should only store basename
    assert conn["connectionAttributes"]["filename"] == "orders.xlsx"
    assert conn["name"] == "orders.xlsx"
    assert "directory" not in conn["connectionAttributes"]


def test_add_file_connection_non_packaged():
    """测试非 packaged 模式 — 完整路径 + directory"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection("orders.xlsx", is_packaged=False)
    conn = builder.connections[conn_id]
    assert conn["isPackaged"] == False
    # Non-packaged should have full path and directory
    attrs = conn["connectionAttributes"]
    assert "directory" in attrs
    assert attrs["filename"].endswith("orders.xlsx")


def test_add_input_excel():
    """测试添加 Excel 输入节点"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection("orders.xlsx")
    
    input_id = builder.add_input_excel(
        name="订单",
        sheet_name="Sheet1",
        connection_id=conn_id,
        fields=[
            {"name": "订单ID", "type": "string"},
            {"name": "金额", "type": "real"},
        ]
    )
    
    assert input_id in builder.nodes
    node = builder.nodes[input_id]
    assert node["nodeType"] == ".v1.LoadExcel"
    assert node["baseType"] == "input"
    assert node["connectionId"] == conn_id
    assert node["relation"]["table"] == "[Sheet1$]"
    assert len(node["fields"]) == 2
    assert node["fields"][0]["name"] == "订单ID"
    assert input_id in builder.initial_nodes


def test_add_input_csv():
    """测试添加 CSV 输入节点"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection("data.csv")
    
    input_id = builder.add_input_csv(
        name="数据",
        connection_id=conn_id,
        charset="UTF-8",
        locale="zh_CN",
    )
    
    assert input_id in builder.nodes
    node = builder.nodes[input_id]
    assert node["nodeType"] == ".v1.LoadCsv"
    assert node["baseType"] == "input"
    assert node["charSet"] == "UTF-8"
    assert node["locale"] == "zh_CN"
    assert node["containsHeaders"] == True
    # connectionAttributes should be empty (not filename/class)
    assert node["connectionAttributes"] == {}


def test_add_input_csv_union():
    """测试添加 CSV 并集输入节点"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection("orders_2015.csv")
    
    input_id = builder.add_input_csv_union(
        name="所有订单",
        connection_id=conn_id,
        file_names=["orders_2015.csv", "orders_2016.csv", "orders_2017.csv"],
    )
    
    assert input_id in builder.nodes
    node = builder.nodes[input_id]
    assert node["nodeType"] == ".v1.LoadCsvInputUnion"
    assert len(node["generatedInputs"]) == 3
    for i, gi in enumerate(node["generatedInputs"]):
        assert gi["inputUnionInputType"] == ".FileInputUnionInput"
        assert gi["inputNode"]["nodeType"] == ".v1.LoadCsv"
        assert gi["inputNode"]["connectionAttributes"] == {}
        assert gi["filePath"] == f"orders_201{5+i}.csv"


def test_add_join_multi_column():
    """测试多字段 join"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_connection("localhost", "root", "test_db")
    
    input1 = builder.add_input_sql("Returns", "SELECT * FROM returns", conn_id)
    input2 = builder.add_input_sql("Orders", "SELECT * FROM orders", conn_id)
    
    join_id = builder.add_join(
        name="Join",
        left_id=input1,
        right_id=input2,
        left_col=["产品 ID", "订单 ID"],
        right_col=["产品 ID", "订单 ID"],
    )
    
    assert join_id in builder.nodes
    conditions = builder.nodes[join_id]["actionNode"]["conditions"]
    assert len(conditions) == 2
    assert conditions[0]["leftExpression"] == "[产品 ID]"
    assert conditions[0]["rightExpression"] == "[产品 ID]"
    assert conditions[1]["leftExpression"] == "[订单 ID]"
    assert conditions[1]["rightExpression"] == "[订单 ID]"


def test_build_packaged():
    """测试 build(is_packaged=True) 标记"""
    from cwprep import TFLBuilder
    
    builder = TFLBuilder(flow_name="Test")
    conn_id = builder.add_file_connection("data.xlsx")
    builder.add_input_excel("Sheet1", "Sheet1", conn_id)
    builder.add_output_server("Output", list(builder.nodes.keys())[0], "DS")
    
    flow, display, meta = builder.build(is_packaged=True)
    
    # maestroMetadata should be marked as packaged
    assert meta["isPackagedMaestroDocument"] == True
    
    # File connections should be marked as packaged
    for conn in flow["connections"].values():
        conn_class = conn.get("connectionAttributes", {}).get("class", "")
        if conn_class in ("excel-direct", "textscan"):
            assert conn["isPackaged"] == True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

