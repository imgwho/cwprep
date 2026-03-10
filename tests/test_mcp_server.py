"""
cwprep MCP Server unit tests.

Tests the core logic of MCP tools without starting a real MCP server.
"""

import json
import os
import zipfile

import pytest


# Skip all tests if mcp is not installed
pytest.importorskip("mcp")


from cwprep.mcp_server import (
    _build_flow,
    generate_tfl,
    list_supported_operations,
    validate_flow_definition,
    mcp as mcp_server,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_connection():
    return {
        "host": "localhost",
        "username": "root",
        "dbname": "test_db",
    }


@pytest.fixture
def sample_nodes():
    return [
        {"type": "input_sql", "name": "orders", "sql": "SELECT * FROM orders"},
        {"type": "input_table", "name": "customers", "table": "customers"},
        {
            "type": "join",
            "name": "joined",
            "left": "orders",
            "right": "customers",
            "left_col": "customer_id",
            "right_col": "id",
            "join_type": "left",
        },
        {
            "type": "filter",
            "name": "filtered",
            "parent": "joined",
            "expression": "[Amount] > 100",
        },
        {
            "type": "output_server",
            "name": "output",
            "parent": "filtered",
            "datasource_name": "Test_Output",
        },
    ]


@pytest.fixture
def tmp_output_dir(workspace_tmp_dir):
    return str(workspace_tmp_dir)


# ── Tests: _build_flow ────────────────────────────────────────────────────────

class TestBuildFlow:
    def test_basic_flow(self, sample_connection, sample_nodes):
        flow, display, meta, node_map, _file_conns = _build_flow(
            "Test Flow", sample_connection, sample_nodes
        )
        assert "nodes" in flow
        assert "connections" in flow
        assert len(node_map) == 5
        assert "orders" in node_map
        assert "customers" in node_map
        assert "joined" in node_map
        assert "output" in node_map

    def test_unknown_node_type(self, sample_connection):
        nodes = [{"type": "unknown_type", "name": "bad"}]
        with pytest.raises(ValueError, match="Unknown node type"):
            _build_flow("Test", sample_connection, nodes)


# ── Tests: generate_tfl ──────────────────────────────────────────────────────

class TestGenerateTfl:
    def test_creates_tfl_file(self, sample_connection, sample_nodes, tmp_output_dir):
        output_path = os.path.join(tmp_output_dir, "test_flow.tfl")
        result = generate_tfl(
            flow_name="Test Flow",
            connection=sample_connection,
            nodes=sample_nodes,
            output_path=output_path,
        )
        assert os.path.exists(output_path)
        assert "Successfully generated" in result
        assert "Test Flow" in result

    def test_output_is_valid_zip(self, sample_connection, sample_nodes, tmp_output_dir):
        output_path = os.path.join(tmp_output_dir, "test_flow.tfl")
        generate_tfl("Test", sample_connection, sample_nodes, output_path)
        assert zipfile.is_zipfile(output_path)

        with zipfile.ZipFile(output_path, "r") as zf:
            names = zf.namelist()
            assert "flow" in names
            assert "displaySettings" in names
            assert "maestroMetadata" in names

    def test_existing_output_is_backed_up(self, sample_connection, sample_nodes, tmp_output_dir):
        output_path = os.path.join(tmp_output_dir, "test_flow.tfl")
        with open(output_path, "wb") as f:
            f.write(b"legacy")

        generate_tfl("Test Flow", sample_connection, sample_nodes, output_path)

        backups = [name for name in os.listdir(tmp_output_dir) if name.startswith("test_flow.tfl.bak-")]
        assert len(backups) == 1
        with open(os.path.join(tmp_output_dir, backups[0]), "rb") as f:
            assert f.read() == b"legacy"

    def test_existing_tflx_is_backed_up(self, sample_connection, sample_nodes, tmp_output_dir):
        output_path = os.path.join(tmp_output_dir, "test_flow.tflx")
        with open(output_path, "wb") as f:
            f.write(b"legacy")

        generate_tfl("Test Flow", sample_connection, sample_nodes, output_path)

        backups = [name for name in os.listdir(tmp_output_dir) if name.startswith("test_flow.tflx.bak-")]
        assert len(backups) == 1
        assert os.path.exists(output_path)

    def test_tflx_embeds_packaged_csv_files(self, workspace_tmp_dir):
        source_csv = workspace_tmp_dir / "orders.csv"
        source_csv.write_text("order_id,amount\n1,120\n2,85\n", encoding="utf-8")
        output_path = workspace_tmp_dir / "file_flow.tflx"
        connection = {"type": "file"}
        nodes = [
            {"type": "input_csv", "name": "orders", "filename": "orders.csv"},
            {
                "type": "output_server",
                "name": "output",
                "parent": "orders",
                "datasource_name": "File_Output",
            },
        ]

        result = generate_tfl(
            "File Flow",
            connection,
            nodes,
            str(output_path),
            data_files={"orders.csv": [str(source_csv)]},
        )

        assert "Successfully generated TFLX" in result
        assert zipfile.is_zipfile(output_path)
        with zipfile.ZipFile(output_path, "r") as zf:
            names = set(zf.namelist())
            assert "flow" in names
            data_members = [
                name for name in names
                if name.startswith("Data/") and name.endswith("/orders.csv")
            ]
            assert len(data_members) == 1
            flow = json.loads(zf.read("flow").decode("utf-8"))

        conn = next(iter(flow["connections"].values()))
        assert conn["isPackaged"] is True
        assert conn["connectionAttributes"]["filename"] == "orders.csv"
        assert "directory" not in conn["connectionAttributes"]

    def test_invalid_flow_raises_before_writing_output(self, sample_connection, tmp_output_dir):
        output_path = os.path.join(tmp_output_dir, "invalid_flow.tfl")
        nodes = [
            {"type": "input_table", "name": "orders", "table": "   "},
            {
                "type": "output_server",
                "name": "output",
                "parent": "orders",
                "datasource_name": "Test_Output",
            },
        ]

        with pytest.raises(ValueError, match="Invalid flow definition"):
            generate_tfl("Test Flow", sample_connection, nodes, output_path)

        assert not os.path.exists(output_path)
        leftovers = [
            name for name in os.listdir(tmp_output_dir)
            if name.startswith("cwprep_build_") or name.startswith("cwprep_output_")
        ]
        assert leftovers == []

    def test_invalid_flow_does_not_touch_existing_output(self, sample_connection, tmp_output_dir):
        output_path = os.path.join(tmp_output_dir, "existing_invalid.tfl")
        with open(output_path, "wb") as f:
            f.write(b"legacy")

        nodes = [
            {"type": "input_table", "name": "orders", "table": "   "},
            {"type": "output_server", "name": "output", "parent": "orders", "datasource_name": "Test_Output"},
        ]

        with pytest.raises(ValueError, match="Invalid flow definition"):
            generate_tfl("Test Flow", sample_connection, nodes, output_path)

        with open(output_path, "rb") as f:
            assert f.read() == b"legacy"
        backups = [name for name in os.listdir(tmp_output_dir) if name.startswith("existing_invalid.tfl.bak-")]
        assert backups == []


# ── Tests: validate_flow_definition ──────────────────────────────────────────

class TestValidateFlowDefinition:
    def test_valid_flow(self, sample_connection, sample_nodes):
        result = json.loads(
            validate_flow_definition("Test", sample_connection, sample_nodes)
        )
        assert result["valid"] is True
        assert result["errors"] == []

    def test_missing_connection_fields(self, sample_nodes):
        result = json.loads(
            validate_flow_definition("Test", {}, sample_nodes)
        )
        assert result["valid"] is False
        assert any("host" in e for e in result["errors"])

    def test_unknown_node_type(self, sample_connection):
        nodes = [{"type": "bad_type", "name": "bad"}]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is False
        assert any("unknown type" in e for e in result["errors"])

    def test_missing_parent_reference(self, sample_connection):
        nodes = [
            {"type": "input_sql", "name": "t1", "sql": "SELECT 1"},
            {"type": "filter", "name": "f1", "parent": "nonexistent", "expression": "[x] > 1"},
        ]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is False
        assert any("nonexistent" in e for e in result["errors"])

    def test_duplicate_node_name(self, sample_connection):
        nodes = [
            {"type": "input_sql", "name": "dup", "sql": "SELECT 1"},
            {"type": "input_sql", "name": "dup", "sql": "SELECT 2"},
        ]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is False
        assert any("duplicate" in e for e in result["errors"])

    def test_no_output_warning(self, sample_connection):
        nodes = [{"type": "input_sql", "name": "t1", "sql": "SELECT 1"}]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is False
        assert any("output" in e.lower() for e in result["errors"])

    def test_empty_nodes(self, sample_connection):
        result = json.loads(
            validate_flow_definition("Test", sample_connection, [])
        )
        assert result["valid"] is False

    def test_empty_flow_name(self, sample_connection, sample_nodes):
        result = json.loads(
            validate_flow_definition("", sample_connection, sample_nodes)
        )
        assert result["valid"] is False

    def test_whitespace_flow_name(self, sample_connection, sample_nodes):
        result = json.loads(
            validate_flow_definition("   ", sample_connection, sample_nodes)
        )
        assert result["valid"] is False
        assert any("flow_name" in e for e in result["errors"])

    def test_whitespace_connection_host(self, sample_nodes):
        connection = {
            "type": "database",
            "host": "   ",
            "username": "root",
            "dbname": "test_db",
        }
        result = json.loads(
            validate_flow_definition("Test", connection, sample_nodes)
        )
        assert result["valid"] is False
        assert any("connection.host is required" in e for e in result["errors"])

    def test_input_table_requires_non_blank_table(self, sample_connection):
        nodes = [
            {"type": "input_table", "name": "orders", "table": "   "},
            {"type": "output_server", "name": "out", "parent": "orders", "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is False
        assert any("field 'table' must be a non-empty string" in e for e in result["errors"])

    def test_input_sql_requires_non_blank_sql(self, sample_connection):
        nodes = [
            {"type": "input_sql", "name": "orders", "sql": "   "},
            {"type": "output_server", "name": "out", "parent": "orders", "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is False
        assert any("field 'sql' must be a non-empty string" in e for e in result["errors"])

    def test_database_flow_requires_database_source(self):
        connection = {
            "type": "database",
            "host": "localhost",
            "username": "root",
            "dbname": "test_db",
        }
        nodes = [
            {"type": "input_csv", "name": "csv", "filename": "orders.csv"},
            {"type": "output_server", "name": "out", "parent": "csv", "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", connection, nodes)
        )
        assert result["valid"] is False
        assert any("at least one valid input_sql.sql or input_table.table" in e for e in result["errors"])

    def test_database_flow_accepts_valid_input_sql_source(self, sample_connection):
        nodes = [
            {"type": "input_sql", "name": "orders", "sql": "SELECT * FROM orders"},
            {"type": "output_server", "name": "out", "parent": "orders", "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is True

    def test_file_connection_rejects_database_inputs(self):
        connection = {"type": "file"}
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {"type": "output_server", "name": "out", "parent": "orders", "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", connection, nodes)
        )
        assert result["valid"] is False
        assert any("requires connection.type='database'" in e for e in result["errors"])

    def test_database_connection_rejects_file_inputs(self, sample_connection):
        nodes = [
            {"type": "input_excel", "name": "orders", "filename": "orders.xlsx", "sheet": "Sheet1"},
            {"type": "output_server", "name": "out", "parent": "orders", "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", sample_connection, nodes)
        )
        assert result["valid"] is False
        assert any("requires connection.type='file'" in e for e in result["errors"])

    def test_input_csv_union_rejects_blank_file_names(self):
        connection = {"type": "file"}
        nodes = [
            {
                "type": "input_csv_union",
                "name": "orders",
                "file_names": ["orders_2024.csv", "   "],
            },
            {"type": "output_server", "name": "out", "parent": "orders", "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", connection, nodes)
        )
        assert result["valid"] is False
        assert any("every file name must be a non-empty string" in e for e in result["errors"])


# ── Tests: list_supported_operations ─────────────────────────────────────────

class TestListSupportedOperations:
    def test_returns_valid_json(self):
        result = json.loads(list_supported_operations())
        assert isinstance(result, list)
        assert len(result) > 0

    def test_all_types_present(self):
        result = json.loads(list_supported_operations())
        types = {op["type"] for op in result}
        expected = {
            "input_sql", "input_table", "input_excel", "input_csv",
            "input_csv_union", "join", "union", "filter",
            "value_filter", "calculation", "aggregate", "keep_only",
            "remove_columns", "rename", "pivot", "unpivot",
            "quick_calc", "change_type", "duplicate_column",
            "output_server",
        }
        assert types == expected


# ── Tests: New node types via MCP ────────────────────────────────────────────

class TestNewNodeTypes:
    def test_build_flow_with_quick_calc(self, sample_connection):
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {
                "type": "quick_calc",
                "name": "lowercase_mode",
                "parent": "orders",
                "column_name": "ship_mode",
                "calc_type": "lowercase",
            },
            {
                "type": "output_server",
                "name": "output",
                "parent": "lowercase_mode",
                "datasource_name": "Test",
            },
        ]
        flow, display, meta, node_map, _fc = _build_flow("Test", sample_connection, nodes)
        assert "lowercase_mode" in node_map
        assert len(node_map) == 3

    def test_build_flow_with_change_type(self, sample_connection):
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {
                "type": "change_type",
                "name": "change_types",
                "parent": "orders",
                "fields": {"profit": "integer"},
            },
            {
                "type": "output_server",
                "name": "output",
                "parent": "change_types",
                "datasource_name": "Test",
            },
        ]
        flow, display, meta, node_map, _fc = _build_flow("Test", sample_connection, nodes)
        assert "change_types" in node_map

    def test_build_flow_with_duplicate(self, sample_connection):
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {
                "type": "duplicate_column",
                "name": "dup_col",
                "parent": "orders",
                "source_column": "row_id",
            },
            {
                "type": "output_server",
                "name": "output",
                "parent": "dup_col",
                "datasource_name": "Test",
            },
        ]
        flow, display, meta, node_map, _fc = _build_flow("Test", sample_connection, nodes)
        assert "dup_col" in node_map

    def test_validate_quick_calc(self, sample_connection):
        nodes = [
            {"type": "input_table", "name": "t1", "table": "t"},
            {"type": "quick_calc", "name": "qc", "parent": "t1", "column_name": "col", "calc_type": "uppercase"},
            {"type": "output_server", "name": "out", "parent": "qc", "datasource_name": "D"},
        ]
        result = json.loads(validate_flow_definition("Test", sample_connection, nodes))
        assert result["valid"] is True


# ── Tests: SQL Server connection via MCP ─────────────────────────────────────

class TestSqlServerConnection:
    def test_build_flow_sqlserver_sspi(self):
        connection = {
            "host": "localhost",
            "db_class": "sqlserver",
            "authentication": "sspi",
            "schema": "dbo",
        }
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {"type": "output_server", "name": "output", "parent": "orders",
             "datasource_name": "Test"},
        ]
        flow, display, meta, node_map, _fc = _build_flow("Test", connection, nodes)
        # Find the connection and verify its attributes
        conn = list(flow["connections"].values())[0]
        assert conn["connectionAttributes"]["class"] == "sqlserver"
        assert conn["connectionAttributes"]["authentication"] == "sspi"
        assert "port" not in conn["connectionAttributes"]
        # Verify table has schema prefix
        orders_node = flow["nodes"][node_map["orders"]]
        assert orders_node["relation"]["table"] == "[dbo].[orders]"

    def test_build_flow_sqlserver_username(self):
        connection = {
            "host": "localhost",
            "username": "sa",
            "db_class": "sqlserver",
            "authentication": "sqlserver",
            "schema": "dbo",
        }
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {"type": "output_server", "name": "output", "parent": "orders",
             "datasource_name": "Test"},
        ]
        flow, display, meta, node_map, _fc = _build_flow("Test", connection, nodes)
        conn = list(flow["connections"].values())[0]
        assert conn["connectionAttributes"]["authentication"] == "sqlserver"
        assert conn["connectionAttributes"]["username"] == "sa"

    def test_validate_sqlserver_sspi_no_username(self):
        """SQL Server SSPI should not require username"""
        connection = {
            "host": "localhost",
            "db_class": "sqlserver",
            "authentication": "sspi",
        }
        nodes = [
            {"type": "input_table", "name": "t1", "table": "t"},
            {"type": "output_server", "name": "out", "parent": "t1",
             "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", connection, nodes)
        )
        assert result["valid"] is True

    def test_validate_sqlserver_auth_requires_username(self):
        """SQL Server 'sqlserver' auth should require username"""
        connection = {
            "host": "localhost",
            "db_class": "sqlserver",
            "authentication": "sqlserver",
        }
        nodes = [
            {"type": "input_table", "name": "t1", "table": "t"},
            {"type": "output_server", "name": "out", "parent": "t1",
             "datasource_name": "D"},
        ]
        result = json.loads(
            validate_flow_definition("Test", connection, nodes)
        )
        assert result["valid"] is False
        assert any("username" in e for e in result["errors"])


# ── Tests: MCP Server instance ───────────────────────────────────────────────

class TestMcpServer:
    def test_server_name(self):
        assert mcp_server.name == "cwprep"

    def test_server_has_tools(self):
        # FastMCP stores tools internally; check that our tools are registered
        assert mcp_server is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

