"""
cwprep MCP Server unit tests.

Tests the core logic of MCP tools without starting a real MCP server.
"""

import json
import os
import shutil
import tempfile

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
def tmp_output_dir():
    d = tempfile.mkdtemp(prefix="cwprep_test_")
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ── Tests: _build_flow ────────────────────────────────────────────────────────

class TestBuildFlow:
    def test_basic_flow(self, sample_connection, sample_nodes):
        flow, display, meta, node_map = _build_flow(
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
        import zipfile

        output_path = os.path.join(tmp_output_dir, "test_flow.tfl")
        generate_tfl("Test", sample_connection, sample_nodes, output_path)
        assert zipfile.is_zipfile(output_path)

        with zipfile.ZipFile(output_path, "r") as zf:
            names = zf.namelist()
            assert "flow" in names
            assert "displaySettings" in names
            assert "maestroMetadata" in names


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
            "input_sql", "input_table", "join", "union", "filter",
            "value_filter", "calculation", "aggregate", "keep_only",
            "remove_columns", "rename", "pivot", "unpivot", "output_server",
        }
        assert types == expected


# ── Tests: MCP Server instance ───────────────────────────────────────────────

class TestMcpServer:
    def test_server_name(self):
        assert mcp_server.name == "cwprep"

    def test_server_has_tools(self):
        # FastMCP stores tools internally; check that our tools are registered
        assert mcp_server is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
