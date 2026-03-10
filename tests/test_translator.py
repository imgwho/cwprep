"""
cwprep SQL Translator unit tests.

Tests the expression translator and SQL translator without real databases.
"""

import json
import zipfile

import pytest

from cwprep.expression_translator import ExpressionTranslator
from cwprep.translator import SQLTranslator
from cwprep import TFLBuilder


# ── ExpressionTranslator Tests ───────────────────────────────────────────────


class TestExpressionTranslator:
    """Test Tableau → SQL expression translation."""

    def setup_method(self):
        self.t = ExpressionTranslator()

    # --- Field references ---
    def test_field_ref(self):
        assert '"Name"' in self.t.translate("[Name]")

    def test_field_ref_with_spaces(self):
        assert '"Order Date"' in self.t.translate("[Order Date]")

    # --- Operators ---
    def test_double_equals(self):
        result = self.t.translate("[Status] == 'Active'")
        assert "==" not in result
        assert "= 'Active'" in result

    def test_not_equals_preserved(self):
        result = self.t.translate("[A] <> [B]")
        assert "<>" in result

    # --- IF / THEN / ELSE / END ---
    def test_if_then_else(self):
        result = self.t.translate(
            "IF [Profit] > 0 THEN 'Good' ELSE 'Bad' END"
        )
        assert "CASE WHEN" in result
        assert "THEN 'Good'" in result
        assert "ELSE 'Bad'" in result
        assert "END" in result

    def test_elseif(self):
        result = self.t.translate(
            "IF [A] > 0 THEN 'pos' ELSEIF [A] = 0 THEN 'zero' ELSE 'neg' END"
        )
        assert "CASE WHEN" in result
        assert "WHEN" in result
        assert "ELSEIF" not in result

    # --- IIF ---
    def test_iif(self):
        result = self.t.translate("IIF([Profit] > 0, 'Profit', 'Loss')")
        assert "CASE WHEN" in result
        assert "THEN 'Profit'" in result
        assert "ELSE 'Loss'" in result

    # --- ISNULL ---
    def test_isnull(self):
        result = self.t.translate("ISNULL([Name])")
        assert "IS NULL" in result

    # --- IFNULL / ZN ---
    def test_ifnull(self):
        result = self.t.translate("IFNULL([Profit], 0)")
        assert "COALESCE" in result

    def test_zn(self):
        result = self.t.translate("ZN([Profit])")
        assert "COALESCE" in result
        assert ", 0)" in result

    # --- String functions ---
    def test_contains(self):
        result = self.t.translate("CONTAINS([Name], 'test')")
        assert "LIKE" in result
        assert "'%'" in result

    def test_startswith(self):
        result = self.t.translate("STARTSWITH([Name], 'abc')")
        assert "LIKE" in result

    def test_endswith(self):
        result = self.t.translate("ENDSWITH([Name], 'xyz')")
        assert "LIKE" in result

    def test_len(self):
        result = self.t.translate("LEN([Name])")
        assert "LENGTH" in result

    def test_proper(self):
        result = self.t.translate("PROPER([Name])")
        assert "INITCAP" in result

    def test_mid(self):
        result = self.t.translate("MID([Name], 2, 5)")
        assert "SUBSTRING" in result
        assert "FROM 2" in result
        assert "FOR 5" in result

    def test_find(self):
        result = self.t.translate("FIND([Name], 'abc')")
        assert "POSITION" in result

    # --- Date functions ---
    def test_datepart(self):
        result = self.t.translate("DATEPART('year', [Order Date])")
        assert "EXTRACT" in result
        assert "year" in result.lower()

    def test_year_month_day(self):
        result = self.t.translate("YEAR([Date])")
        assert "EXTRACT(YEAR FROM" in result

    def test_now_today(self):
        assert "CURRENT_TIMESTAMP" in self.t.translate("NOW()")
        assert "CURRENT_DATE" in self.t.translate("TODAY()")

    def test_dateadd(self):
        result = self.t.translate("DATEADD('month', 3, [Date])")
        assert "INTERVAL" in result
        assert "MONTH" in result

    def test_datetrunc(self):
        result = self.t.translate("DATETRUNC('month', [Date])")
        assert "DATE_TRUNC(" in result

    # --- Type cast ---
    def test_int_cast(self):
        result = self.t.translate("INT([Value])")
        assert "CAST" in result
        assert "INTEGER" in result

    def test_str_cast(self):
        result = self.t.translate("STR([Age])")
        assert "CAST" in result
        assert "VARCHAR" in result

    # --- Aggregate ---
    def test_countd(self):
        result = self.t.translate("COUNTD([Region])")
        assert "COUNT(DISTINCT" in result

    # --- Unsupported ---
    def test_regexp_unsupported(self):
        result = self.t.translate("REGEXP_REPLACE([Name], 'a', 'b')")
        assert "UNSUPPORTED" in result

    # --- Composite ---
    def test_composite_expression(self):
        result = self.t.translate(
            "IF ISNULL([Name]) THEN 'Unknown' ELSE UPPER([Name]) END"
        )
        assert "CASE WHEN" in result
        assert "IS NULL" in result
        assert "UPPER" in result


# ── SQLTranslator Tests ──────────────────────────────────────────────────────


class TestSQLTranslator:
    """Test TFL flow → SQL translation."""

    def _make_simple_flow(self):
        """Build a simple flow: input → output."""
        builder = TFLBuilder(flow_name="Test Flow")
        conn_id = builder.add_connection("localhost", "root", "testdb")
        input_id = builder.add_input_table("orders", "orders", conn_id)
        builder.add_output_server("Output", input_id, "Test_DS")
        flow, _, _ = builder.build()
        return flow

    def _make_join_flow(self):
        """Build a join flow: 2 inputs → join → output."""
        builder = TFLBuilder(flow_name="Join Flow")
        conn_id = builder.add_connection("localhost", "root", "testdb")
        orders = builder.add_input_table("orders", "orders", conn_id)
        customers = builder.add_input_table("customers", "customers", conn_id)
        joined = builder.add_join(
            "Join", orders, customers, "customer_id", "id"
        )
        builder.add_output_server("Output", joined, "Joined_DS")
        flow, _, _ = builder.build()
        return flow

    def _make_filter_flow(self):
        """Build a flow with filter: input → filter → output."""
        builder = TFLBuilder(flow_name="Filter Flow")
        conn_id = builder.add_connection("localhost", "root", "testdb")
        orders = builder.add_input_sql(
            "orders", "SELECT * FROM orders", conn_id
        )
        filtered = builder.add_filter(
            "Filter Active", orders, "[Status] == 'Active'"
        )
        builder.add_output_server("Output", filtered, "Filtered_DS")
        flow, _, _ = builder.build()
        return flow

    def _make_aggregate_flow(self):
        """Build a flow with aggregation."""
        builder = TFLBuilder(flow_name="Agg Flow")
        conn_id = builder.add_connection("localhost", "root", "testdb")
        orders = builder.add_input_table("orders", "orders", conn_id)
        agg = builder.add_aggregate(
            "Summary", orders,
            group_by=["Region"],
            aggregations=[
                {"field": "Sales", "function": "SUM", "output_name": "Total_Sales"},
            ],
        )
        builder.add_output_server("Output", agg, "Agg_DS")
        flow, _, _ = builder.build()
        return flow

    def _make_calculation_flow(self):
        """Build a flow with a calculated field."""
        builder = TFLBuilder(flow_name="Calc Flow")
        conn_id = builder.add_connection("localhost", "root", "testdb")
        orders = builder.add_input_table("orders", "orders", conn_id)
        calc = builder.add_calculation(
            "Add Tax", orders, "amount_with_tax", "[Amount] * 1.1"
        )
        builder.add_output_server("Output", calc, "Calc_DS")
        flow, _, _ = builder.build()
        return flow

    def _make_union_flow(self):
        """Build a flow with union."""
        builder = TFLBuilder(flow_name="Union Flow")
        conn_id = builder.add_connection("localhost", "root", "testdb")
        t1 = builder.add_input_table("table1", "table1", conn_id)
        t2 = builder.add_input_table("table2", "table2", conn_id)
        union = builder.add_union("Merge", [t1, t2])
        builder.add_output_server("Output", union, "Union_DS")
        flow, _, _ = builder.build()
        return flow

    # --- Tests ---

    def test_simple_flow(self):
        flow = self._make_simple_flow()
        sql = SQLTranslator().translate_flow(flow, flow_name="Test Flow")
        assert "WITH" in sql
        assert "SELECT *" in sql
        assert "[orders]" in sql
        assert "FROM" in sql

    def test_simple_flow_no_comments(self):
        flow = self._make_simple_flow()
        sql = SQLTranslator(
            include_comments=False, include_summary=False
        ).translate_flow(flow)
        assert "-- [Step" not in sql
        assert "═══" not in sql

    def test_join_flow(self):
        flow = self._make_join_flow()
        sql = SQLTranslator().translate_flow(flow, flow_name="Join Flow")
        assert "JOIN" in sql
        assert "ON" in sql

    def test_filter_flow(self):
        flow = self._make_filter_flow()
        sql = SQLTranslator().translate_flow(flow, flow_name="Filter Flow")
        assert "WHERE" in sql

    def test_aggregate_flow(self):
        flow = self._make_aggregate_flow()
        sql = SQLTranslator().translate_flow(flow, flow_name="Agg Flow")
        assert "GROUP BY" in sql
        assert "SUM" in sql

    def test_calculation_flow(self):
        flow = self._make_calculation_flow()
        sql = SQLTranslator().translate_flow(flow, flow_name="Calc Flow")
        assert "amount_with_tax" in sql
        assert "1.1" in sql

    def test_union_flow(self):
        flow = self._make_union_flow()
        sql = SQLTranslator().translate_flow(flow, flow_name="Union Flow")
        assert "UNION ALL" in sql

    def test_summary_header(self):
        flow = self._make_simple_flow()
        sql = SQLTranslator().translate_flow(flow, flow_name="My Flow")
        assert "Flow: My Flow" in sql
        assert "步骤明细" in sql

    def test_translate_tfl_file(self, workspace_tmp_dir):
        """Test translating from a .tfl file."""
        # Build a flow and save as .tfl
        builder = TFLBuilder(flow_name="File Test")
        conn_id = builder.add_connection("localhost", "root", "testdb")
        input_id = builder.add_input_table("orders", "orders", conn_id)
        builder.add_output_server("Output", input_id, "Test_DS")
        flow, display, meta = builder.build()

        tmp_path = workspace_tmp_dir / "translator_input.tfl"
        with zipfile.ZipFile(tmp_path, "w") as zf:
            zf.writestr("flow", json.dumps(flow))
            zf.writestr("displaySettings", json.dumps(display))
            zf.writestr("maestroMetadata", json.dumps(meta))

        sql = SQLTranslator().translate_tfl_file(str(tmp_path))
        assert "WITH" in sql
        assert "SELECT *" in sql

    def test_empty_flow(self):
        sql = SQLTranslator().translate_flow({})
        assert "Empty flow" in sql or "empty" in sql.lower()


# ── MCP Integration Tests ────────────────────────────────────────────────────


class TestTranslateToSqlTool:
    """Test the translate_to_sql MCP tool."""

    def setup_method(self):
        # Only run if mcp is installed
        pytest.importorskip("mcp")

    def test_translate_from_definition(self):
        from cwprep.mcp_server import translate_to_sql

        connection = {
            "type": "database",
            "host": "localhost",
            "username": "root",
            "dbname": "testdb",
        }
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {
                "type": "output_server",
                "name": "Output",
                "parent": "orders",
                "datasource_name": "Test",
            },
        ]
        result = translate_to_sql(
            flow_name="MCP Test", connection=connection, nodes=nodes
        )
        assert "WITH" in result
        assert "SELECT" in result

    def test_translate_missing_args(self):
        from cwprep.mcp_server import translate_to_sql

        result = translate_to_sql()
        assert "Error" in result

    def test_translate_with_join(self):
        from cwprep.mcp_server import translate_to_sql

        connection = {
            "type": "database",
            "host": "localhost",
            "username": "root",
            "dbname": "testdb",
        }
        nodes = [
            {"type": "input_table", "name": "orders", "table": "orders"},
            {"type": "input_table", "name": "customers", "table": "customers"},
            {
                "type": "join",
                "name": "Joined",
                "left": "orders",
                "right": "customers",
                "left_col": "customer_id",
                "right_col": "id",
            },
            {
                "type": "output_server",
                "name": "Output",
                "parent": "Joined",
                "datasource_name": "Test",
            },
        ]
        result = translate_to_sql(
            flow_name="Join Test", connection=connection, nodes=nodes
        )
        assert "JOIN" in result
        assert "ON" in result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
