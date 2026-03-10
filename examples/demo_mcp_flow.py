"""Generate review-friendly demo TFL and TFLX files for the MCP flow APIs."""

from __future__ import annotations

import json
from pathlib import Path

try:
    from cwprep.mcp_server import generate_tfl, validate_flow_definition
except ImportError as exc:
    raise SystemExit(
        "This demo requires the optional MCP dependency. Install with `pip install -e .[mcp]`."
    ) from exc


FLOW_NAME = "MCP Review Demo"


def build_nodes(orders_filename: str, customers_filename: str):
    return [
        {"type": "input_csv", "name": "orders", "filename": orders_filename},
        {"type": "input_csv", "name": "customers", "filename": customers_filename},
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
            "name": "completed_only",
            "parent": "joined",
            "expression": "[status] = 'completed'",
        },
        {
            "type": "calculation",
            "name": "amount_bucket",
            "parent": "completed_only",
            "column_name": "amount_bucket",
            "formula": "IF [amount] >= 150 THEN 'large' ELSE 'standard' END",
        },
        {
            "type": "output_server",
            "name": "output",
            "parent": "amount_bucket",
            "datasource_name": "MCP_Review_Demo",
        },
    ]


def validate_or_raise(connection, nodes):
    result = json.loads(validate_flow_definition(FLOW_NAME, connection, nodes))
    if result["valid"]:
        return
    raise SystemExit("Validation failed:\n" + "\n".join(result["errors"]))


def main():
    repo_root = Path(__file__).resolve().parents[1]
    data_dir = Path(__file__).resolve().parent / "demo_data"
    output_dir = repo_root / "demo_output"
    output_dir.mkdir(exist_ok=True)

    preview_nodes = build_nodes(
        str(data_dir / "mcp_orders.csv"),
        str(data_dir / "mcp_customers.csv"),
    )
    packaged_nodes = build_nodes("mcp_orders.csv", "mcp_customers.csv")
    connection = {"type": "file"}

    validate_or_raise(connection, packaged_nodes)

    preview_path = output_dir / "mcp_review_demo.tfl"
    packaged_path = output_dir / "mcp_review_demo.tflx"

    print(generate_tfl(FLOW_NAME, connection, preview_nodes, str(preview_path)))
    print(
        generate_tfl(
            FLOW_NAME,
            connection,
            packaged_nodes,
            str(packaged_path),
            data_files={
                "mcp_orders.csv": [str(data_dir / "mcp_orders.csv")],
                "mcp_customers.csv": [str(data_dir / "mcp_customers.csv")],
            },
        )
    )
    print(f"Preview TFL: {preview_path}")
    print(f"Packaged TFLX: {packaged_path}")
    print("Open the packaged TFLX in Tableau Prep if you want a self-contained review artifact.")


if __name__ == "__main__":
    main()
