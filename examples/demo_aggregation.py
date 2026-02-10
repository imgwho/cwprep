"""
cwprep Aggregation and Pivot Demo

Business Scenario: Regional Monthly Sales Analysis
- Union January and February orders
- Aggregate sales metrics by region
- Monthly comparison with Pivot/Unpivot

Features Covered:
1. add_union - Union merge
2. add_aggregate - Aggregation statistics
3. add_pivot - Rows to columns
4. add_unpivot - Columns to rows

Usage:
    python examples/demo_aggregation.py
"""

from cwprep import TFLBuilder, TFLPackager

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_aggregation_demo():
    print("=" * 50)
    print("cwprep Aggregation and Pivot Demo")
    print("=" * 50)
    print()
    
    builder = TFLBuilder(flow_name="Regional Sales Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"[OK] Add database connection")
    
    # 1. Add January orders
    jan_id = builder.add_input_sql(
        name="Orders Jan 2024",
        sql="""SELECT 
'2024-01' AS month_label,
r.region_name,
r.manager_name,
o.sales,
o.profit
FROM orders o
JOIN regions r ON o.region_id = r.region_id
WHERE o.order_date >= '2024-01-01' 
AND o.order_date < '2024-02-01'""",
        connection_id=conn_id
    )
    print(f"[OK] [1] add_input_sql: January orders")
    
    # 2. Add February orders
    feb_id = builder.add_input_sql(
        name="Orders Feb 2024",
        sql="""SELECT 
'2024-02' AS month_label,
r.region_name,
r.manager_name,
o.sales,
o.profit
FROM orders o
JOIN regions r ON o.region_id = r.region_id
WHERE o.order_date >= '2024-02-01' 
AND o.order_date < '2024-03-01'""",
        connection_id=conn_id
    )
    print(f"[OK] [2] add_input_sql: February orders")
    
    # 3. Union merge
    union_id = builder.add_union(
        name="Union Jan + Feb",
        parent_ids=[jan_id, feb_id]
    )
    print(f"[OK] [3] add_union: Merge Jan-Feb orders")
    
    # 4. Aggregate by region and month
    agg_id = builder.add_aggregate(
        name="Aggregate by Region",
        parent_id=union_id,
        group_by=["region_name", "manager_name", "month_label"],
        aggregations=[
            {"field": "sales", "function": "SUM", "output_name": "total_sales"},
            {"field": "sales", "function": "COUNT", "output_name": "order_count"},
            {"field": "sales", "function": "AVG", "output_name": "avg_order_value"},
            {"field": "profit", "function": "SUM", "output_name": "total_profit"}
        ]
    )
    print(f"[OK] [4] add_aggregate: Regional monthly summary (SUM/COUNT/AVG)")
    
    # 5. Rename fields
    rename_id = builder.add_rename(
        parent_id=agg_id,
        renames={
            "region_name": "Region",
            "manager_name": "Manager",
            "month_label": "Month"
        }
    )
    print(f"[OK] [5] add_rename: Rename fields")
    
    # 6. Pivot: monthly comparison
    pivot_id = builder.add_pivot(
        name="Pivot by Month",
        parent_id=rename_id,
        pivot_column="Month",
        aggregate_column="total_sales",
        new_columns=["2024-01", "2024-02"],
        group_by=["Region", "Manager"],
        aggregation="SUM"
    )
    print(f"[OK] [6] add_pivot: Rows to columns (Month -> Columns)")
    
    # 7. Unpivot
    unpivot_id = builder.add_unpivot(
        name="Unpivot Months",
        parent_id=pivot_id,
        columns_to_unpivot=["2024-01", "2024-02"],
        name_column="Month",
        value_column="Sales"
    )
    print(f"[OK] [7] add_unpivot: Columns to rows")
    
    # Output
    output_id = builder.add_output_server(
        name="Output",
        parent_id=unpivot_id,
        datasource_name="Regional_Sales_Report"
    )
    print(f"[OK] [8] add_output_server: Add output")
    
    # Build
    print()
    flow, display, meta = builder.build()
    
    output_folder = "./demo_output/aggregation"
    output_tfl = "./demo_output/aggregation.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"[OK] Generated: {output_tfl}")
    print()
    print("Features covered:")
    print("  - add_union: Merge multi-month data")
    print("  - add_aggregate: SUM/COUNT/AVG")
    print("  - add_pivot: Rows to columns")
    print("  - add_unpivot: Columns to rows")


if __name__ == "__main__":
    run_aggregation_demo()
