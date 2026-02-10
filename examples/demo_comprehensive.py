"""
cwprep Comprehensive Demo: Sales Analysis Flow

Covers cwprep SDK core features:

1. add_connection - Database connection
2. add_input_table - Table input (customers, regions)
3. add_input_sql - SQL input (Jan/Feb orders with WHERE clause)
4. add_union - Union merge
5. add_join - Multi-table join (x2)
6. add_keep_only - Keep only columns
7. add_rename - Rename columns
8. add_value_filter - Value filter
9. add_calculation - Calculated fields (x2)
10. add_filter - Expression filter
11. add_remove_columns - Remove columns
12. add_clean_step - Clean step
13. add_aggregate - Aggregation
14. add_pivot - Rows to columns
15. add_unpivot - Columns to rows
16. add_output_server - Server output

Business Scenario:
- Merge January and February orders
- Join with customers and regions tables
- Calculate profit rate and order level
- Filter profitable orders
- Summarize by region and month
- Generate monthly comparison report

Usage:
    python examples/demo_comprehensive.py
"""

from cwprep import TFLBuilder, TFLPackager

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_comprehensive_demo():
    print("=" * 60)
    print("cwprep Comprehensive Demo: Sales Analysis Flow")
    print("=" * 60)
    print()
    
    # ========================================
    # 1. Initialize Builder and connection
    # ========================================
    builder = TFLBuilder(flow_name="Superstore Sales Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"[OK] [1] add_connection: {conn_id[:8]}...")
    
    # ========================================
    # 2. Input: January orders (custom SQL with WHERE clause)
    # ========================================
    jan_orders_id = builder.add_input_sql(
        name="Orders_Jan_2024",
        sql="""SELECT
'2024-01' AS month_label,
order_id,
order_date,
ship_mode,
customer_id,
region_id,
city,
state,
product_id,
sales,
quantity,
discount,
profit
FROM orders
WHERE order_date >= '2024-01-01'
AND order_date < '2024-02-01'""",
        connection_id=conn_id
    )
    print(f"[OK] [2] add_input_sql (Jan orders): {jan_orders_id[:8]}...")
    
    # ========================================
    # 3. Input: February orders (custom SQL)
    # ========================================
    feb_orders_id = builder.add_input_sql(
        name="Orders_Feb_2024",
        sql="""SELECT
'2024-02' AS month_label,
order_id,
order_date,
ship_mode,
customer_id,
region_id,
city,
state,
product_id,
sales,
quantity,
discount,
profit
FROM orders
WHERE order_date >= '2024-02-01'
AND order_date < '2024-03-01'""",
        connection_id=conn_id
    )
    print(f"[OK] [3] add_input_sql (Feb orders): {feb_orders_id[:8]}...")
    
    # ========================================
    # 4. Input: customers table (direct table connection)
    # ========================================
    customers_id = builder.add_input_table(
        name="customers",
        table_name="customers",
        connection_id=conn_id
    )
    print(f"[OK] [4] add_input_table (customers): {customers_id[:8]}...")
    
    # ========================================
    # 5. Input: regions table (direct table connection)
    # ========================================
    regions_id = builder.add_input_table(
        name="regions",
        table_name="regions",
        connection_id=conn_id
    )
    print(f"[OK] [5] add_input_table (regions): {regions_id[:8]}...")
    
    # ========================================
    # 6. Union: merge Jan and Feb orders
    # ========================================
    union_id = builder.add_union(
        name="Union_Jan_Feb",
        parent_ids=[jan_orders_id, feb_orders_id]
    )
    print(f"[OK] [6] add_union (merge orders): {union_id[:8]}...")
    
    # ========================================
    # 7. Join 1: orders + customers
    # ========================================
    join1_id = builder.add_join(
        name="Orders_Customers",
        left_id=union_id,
        right_id=customers_id,
        left_col="customer_id",
        right_col="customer_id",
        join_type="left"
    )
    print(f"[OK] [7] add_join (orders+customers): {join1_id[:8]}...")
    
    # ========================================
    # 8. Join 2: join with regions
    # ========================================
    join2_id = builder.add_join(
        name="Orders_Customers_Regions",
        left_id=join1_id,
        right_id=regions_id,
        left_col="region_id",
        right_col="region_id",
        join_type="left"
    )
    print(f"[OK] [8] add_join (with regions): {join2_id[:8]}...")
    
    # ========================================
    # 9. Keep only core columns
    # ========================================
    keep_only_id = builder.add_keep_only(
        name="Keep_Core_Fields",
        parent_id=join2_id,
        columns=["month_label", "order_id", "order_date", "ship_mode",
                 "customer_name", "segment", "region_name", "manager_name",
                 "city", "state", "sales", "quantity", "discount", "profit"]
    )
    print(f"[OK] [9] add_keep_only (core fields): {keep_only_id[:8]}...")
    
    # ========================================
    # 10. Rename columns
    # ========================================
    rename_id = builder.add_rename(
        parent_id=keep_only_id,
        renames={
            "month_label": "Month",
            "customer_name": "Customer",
            "segment": "Segment",
            "region_name": "Region",
            "manager_name": "Manager",
            "ship_mode": "Ship_Mode"
        }
    )
    print(f"[OK] [10] add_rename: {rename_id[:8]}...")
    
    # ========================================
    # 11. Value filter: exclude Same Day shipping
    # ========================================
    value_filter_id = builder.add_value_filter(
        name="Filter_Ship_Mode",
        parent_id=rename_id,
        field="Ship_Mode",
        values=["Same Day"],
        exclude=True
    )
    print(f"[OK] [11] add_value_filter (exclude Same Day): {value_filter_id[:8]}...")
    
    # ========================================
    # 12. Calculate profit rate
    # ========================================
    calc1_id = builder.add_calculation(
        name="Calc_Profit_Rate",
        parent_id=value_filter_id,
        column_name="Profit_Rate",
        formula="IF [sales] = 0 THEN 0 ELSE [profit] / [sales] END"
    )
    print(f"[OK] [12] add_calculation (profit rate): {calc1_id[:8]}...")
    
    # ========================================
    # 13. Calculate order level
    # ========================================
    calc2_id = builder.add_calculation(
        name="Calc_Order_Level",
        parent_id=calc1_id,
        column_name="Order_Level",
        formula="IF [sales] >= 500 THEN 'High' ELSEIF [sales] >= 100 THEN 'Medium' ELSE 'Low' END"
    )
    print(f"[OK] [13] add_calculation (order level): {calc2_id[:8]}...")
    
    # ========================================
    # 14. Expression filter: keep only profitable orders
    # ========================================
    filter_id = builder.add_filter(
        name="Filter_Profitable",
        parent_id=calc2_id,
        expression="[profit] > 0"
    )
    print(f"[OK] [14] add_filter (profitable orders): {filter_id[:8]}...")
    
    # ========================================
    # 15. Remove ID columns
    # ========================================
    remove_id = builder.add_remove_columns(
        name="Remove_IDs",
        parent_id=filter_id,
        columns=["order_id", "city", "state"]
    )
    print(f"[OK] [15] add_remove_columns: {remove_id[:8]}...")
    
    # ========================================
    # 16. Clean step
    # ========================================
    clean_id = builder.add_clean_step(
        name="Data_Validation",
        parent_id=remove_id
    )
    print(f"[OK] [16] add_clean_step: {clean_id[:8]}...")
    
    # ========================================
    # 17. Aggregate: summarize by region and month
    # ========================================
    agg_id = builder.add_aggregate(
        name="Aggregate_By_Region",
        parent_id=clean_id,
        group_by=["Region", "Manager", "Month"],
        aggregations=[
            {"field": "sales", "function": "SUM", "output_name": "Total_Sales"},
            {"field": "profit", "function": "SUM", "output_name": "Total_Profit"},
            {"field": "quantity", "function": "COUNT", "output_name": "Order_Count"},
            {"field": "sales", "function": "AVG", "output_name": "Avg_Order_Value"}
        ]
    )
    print(f"[OK] [17] add_aggregate (by region): {agg_id[:8]}...")
    
    # ========================================
    # 18. Pivot: monthly comparison
    # ========================================
    pivot_id = builder.add_pivot(
        name="Pivot_Monthly",
        parent_id=agg_id,
        pivot_column="Month",
        aggregate_column="Total_Sales",
        new_columns=["2024-01", "2024-02"],
        group_by=["Region", "Manager"],
        aggregation="SUM"
    )
    print(f"[OK] [18] add_pivot (monthly): {pivot_id[:8]}...")
    
    # ========================================
    # 19. Unpivot
    # ========================================
    unpivot_id = builder.add_unpivot(
        name="Unpivot_Months",
        parent_id=pivot_id,
        columns_to_unpivot=["2024-01", "2024-02"],
        name_column="Month",
        value_column="Sales"
    )
    print(f"[OK] [19] add_unpivot: {unpivot_id[:8]}...")
    
    # ========================================
    # 20. Output
    # ========================================
    output_id = builder.add_output_server(
        name="Sales_Analysis_Output",
        parent_id=unpivot_id,
        datasource_name="Superstore_Sales_Analysis"
    )
    print(f"[OK] [20] add_output_server: {output_id[:8]}...")
    
    # ========================================
    # Build and save
    # ========================================
    print()
    print("-" * 60)
    flow, display, meta = builder.build()
    print("[OK] Build complete")
    
    output_folder = "./demo_output/comprehensive"
    output_tfl = "./demo_output/comprehensive.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"[OK] Generated: {output_tfl}")
    print()
    print("=" * 60)
    print("Feature Coverage Summary")
    print("=" * 60)
    print(f"  - Input nodes: 4")
    print(f"    - add_input_sql: Jan/Feb orders (with WHERE clause)")
    print(f"    - add_input_table: customers/regions (direct table)")
    print(f"  - Union node: 1")
    print(f"  - Join nodes: 2")
    print(f"  - Clean nodes: 7 (keep_only/rename/value_filter/calculation*2/filter/remove)")
    print(f"  - Aggregate node: 1")
    print(f"  - Pivot nodes: 2 (Pivot + Unpivot)")
    print(f"  - Output node: 1")
    print()
    print("Next steps:")
    print("  1. Open demo_output/comprehensive.tfl with Tableau Prep")
    print("  2. Configure database connection password")
    print("  3. Run flow to verify results")
    print("=" * 60)


if __name__ == "__main__":
    run_comprehensive_demo()
