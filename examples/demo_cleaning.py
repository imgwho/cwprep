"""
cwprep Data Cleaning Demo

Business Scenario: Profitable Orders Analysis
- Filter orders with profit > 0
- Calculate profit rate
- Rename fields

Features Covered:
1. add_filter - Expression filter
2. add_value_filter - Value filter
3. add_keep_only - Keep only columns
4. add_remove_columns - Remove columns
5. add_rename - Rename columns
6. add_calculation - Calculated fields

Usage:
    python examples/demo_cleaning.py
"""

from cwprep import TFLBuilder, TFLPackager

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_cleaning_demo():
    print("=" * 50)
    print("cwprep Data Cleaning Demo")
    print("=" * 50)
    print()
    
    builder = TFLBuilder(flow_name="Profitable Orders Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"[OK] Add database connection")
    
    # Add orders table (with product info)
    orders_id = builder.add_input_sql(
        name="Orders with Products",
        sql="""SELECT 
o.order_id,
o.order_date,
o.ship_mode,
o.customer_id,
o.city,
o.state,
p.product_name,
p.category,
p.sub_category,
o.sales,
o.quantity,
o.discount,
o.profit
FROM orders o
JOIN products p ON o.product_id = p.product_id""",
        connection_id=conn_id
    )
    print(f"[OK] [1] add_input_sql: Add orders+products table")
    
    # 1. Expression filter: profit > 0
    filter1_id = builder.add_filter(
        name="Filter Profitable",
        parent_id=orders_id,
        expression="[profit] > 0"
    )
    print(f"[OK] [2] add_filter: Filter profit > 0")
    
    # 2. Value filter: keep specific ship modes
    filter2_id = builder.add_value_filter(
        name="Filter Ship Mode",
        parent_id=filter1_id,
        field="ship_mode",
        values=["Standard Class", "First Class"],
        exclude=False
    )
    print(f"[OK] [3] add_value_filter: Keep Standard/First Class")
    
    # 3. Keep only core columns
    keep_id = builder.add_keep_only(
        name="Keep Core Fields",
        parent_id=filter2_id,
        columns=["order_id", "order_date", "customer_id", "category", 
                 "product_name", "sales", "quantity", "profit"]
    )
    print(f"[OK] [4] add_keep_only: Keep 8 core fields")
    
    # 4. Rename fields
    rename_id = builder.add_rename(
        parent_id=keep_id,
        renames={
            "order_id": "Order_ID",
            "order_date": "Order_Date",
            "customer_id": "Customer_ID",
            "category": "Category",
            "product_name": "Product_Name",
            "sales": "Sales",
            "quantity": "Quantity",
            "profit": "Profit"
        }
    )
    print(f"[OK] [5] add_rename: Rename fields")
    
    # 5. Calculate profit rate
    calc1_id = builder.add_calculation(
        name="Calculate Profit Rate",
        parent_id=rename_id,
        column_name="Profit_Rate",
        formula="[Profit] / [Sales]"
    )
    print(f"[OK] [6] add_calculation: Calculate Profit_Rate = Profit/Sales")
    
    # 6. Calculate order level
    calc2_id = builder.add_calculation(
        name="Calculate Order Level",
        parent_id=calc1_id,
        column_name="Order_Level",
        formula="IF [Sales] >= 500 THEN 'High' ELSEIF [Sales] >= 100 THEN 'Medium' ELSE 'Low' END"
    )
    print(f"[OK] [7] add_calculation: Calculate Order_Level")
    
    # 7. Remove customer ID (anonymize)
    remove_id = builder.add_remove_columns(
        name="Remove Customer ID",
        parent_id=calc2_id,
        columns=["Customer_ID"]
    )
    print(f"[OK] [8] add_remove_columns: Remove Customer_ID")
    
    # Output
    output_id = builder.add_output_server(
        name="Output",
        parent_id=remove_id,
        datasource_name="Profitable_Orders"
    )
    print(f"[OK] [9] add_output_server: Add output")
    
    # Build
    print()
    flow, display, meta = builder.build()
    
    output_folder = "./demo_output/cleaning"
    output_tfl = "./demo_output/cleaning.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"[OK] Generated: {output_tfl}")
    print()
    print("Features covered:")
    print("  - add_filter (profit > 0)")
    print("  - add_value_filter (ship_mode)")
    print("  - add_keep_only")
    print("  - add_rename")
    print("  - add_calculation (Profit_Rate, Order_Level)")
    print("  - add_remove_columns")


if __name__ == "__main__":
    run_cleaning_demo()
