"""
cwprep Field Operations Demo

Business Scenario: Customer Data Standardization
- Normalize customer name format (uppercase, trim)
- Correct column data types
- Backup key columns before transformation

Features Covered:
1. add_quick_calc - Quick clean (uppercase, trim whitespace)
2. add_change_type - Change column data types
3. add_duplicate_column - Duplicate (copy) a column

Usage:
    python examples/demo_field_operations.py
"""

from cwprep import TFLBuilder, TFLPackager

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_field_operations_demo():
    print("=" * 50)
    print("cwprep Field Operations Demo")
    print("=" * 50)
    print()
    
    builder = TFLBuilder(flow_name="Customer Data Standardization")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"[OK] Add database connection")
    
    # 1. Add orders + customers table
    data_id = builder.add_input_sql(
        name="Orders with Customers",
        sql="""SELECT 
o.order_id,
o.order_date,
o.sales,
o.profit,
c.customer_name,
c.segment
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id""",
        connection_id=conn_id
    )
    print(f"[OK] [1] add_input_sql: Orders + Customers")
    
    # 2. Duplicate sales column as backup before transformation
    dup_id = builder.add_duplicate_column(
        name="Backup Sales",
        parent_id=data_id,
        source_column="sales",
        new_column_name="original_sales"
    )
    print(f"[OK] [2] add_duplicate_column: Copy sales -> original_sales")
    
    # 3. Change data types
    type_id = builder.add_change_type(
        name="Fix Data Types",
        parent_id=dup_id,
        fields={
            "sales": "real",
            "profit": "real",
            "order_date": "date"
        }
    )
    print(f"[OK] [3] add_change_type: sales->real, profit->real, order_date->date")
    
    # 4. Quick calc: uppercase customer name
    upper_id = builder.add_quick_calc(
        name="Uppercase Name",
        parent_id=type_id,
        column_name="customer_name",
        calc_type="uppercase"
    )
    print(f"[OK] [4] add_quick_calc: customer_name -> UPPERCASE")
    
    # 5. Quick calc: trim whitespace from segment
    trim_id = builder.add_quick_calc(
        name="Trim Segment",
        parent_id=upper_id,
        column_name="segment",
        calc_type="trim_spaces"
    )
    print(f"[OK] [5] add_quick_calc: segment -> TRIM whitespace")
    
    # 6. Rename for clean output
    rename_id = builder.add_rename(
        parent_id=trim_id,
        renames={
            "customer_name": "Customer_Name",
            "segment": "Segment",
            "order_date": "Order_Date",
            "sales": "Sales",
            "profit": "Profit",
            "original_sales": "Original_Sales"
        }
    )
    print(f"[OK] [6] add_rename: Standardize field names")
    
    # Output
    output_id = builder.add_output_server(
        name="Output",
        parent_id=rename_id,
        datasource_name="Standardized_Customer_Data"
    )
    print(f"[OK] [7] add_output_server: Add output")
    
    # Build
    print()
    flow, display, meta = builder.build()
    
    output_folder = "./demo_output/field_operations"
    output_tfl = "./demo_output/field_operations.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"[OK] Generated: {output_tfl}")
    print()
    print("Features covered:")
    print("  - add_duplicate_column: Backup sales column")
    print("  - add_change_type: Fix column data types (real, date)")
    print("  - add_quick_calc: Uppercase name, trim whitespace")


if __name__ == "__main__":
    run_field_operations_demo()
