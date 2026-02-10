"""
cwprep Basic Demo: Input, Join, Output

Business Scenario: Customer Orders Analysis
- Join orders table with customers table
- Display customer purchase records

Usage:
    python examples/demo_basic.py
"""

from cwprep import TFLBuilder, TFLPackager

# Database configuration
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}


def run_basic_demo():
    print("=" * 50)
    print("cwprep Basic Demo: Customer Orders Join")
    print("=" * 50)
    print()
    
    # 1. Create Builder and connection
    builder = TFLBuilder(flow_name="Customer Orders Analysis")
    conn_id = builder.add_connection(**DB_CONFIG)
    print(f"[OK] Add database connection: {conn_id[:8]}...")
    
    # 2. Add orders table
    orders_id = builder.add_input_sql(
        name="Orders",
        sql="""SELECT 
order_id,
order_date,
ship_mode,
customer_id,
city,
state,
sales,
quantity,
profit
FROM orders""",
        connection_id=conn_id
    )
    print(f"[OK] Add orders table: {orders_id[:8]}...")
    
    # 3. Add customers table
    customers_id = builder.add_input_sql(
        name="Customers",
        sql="""SELECT 
customer_id,
customer_name,
segment
FROM customers""",
        connection_id=conn_id
    )
    print(f"[OK] Add customers table: {customers_id[:8]}...")
    
    # 4. Join orders and customers
    join_id = builder.add_join(
        name="Orders + Customers",
        left_id=orders_id,
        right_id=customers_id,
        left_col="customer_id",
        right_col="customer_id",
        join_type="left"
    )
    print(f"[OK] Add join: {join_id[:8]}...")
    
    # 5. Add output
    output_id = builder.add_output_server(
        name="Output",
        parent_id=join_id,
        datasource_name="Customer_Orders"
    )
    print(f"[OK] Add output: {output_id[:8]}...")
    
    # 6. Build and save
    print()
    flow, display, meta = builder.build()
    
    output_folder = "./demo_output/basic"
    output_tfl = "./demo_output/basic.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print(f"[OK] Generated: {output_tfl}")
    print()
    print("Data flow: Orders + Customers -> Join -> Output")


if __name__ == "__main__":
    run_basic_demo()
