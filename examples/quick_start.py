"""
cwprep Quick Start Example

This example shows how to use cwprep SDK to create a simple Tableau Prep data flow.

Usage:
    1. Install cwprep: pip install cwprep
    2. Run this script: python quick_start.py
    3. Open the generated .tfl file in Tableau Prep
"""

from cwprep import TFLBuilder, TFLPackager

def main():
    print("=" * 50)
    print("cwprep Quick Start Example")
    print("=" * 50)
    print()
    
    # 1. Create Builder
    builder = TFLBuilder(flow_name="Demo Flow")
    print("[OK] Create Builder")
    
    # 2. Add database connection
    # Note: Using placeholder values, replace with actual database info
    conn_id = builder.add_connection(
        host="localhost",
        username="root",
        dbname="demo_db"
    )
    print(f"[OK] Add database connection: {conn_id[:8]}...")
    
    # 3. Add input tables
    users_id = builder.add_input_sql(
        name="Users",
        sql="SELECT id, name, email, created_at FROM users",
        connection_id=conn_id
    )
    print(f"[OK] Add input table Users: {users_id[:8]}...")
    
    orders_id = builder.add_input_sql(
        name="Orders", 
        sql="SELECT id, user_id, amount, status FROM orders",
        connection_id=conn_id
    )
    print(f"[OK] Add input table Orders: {orders_id[:8]}...")
    
    # 4. Join two tables
    join_id = builder.add_join(
        name="Users + Orders",
        left_id=users_id,
        right_id=orders_id,
        left_col="id",
        right_col="user_id",
        join_type="left"
    )
    print(f"[OK] Add join node: {join_id[:8]}...")
    
    # 5. Add filter condition
    filter_id = builder.add_filter(
        name="Filter Valid Orders",
        parent_id=join_id,
        expression="[status] = 'completed'"
    )
    print(f"[OK] Add filter: {filter_id[:8]}...")
    
    # 6. Add calculated field
    calc_id = builder.add_calculation(
        name="Order Level",
        parent_id=filter_id,
        column_name="order_level",
        formula="IF [amount] >= 1000 THEN 'High' ELSEIF [amount] >= 100 THEN 'Medium' ELSE 'Low' END"
    )
    print(f"[OK] Add calculated field: {calc_id[:8]}...")
    
    # 7. Add output
    output_id = builder.add_output_server(
        name="Output",
        parent_id=calc_id,
        datasource_name="Demo_Output"
    )
    print(f"[OK] Add output node: {output_id[:8]}...")
    
    # 8. Build and save
    print()
    print("-" * 50)
    flow, display, meta = builder.build()
    print("[OK] Build complete")
    
    # Save to folder
    output_folder = "./demo_output"
    output_tfl = "./demo_output.tfl"
    
    TFLPackager.save_to_folder(output_folder, flow, display, meta)
    TFLPackager.pack_zip(output_folder, output_tfl)
    
    print()
    print("=" * 50)
    print(f"[OK] Successfully generated: {output_tfl}")
    print()
    print("Next steps:")
    print("  1. Open demo_output.tfl with Tableau Prep")
    print("  2. Configure database connection")
    print("  3. Run flow to verify")
    print("=" * 50)


if __name__ == "__main__":
    main()
