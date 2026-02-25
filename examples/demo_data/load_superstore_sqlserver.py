import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

# Add project root to path to use cwprep config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def load_superstore_to_sqlserver():
    xls_path = os.path.join(os.path.dirname(__file__), "Sample - Superstore.xls")
    
    if not os.path.exists(xls_path):
        print(f"Error: File not found at {xls_path}")
        return

    # 1. Configure SQL Server local connection (Windows Authentication)
    host = "127.0.0.1"
    dbname = "superstore"
    # Default: ODBC Driver 17 for SQL Server. Change if you have Driver 18 or another version.
    driver = "ODBC+Driver+17+for+SQL+Server" 
    
    print(f"--- Attempting Local SQL Server Connection ---")
    
    # 2. Connect to master database first; check and create superstore database
    # CREATE DATABASE cannot run inside a transaction, so set isolation_level to AUTOCOMMIT
    master_url = f"mssql+pyodbc://@{host}/master?driver={driver}&Trusted_Connection=yes"
    try:
        master_engine = create_engine(master_url, isolation_level="AUTOCOMMIT")
        with master_engine.connect() as conn:
            # Check if the database exists
            result = conn.execute(text(f"SELECT name FROM sys.databases WHERE name = '{dbname}'")).fetchone()
            if not result:
                print(f"Database '{dbname}' does not exist. Creating it now...")
                conn.execute(text(f"CREATE DATABASE {dbname}"))
                print(f"Database '{dbname}' created successfully.")
            else:
                print(f"Database '{dbname}' already exists.")
    except Exception as e:
        print(f"Failed to connect to master or create database: {e}")
        print("Please ensure 'ODBC Driver 17 for SQL Server' and 'pyodbc' are installed (pip install pyodbc)")
        return

    # 3. Connect to the superstore database and clear old data
    db_url = f"mssql+pyodbc://@{host}/{dbname}?driver={driver}&Trusted_Connection=yes"
    try:
        engine = create_engine(db_url)
        with engine.connect() as conn:
            print("Successfully connected to the superstore database!")
            print("Clearing existing data for a fresh import...")
            # Delete table contents in reverse FK dependency order
            for table in ["returns", "orders", "products", "customers", "regions"]:
                try:
                    conn.execute(text(f"DELETE FROM {table};"))
                    print(f"Cleared table: {table}")
                except Exception as e:
                    # Ignore errors if the table doesn't exist; pandas will auto-create it
                    pass
            conn.commit()
    except Exception as e:
        print(f"Connection/Cleanup Failed: {e}")
        return

    print(f"Reading Excel: {xls_path}")
    # Load sheets
    df_orders_raw = pd.read_excel(xls_path, sheet_name="Orders")
    df_returns_raw = pd.read_excel(xls_path, sheet_name="Returns")
    df_people_raw = pd.read_excel(xls_path, sheet_name="People")

    # Strip whitespace from columns for robustness
    df_orders_raw.columns = df_orders_raw.columns.str.strip()
    df_returns_raw.columns = df_returns_raw.columns.str.strip()
    df_people_raw.columns = df_people_raw.columns.str.strip()

    # --- 1. Regions table ---
    print("Processing Regions...")
    df_regions = df_people_raw.rename(columns={
        "Region": "region_name",
        "Regional Manager": "manager_name",
        "Person": "manager_name" # Fallback
    })[['region_name', 'manager_name']]
    
    # Write regions table
    df_regions.to_sql("regions", engine, if_exists="append", index=False)
    
    # Re-read regions table to get auto-increment IDs if available
    # Note: pandas to_sql doesn't create auto-increment PKs by default
    # To maintain consistency with original logic, we use implicit DB mapping
    try:
        regions_map_df = pd.read_sql("SELECT * FROM regions", engine)
        # If region_id column is missing (typical with pandas-created tables), handle specially
        if 'region_id' in regions_map_df.columns:
            region_id_map = dict(zip(regions_map_df['region_name'], regions_map_df['region_id']))
        else:
             # If table was just created by pandas without region_id, generate a mapping
             # This ensures orders table can reference region IDs consistently
             region_id_map = {name: i+1 for i, name in enumerate(df_regions['region_name'].unique())}
             print("Warning: 'region_id' column not found, generating temporary mapping in memory.")
    except Exception as e:
        print(f"Error mapping regions: {e}")
        return

    # --- 2. Customers table ---
    print("Processing Customers...")
    df_customers = df_orders_raw[[
        "Customer ID", "Customer Name", "Segment"
    ]].drop_duplicates(subset=["Customer ID"]).rename(columns={
        "Customer ID": "customer_id",
        "Customer Name": "customer_name",
        "Segment": "segment"
    })
    df_customers.to_sql("customers", engine, if_exists="append", index=False)

    # --- 3. Products table ---
    print("Processing Products...")
    df_products = df_orders_raw[[
        "Product ID", "Product Name", "Category", "Sub-Category"
    ]].drop_duplicates(subset=["Product ID"]).rename(columns={
        "Product ID": "product_id",
        "Product Name": "product_name",
        "Category": "category",
        "Sub-Category": "sub_category"
    })
    df_products.to_sql("products", engine, if_exists="append", index=False)

    # --- 4. Orders table ---
    print("Processing Orders...")
    df_orders = df_orders_raw.copy()
    
    def flex_rename(df, mapping):
        cols = {c.lower(): c for c in df.columns}
        new_map = {}
        for target, aliases in mapping.items():
            found = False
            for alias in aliases:
                if alias.lower() in cols:
                    new_map[cols[alias.lower()]] = target
                    found = True
                    break
            if not found:
                print(f"Warning: Could not find column for '{target}' (tried: {aliases})")
        return df.rename(columns=new_map)

    order_map = {
        "row_id": ["Row ID"],
        "order_id": ["Order ID"],
        "order_date": ["Order Date"],
        "ship_date": ["Ship Date"],
        "ship_mode": ["Ship Mode"],
        "customer_id": ["Customer ID"],
        "city": ["City"],
        "state": ["State", "State/Province", "Province"],
        "postal_code": ["Postal Code", "Zip Code", "Postcode"],
        "product_id": ["Product ID"],
        "sales": ["Sales"],
        "quantity": ["Quantity"],
        "discount": ["Discount"],
        "profit": ["Profit"],
        "region_name": ["Region"] 
    }

    df_orders = flex_rename(df_orders, order_map)
    
    required_cols = [
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "region_name", "city", "state", "postal_code",
        "product_id", "sales", "quantity", "discount", "profit"
    ]
    missing = [c for c in required_cols if c not in df_orders.columns]
    if missing:
        print(f"Error: Missing required columns in Orders: {missing}")
        return

    # Map region_id
    df_orders['region_id'] = df_orders['region_name'].map(region_id_map)
    
    # Select columns
    df_orders = df_orders[[
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "region_id", "city", "state", "postal_code",
        "product_id", "sales", "quantity", "discount", "profit"
    ]]
    
    df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
    df_orders['ship_date'] = pd.to_datetime(df_orders['ship_date'])
    
    df_orders.to_sql("orders", engine, if_exists="append", index=False)

    # --- 5. Returns table ---
    print("Processing Returns...")
    returns_map = {
        "order_id": ["Order ID"],
        "returned": ["Returned"]
    }
    df_returns = flex_rename(df_returns_raw, returns_map)
    df_returns = df_returns[["order_id", "returned"]]
    
    df_returns.to_sql("returns", engine, if_exists="append", index=False)

    print("Data loading completed successfully!")

if __name__ == "__main__":
    load_superstore_to_sqlserver()
