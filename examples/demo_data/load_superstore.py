import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

# Add project root to path to use cwprep config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.cwprep.config import load_config

def load_superstore_to_db():
    xls_path = os.path.join(os.path.dirname(__file__), "Sample - Superstore.xls")
    
    if not os.path.exists(xls_path):
        print(f"Error: File not found at {xls_path}")
        return

    # 1. Force local connection parameters as requested
    host = "127.0.0.1"
    port = "3306"
    user = "root"
    password = ""
    dbname = "superstore"
    
    print(f"--- Attempting Local Connection ---")
    print(f"Target: {user}@{host}:{port}/{dbname}")
    
    # Use pymysql
    db_driver = "mysql+pymysql"
    # Build URL: No password means no colon
    db_url = f"{db_driver}://{user}@{host}:{port}/{dbname}"
    
    try:
        engine = create_engine(db_url)
        # Test connection and clear existing data
        with engine.connect() as conn:
            print("Successfully connected to the database!")
            print("Clearing existing data for a fresh import...")
            # Disable FK checks to allow truncation/deletion
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
            for table in ["returns", "orders", "products", "customers", "regions"]:
                conn.execute(text(f"DELETE FROM {table};"))
            conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
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
    # Mapping People sheet (Regional Manager, Region) to regions table
    # Note: XLS usually has "Regional Manager" and "Region"
    df_regions = df_people_raw.rename(columns={
        "Region": "region_name",
        "Regional Manager": "manager_name",
        "Person": "manager_name" # Fallback
    })[['region_name', 'manager_name']]
    
    # Insert and get mapping for region_id
    df_regions.to_sql("regions", engine, if_exists="append", index=False)
    
    # Reload regions to get the auto-increment IDs
    regions_map_df = pd.read_sql("SELECT region_id, region_name FROM regions", engine)
    region_id_map = dict(zip(regions_map_df['region_name'], regions_map_df['region_id']))

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
    
    # Helper for flexible renaming
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
        "region_name": ["Region"] # For mapping to region_id
    }

    df_orders = flex_rename(df_orders, order_map)
    
    # Verify we have everything needed for the selection
    required_cols = [
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "region_name", "city", "state", "postal_code",
        "product_id", "sales", "quantity", "discount", "profit"
    ]
    missing = [c for c in required_cols if c not in df_orders.columns]
    if missing:
        print(f"Error: Missing required columns in Orders: {missing}")
        print(f"Available columns: {list(df_orders.columns)}")
        return

    # Map region_id
    df_orders['region_id'] = df_orders['region_name'].map(region_id_map)
    
    # Select columns
    df_orders = df_orders[[
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "region_id", "city", "state", "postal_code",
        "product_id", "sales", "quantity", "discount", "profit"
    ]]
    
    # Ensure date formats are compatible with SQL
    df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
    df_orders['ship_date'] = pd.to_datetime(df_orders['ship_date'])
    
    df_orders.to_sql("orders", engine, if_exists="append", index=False)

    # --- 5. Returns table ---
    print("Processing Returns...")
    # Flexible rename for returns too
    returns_map = {
        "order_id": ["Order ID"],
        "returned": ["Returned"]
    }
    df_returns = flex_rename(df_returns_raw, returns_map)
    df_returns = df_returns[["order_id", "returned"]]
    
    df_returns.to_sql("returns", engine, if_exists="append", index=False)

    print("Data loading completed successfully!")

if __name__ == "__main__":
    load_superstore_to_db()
