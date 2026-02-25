import pandas as pd
from sqlalchemy import create_engine, text
import os
import sys

# Add project root to path to use cwprep config
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

def load_superstore_to_postgresql():
    xls_path = os.path.join(os.path.dirname(__file__), "Sample - Superstore.xls")
    
    if not os.path.exists(xls_path):
        print(f"Error: File not found at {xls_path}")
        return

    # 1. Configure PostgreSQL local connection parameters
    host = "localhost"
    port = "5432"
    user = "postgres"       # Database username
    password = "qwer123"   # Database password
    dbname = "superstore"
    
    print(f"--- Attempting Local PostgreSQL Connection ---")
    
    # 2. Connect to the default postgres database; check and create superstore database
    # PostgreSQL's CREATE DATABASE also cannot run inside a transaction block
    # Note: connecting as the provided user to create the target database
    try:
        # Connect to default database to create target database
        conn_str_default = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/postgres"
        engine_default = create_engine(conn_str_default, isolation_level="AUTOCOMMIT")
        
        with engine_default.connect() as conn:
            # Check if the database exists
            exists = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{dbname}'")).fetchone()
            if not exists:
                print(f"Database '{dbname}' does not exist. Creating it now...")
                conn.execute(text(f"CREATE DATABASE {dbname}"))
                print(f"Database '{dbname}' created successfully.")
            else:
                print(f"Database '{dbname}' already exists.")
    except Exception as e:
        print(f"Failed to connect to PostgreSQL or create database: {e}")
        print("Hint: Please ensure 'psycopg2-binary' is installed (pip install psycopg2-binary)")
        print("Also verify that the username and password are correct (default is usually 'postgres')")
        return

    # 3. Connect to the superstore database and process data
    conn_str_target = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    try:
        engine = create_engine(conn_str_target)
        with engine.connect() as conn:
            print(f"Successfully connected to the {dbname} database!")
            print("Clearing existing data for a fresh import...")
            # PostgreSQL supports TRUNCATE ... CASCADE for cleaning related tables
            tables = ["returns", "orders", "products", "customers", "regions"]
            for table in tables:
                try:
                    # Use DROP TABLE for a clean slate; tables will be recreated by pandas
                    conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
                    print(f"Dropped table (if existed): {table}")
                except Exception as e:
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

    # Strip whitespace from columns
    df_orders_raw.columns = df_orders_raw.columns.str.strip()
    df_returns_raw.columns = df_returns_raw.columns.str.strip()
    df_people_raw.columns = df_people_raw.columns.str.strip()

    # --- 1. Regions table ---
    print("Processing Regions...")
    df_regions = df_people_raw.rename(columns={
        "Region": "region_name",
        "Regional Manager": "manager_name",
        "Person": "manager_name"
    })[['region_name', 'manager_name']]
    
    # Write regions table
    df_regions.to_sql("regions", engine, if_exists="replace", index=False)
    
    # Generate region_id mapping (simple index-based mapping after writing)
    region_id_map = {name: i+1 for i, name in enumerate(df_regions['region_name'].unique())}

    # --- 2. Customers table ---
    print("Processing Customers...")
    df_customers = df_orders_raw[[
        "Customer ID", "Customer Name", "Segment"
    ]].drop_duplicates(subset=["Customer ID"]).rename(columns={
        "Customer ID": "customer_id",
        "Customer Name": "customer_name",
        "Segment": "segment"
    })
    df_customers.to_sql("customers", engine, if_exists="replace", index=False)

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
    df_products.to_sql("products", engine, if_exists="replace", index=False)

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
        return df.rename(columns=new_map)

    order_map = {
        "row_id": ["Row ID"],
        "order_id": ["Order ID"],
        "order_date": ["Order Date"],
        "ship_date": ["Ship Date"],
        "ship_mode": ["Ship Mode"],
        "customer_id": ["Customer ID"],
        "city": ["City"],
        "state": ["State", "State/Province"],
        "postal_code": ["Postal Code"],
        "product_id": ["Product ID"],
        "sales": ["Sales"],
        "quantity": ["Quantity"],
        "discount": ["Discount"],
        "profit": ["Profit"],
        "region_name": ["Region"] 
    }

    df_orders = flex_rename(df_orders, order_map)
    df_orders['region_id'] = df_orders['region_name'].map(region_id_map)
    
    df_orders = df_orders[[
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "region_id", "city", "state", "postal_code",
        "product_id", "sales", "quantity", "discount", "profit"
    ]]
    
    df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
    df_orders['ship_date'] = pd.to_datetime(df_orders['ship_date'])
    
    df_orders.to_sql("orders", engine, if_exists="replace", index=False)

    # --- 5. Returns table ---
    print("Processing Returns...")
    returns_map = {
        "order_id": ["Order ID"],
        "returned": ["Returned"]
    }
    df_returns = flex_rename(df_returns_raw, returns_map)
    df_returns = df_returns[["order_id", "returned"]]
    
    df_returns.to_sql("returns", engine, if_exists="replace", index=False)

    print("Data loading to PostgreSQL completed successfully!")

if __name__ == "__main__":
    load_superstore_to_postgresql()
