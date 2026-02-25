"""
Unified Superstore Data Loader
Load Sample - Superstore.xls into MySQL, PostgreSQL, or SQL Server.

Usage:
    python load_superstore.py --db mysql
    python load_superstore.py --db postgresql
    python load_superstore.py --db sqlserver

    # Override default connection parameters:
    python load_superstore.py --db postgresql --host localhost --port 5432 --user postgres --password secret
    python load_superstore.py --db sqlserver --driver "ODBC+Driver+18+for+SQL+Server"
"""

import argparse
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


# ---------------------------------------------------------------------------
# Database configuration defaults
# ---------------------------------------------------------------------------

DB_DEFAULTS = {
    "mysql": {
        "host": "127.0.0.1",
        "port": "3306",
        "user": "root",
        "password": "",
        "dbname": "superstore",
        "driver": "mysql+pymysql",
    },
    "postgresql": {
        "host": "localhost",
        "port": "5432",
        "user": "postgres",
        "password": "qwer123",
        "dbname": "superstore",
        "driver": "postgresql+psycopg2",
    },
    "sqlserver": {
        "host": "127.0.0.1",
        "port": None,
        "user": None,
        "password": None,
        "dbname": "superstore",
        "driver": "mssql+pyodbc",
        "odbc_driver": "ODBC+Driver+17+for+SQL+Server",
    },
}


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------

def _build_url(cfg: dict, dbname: str | None = None) -> str:
    """Build a SQLAlchemy connection URL from config dict."""
    db = dbname or cfg["dbname"]
    db_type = cfg["_type"]

    if db_type == "sqlserver":
        odbc = cfg.get("odbc_driver", "ODBC+Driver+17+for+SQL+Server")
        if cfg.get("user"):
            # SQL Server Authentication
            return f"{cfg['driver']}://{cfg['user']}:{cfg['password']}@{cfg['host']}/{db}?driver={odbc}"
        else:
            # Windows Authentication (SSPI)
            return f"{cfg['driver']}://@{cfg['host']}/{db}?driver={odbc}&Trusted_Connection=yes"

    if db_type == "mysql":
        if cfg["password"]:
            return f"{cfg['driver']}://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{db}"
        else:
            return f"{cfg['driver']}://{cfg['user']}@{cfg['host']}:{cfg['port']}/{db}"

    # postgresql
    return f"{cfg['driver']}://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/{db}"


# ---------------------------------------------------------------------------
# Database preparation (create DB + cleanup)
# ---------------------------------------------------------------------------

def _prepare_mysql(cfg: dict):
    """Connect to MySQL, ensure database exists, clear old data."""
    engine = create_engine(_build_url(cfg))
    with engine.connect() as conn:
        print("Successfully connected to MySQL!")
        print("Clearing existing data...")
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 0;"))
        for table in ["returns", "orders", "products", "customers", "regions"]:
            conn.execute(text(f"DELETE FROM {table};"))
        conn.execute(text("SET FOREIGN_KEY_CHECKS = 1;"))
        conn.commit()
    return engine


def _prepare_postgresql(cfg: dict):
    """Connect to PostgreSQL, create database if missing, clear old data."""
    # Connect to default 'postgres' database to create target
    admin_url = _build_url(cfg, dbname="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as conn:
        exists = conn.execute(
            text(f"SELECT 1 FROM pg_database WHERE datname = '{cfg['dbname']}'")
        ).fetchone()
        if not exists:
            print(f"Database '{cfg['dbname']}' does not exist. Creating...")
            conn.execute(text(f"CREATE DATABASE {cfg['dbname']}"))
            print(f"Database '{cfg['dbname']}' created.")
        else:
            print(f"Database '{cfg['dbname']}' already exists.")

    # Connect to target database and drop tables
    engine = create_engine(_build_url(cfg))
    with engine.connect() as conn:
        print("Successfully connected to PostgreSQL!")
        print("Clearing existing data...")
        for table in ["returns", "orders", "products", "customers", "regions"]:
            conn.execute(text(f"DROP TABLE IF EXISTS {table} CASCADE;"))
        conn.commit()
    return engine


def _prepare_sqlserver(cfg: dict):
    """Connect to SQL Server, create database if missing, clear old data."""
    # Connect to master to create target database
    master_url = _build_url(cfg, dbname="master")
    master_engine = create_engine(master_url, isolation_level="AUTOCOMMIT")
    with master_engine.connect() as conn:
        result = conn.execute(
            text(f"SELECT name FROM sys.databases WHERE name = '{cfg['dbname']}'")
        ).fetchone()
        if not result:
            print(f"Database '{cfg['dbname']}' does not exist. Creating...")
            conn.execute(text(f"CREATE DATABASE {cfg['dbname']}"))
            print(f"Database '{cfg['dbname']}' created.")
        else:
            print(f"Database '{cfg['dbname']}' already exists.")

    # Connect to target database and clear data
    engine = create_engine(_build_url(cfg))
    with engine.connect() as conn:
        print("Successfully connected to SQL Server!")
        print("Clearing existing data...")
        for table in ["returns", "orders", "products", "customers", "regions"]:
            try:
                conn.execute(text(f"DELETE FROM {table};"))
            except Exception:
                pass
        conn.commit()
    return engine


_PREPARE_FN = {
    "mysql": _prepare_mysql,
    "postgresql": _prepare_postgresql,
    "sqlserver": _prepare_sqlserver,
}


# ---------------------------------------------------------------------------
# Data transformation (shared across all databases)
# ---------------------------------------------------------------------------

def _flex_rename(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Rename columns with flexible alias matching (case-insensitive)."""
    cols = {c.lower(): c for c in df.columns}
    new_map = {}
    for target, aliases in mapping.items():
        for alias in aliases:
            if alias.lower() in cols:
                new_map[cols[alias.lower()]] = target
                break
        else:
            print(f"Warning: Could not find column for '{target}' (tried: {aliases})")
    return df.rename(columns=new_map)


def transform_data(xls_path: str) -> dict[str, pd.DataFrame]:
    """
    Read Excel and return a dict of 5 DataFrames ready to load:
    regions, customers, products, orders, returns.
    """
    print(f"Reading Excel: {xls_path}")
    df_orders_raw = pd.read_excel(xls_path, sheet_name="Orders")
    df_returns_raw = pd.read_excel(xls_path, sheet_name="Returns")
    df_people_raw = pd.read_excel(xls_path, sheet_name="People")

    # Strip whitespace from column names
    for df in [df_orders_raw, df_returns_raw, df_people_raw]:
        df.columns = df.columns.str.strip()

    # --- 1. Regions ---
    print("Processing Regions...")
    df_regions = df_people_raw.rename(columns={
        "Region": "region_name",
        "Regional Manager": "manager_name",
        "Person": "manager_name",
    })[["region_name", "manager_name"]]

    region_id_map = {name: i + 1 for i, name in enumerate(df_regions["region_name"].unique())}

    # --- 2. Customers ---
    print("Processing Customers...")
    df_customers = (
        df_orders_raw[["Customer ID", "Customer Name", "Segment"]]
        .drop_duplicates(subset=["Customer ID"])
        .rename(columns={
            "Customer ID": "customer_id",
            "Customer Name": "customer_name",
            "Segment": "segment",
        })
    )

    # --- 3. Products ---
    print("Processing Products...")
    df_products = (
        df_orders_raw[["Product ID", "Product Name", "Category", "Sub-Category"]]
        .drop_duplicates(subset=["Product ID"])
        .rename(columns={
            "Product ID": "product_id",
            "Product Name": "product_name",
            "Category": "category",
            "Sub-Category": "sub_category",
        })
    )

    # --- 4. Orders ---
    print("Processing Orders...")
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
        "region_name": ["Region"],
    }

    df_orders = _flex_rename(df_orders_raw.copy(), order_map)
    df_orders["region_id"] = df_orders["region_name"].map(region_id_map)
    df_orders = df_orders[[
        "row_id", "order_id", "order_date", "ship_date", "ship_mode",
        "customer_id", "region_id", "city", "state", "postal_code",
        "product_id", "sales", "quantity", "discount", "profit",
    ]]
    df_orders["order_date"] = pd.to_datetime(df_orders["order_date"])
    df_orders["ship_date"] = pd.to_datetime(df_orders["ship_date"])

    # --- 5. Returns ---
    print("Processing Returns...")
    df_returns = _flex_rename(df_returns_raw, {
        "order_id": ["Order ID"],
        "returned": ["Returned"],
    })[["order_id", "returned"]]

    return {
        "regions": df_regions,
        "customers": df_customers,
        "products": df_products,
        "orders": df_orders,
        "returns": df_returns,
    }


# ---------------------------------------------------------------------------
# Load tables to database
# ---------------------------------------------------------------------------

def load_tables(engine, tables: dict[str, pd.DataFrame], if_exists: str = "replace"):
    """Write all DataFrames to the database in dependency order."""
    write_order = ["regions", "customers", "products", "orders", "returns"]
    for name in write_order:
        df = tables[name]
        df.to_sql(name, engine, if_exists=if_exists, index=False)
        print(f"  ✓ {name}: {len(df)} rows loaded")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Load Sample - Superstore.xls into a local database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python load_superstore.py --db mysql
  python load_superstore.py --db postgresql
  python load_superstore.py --db sqlserver
  python load_superstore.py --db postgresql --host localhost --port 5432 --user myuser --password mypass
  python load_superstore.py --db sqlserver --driver "ODBC+Driver+18+for+SQL+Server"
        """,
    )
    parser.add_argument(
        "--db", required=True, choices=["mysql", "postgresql", "sqlserver"],
        help="Target database type",
    )
    parser.add_argument("--host", help="Database host (overrides default)")
    parser.add_argument("--port", help="Database port (overrides default)")
    parser.add_argument("--user", help="Database username (overrides default)")
    parser.add_argument("--password", help="Database password (overrides default)")
    parser.add_argument("--dbname", help="Database name (default: superstore)")
    parser.add_argument(
        "--driver",
        help="ODBC driver string for SQL Server (default: ODBC+Driver+17+for+SQL+Server)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    db_type = args.db

    # Build config from defaults + CLI overrides
    cfg = {**DB_DEFAULTS[db_type], "_type": db_type}
    if args.host:
        cfg["host"] = args.host
    if args.port:
        cfg["port"] = args.port
    if args.user:
        cfg["user"] = args.user
    if args.password:
        cfg["password"] = args.password
    if args.dbname:
        cfg["dbname"] = args.dbname
    if args.driver:
        cfg["odbc_driver"] = args.driver

    # Locate Excel file
    xls_path = os.path.join(os.path.dirname(__file__), "Sample - Superstore.xls")
    if not os.path.exists(xls_path):
        print(f"Error: File not found at {xls_path}")
        sys.exit(1)

    # Prepare database
    print(f"--- Loading Superstore data into {db_type.upper()} ---")
    print(f"Target: {cfg.get('user', 'SSPI')}@{cfg['host']}/{cfg['dbname']}")

    try:
        engine = _PREPARE_FN[db_type](cfg)
    except Exception as e:
        print(f"Database preparation failed: {e}")
        sys.exit(1)

    # Transform & load
    tables = transform_data(xls_path)
    load_tables(engine, tables)

    print(f"\n✅ Data loading to {db_type.upper()} completed successfully!")


if __name__ == "__main__":
    main()
