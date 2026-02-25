# cwprep Examples Directory

This directory contains usage examples for the cwprep SDK, based on the **Sample Superstore** dataset.

## Quick Start

### 1. Initialize Example Database

You can initialize the database using either the SQL script or the Python loader:

**Option A: Using SQL (Manual)**
Execute in your MySQL client:
```sql
examples/demo_data/init_superstore.sql
```

**Option B: Using Python — MySQL (Automated from Excel)**
```bash
pip install pymysql pandas xlrd
python examples/demo_data/load_superstore.py
```

**Option C: Using Python — SQL Server**
```bash
pip install pyodbc pandas xlrd sqlalchemy
python examples/demo_data/load_superstore_sqlserver.py
```

**Option D: Using Python — PostgreSQL**
```bash
pip install psycopg2-binary pandas xlrd sqlalchemy
python examples/demo_data/load_superstore_postgresql.py
```

### 2. Run Example Scripts

```bash
# Basic features (Input, Join, Output)
python examples/demo_basic.py

# Data cleaning (Filter, Rename, Calculation)
python examples/demo_cleaning.py

# Field operations (Quick Calc, Change Type, Duplicate Column)
python examples/demo_field_operations.py

# Aggregation and Transpose (Union, Aggregate, Pivot)
python examples/demo_aggregation.py

# Comprehensive demo (Covers all SDK methods)
python examples/demo_comprehensive.py
```

### 3. Verify Results

Open the generated `.tfl` files in the `demo_output/` directory using Tableau Prep.

---

## Example Scripts

### quick_start.py - Minimal Quick Start
**Business Requirement**: Demonstrate the simplest cwprep workflow — connect, input, join, filter, output

**Data Flow**:
```
Users + Orders → Left Join (id) → Filter completed → Calculate level → Output
```

**Features Covered**: `add_connection`, `add_input_sql`, `add_join`, `add_filter`, `add_calculation`, `add_output_server`

---

### demo_basic.py - Customer Orders Join
**Business Requirement**: Join orders table with customers table to display customer purchase records

**Data Flow**:
```
Orders + Customers → Left Join (customer_id) → Server Output
```

**Features Covered**: `add_input_sql`, `add_join`, `add_output_server`

---

### demo_cleaning.py - Profitable Orders Analysis
**Business Requirement**: Filter profitable orders, calculate profit rate, and classify order levels

**Data Flow**:
```
Orders+Products → Filter profit>0 → Filter ship mode → Keep core fields 
               → Rename → Calculate profit rate → Calculate order level → Remove customer ID → Output
```

**Features Covered**: `add_filter`, `add_value_filter`, `add_keep_only`, `add_rename`, `add_calculation`, `add_remove_columns`

---

### demo_field_operations.py - Customer Data Standardization
**Business Requirement**: Normalize customer data — backup key columns, fix data types, standardize text format

**Data Flow**:
```
Orders+Customers → Duplicate sales column → Fix data types → Uppercase name
                → Trim whitespace → Rename → Output
```

**Features Covered**: `add_duplicate_column`, `add_change_type`, `add_quick_calc` (uppercase, trim)

---

### demo_aggregation.py - Regional Monthly Sales Analysis
**Business Requirement**: Merge Jan-Feb orders, aggregate sales metrics by region, generate monthly comparison report

**Data Flow**:
```
Jan Orders + Feb Orders → Union → Aggregate by region/month (SUM/COUNT/AVG)
                       → Rename → Pivot (month) → Unpivot → Output
```

**Features Covered**: `add_union`, `add_aggregate`, `add_pivot`, `add_unpivot`, `add_rename`

---

### demo_comprehensive.py - Complete Sales Analysis Flow
**Business Requirements**:
- Merge January and February order data
- Join with customer and region information
- Calculate profit rate and order level
- Filter profitable orders only
- Summarize by region and month
- Generate monthly comparison report

**Data Flow**:
```
Jan Orders ─┬─ Union → Join Customers → Join Regions → Keep Core Fields → Rename
Feb Orders ─┘
            → Exclude Same Day → Calc Profit Rate → Calc Order Level → Filter Profitable
            → Remove IDs → Data Validation → Aggregate by Region → Pivot → Unpivot → Output
```

**Covers all SDK methods**: Complete demonstration of cwprep core functionality

---

| Script | Feature Count | Difficulty |
|------|:---:|:---:|
| `quick_start.py` | 5 | ⭐ Quick Start |
| `demo_basic.py` | 3 | ⭐ Beginner |
| `demo_cleaning.py` | 6 | ⭐⭐ Basic |
| `demo_field_operations.py` | 3 | ⭐⭐ Basic |
| `demo_aggregation.py` | 5 | ⭐⭐ Basic |
| `demo_comprehensive.py` | 18 | ⭐⭐⭐ Advanced |

---

## MCP Prompt Examples

See [prompts.md](prompts.md) for **8 ready-to-use prompt templates** for generating TFL flows via AI clients (Claude, Gemini, Cursor, etc.) with the cwprep MCP server.

Each prompt includes:
- **Business Context** — Real-world scenario description
- **Full Prompt** — Detailed prompt with explicit logic
- **Capability Test Prompt** — Minimalist prompt to test AI inference capability

---

## Data Model

```
                    ┌─────────────┐
                    │   regions   │
                    ├─────────────┤
                    │ region_id   │
                    │ region_name │
                    │ manager_name│
                    └──────┬──────┘
                           │
 ┌─────────────┐    ┌───────┴───────┐    ┌─────────────┐
 │  customers  │    │    orders     │    │  products   │
 ├─────────────┤    ├───────────────┤    ├─────────────┤
 │ customer_id │◄───│ customer_id   │───►│ product_id  │
 │ customer_name│   │ region_id     │    │ product_name│
 │ segment     │    │ product_id    │    │ category    │
 └─────────────┘    │ sales/profit  │    │ sub_category│
                    └───────┬───────┘    └─────────────┘
                           │
                    ┌──────┴──────┐
                    │   returns   │
                    ├─────────────┤
                    │ order_id    │
                    │ returned    │
                    └─────────────┘
```

---

## Database Configuration

```python
DB_CONFIG = {
    "host": "localhost",
    "username": "root",
    "dbname": "superstore",
    "port": "3306"
}
```
