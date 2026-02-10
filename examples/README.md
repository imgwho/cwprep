# cwprep Examples Directory

This directory contains usage examples for the cwprep SDK, based on the **Sample Superstore** dataset.

## Quick Start

### 1. Initialize Example Database

Execute in your MySQL client:

```sql
examples/demo_data/init_superstore.sql
```

### 2. Run Example Scripts

```bash
# Basic features (Input, Join, Output)
python examples/demo_basic.py

# Data cleaning (Filter, Rename, Calculation)
python examples/demo_cleaning.py

# Aggregation and Transpose (Union, Aggregate, Pivot)
python examples/demo_aggregation.py

# Comprehensive demo (Covers all 15 SDK methods)
python examples/demo_comprehensive.py
```

### 3. Verify Results

Open the generated `.tfl` files in the `demo_output/` directory using Tableau Prep.

---

## Example Scripts Description

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

**Covers all 15 SDK methods**: Complete demonstration of cwprep core functionality

---

| Script | Feature Count | Difficulty |
|------|:---:|:---:|
| `demo_basic.py` | 3 | ⭐ Beginner |
| `demo_cleaning.py` | 6 | ⭐⭐ Basic |
| `demo_aggregation.py` | 5 | ⭐⭐ Basic |
| `demo_comprehensive.py` | 15 | ⭐⭐⭐ Advanced |

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
