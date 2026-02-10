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

| Script | Feature Coverage | Business Scenario |
|------|----------|----------|
| `demo_basic.py` | Input, Join, Output | Customer and Order mapping |
| `demo_cleaning.py` | Filter, Keep, Rename, Calculation | Profitable order analysis |
| `demo_aggregation.py` | Union, Aggregate, Pivot/Unpivot | Regional monthly comparison |
| `demo_comprehensive.py` | **Full 15 SDK Methods** | Complete sales analysis |

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
