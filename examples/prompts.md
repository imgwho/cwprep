# cwprep MCP Prompt Examples

Example prompts for generating Tableau Prep `.tfl` flows via AI clients (Claude, Gemini, Cursor, etc.) with the cwprep MCP server.

## Configuration Header

Include the following at the beginning of each prompt to ensure accurate database connections and field definitions:

> "Please read `db_superstore.md` to get the table schema and strictly follow the field name casing. Use `cwprep` to generate a `.tfl` flow: connect to the local localhost MySQL database 'superstore', username 'root', password empty. **Please assign a concise, descriptive name to the output node and output file based on the business logic.** The specific business requirement is as follows:"

---

## Prompt Scenarios

### 1. Orders & Returns Join Query ⭐

**Business Context**:
The operations team needs to quickly review high-value orders and their associated return statuses. By joining the orders table with the returns table and filtering for sales exceeding $500, the team can focus on significant transactions that may require attention.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Review high-value orders with return status. **Logic**: Left Join the orders table with the returns table on order_id; filter records where sales > 500; output to the server. Please auto-generate the output node name and save the file."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Show all orders above $500 along with their return status. Based on the table schema, automatically infer the join and filter logic, generate an output node, and save the flow."

---

### 2. Loss-Making Orders Analysis ⭐

**Business Context**:
The finance team wants to identify products that are losing money even without returns. By joining orders with returns and creating a calculated 'Actual Sales' field (recording returned orders as 0), then filtering for non-returned orders with negative profit, the team can pinpoint genuinely unprofitable products.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Identify loss-making products excluding return-related losses. **Logic**: Left Join Orders with Returns on order_id (retain all orders); add a calculated field 'Actual_Sales' = IF [returned] = 'yes' THEN 0 ELSE [sales] END; filter records where returned IS NULL (no return) AND profit < 0; output to the server. Please auto-generate the output node name and save the file."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Find products that are genuinely losing money — orders with no returns but negative profit. Create a calculated field to distinguish returned vs. non-returned orders. Generate an output node and save the flow. Do not ask me for specific formulas — use your own judgment."

---

### 3. Logistics Efficiency & Loss Warning ⭐⭐

**Business Context**:
The logistics monitoring center discovered that some long-distance shipments have recently resulted in losses. We need to identify 'inefficient and loss-making' orders where shipping took more than 5 days and profit was negative, to review carrier performance or adjust pricing strategies.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Identify anomalous orders with low shipping efficiency and negative profit. **Logic**: Add a calculated field shipping_duration = DATEDIFF('day', [order_date], [ship_date]) to the Orders table; filter records where shipping_duration > 5 AND profit < 0; retain order_id, product_id, ship_mode, shipping_duration, and profit fields. Please auto-generate a descriptive output node name and save the file."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Find loss-making orders with excessive shipping duration. Based on the table schema, automatically infer the shipping duration calculation and filter conditions, generate an output node, and save the flow. Do not ask me for specific day thresholds or field names — use your own judgment."

---

### 4. Regional Sales Report ⭐⭐

**Business Context**:
The management team needs a regional sales report showing how each manager's product categories performed in 2024. This requires connecting orders with region and product tables, then aggregating total sales and average discount by category per manager — providing an overview for strategic planning.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Generate a 2024 regional product performance report. **Logic**: Join Orders with Regions on region_id, then Join with Products on product_id; filter order_date within year 2024; aggregate by manager_name and category, computing SUM(sales) as total_sales and AVG(discount) as avg_discount; output to the server. Please auto-generate the output node name and save the file."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Generate a report showing total sales and average discount rates for each product category under each regional manager, for 2024 data only. Automatically infer the joins, filters, and aggregation logic, generate an output node, and save the flow."

---

### 5. VIP Customer List ⭐⭐

**Business Context**:
Marketing wants a curated VIP customer list for an upcoming campaign. The criteria: only Corporate segment customers with total spending above $5000, excluding anyone who has had a return and excluding slow 'Standard Class' shipments. The final list should show customer name, total amount, and segment.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Build a VIP customer list for precision marketing. **Logic**: Join Orders with Customers on customer_id; Left Join Returns on order_id, then filter to exclude records where returned = 'yes'; use value filter to exclude ship_mode = 'Standard Class'; aggregate by customer_name and segment, computing SUM(sales) as total_sales; filter total_sales > 5000 AND segment = 'Corporate'; keep only customer_name, total_sales, segment; output to the server. Please auto-generate the output node name and save the file."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Create a VIP customer list — Corporate customers spending over $5000 total, excluding anyone with returns and excluding Standard Class shipping. Output customer name, total amount, and segment. Automatically infer all the join, filter, aggregate logic and save the flow."

---

### 6. High-Value Corporate Customer Analysis ⭐⭐⭐

**Business Context**:
The sales department plans to launch a precision marketing campaign for key accounts. We need to filter transactions from the 'Corporate' segment with a single order value exceeding $1000. By associating customer info and regional managers, sales reps can directly contact the relevant stakeholders for VIP maintenance.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Filter high-value Corporate orders for VIP follow-up. **Logic**: Join Orders with Customers on customer_id, then Join Regions on region_id; filter records where segment = 'Corporate' AND sales > 1000; keep only customer_name, order_date, sales, region_name, and manager_name; output to the server. Please auto-generate the output node name and save the file."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Identify high-value orders from Corporate customers and retrieve the responsible regional manager. Based on the table schema, automatically infer the required joins and filter conditions, generate an output node, and save the flow. Do not ask me for specific thresholds or field definitions — use your own judgment."

---

### 7. Product Category Return Rate Statistics ⭐⭐⭐

**Business Context**:
Supply chain management needs to assess quality control across all categories. Since return data is stored in a separate table, we need to perform a Left Join between all orders and returns to calculate the return percentage for each category, identifying which product lines need quality improvement or logistics optimization.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Calculate product category return rates for quality monitoring. **Logic**: Join Orders with Products on product_id, then Left Join Returns on order_id; add a calculated field is_returned = IF [returned] = 'yes' THEN 1 ELSE 0 END; aggregate by category to compute SUM(is_returned) as return_count and COUNT as total_orders; output to the server. Please auto-generate a descriptive output node name (e.g. 'Category_Return_Analysis') and save the flow."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Calculate the return rate for each product category. Based on the structure of the Orders and Returns tables, automatically infer the join logic and compute the return rate, generate an output node, and save the flow. Do not ask me for specific formulas — use your own judgment on how to handle the return flag."

---

### 8. Regional Manager Performance Summary ⭐⭐⭐

**Business Context**:
At the end of the quarter, regional managers' sales performance needs to be ranked. To filter out noise, we need to exclude small, sporadic orders with a quantity less than 2, and only aggregate sales and profit for core transactions, joined with manager names for a performance brief.

**Full Prompt**:
> "Please read `db_superstore.md`. Use `cwprep` to generate a .tfl: connect to the local localhost database 'superstore' (user: root, pwd: empty). **Business context**: Prepare aggregated data for the quarterly regional performance review. **Logic**: First filter Orders to keep only quantity >= 2; aggregate SUM(sales) as total_sales and SUM(profit) as total_profit by region_id; then Join Regions on region_id to retrieve manager_name and region_name; output to the server. Please auto-generate an output node name based on the 'Manager Performance Summary' theme and save the flow."

**Capability Test Prompt (Minimalist)**:
> "Please read `db_superstore.md`. Use `cwprep` to connect to the local localhost database 'superstore'. **Business goal**: Summarize each regional manager's sales and profit performance, excluding minor sporadic orders. Based on the table schema, automatically infer the filter conditions, aggregation dimensions, and the join to manager information, generate an output node, and save the flow. Do not ask me for specific fields or thresholds — use your own judgment."
