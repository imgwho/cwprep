-- ============================================
-- cwprep Superstore Sample Database
-- ============================================
-- Based on Tableau Sample - Superstore dataset
-- Normalized into 5 relational tables for Tableau Prep join and data processing demos
--
-- Usage:
--   In MySQL client: source init_superstore.sql
-- ============================================

CREATE DATABASE IF NOT EXISTS superstore 
    CHARACTER SET utf8mb4 
    COLLATE utf8mb4_unicode_ci;

USE superstore;

-- ============================================
-- 1. Regions table
-- ============================================
DROP TABLE IF EXISTS regions;
CREATE TABLE regions (
    region_id INT PRIMARY KEY AUTO_INCREMENT,
    region_name VARCHAR(50) NOT NULL COMMENT 'Region name',
    manager_name VARCHAR(100) NOT NULL COMMENT 'Regional manager'
) COMMENT='Regions and managers lookup table';

INSERT INTO regions (region_id, region_name, manager_name) VALUES
(1, 'West', 'Sadie Pawthorne'),
(2, 'East', 'Chuck Magee'),
(3, 'Central', 'Roxanne Rodriguez'),
(4, 'South', 'Fred Suzuki');

-- ============================================
-- 2. Customers table
-- ============================================
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
    customer_id VARCHAR(20) PRIMARY KEY COMMENT 'Customer ID',
    customer_name VARCHAR(100) NOT NULL COMMENT 'Customer name',
    segment ENUM('Consumer', 'Corporate', 'Home Office') NOT NULL COMMENT 'Customer segment'
) COMMENT='Customer master data';

INSERT INTO customers (customer_id, customer_name, segment) VALUES
('CG-12520', 'Claire Gute', 'Consumer'),
('DV-13045', 'Darrin Van Huff', 'Corporate'),
('SO-20335', 'Sean O\'Donnell', 'Consumer'),
('BH-11710', 'Brosina Hoffman', 'Consumer'),
('AA-10480', 'Andrew Allen', 'Consumer'),
('TB-21400', 'Tom Boeckenhauer', 'Corporate'),
('NK-18475', 'Nick Kotsogiannis', 'Consumer'),
('PO-19195', 'Pete Okada', 'Home Office'),
('JL-15835', 'Jack Lebron', 'Consumer'),
('CM-12385', 'Christopher Martinez', 'Home Office'),
('SS-20440', 'Sanjit Shah', 'Consumer'),
('EM-14155', 'Emily McKay', 'Corporate'),
('LH-16900', 'Lena Hernandez', 'Home Office'),
('RW-19450', 'Randy Wu', 'Consumer'),
('KG-16585', 'Kevin Graham', 'Corporate');

-- ============================================
-- 3. Products table
-- ============================================
DROP TABLE IF EXISTS products;
CREATE TABLE products (
    product_id VARCHAR(20) PRIMARY KEY COMMENT 'Product ID',
    product_name VARCHAR(255) NOT NULL COMMENT 'Product name',
    category ENUM('Furniture', 'Office Supplies', 'Technology') NOT NULL COMMENT 'Category',
    sub_category VARCHAR(50) NOT NULL COMMENT 'Sub-category'
) COMMENT='Product master data';

INSERT INTO products (product_id, product_name, category, sub_category) VALUES
('FUR-BO-10001798', 'Bush Somerset Collection Bookcase', 'Furniture', 'Bookcases'),
('FUR-CH-10000454', 'Hon Deluxe Fabric Upholstered Task Chairs', 'Furniture', 'Chairs'),
('OFF-LA-10000240', 'Self-Adhesive Mailing Labels', 'Office Supplies', 'Labels'),
('OFF-ST-10000760', 'Eldon Expressions Punched Metal Desktop Organizer', 'Office Supplies', 'Storage'),
('OFF-AR-10002833', 'Newell 322 Stapler', 'Office Supplies', 'Art'),
('TEC-PH-10002275', 'Mitel Cordless Phone', 'Technology', 'Phones'),
('OFF-BI-10003910', 'GBC Ibimaster 500 Manual Binding System', 'Office Supplies', 'Binders'),
('OFF-AP-10002892', 'Eureka Disposable Bags', 'Office Supplies', 'Appliances'),
('FUR-TA-10000577', 'Chromcraft Rectangular Conference Tables', 'Furniture', 'Tables'),
('OFF-PA-10002365', 'Xerox 1967 Premium Copy Paper', 'Office Supplies', 'Paper'),
('TEC-AC-10003832', 'Belkin USB Cable', 'Technology', 'Accessories'),
('TEC-CO-10004722', 'Canon Fax Phone L170', 'Technology', 'Copiers'),
('OFF-EN-10001990', 'White Envelopes', 'Office Supplies', 'Envelopes'),
('TEC-MA-10002412', 'Logitech Wireless Mouse', 'Technology', 'Machines'),
('FUR-FU-10001889', 'Eldon File Cart', 'Furniture', 'Furnishings');

-- ============================================
-- 4. Orders table
-- ============================================
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
    row_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id VARCHAR(20) NOT NULL COMMENT 'Order ID',
    order_date DATE NOT NULL COMMENT 'Order date',
    ship_date DATE COMMENT 'Ship date',
    ship_mode ENUM('Standard Class', 'Second Class', 'First Class', 'Same Day') NOT NULL COMMENT 'Shipping mode',
    customer_id VARCHAR(20) NOT NULL COMMENT 'Customer ID',
    region_id INT NOT NULL COMMENT 'Region ID',
    city VARCHAR(100) COMMENT 'City',
    state VARCHAR(100) COMMENT 'State',
    postal_code VARCHAR(20) COMMENT 'Postal code',
    product_id VARCHAR(20) NOT NULL COMMENT 'Product ID',
    sales DECIMAL(12,4) NOT NULL COMMENT 'Sales amount',
    quantity INT NOT NULL COMMENT 'Quantity',
    discount DECIMAL(5,2) DEFAULT 0 COMMENT 'Discount rate',
    profit DECIMAL(12,4) NOT NULL COMMENT 'Profit',
    INDEX idx_order_date (order_date),
    INDEX idx_customer (customer_id),
    INDEX idx_region (region_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (region_id) REFERENCES regions(region_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
) COMMENT='Order details table';

-- January 2024 orders
INSERT INTO orders (order_id, order_date, ship_date, ship_mode, customer_id, region_id, city, state, postal_code, product_id, sales, quantity, discount, profit) VALUES
('US-2024-100001', '2024-01-03', '2024-01-07', 'Standard Class', 'CG-12520', 1, 'Los Angeles', 'California', '90001', 'FUR-BO-10001798', 261.96, 2, 0.00, 41.91),
('US-2024-100002', '2024-01-04', '2024-01-08', 'Second Class', 'DV-13045', 2, 'New York', 'New York', '10001', 'OFF-LA-10000240', 14.62, 2, 0.00, 6.87),
('US-2024-100003', '2024-01-05', '2024-01-07', 'First Class', 'SO-20335', 3, 'Chicago', 'Illinois', '60601', 'FUR-CH-10000454', 731.94, 3, 0.00, 219.58),
('US-2024-100004', '2024-01-06', '2024-01-06', 'Same Day', 'BH-11710', 4, 'Houston', 'Texas', '77001', 'OFF-ST-10000760', 957.58, 5, 0.45, -383.03),
('US-2024-100005', '2024-01-08', '2024-01-12', 'Standard Class', 'AA-10480', 1, 'San Francisco', 'California', '94102', 'TEC-PH-10002275', 907.15, 5, 0.20, 90.72),
('US-2024-100006', '2024-01-10', '2024-01-14', 'Second Class', 'TB-21400', 2, 'Philadelphia', 'Pennsylvania', '19101', 'OFF-BI-10003910', 22.37, 3, 0.20, 2.52),
('US-2024-100007', '2024-01-12', '2024-01-16', 'Standard Class', 'NK-18475', 3, 'Detroit', 'Michigan', '48201', 'OFF-AP-10002892', 48.86, 7, 0.00, 14.17),
('US-2024-100008', '2024-01-14', '2024-01-18', 'First Class', 'PO-19195', 4, 'Dallas', 'Texas', '75201', 'FUR-TA-10000577', 1706.18, 9, 0.20, 85.31),
('US-2024-100009', '2024-01-15', '2024-01-19', 'Standard Class', 'JL-15835', 1, 'Seattle', 'Washington', '98101', 'OFF-PA-10002365', 15.55, 5, 0.00, 5.44),
('US-2024-100010', '2024-01-18', '2024-01-22', 'Second Class', 'CM-12385', 2, 'Boston', 'Massachusetts', '02101', 'TEC-AC-10003832', 11.78, 3, 0.20, 3.33),
('US-2024-100011', '2024-01-20', '2024-01-24', 'Standard Class', 'SS-20440', 3, 'Minneapolis', 'Minnesota', '55401', 'TEC-CO-10004722', 2573.82, 4, 0.00, 772.15),
('US-2024-100012', '2024-01-22', '2024-01-26', 'First Class', 'EM-14155', 4, 'Atlanta', 'Georgia', '30301', 'OFF-EN-10001990', 6.48, 2, 0.00, 3.11),
('US-2024-100013', '2024-01-24', '2024-01-28', 'Standard Class', 'LH-16900', 1, 'Portland', 'Oregon', '97201', 'TEC-MA-10002412', 127.42, 2, 0.00, 44.60),
('US-2024-100014', '2024-01-26', '2024-01-30', 'Second Class', 'RW-19450', 2, 'Miami', 'Florida', '33101', 'FUR-FU-10001889', 407.98, 3, 0.00, 81.60),
('US-2024-100015', '2024-01-28', '2024-01-30', 'Same Day', 'KG-16585', 3, 'Denver', 'Colorado', '80201', 'OFF-AR-10002833', 9.94, 2, 0.00, 2.98);

-- February 2024 orders (growth)
INSERT INTO orders (order_id, order_date, ship_date, ship_mode, customer_id, region_id, city, state, postal_code, product_id, sales, quantity, discount, profit) VALUES
('US-2024-100016', '2024-02-02', '2024-02-06', 'Standard Class', 'CG-12520', 1, 'Los Angeles', 'California', '90001', 'TEC-PH-10002275', 1089.18, 6, 0.10, 130.70),
('US-2024-100017', '2024-02-03', '2024-02-07', 'Second Class', 'DV-13045', 2, 'New York', 'New York', '10001', 'FUR-CH-10000454', 487.96, 2, 0.00, 146.39),
('US-2024-100018', '2024-02-05', '2024-02-09', 'First Class', 'SO-20335', 3, 'Chicago', 'Illinois', '60601', 'OFF-ST-10000760', 191.52, 1, 0.00, 86.18),
('US-2024-100019', '2024-02-06', '2024-02-10', 'Standard Class', 'BH-11710', 4, 'Houston', 'Texas', '77001', 'TEC-AC-10003832', 35.34, 9, 0.00, 9.98),
('US-2024-100020', '2024-02-08', '2024-02-12', 'Second Class', 'AA-10480', 1, 'San Francisco', 'California', '94102', 'OFF-BI-10003910', 111.85, 5, 0.20, 12.60),
('US-2024-100021', '2024-02-10', '2024-02-14', 'First Class', 'TB-21400', 2, 'Philadelphia', 'Pennsylvania', '19101', 'FUR-BO-10001798', 392.94, 3, 0.00, 62.87),
('US-2024-100022', '2024-02-12', '2024-02-16', 'Standard Class', 'NK-18475', 3, 'Detroit', 'Michigan', '48201', 'TEC-CO-10004722', 3217.28, 5, 0.00, 965.18),
('US-2024-100023', '2024-02-14', '2024-02-18', 'Same Day', 'PO-19195', 4, 'Dallas', 'Texas', '75201', 'OFF-PA-10002365', 31.10, 10, 0.00, 10.89),
('US-2024-100024', '2024-02-15', '2024-02-19', 'Standard Class', 'JL-15835', 1, 'Seattle', 'Washington', '98101', 'FUR-TA-10000577', 2275.57, 12, 0.15, 113.78),
('US-2024-100025', '2024-02-18', '2024-02-22', 'Second Class', 'CM-12385', 2, 'Boston', 'Massachusetts', '02101', 'OFF-LA-10000240', 29.24, 4, 0.00, 13.74),
('US-2024-100026', '2024-02-20', '2024-02-24', 'First Class', 'SS-20440', 3, 'Minneapolis', 'Minnesota', '55401', 'TEC-MA-10002412', 254.84, 4, 0.00, 89.19),
('US-2024-100027', '2024-02-22', '2024-02-26', 'Standard Class', 'EM-14155', 4, 'Atlanta', 'Georgia', '30301', 'FUR-FU-10001889', 543.97, 4, 0.00, 108.79),
('US-2024-100028', '2024-02-24', '2024-02-28', 'Same Day', 'LH-16900', 1, 'Portland', 'Oregon', '97201', 'OFF-AP-10002892', 69.80, 10, 0.00, 20.24),
('US-2024-100029', '2024-02-26', '2024-02-28', 'First Class', 'RW-19450', 2, 'Miami', 'Florida', '33101', 'OFF-AR-10002833', 14.91, 3, 0.00, 4.47),
('US-2024-100030', '2024-02-28', '2024-02-28', 'Same Day', 'KG-16585', 3, 'Denver', 'Colorado', '80201', 'OFF-EN-10001990', 19.44, 6, 0.00, 9.33);

-- ============================================
-- 5. Returns table
-- ============================================
DROP TABLE IF EXISTS returns;
CREATE TABLE returns (
    return_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id VARCHAR(20) NOT NULL COMMENT 'Order ID',
    returned ENUM('Yes', 'No') DEFAULT 'Yes' COMMENT 'Returned flag'
) COMMENT='Returns records table';

INSERT INTO returns (order_id, returned) VALUES
('US-2024-100004', 'Yes'),  -- BH-11710's negative profit order
('US-2024-100006', 'Yes'),  -- Small order return
('US-2024-100020', 'Yes');  -- February order return

-- ============================================
-- Verify data
-- ============================================
SELECT 'regions' AS table_name, COUNT(*) AS row_count FROM regions
UNION ALL SELECT 'customers', COUNT(*) FROM customers
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'orders', COUNT(*) FROM orders
UNION ALL SELECT 'returns', COUNT(*) FROM returns;

-- Sales summary by region
SELECT 
    r.region_name AS Region,
    r.manager_name AS Manager,
    COUNT(o.row_id) AS Orders,
    ROUND(SUM(o.sales), 2) AS Sales,
    ROUND(SUM(o.profit), 2) AS Profit
FROM orders o
JOIN regions r ON o.region_id = r.region_id
GROUP BY r.region_id
ORDER BY Sales DESC;

-- Monthly summary
SELECT 
    DATE_FORMAT(order_date, '%Y-%m') AS Month,
    COUNT(*) AS Orders,
    ROUND(SUM(sales), 2) AS Sales,
    ROUND(SUM(profit), 2) AS Profit
FROM orders
GROUP BY DATE_FORMAT(order_date, '%Y-%m');
