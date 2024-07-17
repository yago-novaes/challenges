
README - SQL Data Integrity and Consistency Tests
--------------------------------------------------

This document summarizes the data integrity and consistency tests performed on various tables of the 'bigquery-public-data.thelook_ecommerce' dataset, as outlined in the provided SQL files. Each file addresses a specific table and conducts a series of tests to ensure data quality.

1. Tests on the 'orders' Table (generic-tests-table-orders.sql):
   - Checks for duplicate order records based on the 'order_id'.
   - Identifies null values in key columns like 'order_id', 'user_id', and 'created_at'.
   - Evaluates logical inconsistencies in time-related fields (e.g., 'shipped_at' before 'created_at').

2. Tests on the 'order_items' Table (generic-tests-table-orders-items.sql):
   - Searches for duplicate order item entries.
   - Checks for null values in essential columns such as 'id', 'order_id', 'product_id', etc.
   - Assesses data consistency in pricing fields (e.g., sale prices, inventory item statuses).

3. Tests on the 'products' Table (generic-tests-table-products.sql):
   - Identifies duplicates in the product records by 'id'.
   - Examines null values across various product-related columns like 'id', 'cost', 'category', etc.
   - Ensures consistency in product costs and retail prices.

4. Tests on the 'users' Table (generic-tests-table-users.sql):
   - Checks for duplicate user records based on 'id'.
   - Identifies null values in multiple user-related fields such as 'id', 'first_name', 'last_name', 'email', etc.
   - Ensures overall data consistency across user attributes (e.g., address, traffic source).

Each test plays a crucial role in maintaining the integrity and quality of the dataset, ensuring the data is reliable for analysis and decision-making purposes.
