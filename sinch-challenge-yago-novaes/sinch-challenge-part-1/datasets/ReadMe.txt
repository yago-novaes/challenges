
README - SQL Query Explanation
-------------------------------------

This SQL query performs a comprehensive analysis of sales data from the 'bigquery-public-data.thelook_ecommerce' dataset. The query is structured into several Common Table Expressions (CTEs) and a final select statement, each performing specific tasks as outlined below:

1. Source Tables Preparation (orders, order_items, products, users):
   - These CTEs prepare the source tables by selecting relevant columns for further analysis. 
   - 'orders': Selects order-related fields including the truncation of 'delivered_at' to the month.
   - 'order_items': Extracts order line item details including the sale price.
   - 'products': Selects product details including cost.
   - 'users': Extracts user details, focusing on the traffic source.

2. Filter and Join (filter_join):
   - This CTE performs joins across the orders, order_items, products, and users tables.
   - It filters data for 'Complete' orders, users coming from 'Facebook', and orders created between '2022-01-01' and '2023-06-30'.
   - The join consolidates fields necessary for calculating sales metrics.

3. Aggregations (monthly_sales):
   - Aggregates monthly sales metrics such as total revenue, total profit, product, orders, and user counts.
   - Grouping is done by delivery month and traffic source.

4. Moving Average Calculation (moving_avg):
   - Calculates the 3-month moving average for total revenue and total profit, provided there's sufficient historical data.
   - This is done using a window function over the aggregated monthly sales data.

5. Final Calculations (final):
   - This CTE combines data from 'monthly_sales' and 'moving_avg' to calculate final metrics.
   - It computes the difference in total revenue and total profit against their respective moving averages, the previous month, and the previous year.

6. Final Select Statement:
   - The final output is selected from the 'final' CTE.
   - It includes all the calculated metrics, ordered by delivery month and traffic source.

Overall, this query is designed to provide a detailed monthly sales analysis, focusing on revenue and profit trends over time, specifically for users acquired through Facebook. It effectively utilizes SQL joins, aggregation functions, window functions, and date operations to achieve a multi-faceted analysis of sales performance.
