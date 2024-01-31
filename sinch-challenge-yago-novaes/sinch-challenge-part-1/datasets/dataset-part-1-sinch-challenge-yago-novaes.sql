--- sources

with 

orders as (

  select
    order_id, 
    user_id, 
    created_at,
    date_trunc(delivered_at, MONTH) as delivery_month,
    status 
  from bigquery-public-data.thelook_ecommerce.orders 

),

order_items as (

  select 
    order_id, 
    product_id, 
    sale_price 
  from bigquery-public-data.thelook_ecommerce.order_items 

),

products as (

  select 
    id, 
    cost
  from bigquery-public-data.thelook_ecommerce.products

),

users as (

  select
    id, 
    traffic_source 
  from bigquery-public-data.thelook_ecommerce.users 

),

-- filter and join
-- perform joins on several tables (orders, order_items, products, and users) to filter relevant data

filter_join as (

  select 
    orders.order_id,
    orders.user_id,
    order_items.product_id,
    orders.delivery_month,
    orders.status,
    order_items.sale_price,
    products.cost,
    users.traffic_source
  from orders
  join order_items on orders.order_id = order_items.order_id
  join products on order_items.product_id = products.id
  join users on orders.user_id = users.id
  where orders.status = 'Complete' 
      and users.traffic_source = 'Facebook'
      and orders.created_at between '2022-01-01' and '2023-06-30'

),

-- agreggations CTEs
-- calculate aggregated monthly metrics (total_revenue, total_profit, product_quantity, orders_quantity, users_quantity)

monthly_sales as (

 select 
    delivery_month,
    traffic_source,
    sum(sale_price) as total_revenue,
    sum(sale_price) - sum(cost) as total_profit,
    count(distinct product_id) as product_quantity,
    count(distinct order_id) as orders_quantity,
    count(distinct user_id) as users_quantity
  from filter_join
  group by delivery_month, traffic_source

),

-- calculate the moving average for the last 3 months (if there is enough data)

moving_avg as (

  select 
    delivery_month,
    traffic_source,
    case when count(delivery_month) over(partition by traffic_source order by delivery_month rows between 2 preceding and current row) >= 3
         then avg(total_revenue) over(partition by traffic_source order by delivery_month rows between 2 preceding and current row)
         else null end as revenue_moving_avg,
    case when count(delivery_month) over(partition by traffic_source order by delivery_month rows between 2 preceding and current row) >= 3
         then avg(total_profit) over(partition by traffic_source order by delivery_month rows between 2 preceding and current row)
         else null end as profit_moving_avg
  from monthly_sales

),

-- final calculate CTE
-- calculate final metrics and compare with previous values

final as (

  select 
    monthly_sales.delivery_month,
    monthly_sales.traffic_source,
    monthly_sales.total_revenue,
    monthly_sales.total_profit,
    monthly_sales.product_quantity,
    monthly_sales.orders_quantity,
    monthly_sales.users_quantity,
    monthly_sales.total_revenue - moving_avg.revenue_moving_avg as revenue_vs_movingAvg,
    monthly_sales.total_revenue - lag(monthly_sales.total_revenue, 1) over(partition by monthly_sales.traffic_source order by monthly_sales.delivery_month) 
      as revenue_vs_prior_month,
    monthly_sales.total_revenue - lag(monthly_sales.total_revenue, 12) over(partition by monthly_sales.traffic_source order by monthly_sales.delivery_month) 
      as revenue_vs_prior_year,
    monthly_sales.total_profit - moving_avg.profit_moving_avg as profit_vs_movingAvg,
    monthly_sales.total_profit - lag(monthly_sales.total_profit, 1) over(partition by monthly_sales.traffic_source order by monthly_sales.delivery_month) 
      as profit_vs_prior_month,
    monthly_sales.total_profit - lag(monthly_sales.total_profit, 12) over(partition by monthly_sales.traffic_source order by monthly_sales.delivery_month) 
      as profit_vs_prior_year
  from monthly_sales
  left join moving_avg on monthly_sales.delivery_month = moving_avg.delivery_month 
                          and monthly_sales.traffic_source = moving_avg.traffic_source

)

-- final select statement

select 
  * 
from final
order by delivery_month, traffic_source;