with
    -- Identifying duplicate records in the orders table based on order_id

    duplicates as (

        select 
            order_id, count(*) 
        from bigquery-public-data.thelook_ecommerce.orders  
        group by order_id 
        having count(*) > 1

    ),

    -- Counting total orders and identifying null values in key columns of the orders table

    null_values as (

        select 
            count(*) as total_orders,
            countif(order_id is null) as null_order_id,
            countif(user_id is null) as null_user_id,
            countif(created_at is null) as null_created_at
        from bigquery-public-data.thelook_ecommerce.orders

    ),

    -- Checking for orders with illogical time intervals (shipped before creation or delivered before shipping)

    logical_time_intervals as (

        select 
            count(*) 
        from bigquery-public-data.thelook_ecommerce.orders 
        where shipped_at < created_at or delivered_at < shipped_at

    )
    
-- Final select statement to evaluate data consistency and integrity in the orders table

select
    (select 
        case 
            when count(*) = 0 then 1
            else 0
        end
     from duplicates) as duplicates,
    (select 
        case
            when total_orders is not null and null_order_id = 0 and null_user_id = 0 and null_created_at = 0
            then 1
            else 0
        end
     from null_values) as null_values,
    (select 
        case 
            when count(*) = 0 then 1
            else 0
        end
     from logical_time_intervals) as logical_time_intervals;
