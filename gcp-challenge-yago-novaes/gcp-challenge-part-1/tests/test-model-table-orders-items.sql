with
    
    -- Identifying duplicate records in order_items table

    duplicates as (

        select 
            *, count(*) as count
        from bigquery-public-data.thelook_ecommerce.order_items
        group by id, order_id, user_id, product_id, inventory_item_id, status, created_at, shipped_at, delivered_at, returned_at, sale_price
        having count(*) > 1

    ),

    -- Counting null values in key columns of order_items table

    null_values as (

        select 
            countif(id is null) as null_ids,
            countif(order_id is null) as null_order_ids,
            countif(user_id is null) as null_user_ids,
            countif(product_id is null) as null_product_ids,
            countif(sale_price is null) as null_sale_prices
        from bigquery-public-data.thelook_ecommerce.order_items

    ),

    -- Checking for records with inconsistent date fields (shipped after delivery or delivered after return)

    inconsistent_dates as (

        select 
            count(*) as inconsistent_date_records
        from bigquery-public-data.thelook_ecommerce.order_items
        where shipped_at > delivered_at or delivered_at > returned_at

    ),

    -- Identifying records with invalid price values (negative or extremely high)

    price_ranges as (

        select 
            countif(sale_price < 0) as negative_prices,
            countif(sale_price > 10000) as unusually_high_prices
        from bigquery-public-data.thelook_ecommerce.order_items

    ),

    -- Checking for order_items that refer to non-existent orders (broken referential integrity)

    referential_integrity as (

        select 
            oi.order_id
        from bigquery-public-data.thelook_ecommerce.order_items oi
        left join bigquery-public-data.thelook_ecommerce.orders o on oi.order_id = o.order_id
        where o.order_id is null

    )
    
-- Final select statement to check the integrity of the dataset based on the above validations

select
    (select 
        case 
            when count(*) = 0 then 1
            else 0
        end
     from duplicates) as duplicates,
    (select 
        case
            when null_ids = 0 and null_order_ids = 0 and null_user_ids = 0 and null_product_ids = 0 and null_sale_prices = 0
            then 1
            else 0
        end
     from null_values) as null_values,
    (select 
        case
            when inconsistent_date_records = 0
            then 1
            else 0
        end
     from inconsistent_dates) as inconsistent_dates,
    (select 
        case
            when negative_prices = 0 and unusually_high_prices = 0
            then 1
            else 0
        end
     from price_ranges) as price_ranges,
    (select 
        case
            when count(*) = 0
            then 1
            else 0
        end
     from referential_integrity) as referential_integrity;