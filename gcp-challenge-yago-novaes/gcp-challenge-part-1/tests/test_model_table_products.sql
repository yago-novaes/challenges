with

    -- CTE for counting the total number of rows in the products table and identifying null values across various columns.

    null_values as (

        select 
            count(*) as total_rows,
            countif(id is null) as null_id,
            countif(cost is null) as null_cost,
            countif(category is null) as null_category,
            countif(name is null) as null_name,
            countif(brand is null) as null_brand,
            countif(retail_price is null) as null_retail_price,
            countif(department is null) as null_department,
            countif(sku is null) as null_sku,
            countif(distribution_center_id is null) as null_distribution_center_id
        from bigquery-public-data.thelook_ecommerce.products

    ),

    -- CTE for identifying duplicate records in the products table based on the id.

    duplicates as (

        select 
            id, count(*) as count
        from bigquery-public-data.thelook_ecommerce.products
        group by id
        having count(*) > 1

    ),

    -- CTE for checking data consistency by identifying products where the cost is greater than the retail price.

    data_consistency as (

        select 
            *
        from bigquery-public-data.thelook_ecommerce.products
        where cost > retail_price

    ),

    -- CTE for checking uniqueness of the sku field by identifying any duplicates.

    unique_sku as (

        select 
            sku, count(*) as count
        from bigquery-public-data.thelook_ecommerce.products
        group by sku
        having count(*) > 1

    )
  
-- The final select statement aggregates the results from the above CTEs to assess overall data integrity for the products table.

select
    (select 
        case
            when total_rows is not null and null_id = 0 and null_cost = 0 and null_category = 0 and null_name = 0 and null_brand = 0 
                and null_retail_price = 0 and null_department = 0 and null_sku = 0 and null_distribution_center_id = 0
            then 1
            else 0
        end
     from null_values) as null_values,
    (select 
        case
            when count(*) = 0 then 1
            else 0
        end
     from duplicates) as duplicates,
    (select 
        case
            when count(*) = 0 then 1
            else 0
        end
     from data_consistency) as data_consistency,
    (select 
        case
            when count(*) = 0 then 1
            else 0
        end 
     from unique_sku) as unique_sku;