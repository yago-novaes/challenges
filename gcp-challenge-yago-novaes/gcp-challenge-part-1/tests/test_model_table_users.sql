with

  -- CTE to identify duplicate user records based on the id column.

  duplicates as (

    select 
      id, count(*) as num_occurrences
    from bigquery-public-data.thelook_ecommerce.users
    group by id
    having count(*) > 1

  ),

  -- CTE to count total rows and identify null values in various columns of the users table.

  null_values as (

    select 
      count(*) as total_rows,
      countif(id is null) as null_id,
      countif(first_name is null) as null_first_name,
      countif(last_name is null) as null_last_name,
      countif(email is null) as null_email,
      countif(age is null) as null_age,
      countif(gender is null) as null_gender,
      countif(state is null) as null_state,
      countif(street_address is null) as null_street_address,
      countif(postal_code is null) as null_postal_code,
      countif(city is null) as null_city,
      countif(country is null) as null_country,
      countif(latitude is null) as null_latitude,
      countif(longitude is null) as null_longitude,
      countif(traffic_source is null) as null_traffic_source,
      countif(created_at is null) as null_created_at
    from bigquery-public-data.thelook_ecommerce.users

  )

-- Final select statement to evaluate data quality in the users table. It checks for any duplicate records and the presence of null values in critical columns.

select
  (select 
      case
          when count(*) = 0 then 1
          else 0
      end
   from duplicates) as duplicates,
  (select 
      case
          when total_rows is not null and null_id = 0 and null_first_name = 0 and null_last_name = 0
              and null_email = 0 and null_age = 0 and null_gender = 0 and null_state = 0
              and null_street_address = 0 and null_postal_code = 0 and null_city = 0
              and null_country = 0 and null_latitude = 0 and null_longitude = 0
              and null_traffic_source = 0 and null_created_at = 0
          then 1
          else 0
      end
   from null_values) as null_values;
