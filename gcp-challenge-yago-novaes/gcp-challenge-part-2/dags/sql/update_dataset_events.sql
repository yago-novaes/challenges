-- sql/update_datasets_events.sql
SELECT
    TIMESTAMP_TRUNC(created_at, HOUR) AS created_at,
    traffic_source,
    postal_code,
    COUNT(DISTINCT session_id) AS sessions,
    COUNTIF(event_type = 'purchase') AS purchase_events,
    COUNT(*) AS total_events
FROM
    `bigquery-public-data.thelook_ecommerce.events`
WHERE
    CAST(created_at AS DATE) > DATE_SUB(CURRENT_DATE(), INTERVAL 3 DAY)
GROUP BY
    created_at, traffic_source, postal_code;
