{{
  config(
    materialized = 'table',
    )
}}

WITH 

hosts AS (
    SELECT * FROM {{ ref('dim_hosts_cleansed') }}
),

listings AS (
    SELECT * FROM {{ ref('dim_listings_cleansed') }}
),

final AS (
    SELECT 
        l.listing_id,
        l.listing_name,
        l.room_type,
        l.minimum_nights,
        l.price,
        l.host_id,
        h.host_name,
        h.is_superhost AS host_is_superhost,
        l.created_at,
        GREATEST(l.updated_at, h.updated_at) AS updated_at,
        CURRENT_TIMESTAMP() AS processed_at
    FROM
        listings AS l
    LEFT JOIN
        hosts AS h
            ON h.host_id = l.host_id

)

SELECT * FROM final