{{
  config(
    materialized = 'table',
    )
}}

WITH 

fct_reviews AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),

full_moon AS (
    SELECT * FROM {{ ref('seed_full_moon_dates') }}
),

final AS (
    SELECT
        fct.listing_id, 
        fct.review_date, 
        fct.reviewer_name, 
        fct.review_text, 
        fct.review_sentiment,
        CASE
            WHEN moon.full_moon_date IS NULL THEN 'not full moon'
            ELSE 'full moon'
        END AS is_full_moon,
        CURRENT_TIMESTAMP() AS processed_at
    FROM 
        fct_reviews AS fct
    LEFT JOIN
        full_moon AS moon
            ON TO_DATE(fct.review_date) = DATEADD(DAY, 1, moon.full_moon_date)
)

SELECT * FROM final