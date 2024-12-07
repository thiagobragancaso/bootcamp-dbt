WITH

fct AS (
    SELECT * FROM {{ ref('fct_reviews') }}
),

dim AS (
    SELECT * FROM {{ ref('dim_listings_cleansed') }}
)


SELECT *
FROM fct
LEFT JOIN dim
    ON fct.listing_id = dim.listing_id
WHERE 1=1
    AND fct.review_date < dim.created_at
LIMIT