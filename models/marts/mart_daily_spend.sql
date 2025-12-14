{{ config(materialized='table') }}

SELECT
    posting_date,
    merchant_category,
    city,
    COUNT(*) AS txn_count,
    SUM(amount) AS total_spend
FROM {{ ref('stg_retail_card_transactions') }}
GROUP BY 1,2,3
