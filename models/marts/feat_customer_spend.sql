{{ config(materialized='table') }}

SELECT
    customer_id,
    COUNT(*) AS total_txns,
    SUM(amount) AS total_spend,
    AVG(amount) AS avg_txn_amount,
    SUM(CASE WHEN fraud_flag = 'CONFIRMED' THEN 1 ELSE 0 END) AS fraud_txn_count
FROM {{ ref('stg_retail_card_transactions') }}
GROUP BY customer_id
