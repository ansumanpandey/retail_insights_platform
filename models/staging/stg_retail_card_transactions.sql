{{ config(materialized='view') }}

SELECT
    transaction_id,
    customer_id,
    card_network,
    card_last4,
    transaction_type,
    posting_date,
    transaction_timestamp,
    mcc,
    merchant_category,
    merchant_name,
    city,
    country,
    amount,
    fraud_flag,
    load_date
FROM {{ source('raw', 'retail_card_transactions') }}
