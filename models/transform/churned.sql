{{
    config(
        tags=['churn'],
    )
}}

with transactions as (
    select distinct customer_id, payment_date
    from {{ ref('stg_transactions') }}
),
 
customer_lifetime as (
    select
        customer_id,
        min(payment_date) as first_purchase_month,
        max(payment_date) as last_purchase_month
    from transactions
    group by customer_id
),
 
churn_analysis as (
    select
        payment_date,
        count(distinct case when first_purchase_month = payment_date then customer_id end) as new_customers,
        count(distinct case when last_purchase_month = payment_date then customer_id end) as churned_customers
    from transactions
    join customer_lifetime using (customer_id)
    group by payment_date
)

select * from churn_analysis

