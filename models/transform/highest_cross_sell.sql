{{
    config(
        tags=['crossell'],
    )
}}

with first_payment as (
    select
        customer_id,
        min(payment_date) as first_payment_date
    from
        {{ ref('stg_transactions') }}
    group by
        customer_id
),
 
trans_data as (
    select
        customer_id,
        product_id,
        payment_date
    from
        {{ ref('stg_transactions') }}
)
 
select
    trans_data.customer_id,
    count(distinct product_id) as cross_sell,
    dense_rank() over (order by cross_sell desc) as product_rank
from trans_data
left join first_payment as fp
on trans_data.customer_id = fp.customer_id
where fp.first_payment_date <> payment_date
group by trans_data.customer_id
order by product_rank
