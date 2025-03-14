{{
    config(
        tags=['crossell'],
    )
}}

with c1 as (
    select 
        customer_id, product_id,
        date_trunc('MONTH', payment_date) pay_month,
        count(*) over(partition by pay_month order by pay_month, product_id) as part
    from {{ref('stg_transactions')}}
)
select * from c1
