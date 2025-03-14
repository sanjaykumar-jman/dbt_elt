{{
    config(
        tags=['fy_custs'],
    )
}}

with add_fy as( 
    select 
        trans.customer_id,
        custs.customername,
        trans.payment_date,
        (case 
            WHEN MONTH(trans.payment_date) >= 4 THEN YEAR(trans.payment_date)
            ELSE YEAR(trans.payment_date) - 1
        end) as fiscal_year,
        trans.revenue,
        trans.revenue_type
    from {{ ref('stg_transactions') }} trans                                                                              
    left join {{ ref('stg_customers') }} custs 
    on trans.customer_id = custs.customer_id
    order by trans.payment_date
),
fy_partition as (-- now the data shows first transaction in each fiscal year
    select distinct
        customer_id,
        customername,
        min(payment_date) as first_in_fy,
        fiscal_year,
        row_number() over(partition by customername, customer_id order by first_in_fy) as uid
    from add_fy 
    group by fiscal_year, customer_id, customername
    order by customername
),
new_in_fy as (-- we need to only select the customers who performed the first transaction, not first transaction every FY
    select 
        customer_id,
        customername,
        first_in_fy,
        fiscal_year
    from fy_partition
    where uid = 1
)
select * from new_in_fy
