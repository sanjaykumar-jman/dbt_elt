{{
    config(
        tags=['transform', 'rank_custs'],
    )
}}

with c1 as(
    select 
        trans.customer_id,
        custs.customername,
        sum(trans.revenue*trans.quantity) as total_revenue
    from {{ref('stg_transactions')}} trans left join {{ref('stg_customers')}} custs 
    on trans.customer_id = custs.customer_id
    group by trans.customer_id, custs.customername
    order by total_revenue desc
)
select *, rank() over(order by total_revenue desc) as revenue_rank from c1
