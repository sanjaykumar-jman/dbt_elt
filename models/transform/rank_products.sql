{{
    config(
        tags=['rank_prods'],
    )
}}

with c1 as(
    select 
        trans.product_id,
        prods.product_family,
        sum(trans.revenue*trans.quantity) as total_revenue
    from {{ref('stg_transactions')}} trans left join {{ref('stg_products')}} prods
    on trans.product_id = prods.product_id
    group by trans.product_id, prods.product_family
    order by total_revenue desc
)
select *, rank() over(order by total_revenue desc) as revenue_rank from c1