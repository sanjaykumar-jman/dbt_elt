{{
    config(
        tags=['stage'],
    )
}}


with c1 as (
    select * from {{source('source', 'products')}}
    where lower(trim(product_id)) not like '%product id%'
),
c2 as (
	select 
		trim(product_id) as product_id, 
		trim(product_family) as product_family, 
		trim(product_sub_family) as product_sub_family
	from c1
)

select * from c2 
