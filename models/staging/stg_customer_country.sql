{{
    config(
        tags=['stage'],
    )
}}

with c1 as (
    select 
        try_cast(customer_id as int) as customer_id,
        try_cast(country as varchar(50)) as country,
        try_cast(region as varchar(50)) as region
    from {{ source('source', 'customer_country') }}
),
c2 as (
    select * from c1 where customer_id is not null
)

select * from c2
