{{
    config(
        tags=['stage'],
    )
}}


with c1 as (
    select * from {{ source('source', 'customers') }}
    where company <> 'company' or customername != 'customername'
),
c2 as (
    select 
        TRY_CAST(customer_id as INT) as customer_id,
        company,
        customername
    from c1
),
c3 as (
    select * from c2 where customer_id is not null
)

select * from c3