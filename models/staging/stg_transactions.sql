{{
    config(
        tags=['stage'],
    )
}}


with c1 as (
    select 
        *
    from {{ source('source', 'transactions') }}
    where lower(trim(customer_id)) not like '%customer id%'
    
),
c2 as (
    select
        try_cast(customer_id as int) as customer_id,
        trim(product_id) as product_id,
        TRY_TO_DATE(payment_date, 'DD-MM-YYYY') as payment_date,
        try_cast(revenue_type as int) as revenue_type,
        try_cast(revenue as float) as revenue,
        try_cast(quantity as int) as quantity, 
        try_cast(dim1 as varchar(1)) as dim1,
        try_cast(dim2 as varchar(1)) as dim2,
        try_cast(dim3 as varchar(1)) as dim3,
        try_cast(dim4 as varchar(1)) as dim4,
        try_cast(dim5 as varchar(1)) as dim5,
        try_cast(dim6 as varchar(1)) as dim6,
        try_cast(dim7 as varchar(1)) as dim7,
        try_cast(dim8 as varchar(1)) as dim8,
        try_cast(dim9 as varchar(1)) as dim9,
        try_cast(dim10 as varchar(1)) as dim10
    from c1
)
select * from c2
where customer_id is not null