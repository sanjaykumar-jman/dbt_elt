{{
    config(
        tags=['contraction'],
    )
}}

with c1 as (
    select 
        date_trunc('MONTH', payment_date) pay_month,
        sum(revenue*quantity) as revenuepmonth,
        lag(revenuepmonth) over (order by pay_month) as revenuelag
    from {{ref('stg_transactions')}}
    group by pay_month
),
c2 as (
    select pay_month, revenuepmonth, coalesce(revenuelag, 0) revenuelag from c1
),
c3 as (
    select 
        pay_month, 
        revenuepmonth, 
        revenuelag,
        (revenuepmonth - revenuelag) as contraction
    from c2
    where revenuelag > revenuepmonth
)
select * from c3

/*

*/