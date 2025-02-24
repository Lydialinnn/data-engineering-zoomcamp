{{ config(materialized='table') }}

with quarterly_revenue as (
    select 
        service_type,
        year,
        quarter,
        year_quarter,
        sum(total_amount) as total_revenue
    from {{ ref('fact_trips') }}
    group by service_type, year, quarter, year_quarter
),
revenue_growth as (
    select 
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.year_quarter,
        sum(q1.total_revenue) as total_revenue,  -- Aggregate total_revenue
        sum(q2.total_revenue) as last_year_revenue,
        round(((sum(q1.total_revenue) - sum(q2.total_revenue)) / nullif(sum(q2.total_revenue), 0)) * 100, 2) as yoy_growth
    from quarterly_revenue q1
    left join quarterly_revenue q2
        on q1.service_type = q2.service_type
        and q1.quarter = q2.quarter
        and q1.year = q2.year + 1
    group by q1.service_type, q1.year, q1.quarter, q1.year_quarter
)
select * from revenue_growth
order by service_type, year, quarter
