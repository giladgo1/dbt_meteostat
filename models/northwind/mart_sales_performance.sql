with sales as (
    select * from {{ ref('prep_sales') }}
),
order_level as (
    select
        order_year,
        order_month,
        category_name,
        order_id,
        sum(revenue) as order_revenue
    from sales
    group by 1,2,3,4
)
select
    order_year,
    order_month,
    category_name,
    sum(order_revenue) as total_revenue,
    count(order_id) as total_orders,
    avg(order_revenue) as avg_revenue_per_order,
    stddev(order_revenue) as stddev_revenue_per_order
from order_level
group by 1,2,3
order by 1,2,3