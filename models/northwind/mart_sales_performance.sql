with sales as (
    select * from {{ ref('prep_sales') }}
)
select
    order_year,
    order_month,
    category_name,
    sum(revenue) as total_revenue,
    count(distinct order_id) as total_orders,
    sum(revenue) / nullif(count(distinct order_id), 0) as avg_revenue_per_order
from sales
group by
    order_year,
    order_month,
    category_name
order by
    order_year,
    order_month,
    category_name