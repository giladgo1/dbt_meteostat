with orders as (
    select * from {{ ref('staging_orders') }}
),
order_details as (
    select * from {{ ref('staging_order_details') }}
),
products as (
    select * from {{ ref('staging_products') }}
),
categories as (
    select * from {{ ref('staging_categories') }}
),
prep_sales as (
    select
        od.order_id,
        o.order_date,
        extract(year from o.order_date) as order_year,
        extract(month from o.order_date) as order_month,
        od.product_id,
        p.product_name,
        p.category_id,
        c.category_name,
        od.unit_price,
        od.quantity,
        od.discount,
        od.unit_price * od.quantity * (1 - od.discount) as revenue
    from order_details od
    inner join orders o
        on od.order_id = o.order_id
    inner join products p
        on od.product_id = p.product_id
    left join categories c
        on p.category_id = c.category_id
)
select *
from prep_sales