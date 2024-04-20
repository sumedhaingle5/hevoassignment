{{ config(materialized='table') }}

-- dataset to select customer ID, first name, and last name from the customers table
with customer_info as (
    select
        id as customer_id,
        first_name,
        last_name
    from customers.customers.hevo_raw_customers
),

order_info as (
    select
        a.user_id as customer_id,
        a.order_date,
        a.id,
        sum(b.amount) as order_amount,
        b.order_id
    from customers.customers.hevo_raw_orders as a
    inner join customers.customers.hevo_raw_payments as b on a.id = b.order_id
    group by b.order_id
),

target as (
    select
        ci.customer_id as customer_id,
        ci.first_name as first_name,
        ci.last_name as last_name,
        oi.order_date as order_date,
        oi.order_id as order_id,
        oi.order_amount as order_amount
    from customer_info as ci
    inner join order_info as oi on ci.customer_id = oi.customer_id
)

select
    customer_id,
    first_name,
    last_name,
    min(order_date) as first_order,
    max(order_date) as last_order,
    count(order_id) as order_count,
    sum(order_amount) as customer_lifetime_value
from
    target
group by customer_id

    
