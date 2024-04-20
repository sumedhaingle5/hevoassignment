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
        a.id as order_id,
        sum(b.amount) as order_amount,
        b.order_id
    from customers.customers.hevo_raw_orders as a
    inner join customers.customers.hevo_raw_payments as b on a.id = b.order_id
    group by a.id
),

target as (
    select *
    from customer_info
    inner join order_info on customer_info.customer_id = order_info.customer_id
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

    
