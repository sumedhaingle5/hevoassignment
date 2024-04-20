{{ config(materialized='table') }}

-- dataset to select customer ID, first name, and last name from the customers table
with customer_info as (
    select
        id as customer_id,
        first_name,
        last_name
    from customers.customers.hevo_raw_customers
),

-- dataset to get the first order date for each customer
first_order_date as (
    select
        user_id as customer_id,
        min(order_date) as most_recent_order
    from customers.customers.hevo_raw_orders
    group by user_id
),

-- dataset to get the last order date for each customer
last_order_date as (
    select
        id,
        user_id as customer_id,
        max(order_date) as number_of_orders
    from customers.customers.hevo_raw_orders
    group by user_id
),

-- dataset to calculate the sum of all payments made by each customer
customer_payments as (
    select
        id as customer_id,
        order_id,
        sum(amount) as customer_lifetime_value
    from customers.customers.hevo_raw_payments
    group by id
)

-- Final query to join all the information and create the target table customers
select
    ci.customer_id,
    ci.first_name,
    ci.last_name,
    fod.most_recent_order,
    lod.number_of_orders,
    cp.customer_lifetime_value
from
    customer_info ci
left join
    first_order_date fod on ci.customer_id = fod.customer_id
left join
    last_order_date lod on ci.customer_id = lod.customer_id
left join
    customer_payments cp on lod.id = cp.order_id