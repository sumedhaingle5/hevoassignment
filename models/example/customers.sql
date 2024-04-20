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
first_order as (
    select
        user_id as customer_id,
        min(order_date) as first_order
    from customers.customers.hevo_raw_orders
    group by user_id
)

-- -- dataset to get the last order date for each customer

most_recent_order as (
    select
        user_id as customer_id,
        max(order_date) as most_recent_order
    from customers.customers.hevo_raw_orders
    group by user_id
),

number_of_orders as (
    select
        user_id as customer_id
        count(id) as number_of_orders
    from customers.customers.hevo_raw_orders
    group by user_id
),

-- dataset to calculate the sum of all payments made by each customer
-- customer_lifetime_value as (
--     select
--         id as customer_id,
--         order_id,
--         sum(amount) as customer_lifetime_value
--     from customers.customers.hevo_raw_payments
--     group by user_id
-- )

select
    ci.customer_id,
    ci.first_name,
    ci.last_name,
    first_order.first_order
from
    customer_info as ci
inner join
    first_order on ci.customer_id = first_order.customer_id
inner join
    most_recent_order on ci.customer_id = most_recent_order.customer_id
inner join
    number_of_orders on ci.customer_id = number_of_orders.customer_id
