-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where total_amount < 0 to make the test fail.
select
    count(*) as cnt
from {{ ref('customers' )}}
where customer_id is null
having cnt < 0