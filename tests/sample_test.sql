-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where total_amount < 0 to make the test fail.
select
    customer_id
from {{ ref('customers' )}}
group by 1
having total_amount > 0