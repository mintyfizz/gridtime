-- Silver must never hold two versions of the same (minute, measure) received at the
-- same instant. If it does, two writers ran concurrently (see docs/decisions/003).
select valid_from, measure, transaction_time, count(*) as n
from {{ ref('stg_silver__versions') }}
group by valid_from, measure, transaction_time
having count(*) > 1
