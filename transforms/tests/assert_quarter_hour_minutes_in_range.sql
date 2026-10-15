-- A quarter-hour holds at most 15 one-minute observations.
select quarter_hour, minutes_observed
from {{ ref('fct_quarter_hour') }}
where minutes_observed < 1 or minutes_observed > 15
