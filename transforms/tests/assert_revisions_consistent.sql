-- One revision row per (minute, measure); historical never seen before first publication;
-- revision arithmetic holds.
select valid_from, measure, 'duplicate' as problem
from {{ ref('fct_revisions') }}
group by valid_from, measure
having count(*) > 1

union all

select valid_from, measure, 'historical before first publication'
from {{ ref('fct_revisions') }}
where historical_seen < first_seen

union all

select valid_from, measure, 'revision arithmetic'
from {{ ref('fct_revisions') }}
where revision is distinct from historical_value - first_value
