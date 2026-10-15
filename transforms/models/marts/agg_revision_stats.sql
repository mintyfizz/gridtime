-- How much does Elia revise each measure between first publication and the historical feed?
select
    measure,
    any_value(unit) as unit,
    count(*) as minutes_compared,
    count(*) filter (where first_value is not null and historical_value is not null)
        as minutes_with_both_values,
    avg(case when abs_revision > 1e-9 then 1.0 else 0.0 end)
        filter (where abs_revision is not null) as share_changed,
    avg(case when (first_value is null) != (historical_value is null) then 1.0 else 0.0 end)
        as share_null_mismatch,
    avg(revision) as mean_revision,
    avg(abs_revision) as mean_abs_revision,
    approx_percentile(abs_revision, 0.95) as p95_abs_revision,
    max(abs_revision) as max_abs_revision,
    approx_percentile(hours_to_historical, 0.5) as median_hours_to_historical
from {{ ref('fct_revisions') }}
group by measure
