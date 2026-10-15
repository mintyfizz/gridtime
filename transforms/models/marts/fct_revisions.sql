-- For every (minute, measure) seen in both feeds: what was first published in near
-- real time vs the latest historical value. This is the core output of GridTime.
with versions as (
    select * from {{ ref('stg_silver__versions') }}
),

first_nrt as (
    select *
    from versions
    where stage = 'near_real_time'
    qualify row_number() over (
        partition by valid_from, measure order by transaction_time
    ) = 1
),

latest_hist as (
    select *
    from versions
    where stage = 'historical'
    qualify row_number() over (
        partition by valid_from, measure order by transaction_time desc
    ) = 1
),

version_counts as (
    select valid_from, measure, count(*) as versions
    from versions
    group by all
)

select
    f.valid_from,
    f.quarter_hour,
    f.measure,
    f.unit,
    f.value as first_value,
    h.value as historical_value,
    h.value - f.value as revision,
    abs(h.value - f.value) as abs_revision,
    f.quality_status as first_quality_status,
    h.quality_status as historical_quality_status,
    f.transaction_time as first_seen,
    h.transaction_time as historical_seen,
    date_diff('minute', f.transaction_time, h.transaction_time) / 60.0 as hours_to_historical,
    c.versions
from first_nrt f
join latest_hist h using (valid_from, measure)
join version_counts c using (valid_from, measure)
