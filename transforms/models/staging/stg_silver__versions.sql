-- Every stored version, typed and tidied. One row per (minute, measure, version).
select
    valid_from,
    valid_to,
    coalesce(quarter_hour, date_add('minute', -(minute(valid_from) % 15), date_trunc('minute', valid_from))) as quarter_hour,
    measure,
    value,
    unit,
    stage,
    stage = 'historical' as is_historical,
    quality_status,
    transaction_time,
    source_dataset,
    payload_hash
from {{ source('silver', 'balancing_minute') }}
