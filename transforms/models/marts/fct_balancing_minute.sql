-- Latest known value of every measure, one row per minute (wide).
with latest as (
    select *
    from {{ ref('stg_silver__versions') }}
    qualify row_number() over (
        partition by valid_from, measure order by transaction_time desc
    ) = 1
)

select
    valid_from,
    any_value(valid_to) as valid_to,
    any_value(quarter_hour) as quarter_hour,
    {% for m in var('wide_measures') -%}
    max(value) filter (where measure = '{{ m }}') as {{ m }},
    {% endfor -%}
    bool_or(is_historical) as is_historical,
    arg_max(quality_status, transaction_time) as quality_status,
    max(transaction_time) as known_since
from latest
group by valid_from
