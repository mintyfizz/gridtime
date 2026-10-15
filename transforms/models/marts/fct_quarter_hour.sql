-- Quarter-hour view of the grid: the resolution at which Belgian imbalance is settled.
select
    quarter_hour,
    count(*) as minutes_observed,
    avg(system_imbalance) as avg_system_imbalance,
    min(system_imbalance) as min_system_imbalance,
    max(system_imbalance) as max_system_imbalance,
    avg(price_up_marginal) as avg_price_up,
    max(price_up_marginal) as max_price_up,
    avg(price_down_marginal) as avg_price_down,
    min(price_down_marginal) as min_price_down,
    avg(case when system_imbalance < 0 then 1.0 else 0.0 end) as share_minutes_short,
    avg(case when quality_status = 'DataIssue' then 1.0 else 0.0 end) as share_data_issue,
    bool_and(is_historical) as fully_historical,
    max(known_since) as known_since
from {{ ref('fct_balancing_minute') }}
group by quarter_hour
