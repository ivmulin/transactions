{% test freshness(model, column_name, group_by_column='symbol', warn_after_seconds=120, error_after_seconds=180) %}

with freshness_status_cte as (
    select
        {{ group_by_column }},
        max({{ column_name }}) as max_{{ column_name }},
        now() as current_dwh_time,
        round(extract(epoch from (now() - max({{ column_name }})))) as lag_seconds,
        round(extract(epoch from (now() - max({{ column_name }}))) / 60, 2) as lag_minutes,
        case
            when extract(epoch from (now() - max({{ column_name }}))) > {{ error_after_seconds }}
                then 'CRITICAL: Lag > ' || ({{ error_after_seconds }} / 60) || 'm'
            when extract(epoch from (now() - max({{ column_name }}))) > {{ warn_after_seconds }}
                then 'WARNING: Lag > ' || ({{ warn_after_seconds }} / 60) || 'm'
            else 'OK'
        end as status
    from {{ model }}
    group by {{ group_by_column }}
)

select * from freshness_status_cte
where status != 'OK'

{% endtest %}
