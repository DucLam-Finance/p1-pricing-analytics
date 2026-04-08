with recursive date_spine as (
    select cast('2016-01-01' as date) as date_day
    union all
    select cast(date_day + 1 as date)
    from date_spine
    where date_day < cast('2018-12-31' as date)
)

select
    cast(year(date_day) * 10000 + month(date_day) * 100 + day(date_day) as integer) as date_key,
    date_day                                                 as full_date,
    year(date_day)                                           as year,
    quarter(date_day)                                        as quarter,
    month(date_day)                                          as month,
    monthname(date_day)                                      as month_name,
    left(monthname(date_day), 3)                             as month_short,
    cast(year(date_day) as varchar) || '-' || lpad(cast(month(date_day) as varchar), 2, '0') as year_month,
    day(date_day)                                            as day_of_month,
    dayofweek(date_day)                                      as day_of_week,
    dayname(date_day)                                        as day_name,
    case when dayofweek(date_day) in (0, 6) then true else false end as is_weekend,
    'Q' || cast(quarter(date_day) as varchar) || ' ' || cast(year(date_day) as varchar) as quarter_label,
    cast(year(date_day) * 100 + month(date_day) as integer)  as year_month_key,
    cast(year(date_day) * 10 + quarter(date_day) as integer) as year_quarter_key

from date_spine