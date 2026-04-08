-- Matches Zebra BI: Comment (date, kpi_id, comment)

select
    cast("date" as date)    as "date",
    kpi_id,
    kpi_name,
    comment

from {{ ref('seed_comments') }}
