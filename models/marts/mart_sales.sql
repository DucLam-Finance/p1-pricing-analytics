-- mart_sales: Matches Zebra BI Sales table structure
-- All scenarios (AC, PL, PY, FC) as rows with scenario column
-- PVM decomposition done in Power BI DAX, NOT here

with cost_margins as (
    select * from {{ ref('seed_cost_margins') }}
),

-- ========== AC (ACTUAL) from fct_order_items ==========
ac as (
    select
        cast(date_trunc('month', order_date) as timestamp)  as "date",
        businessunitid,
        category_group,
        product_category_name,
        customer_state,
        seller_state,
        primary_payment_type,
        sum(price)                                          as revenue,
        sum(item_count)                                     as units,
        count(distinct order_id)                            as order_count,
        avg(COALESCE(cast(review_score as INT),0))          as avg_review_score,
        avg(shipping_duration_days)                         as avg_shipping_days,
        sum(case when delivery_status = 'late' then 1 else 0 end) as late_deliveries,
        sum(case when delivery_status = 'on_time' then 1 else 0 end) as ontime_deliveries,
        'AC'                                                as scenario
    from (
        select *, 1 as item_count from {{ ref('fct_order_items') }}
        where order_status not in ('canceled', 'unavailable')
    )
    group by 1, 2, 3, 4, 5, 6, 7
),

ac_with_cost as (
    select
        a."date",
        a.businessunitid,
        a.category_group,
        a.product_category_name,
        a.customer_state,
        a.seller_state,
        a.primary_payment_type,
        cast(a.revenue as decimal(18,2))                    as revenue,
        cast(a.revenue * cm.cost_pct as decimal(18,2))      as cost,
        cast(a.revenue * (1 - cm.cost_pct) as decimal(18,2)) as gross_profit,
        a.units,
        a.order_count,
        a.avg_review_score,
        a.avg_shipping_days,
        a.late_deliveries,
        a.ontime_deliveries,
        a.scenario
    from ac a
    left join cost_margins cm on a.category_group = cm.category_group
),

-- ========== PL (PLAN / BUDGET) from seed ==========
pl as (
    select
        cast(make_date(b.year, b.month, 1) as timestamp)    as "date",
        bu.businessunitid,
        b.category_group,
        cast(null as varchar)                               as product_category_name,
        cast(null as varchar)                               as customer_state,
        cast(null as varchar)                               as seller_state,
        cast(null as varchar)                               as primary_payment_type,
        cast(b.budget_revenue as decimal(18,2))             as revenue,
        cast(b.budget_revenue * cm.cost_pct as decimal(18,2)) as cost,
        cast(b.budget_revenue * (1 - cm.cost_pct) as decimal(18,2)) as gross_profit,
        b.budget_units                                      as units,
        cast(null as bigint)                                as order_count,
        cast(null as double)                                as avg_review_score,
        cast(null as double)                                as avg_shipping_days,
        cast(null as bigint)                                as late_deliveries,
        cast(null as bigint)                                as ontime_deliveries,
        'PL'                                                as scenario
    from {{ ref('seed_budget_targets') }} b
    left join cost_margins cm on b.category_group = cm.category_group
    left join {{ ref('dim_business_unit') }} bu on b.category_group = bu.businessunit
),

-- ========== PY (PRIOR YEAR) = AC shifted +1 year ==========
py as (
    select
        cast("date" + interval '1 year' as timestamp)       as "date",
        businessunitid,
        category_group,
        product_category_name,
        customer_state,
        seller_state,
        primary_payment_type,
        revenue,
        cost,
        gross_profit,
        units,
        order_count,
        avg_review_score,
        avg_shipping_days,
        late_deliveries,
        ontime_deliveries,
        'PY'                                                as scenario
    from ac_with_cost
),

-- ========== FC (FORECAST) = PL × growth factor ==========
fc as (
    select
        "date",
        businessunitid,
        category_group,
        product_category_name,
        customer_state,
        seller_state,
        primary_payment_type,
        cast(revenue * 1.03 as decimal(18,2))               as revenue,
        cast(cost * 1.02 as decimal(18,2))                  as cost,
        cast(revenue * 1.03 - cost * 1.02 as decimal(18,2)) as gross_profit,
        cast(units * 1.02 as integer)                       as units,
        order_count,
        avg_review_score,
        avg_shipping_days,
        late_deliveries,
        ontime_deliveries,
        'FC'                                                as scenario
    from pl
),

-- ========== UNION ==========
unioned as (
    select * from ac_with_cost
    union all
    select * from pl
    union all
    select * from py
    union all
    select * from fc
)

select
    "date",
    cast(strftime(cast("date" as date), '%Y%m%d') as integer) as date_key,
    businessunitid,
    category_group,
    product_category_name,
    customer_state,
    seller_state,
    primary_payment_type,
    revenue,
    cost,
    gross_profit,
    units,
    order_count,
    avg_review_score,
    avg_shipping_days,
    late_deliveries,
    ontime_deliveries,
    scenario

from unioned
where "date" is not null
