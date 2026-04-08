-- Core fact table: joins all staging models
-- Grain: one row per order line item
-- Keeps ALL attributes from every source for maximum analytical flexibility

with items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('dim_product') }}
),

sellers as (
    select * from {{ ref('dim_seller') }}
),

bu as (
    select * from {{ ref('dim_business_unit') }}
),

payments_agg as (
    select
        order_id,
        sum(payment_value)                                  as total_payment_value,
        count(distinct payment_type)                        as payment_method_count,
        max(case when payment_sequential = 1
            then payment_type end)                          as primary_payment_type,
        max(case when payment_type = 'credit_card'
            then payment_installments else 0 end)           as max_installments
    from {{ ref('stg_order_payments') }}
    group by order_id
),

reviews as (
    select
        order_id,
        review_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        case
            when review_score >= 4 then 'positive'
            when review_score = 3 then 'neutral'
            else 'negative'
        end as review_sentiment
    from {{ ref('stg_order_reviews') }}
    qualify row_number() over (partition by order_id order by review_creation_date desc) = 1
)

select
    -- === ORDER KEYS ===
    i.order_id,
    i.order_item_id,

    -- === DIMENSION KEYS (for Power BI relationships) ===
    cast(strftime(cast(o.order_purchase_timestamp as date), '%Y%m%d') as integer) as date_key,
    p.productid,
    c.customer_unique_id,
    s.salespersonid,
    bu.businessunitid,

    -- === ORDER ATTRIBUTES (from stg_orders) ===
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    cast(o.order_purchase_timestamp as date)                as order_date,
    extract(year from o.order_purchase_timestamp)           as order_year,
    extract(month from o.order_purchase_timestamp)          as order_month,
    extract(quarter from o.order_purchase_timestamp)        as order_quarter,

    -- === ITEM ATTRIBUTES (from stg_order_items) ===
    i.product_id,
    i.seller_id,
    i.shipping_limit_date,
    i.price,
    i.freight_value,
    cast(i.price + i.freight_value as decimal(18,2))        as total_item_value,

    -- === PRODUCT ATTRIBUTES (from dim_product) ===
    p.product                                               as product_category_name,
    p.productgroupid                                        as category_group,
    p.product_category_name_pt,
    p.product_name_length,
    p.product_description_length,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    p.product_volume_cm3,
    p.weight_tier,

    -- === CUSTOMER ATTRIBUTES (from stg_customers) ===
    c.customer_id                                           as original_customer_id,
    c.customer_unique_id,
    c.customer_zip_code_prefix,
    c.customer_city,
    c.customer_state,

    -- === SELLER ATTRIBUTES (from dim_seller) ===
    s.seller_city,
    s.seller_state,
    s.seller_zip_code_prefix,

    -- === BUSINESS UNIT ATTRIBUTES (from dim_business_unit) ===
    bu.businessunit,
    bu.division,
    bu."group",

    -- === PAYMENT ATTRIBUTES (aggregated per order) ===
    coalesce(pa.primary_payment_type, 'unknown')            as primary_payment_type,
    coalesce(pa.total_payment_value, 0)                     as order_total_payment,
    coalesce(pa.payment_method_count, 0)                    as payment_method_count,
    coalesce(pa.max_installments, 0)                        as max_installments,

    -- === REVIEW ATTRIBUTES (from stg_order_reviews) ===
    r.review_id,
    r.review_score,
    r.review_comment_title,
    r.review_comment_message,
    r.review_creation_date,
    r.review_answer_timestamp,
    r.review_sentiment,

    -- === DELIVERY PERFORMANCE (derived) ===
    case when o.order_delivered_customer_date is not null and o.order_purchase_timestamp is not null
        then datediff('day', o.order_purchase_timestamp, o.order_delivered_customer_date)
        else null
    end as delivery_total_days,

    case when o.order_delivered_customer_date is not null and o.order_delivered_carrier_date is not null
        then datediff('day', o.order_delivered_carrier_date, o.order_delivered_customer_date)
        else null
    end as shipping_duration_days,

    case when o.order_delivered_customer_date is not null and o.order_estimated_delivery_date is not null
        then datediff('day', o.order_estimated_delivery_date, o.order_delivered_customer_date)
        else null
    end as delivery_delay_days,

    case
        when o.order_delivered_customer_date is null then 'not_delivered'
        when o.order_delivered_customer_date <= o.order_estimated_delivery_date then 'on_time'
        else 'late'
    end as delivery_status,

    case when o.order_approved_at is not null and o.order_purchase_timestamp is not null
        then datediff('hour', o.order_purchase_timestamp, o.order_approved_at)
        else null
    end as approval_hours

from items i
inner join orders o on i.order_id = o.order_id
inner join customers c on o.customer_id = c.customer_id
inner join products p on i.product_id = p.product_id
left join sellers s on i.seller_id = s.seller_id
left join bu on p.productgroupid = bu.businessunit
left join payments_agg pa on o.order_id = pa.order_id
left join reviews r on o.order_id = r.order_id
