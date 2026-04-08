-- mart_sales_detail: Order-level detail (AC only) for drill-down
-- ALL attributes preserved from fct_order_items

with cost_margins as (
    select * from {{ ref('seed_cost_margins') }}
)

select
    f.order_id,
    f.order_item_id,
    f.date_key,
    f.order_date,
    f.order_year,
    f.order_month,
    f.order_quarter,
    f.productid,
    f.customer_unique_id,
    f.salespersonid,
    f.businessunitid,
    f.order_status,
    f.order_purchase_timestamp,
    f.order_approved_at,
    f.order_delivered_carrier_date,
    f.order_delivered_customer_date,
    f.order_estimated_delivery_date,
    f.product_id,
    f.seller_id,
    f.shipping_limit_date,

    -- Revenue / Cost / GP (matches Zebra BI Sales)
    f.price                                                 as revenue,
    cast(f.price * cm.cost_pct as decimal(18,2))            as cost,
    cast(f.price * (1 - cm.cost_pct) as decimal(18,2))      as gross_profit,
    f.freight_value,
    f.total_item_value,

    -- Product attributes
    f.product_category_name,
    f.category_group,
    f.product_category_name_pt,
    f.product_name_length,
    f.product_description_length,
    f.product_photos_qty,
    f.product_weight_g,
    f.product_length_cm,
    f.product_height_cm,
    f.product_width_cm,
    f.product_volume_cm3,
    f.weight_tier,

    -- Customer attributes
    f.original_customer_id,
    f.customer_unique_id,
    f.customer_zip_code_prefix,
    f.customer_city,
    f.customer_state,

    -- Seller attributes
    f.seller_city,
    f.seller_state,
    f.seller_zip_code_prefix,

    -- BU attributes
    f.businessunit,
    f.division,
    f."group",

    -- Payment attributes
    f.primary_payment_type,
    f.order_total_payment,
    f.payment_method_count,
    f.max_installments,

    -- Review attributes
    f.review_id,
    f.review_score,
    f.review_comment_title,
    f.review_comment_message,
    f.review_creation_date,
    f.review_answer_timestamp,
    f.review_sentiment,

    -- Delivery performance
    f.delivery_total_days,
    f.shipping_duration_days,
    f.delivery_delay_days,
    f.delivery_status,
    f.approval_hours,

    'AC'                                                    as scenario

from {{ ref('fct_order_items') }} f
left join cost_margins cm on f.category_group = cm.category_group
where f.order_status not in ('canceled', 'unavailable')
