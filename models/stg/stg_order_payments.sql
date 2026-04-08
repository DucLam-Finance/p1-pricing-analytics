with source as (
    select * from {{ source('raw', 'order_payments') }}
)

select
    cast(order_id as varchar)                       as order_id,
    cast(payment_sequential as integer)             as payment_sequential,
    cast(lower(trim(payment_type)) as varchar)      as payment_type,
    cast(payment_installments as integer)            as payment_installments,
    cast(payment_value as decimal(18,2))             as payment_value

from source
where order_id is not null
