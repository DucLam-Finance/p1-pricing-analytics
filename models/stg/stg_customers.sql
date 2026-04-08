with source as (
    select * from {{ source('raw', 'customers') }}
)

select
    cast(customer_id as varchar)                    as customer_id,
    cast(customer_unique_id as varchar)             as customer_unique_id,
    cast(customer_zip_code_prefix as integer)       as customer_zip_code_prefix,
    cast(trim(customer_city) as varchar)   as customer_city,
    cast(upper(trim(customer_state)) as varchar)    as customer_state

from source
where customer_id is not null
