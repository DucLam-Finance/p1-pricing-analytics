with source as (
    select * from {{ source('raw', 'sellers') }}
)

select
    cast(seller_id as varchar)                      as seller_id,
    cast(seller_zip_code_prefix as integer)         as seller_zip_code_prefix,
    cast(trim(seller_city) as varchar)     as seller_city,
    cast(upper(trim(seller_state)) as varchar)      as seller_state

from source
where seller_id is not null
