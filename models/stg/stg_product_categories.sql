with source as (
    select * from {{ source('raw', 'category_translation') }}
)

select
    cast(lower(trim(product_category_name)) as varchar)         as product_category_name,
    cast(lower(trim(product_category_name_english)) as varchar) as product_category_name_english

from source
where product_category_name is not null
