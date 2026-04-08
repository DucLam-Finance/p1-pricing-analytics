with sellers as (
    select * from {{ ref('stg_sellers') }}
)

select
    row_number() over (order by seller_id)  as salespersonid,
    seller_id,
    seller_id                               as salesperson,
    seller_zip_code_prefix,
    seller_city,
    seller_state

from sellers
