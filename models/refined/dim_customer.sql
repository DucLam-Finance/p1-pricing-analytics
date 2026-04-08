with customers as (
    select * from {{ ref('stg_customers') }}
),

deduplicated as (
    select
        customer_unique_id,
        first(customer_id order by customer_id)             as first_customer_id,
        first(customer_zip_code_prefix order by customer_id) as customer_zip_code_prefix,
        first(customer_city order by customer_id)           as customer_city,
        first(customer_state order by customer_id)          as customer_state,
        count(distinct customer_id)                         as identity_count
    from customers
    group by customer_unique_id
)

select
    row_number() over (order by customer_unique_id)         as customerid,
    customer_unique_id,
    first_customer_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state                                          as customer,
    case
        when customer_state in ('SP','RJ','MG','ES')                                        then 'Southeast'
        when customer_state in ('PR','SC','RS')                                              then 'South'
        when customer_state in ('BA','PE','CE','MA','PB','RN','AL','SE','PI')                then 'Northeast'
        when customer_state in ('DF','GO','MT','MS')                                         then 'Central-West'
        when customer_state in ('AM','PA','AC','RO','RR','AP','TO')                          then 'North'
        else 'Other'
    end                                                     as region,
    case
        when customer_state in ('SP','RJ','MG','ES')                                        then 1
        when customer_state in ('PR','SC','RS')                                              then 2
        when customer_state in ('BA','PE','CE','MA','PB','RN','AL','SE','PI')                then 3
        when customer_state in ('DF','GO','MT','MS')                                         then 4
        when customer_state in ('AM','PA','AC','RO','RR','AP','TO')                          then 5
        else 6
    end                                                     as regionid,
    'BR'                                                    as countryid,
    identity_count

from deduplicated
