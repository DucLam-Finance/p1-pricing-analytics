with products as (
    select * from {{ ref('stg_products') }}
),

translation as (
    select * from {{ ref('stg_product_categories') }}
),

enriched as (
    select
        p.product_id,
        p.product_category_name                             as product_category_name_pt,
        coalesce(t.product_category_name_english,
            coalesce(p.product_category_name, 'uncategorized')) as product_category_name,
        case
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('computers_accessories','computers','electronics','tablets_printing_image',
                 'telephony','fixed_telephony','signaling_and_security','consoles_games')
                then 'Technology'
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('health_beauty','perfumery','diapers_and_hygiene')
                then 'Health & Beauty'
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('sports_leisure','fashion_bags_accessories','fashion_shoes',
                 'fashion_underwear_beach','fashion_male_clothing','fashion_female_clothing',
                 'fashion_childrens_clothes','fashio_female_clothing','fashion_sport',
                 'luggage_accessories')
                then 'Fashion & Sports'
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('bed_bath_table','furniture_decor','housewares','home_comfort_2','home_confort',
                 'home_construction','garden_tools','flowers',
                 'kitchen_dining_laundry_garden_furniture','furniture_living_room',
                 'furniture_bedroom','furniture_mattress_and_upholstery','office_furniture',
                 'la_cuisine','small_appliances','air_conditioning',
                 'small_appliances_home_oven_and_coffee','portable_kitchen_food_processors')
                then 'Home & Living'
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('watches_gifts','christmas_supplies','arts_and_craftmanship','party_supplies',
                 'costruction_tools_garden','costruction_tools_tools',
                 'construction_tools_construction','construction_tools_lights',
                 'construction_tools_safety','market_place')
                then 'Gifts & Tools'
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('toys','baby','cool_stuff','cds_dvds_musicals','dvds_blu_ray','music',
                 'musical_instruments','books_general_interest','books_technical',
                 'books_imported','audio','cine_photo','art')
                then 'Entertainment & Kids'
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('auto','industry_commerce_and_business','agro_industry_and_commerce',
                 'food','food_drink','drinks')
                then 'Auto Food & Industry'
            when coalesce(t.product_category_name_english, p.product_category_name) in
                ('stationery','pet_shop','security_and_services')
                then 'Office & Services'
            else 'Other'
        end as category_group,

        -- All physical attributes preserved
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        coalesce(p.product_length_cm,0) * coalesce(p.product_height_cm,0)
            * coalesce(p.product_width_cm,0)                as product_volume_cm3,

        case
            when p.product_weight_g <= 500 then 'Light (<500g)'
            when p.product_weight_g <= 2000 then 'Medium (500g-2kg)'
            when p.product_weight_g <= 10000 then 'Heavy (2-10kg)'
            when p.product_weight_g > 10000 then 'Very Heavy (>10kg)'
            else 'Unknown'
        end as weight_tier

    from products p
    left join translation t on p.product_category_name = t.product_category_name
)

select
    row_number() over (order by product_id)                 as productid,
    product_id,
    product_category_name                                   as product,
    product_category_name                                   as productcategoryid,
    category_group                                          as productgroupid,
    product_category_name_pt,
    category_group,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_volume_cm3,
    weight_tier

from enriched
