-- Maps category_group → BusinessUnit → Group →  Division
-- Matches Zebra BI: businessunitid, businessunit, group, division

select * from (
    values
        (1, 'Technology',          'Digital & Media',       'Olist Marketplace'),
        (2, 'Health & Beauty',     'Personal & Lifestyle',  'Olist Marketplace'),
        (3, 'Fashion & Sports',    'Personal & Lifestyle',  'Olist Marketplace'),
        (4, 'Home & Living',       'Home & Garden',         'Olist Marketplace'),
        (5, 'Gifts & Tools',       'Home & Garden',         'Olist Marketplace'),
        (6, 'Entertainment & Kids','Digital & Media',       'Olist Marketplace'),
        (7, 'Auto Food & Industry','B2B & Services',        'Olist Marketplace'),
        (8, 'Office & Services',   'B2B & Services',        'Olist Marketplace'),
        (9, 'Other',               'Other',                 'Olist Marketplace')
) as t(businessunitid, businessunit, division,"group"
)
