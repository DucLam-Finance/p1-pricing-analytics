select * from (
    values
        ('credit_card',  'Credit Card',  'Card'),
        ('debit_card',   'Debit Card',   'Card'),
        ('boleto',       'Boleto',        'Bank Transfer'),
        ('voucher',      'Voucher',       'Voucher'),
        ('not_defined',  'Not Defined',   'Other')
) as t(payment_type, payment_name, payment_group)
