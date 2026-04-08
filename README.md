# P1+P2: Commercial Finance Dashboard — Olist E-Commerce

dbt project: Olist raw data → Compatible star schema → Power BI with PVM.

## 3-Layer Architecture

```
STAGING (stg_)              REFINED (dim_ / fct_)           MARTS (mart_)
━━━━━━━━━━━━━━━             ━━━━━━━━━━━━━━━━━━━━            ━━━━━━━━━━━━━
Raw CSV → clean types       Business logic + joins           Zebra BI output

stg_orders          ──┐
stg_order_items     ──┤     dim_date                        mart_dim_date
stg_order_payments  ──┤     dim_customer     ──────────►    mart_dim_customer
stg_order_reviews   ──┼──►  dim_product      ──────────►    mart_dim_product
stg_customers       ──┤     dim_seller       ──────────►    mart_dim_seller
stg_products        ──┤     dim_business_unit ─────────►    mart_dim_business_unit
stg_sellers         ──┤     dim_payment_method ────────►    mart_dim_payment_method
stg_product_cats    ──┘     fct_order_items  ──────────►    mart_sales (AC+PL+PY+FC)
                                                            mart_sales_detail (AC only)
seeds:                                                      mart_comments
  seed_budget_targets.csv (PL)
  seed_cost_margins.csv (COGS%)
  seed_comments.csv (annotations)
```

## Table Mapping

| BI_Mapping | Mart table | Key columns |
|---|---|---|
| Sales | mart_sales | date, revenue, cost, gross_profit, scenario, businessunitid |
| Product | mart_dim_product | productid, product, productcategoryid, productgroupid |
| Customer | mart_dim_customer | customerid, customer, region, regionid, countryid |
| Salesperson | mart_dim_seller | salespersonid, salesperson |
| BusinessUnit | mart_dim_business_unit | businessunitid, businessunit, division, group |
| Comment | mart_comments | date, kpi_id, comment |

## Additional tables

| Table | Purpose |
|---|---|
| mart_sales_detail | Order-level drill-down (110K rows, AC only, ALL attributes) |
| mart_dim_date | Date dimension for Power BI time intelligence |
| mart_dim_payment_method | Payment type grouping (Card/Transfer/Voucher) |

## Setup

```bash
pip install dbt-duckdb
cp profiles_example.yml ~/.dbt/profiles.yml
# Place Olist CSVs in data/
dbt deps && dbt seed && dbt run && dbt test
```

## Author
Nguyen Duc Lam | CMA, FMVA | Commercial Finance  
GitHub: https://github.com/DucLam-Finance  
LinkedIn: https://linkedin.com/in/lam-duc
