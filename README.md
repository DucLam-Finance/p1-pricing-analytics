# 🛒 Commercial Finance Dashboard — Olist E-Commerce
### Price-Volume-Mix (PVM) Analysis · dbt + DuckDB + Power BI · IBCS Framework

---

## 🔍 The Problem FP&A Faces Every Day

> FP&A teams spend ~70% of their time collecting, cleaning, and validating data —
> leaving only 30% for the work that actually matters: **performance management and decision support**.

Two pain points kill FP&A productivity:

|
 Pain Point 
|
 Impact 
|
|
---
|
---
|
|
 📥 
**
Data collection & cleaning
**
|
 Manual, error-prone, no single source of truth 
|
|
 📊 
**
Revenue performance analysis
**
|
 No structured framework to explain 
*
why
*
 revenue changed 
|

This project solves both — end-to-end.

---

## 💡 The Solution

A **Commercial Finance Dashboard** built on a modern data stack:
Olist Raw CSVs → DuckDB → dbt (Staging → Refined → Mart) → Power BI (PVM · IBCS)


**Two pillars:**
1. **Automated data pipeline** via dbt — eliminates manual collection, enforces data quality & audit trail
2. **PVM (Price-Volume-Mix) model** — decomposes revenue variance into root causes, enabling targeted commercial action

---

## 🏗️ 3-Layer Architecture
STAGING (stg_) REFINED (dim_ / fct_) MARTS (mart_)
━━━━━━━━━━━━━━━━ ━━━━━━━━━━━━━━━━━━━━━━ ━━━━━━━━━━━━━━━
Raw CSV → clean types Business logic + joins Native Power BI · IBCS

stg_orders ──┐
stg_order_items ──┤ dim_date mart_dim_date
stg_order_payments ──┤ dim_customer ──────────► mart_dim_customer
stg_order_reviews ──┼──► dim_product ──────────► mart_dim_product
stg_customers ──┤ dim_seller ──────────► mart_dim_seller
stg_products ──┤ dim_business_unit ──────────► mart_dim_business_unit
stg_sellers ──┤ dim_payment_method ─────────► mart_dim_payment_method
stg_product_cats ──┘ fct_order_items ──────────► mart_sales (AC · PL · PY · FC)
mart_sales_detail (AC, 110K rows)
seeds: mart_comments (annotations)
seed_budget_targets.csv → Plan (PL)
seed_cost_margins.csv → COGS %
seed_comments.csv → Contextual annotations


### Why 3 layers?
| Layer | Role | Benefit |
|---|---|---|
| **Staging** | Cleans & standardizes raw data | Single source of truth, audit-ready |
| **Refined** | Applies business logic & joins | Reusable, governed business concepts |
| **Mart** | Optimized for BI consumption | Fast queries, IBCS-ready for Power BI |

---

## 🏢 Business Unit Hierarchy
Group: Olist Marketplace
│
Division: ┌──────────────┬──────────────┬──────────────┬──────────────┐
Digital & Personal & Home & B2B &
Media Lifestyle Garden Services
│ │ │ │
BU: Technology Health & Beauty Home & Living Auto, Food & Ind.
Entertainment Fashion & Sports Gifts & Tools Office & Services


---

## 📊 PVM Model — Revenue Decomposition

The **Price-Volume-Mix (PVM)** framework breaks down the revenue gap (AC vs. PY) into three drivers:

| Driver | Question answered | Commercial Action |
|---|---|---|
| **ΔPrice** | Did we charge more or less per unit? | Pricing strategy, discount control |
| **ΔVolume** | Did we sell more or fewer units? | Sales execution, demand generation |
| **ΔMix** | Did high-value products grow faster? | Portfolio management, product prioritization |

> **Example (Jun 2018 MTD):**
> Total revenue AC = **0.86M** vs. PY = **0.43M** (+100%)
> Driven by: Volume **+0.42M** · Mix **+0.44M** · Price **flat**
> → Growth is volume & mix-led, not price-led → action: protect margin by improving product mix quality

---

## 📋 Table Mapping

| BI Dimension | Mart Table | Key Columns |
|---|---|---|
| Sales | `mart_sales` | date, revenue, cost, gross_profit, scenario, businessunitid |
| Product | `mart_dim_product` | productid, product, productcategoryid, productgroupid |
| Customer | `mart_dim_customer` | customerid, customer, region, regionid, countryid |
| Salesperson | `mart_dim_seller` | salespersonid, salesperson |
| Business Unit | `mart_dim_business_unit` | businessunitid, businessunit, division, group |
| Comments | `mart_comments` | date, kpi_id, comment |

### Supporting Tables
| Table | Purpose |
|---|---|
| `mart_sales_detail` | Order-level drill-down (110K rows, AC only, all attributes) |
| `mart_dim_date` | Date dimension for Power BI time intelligence |
| `mart_dim_payment_method` | Payment grouping: Card / Transfer / Voucher |

---

## 📐 IBCS Dashboard Design Principles

The Power BI dashboard follows the **International Business Communication Standards (IBCS)**:
- ⬛ **Black** = Actual (AC)
- ◻️ **Outlined** = Prior Year (PY) / Plan (PL)
- 🟩 **Green** = Positive variance
- 🟥 **Red** = Negative variance
- Waterfall charts for PVM decomposition at Group & Division level

---

## ⚙️ Setup

```bash
# 1. Install dbt with DuckDB adapter
pip install dbt-duckdb

# 2. Configure profile
cp profiles.yml ~/.dbt/profiles.yml

# 3. Place Olist CSVs in /data folder

# 4. Run full pipeline
dbt deps && dbt seed && dbt run && dbt test
🗂️ Project Structure
├── models/
│   ├── staging/        # Raw → clean types
│   ├── refined/        # Business logic (dims + facts)
│   └── marts/          # Power BI-ready tables
├── seeds/              # Budget, COGS%, comments
├── tests/              # Data quality checks
└── profiles.yml        # DuckDB connection
👤 Author
Nguyen Duc Lam | CMA (US) · FMVA | Commercial Finance
🔗 GitHub · LinkedIn

