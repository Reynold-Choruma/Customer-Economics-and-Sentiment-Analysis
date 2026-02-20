# Olist Customer Economics & Sentiment Analysis
End‑to‑end analysis of the Olist Brazilian e‑commerce marketplace, moving from raw relational data to customer economics, churn risk, and sentiment‑driven strategy. The project combines SQL, Python, and Tableau to answer a key question:

Why is the repeat purchase rate below 1% despite high customer satisfaction?

# Project Objectives
Build a customer economics framework: CLV, value tiers, cohorts, and churn risk from the Olist public dataset.
​

Quantify the impact of logistics and freight cost on profitability and customer value across Brazilian states.
​

Understand customer sentiment using review data and its relationship to repeat behavior and value tier.

Translate analytical findings into actionable business recommendations for growth and retention.

# Data Sources
Olist public dataset (Kaggle) split into multiple tables, stored in MySQL:

olist_customers_dataset – customer IDs and geography.
​

olist_orders_dataset – order lifecycle and timestamps.
​

olist_order_items_dataset – order line items, price, freight.
​

olist_order_payments_dataset – payment values and methods.
​

olist_products_dataset – product metadata and categories.
​

olist_sellers_dataset – seller information.
​

product_category_name_translation – PT→EN category mapping (separate DB).
​

olist_order_reviews_dataset – review score, title, and comment (loaded in Python, not in MySQL).

# Tech Stack
Database & SQL: MySQL for data modeling, aggregation, and exports.
​

Analytics & NLP: Python (Pandas, Matplotlib, Seaborn) for customer‑level panels and sentiment analysis.

Visualization: Tableau for interactive dashboards (customer economics, geography, value tiers).
​

# SQL Layer – Customer Economics
All main SQL is in customer_economics.sql (staged script). Key components:
​

# Data verification & executive summary

Row counts by table, translation‑table checks.

Portfolio KPIs: total customers, orders, revenue, average CLV, average order value, repeat rate.
​

# Customer value and tiers

CLV per customer using sum of payment_value on delivered orders.
​

Value tiers based on CLV: Low, Medium, High, VIP.
​

 Pareto analysis (80/20) showing revenue concentration among top customers.
​

# Geography & freight (“Freight Tax”)

 CLV and average order value by state and city.
​

Freight cost per order and gross‑margin proxy (revenue – freight) by state.
​

Insight: remote states pay a high freight share (>20% of product value), constraining high‑value tiers.

# Cohorts, churn, and CAC proxy

Acquisition cohorts by first purchase month and their revenue trajectories.
​

Recency‑based churn segments: Active, At Risk, Churning, Churned.
​

High‑value customers at risk and churn risk by state.
​

Simple CAC proxy and LTV:CAC ratio by cohort.
​

# Product/category economics

Top categories by revenue and margin proxy.
​

Category preferences by value tier and categories with broad appeal across all tiers.
​

# Export tables

Customer Master (Tableau & Python): customer‑level CLV, value tier, churn segment, freight cost, cohorts.
​

Product Category Performance: revenue, freight, margin proxy per category.
​

Monthly Cohort Export: cohort‑level revenue and average value for Python/BI.
​

# Python Layer – Review‑Based Sentiment Analysis
Python is used purely for sentiment analysis on reviews and visualization; customer economics (CLV, tiers, cohorts) is computed in SQL/Tableau.
​

# Main steps:

Data loading & join

Load olist_order_reviews_dataset.csv in Python (Pandas).

Load an order_customer_mapping.csv exported from MySQL to map order_id → customer_id and value tier info (e.g. customer_tier, total_orders) if needed.

Join reviews with the mapping to attach customer_tier and a is_one_time flag (based on total_orders).

# Sentiment construction

Use the numeric review_score (1–5 stars) as sentiment:

5 → Very Positive, 4 → Positive, 3 → Neutral, 2 → Negative, 1 → Very Negative.

Create frequency tables:

Sentiment by customer_tier (Low, Medium, High, VIP).

Sentiment for one‑time vs repeat customers.

# Visualizations

Matplotlib/Seaborn stacked bar chart: Customer Sentiment by Value Tier.
​

Additional bar chart/summary for one‑time vs repeat average review scores.

# Key findings from Python

Around 77–79% of reviews are positive (4–5 stars).

One‑time customers have a higher average review score than repeat customers, so low repeat rate is not explained by bad experience.

VIP customers show slightly more negative sentiment than Low‑Value customers, mainly around delivery expectations for high‑ticket orders.
.
├── sql/
│   └── customer_economics.sql
├── python/
│   └── sentiment_analysis.ipynb
│       # uses:
│       # - olist_order_reviews_dataset.csv
│       # - order_customer_mapping.csv (order → customer_id + tier info)
├── tableau/
│   └── Olist_Customer_Economics.twb(x)
├── data/
│   ├── olist_order_reviews_dataset.csv
│   └── order_customer_mapping.csv
└── README.md

