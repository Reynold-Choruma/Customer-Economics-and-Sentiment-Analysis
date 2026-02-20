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

# Business Insights

### 1. The Satisfaction vs. Habit Paradox
**The Finding:** ~79% of reviews are positive, yet the repeat purchase rate is below 1%.  
**The Insight:** One-time customers actually report higher satisfaction than repeat buyers. This proves the "retention problem" isn't a failure of service quality, but a **category-habit mismatch**. The marketplace is currently optimized for "one-off" needs (furniture, large electronics) rather than habitual daily or weekly shopping.

### 2. The Geographic "Freight Tax"
**The Finding:** Logistics costs in remote regions often exceed 20% of the total order value.  
**The Insight:** High shipping costs act as a natural ceiling for **Customer Lifetime Value (CLV)**. In distance-penalized states, Olist is essentially a "utility of last resort" rather than a competitive shopping destination, making it difficult to scale high-value customer tiers outside the South/Southeast.

### 3. VIP Fragility & Expectations
**The Finding:** VIP customers show a **15.8% negative sentiment rate**—nearly double that of lower tiers.  
**The Insight:** High-ticket shoppers have a much lower tolerance for logistics friction. A two-day delay might be acceptable for a low-cost item, but it triggers significant brand damage for "VIP" purchases. Protecting the top 10% of revenue requires **logistics precision**, not just product quality.
### 📊 Pareto Analysis (80/20 Rule)

| Customer Segment | Customer Count | Segment Revenue (R$) | Avg. Customer Value (R$) | Cumulative Revenue % |
| :--- | :--- | :--- | :--- | :--- |
| **Top 10%** | 9,652 | 5,869,246.53 | 608.09 | 38.06% |
| **Top 20%** | 9,648 | 2,335,069.97 | 242.03 | 53.20% |
| **Top 50%** | 28,943 | 4,214,495.07 | 145.61 | 80.52% |
| **Top 80%** | 28,943 | 2,248,580.86 | 77.69 | 95.10% |
| **Bottom 20%** | 19,291 | 755,069.34 | 39.14 | 100.00% |


# Strategic Recommendations
Protect the VIP core

Pilot “priority logistics” or SLA guarantees for VIP/high‑ticket orders to reduce delay‑driven dissatisfaction.

Shift the product mix

Increase exposure to high‑frequency categories (consumables, beauty, health) to create reasons to return.

Regional logistics strategy

Explore additional fulfillment centers beyond São Paulo to reduce freight share in high‑potential but remote states.


# Repository Structure
.
├── sql/
│   └── customer_economics.sql
├── python/
│   ├── 01_customer_master_exploration.ipynb
│   ├── 02_sentiment_analysis.ipynb
│   └── data/
│       ├── customer_master.csv
│       ├── order_customer_mapping.csv
│       └── olist_order_reviews_dataset.csv
├── tableau/
│   └── Olist_Customer_Economics.twb(x)
├── README.md

