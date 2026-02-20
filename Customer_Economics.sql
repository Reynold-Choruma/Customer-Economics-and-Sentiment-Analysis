-- ============================================================================
-- Olist Customer Economics – SQL Layer
-- Author: Reynold Choruma
-- Description: Customer economics, freight, cohorts, churn, and export tables
--              for the Olist marketplace (delivered orders only).
-- ============================================================================


-- ============================================================================
-- STAGE 1 - PART 1: DATA VERIFICATION
-- ============================================================================

USE `Customer Economics`;

-- Check your main tables
SELECT '=== DATA VERIFICATION ===' AS section_title;

SELECT 'olist_customers_dataset' AS table_name, COUNT(*) AS record_count 
FROM olist_customers_dataset

UNION ALL 

SELECT 'olist_orders_dataset', COUNT(*) 
FROM olist_orders_dataset

UNION ALL

SELECT 'olist_order_items_dataset', COUNT(*) 
FROM olist_order_items_dataset

UNION ALL

SELECT 'olist_order_payments_dataset', COUNT(*) 
FROM olist_order_payments_dataset

UNION ALL

SELECT 'olist_products_dataset', COUNT(*) 
FROM olist_products_dataset

UNION ALL

SELECT 'olist_sellers_dataset', COUNT(*) 
FROM olist_sellers_dataset;

-- ============================================================================
-- STAGE 1 - PART 2: TRANSLATION TABLE CHECK
-- ============================================================================

-- Check translation table in olist_db
SELECT '=== TRANSLATION TABLE CHECK ===' AS section_title;

SELECT 'product_category_name_translation' AS table_name, 
       COUNT(*) AS record_count 
FROM olist_db.product_category_name_translation;

-- See sample translations
SELECT 'Sample Translations:' AS info;

SELECT 
    product_category_name AS portuguese,
    product_category_name_english AS english
FROM olist_db.product_category_name_translation
LIMIT 10;

-- ============================================================================
-- STAGE 1 - PART 3: EXECUTIVE SUMMARY
-- ============================================================================

SELECT '=== EXECUTIVE SUMMARY ===' AS section_title;

SELECT 
    'Total Customers' AS metric, 
    COUNT(DISTINCT c.customer_id) AS value
FROM olist_customers_dataset c
JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
WHERE o.order_status = 'delivered'

UNION ALL

SELECT 
    'Total Orders', 
    COUNT(o.order_id)
FROM olist_orders_dataset o
WHERE o.order_status = 'delivered'

UNION ALL

SELECT 
    'Total Revenue (R$)', 
    ROUND(SUM(py.payment_value), 2)
FROM olist_order_payments_dataset py
JOIN olist_orders_dataset o ON py.order_id = o.order_id
WHERE o.order_status = 'delivered'

UNION ALL

SELECT 
    'Average Customer LTV (R$)', 
    ROUND(SUM(py.payment_value) / COUNT(DISTINCT o.customer_id), 2)
FROM olist_orders_dataset o
JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
WHERE o.order_status = 'delivered'

UNION ALL

SELECT 
    'Average Order Value (R$)', 
    ROUND(AVG(py.payment_value), 2)
FROM olist_order_payments_dataset py
JOIN olist_orders_dataset o ON py.order_id = o.order_id
WHERE o.order_status = 'delivered';

-- ============================================================================
-- STAGE 1 - PART 4: REPEAT CUSTOMER RATE
-- ============================================================================

SELECT '=== REPEAT CUSTOMER RATE ===' AS section_title;

SELECT 
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(CASE WHEN order_count = 1 THEN 1 ELSE 0 END) AS one_time_customers,
    SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) AS repeat_customers,
    ROUND(100.0 * SUM(CASE WHEN order_count >= 2 THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_rate_pct
FROM (
    SELECT 
        customer_id, 
        COUNT(order_id) AS order_count
    FROM olist_orders_dataset
    WHERE order_status = 'delivered'
    GROUP BY customer_id
) AS customer_orders;

-- ============================================================================
-- STAGE 1 - PART 5: ORDER DISTRIBUTION CHECK
-- ============================================================================

SELECT '=== ORDER COUNT DISTRIBUTION ===' AS section_title;

-- Check how many orders each customer has
SELECT 
    order_count,
    COUNT(*) AS customer_count
FROM (
    SELECT 
        customer_id, 
        COUNT(order_id) AS order_count
    FROM olist_orders_dataset
    WHERE order_status = 'delivered'
    GROUP BY customer_id
) AS customer_orders
GROUP BY order_count
ORDER BY order_count;

-- ============================================================================
-- STAGE 1 - PART 6: MONTHLY REVENUE TRENDS
-- ============================================================================

SELECT '=== MONTHLY REVENUE TRENDS ===' AS section_title;

SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS month,
    COUNT(DISTINCT o.customer_id) AS new_customers,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(p.payment_value), 2) AS monthly_revenue_R$,
    ROUND(AVG(p.payment_value), 2) AS avg_order_value_R$
FROM olist_orders_dataset o
JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY month
ORDER BY month ASC;

-- ============================================================================
-- STAGE 1 - PART 7: TOP 20 CUSTOMERS
-- ============================================================================

SELECT '=== TOP 20 CUSTOMERS BY SPEND ===' AS section_title;

SELECT 
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    ROUND(SUM(p.payment_value), 2) AS total_spent_R$,
    ROUND(AVG(p.payment_value), 2) AS avg_order_value_R$,
    COUNT(o.order_id) AS total_orders
FROM olist_customers_dataset c
JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
JOIN olist_order_payments_dataset p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id, c.customer_state, c.customer_city
ORDER BY total_spent_R$ DESC
LIMIT 20;

-- ============================================================================
-- STAGE 1 - PART 8: PAYMENT METHOD ANALYSIS (FINAL)
-- ============================================================================

SELECT '=== PAYMENT METHOD DISTRIBUTION ===' AS section_title;

SELECT 
    payment_type,
    COUNT(*) AS transaction_count,
    ROUND(SUM(payment_value), 2) AS total_revenue_R$,
    ROUND(AVG(payment_value), 2) AS avg_transaction_R$,
    ROUND(AVG(payment_installments), 1) AS avg_installments,
    ROUND(100.0 * SUM(payment_value) / 
        (SELECT SUM(payment_value) FROM olist_order_payments_dataset), 2) AS revenue_share_pct
FROM olist_order_payments_dataset
GROUP BY payment_type
ORDER BY total_revenue_R$ DESC;

-- ============================================================================
-- STAGE 2 - PART 1: CLV BY STATE
-- ============================================================================

SELECT '=== CUSTOMER LIFETIME VALUE BY STATE (TOP 15) ===' AS section_title;

SELECT 
    c.customer_state,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    ROUND(SUM(py.payment_value), 2) AS total_revenue_R$,
    ROUND(SUM(py.payment_value) / COUNT(DISTINCT c.customer_id), 2) AS avg_customer_value_R$,
    ROUND(AVG(py.payment_value), 2) AS avg_order_value_R$
FROM olist_customers_dataset c
JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_revenue_R$ DESC
LIMIT 15;

-- ============================================================================
-- STAGE 2 - PART 2: TOP 20 CITIES BY REVENUE
-- ============================================================================

SELECT '=== TOP 20 CITIES BY REVENUE ===' AS section_title;

SELECT 
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    ROUND(SUM(py.payment_value), 2) AS total_revenue_R$,
    ROUND(SUM(py.payment_value) / COUNT(DISTINCT c.customer_id), 2) AS avg_customer_value_R$,
    ROUND(AVG(py.payment_value), 2) AS avg_order_value_R$
FROM olist_customers_dataset c
JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_city, c.customer_state
HAVING customer_count >= 10
ORDER BY total_revenue_R$ DESC
LIMIT 20;

-- ============================================================================
-- STAGE 2 - PART 3: DETAILED STATE COMPARISON (FINAL)
-- ============================================================================

SELECT '=== DETAILED STATE COMPARISON WITH FREIGHT ===' AS section_title;

SELECT 
    c.customer_state,
    COUNT(DISTINCT c.customer_id) AS customers,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(py.payment_value), 2) AS revenue_R$,
    ROUND(AVG(py.payment_value), 2) AS avg_order_value_R$,
    ROUND(SUM(oi.freight_value), 2) AS total_freight_R$,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight_per_order_R$
FROM olist_customers_dataset c
JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY revenue_R$ DESC;

-- Check if some customers have multiple orders
SELECT 
    order_count,
    COUNT(*) AS customer_count
FROM (
    SELECT 
        customer_id, 
        COUNT(order_id) AS order_count
    FROM olist_orders_dataset
    WHERE order_status = 'delivered'
    GROUP BY customer_id
) AS customer_orders
GROUP BY order_count
ORDER BY order_count DESC
LIMIT 10;

-- ============================================================================
-- STAGE 3 - STEP 1B: CUSTOMER VALUE SUMMARY STATISTICS
-- ============================================================================

SELECT '=== CUSTOMER VALUE DISTRIBUTION SUMMARY ===' AS section_title;

WITH CustomerValue AS (
    SELECT 
        c.customer_id,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$,
        COUNT(o.order_id) AS total_orders,
        MIN(o.order_purchase_timestamp) AS first_purchase_date,
        MAX(o.order_purchase_timestamp) AS last_purchase_date
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
)
SELECT 
    COUNT(*) AS total_customers,
    ROUND(MIN(total_spent_R$), 2) AS min_spend_R$,
    ROUND(MAX(total_spent_R$), 2) AS max_spend_R$,
    ROUND(AVG(total_spent_R$), 2) AS avg_spend_R$,
    ROUND(STDDEV(total_spent_R$), 2) AS stddev_spend_R$,
    ROUND(SUM(total_spent_R$), 2) AS total_revenue_R$
FROM CustomerValue;

-- ============================================================================
-- STAGE 3 - STEP 2: VALUE TIER SEGMENTATION
-- ============================================================================

SELECT '=== CUSTOMER VALUE TIERS ===' AS section_title;

WITH CustomerValue AS (
    SELECT 
        c.customer_id,
        c.customer_state,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_state
)
SELECT 
    CASE 
        WHEN total_spent_R$ >= 1000 THEN 'VIP'
        WHEN total_spent_R$ >= 500 THEN 'High Value'
        WHEN total_spent_R$ >= 200 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_tier,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_spent_R$), 2) AS tier_revenue_R$,
    ROUND(AVG(total_spent_R$), 2) AS avg_customer_value_R$,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CustomerValue), 2) AS pct_of_customers,
    ROUND(100.0 * SUM(total_spent_R$) / (SELECT SUM(total_spent_R$) FROM CustomerValue), 2) AS pct_of_revenue
FROM CustomerValue
GROUP BY value_tier
ORDER BY tier_revenue_R$ DESC;

-- ============================================================================
-- STAGE 3 - STEP 3: PARETO ANALYSIS (80/20 RULE)
-- ============================================================================

SELECT '=== PARETO ANALYSIS - REVENUE CONCENTRATION ===' AS section_title;

WITH CustomerRevenue AS (
    SELECT 
        c.customer_id,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
),
RankedCustomers AS (
    SELECT 
        customer_id,
        total_spent_R$,
        ROW_NUMBER() OVER (ORDER BY total_spent_R$ DESC) AS customer_rank,
        COUNT(*) OVER () AS total_customers,
        SUM(total_spent_R$) OVER () AS total_revenue
    FROM CustomerRevenue
),
CumulativeRevenue AS (
    SELECT 
        customer_id,
        total_spent_R$,
        customer_rank,
        total_customers,
        total_revenue,
        SUM(total_spent_R$) OVER (ORDER BY customer_rank) AS cumulative_revenue_R$,
        ROUND(100.0 * customer_rank / total_customers, 2) AS pct_customers,
        ROUND(100.0 * SUM(total_spent_R$) OVER (ORDER BY customer_rank) / total_revenue, 2) AS cumulative_pct_revenue
    FROM RankedCustomers
)
SELECT 
    CASE 
        WHEN pct_customers <= 10 THEN 'Top 10%'
        WHEN pct_customers <= 20 THEN 'Top 20%'
        WHEN pct_customers <= 50 THEN 'Top 50%'
        WHEN pct_customers <= 80 THEN 'Top 80%'
        ELSE 'Bottom 20%'
    END AS customer_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(total_spent_R$), 2) AS segment_revenue_R$,
    ROUND(AVG(total_spent_R$), 2) AS avg_customer_value_R$,
    MAX(cumulative_pct_revenue) AS cumulative_revenue_pct
FROM CumulativeRevenue
GROUP BY customer_segment
ORDER BY segment_revenue_R$ DESC;

-- ============================================================================
-- STAGE 3 - STEP 4: SPENDING BEHAVIOR ANALYSIS
-- ============================================================================

SELECT '=== SPENDING BEHAVIOR BY VALUE TIER ===' AS section_title;

WITH CustomerValue AS (
    SELECT 
        c.customer_id,
        c.customer_state,
        COUNT(o.order_id) AS total_orders,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$,
        ROUND(AVG(py.payment_value), 2) AS avg_order_value_R$,
        CASE 
            WHEN SUM(py.payment_value) >= 1000 THEN 'VIP'
            WHEN SUM(py.payment_value) >= 500 THEN 'High Value'
            WHEN SUM(py.payment_value) >= 200 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_tier
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_state
)
SELECT 
    value_tier,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent_R$), 2) AS avg_lifetime_value_R$,
    ROUND(AVG(avg_order_value_R$), 2) AS avg_order_size_R$,
    ROUND(AVG(total_orders), 2) AS avg_orders_per_customer,
    ROUND(MIN(total_spent_R$), 2) AS min_spend_R$,
    ROUND(MAX(total_spent_R$), 2) AS max_spend_R$
FROM CustomerValue
GROUP BY value_tier
ORDER BY avg_lifetime_value_R$ DESC;

-- ============================================================================
-- STAGE 4 - STEP 1: MONTHLY COHORT ACQUISITION
-- ============================================================================

SELECT '=== CUSTOMER ACQUISITION BY COHORT (MONTHLY) ===' AS section_title;

WITH FirstPurchase AS (
    SELECT 
        c.customer_id,
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS cohort_month,
        MIN(o.order_purchase_timestamp) AS first_purchase_date
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) AS new_customers,
    MIN(first_purchase_date) AS cohort_start_date
FROM FirstPurchase
GROUP BY cohort_month
ORDER BY cohort_month;

-- ============================================================================
-- STAGE 4 - STEP 2: COHORT REVENUE PERFORMANCE
-- ============================================================================

SELECT '=== REVENUE BY ACQUISITION COHORT ===' AS section_title;

WITH FirstPurchase AS (
    SELECT 
        c.customer_id,
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS cohort_month
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
),
CohortRevenue AS (
    SELECT 
        fp.cohort_month,
        fp.customer_id,
        ROUND(SUM(py.payment_value), 2) AS customer_ltv_R$
    FROM FirstPurchase fp
    JOIN olist_orders_dataset o ON fp.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY fp.cohort_month, fp.customer_id
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size,
    ROUND(SUM(customer_ltv_R$), 2) AS total_cohort_revenue_R$,
    ROUND(AVG(customer_ltv_R$), 2) AS avg_customer_ltv_R$,
    ROUND(MIN(customer_ltv_R$), 2) AS min_ltv_R$,
    ROUND(MAX(customer_ltv_R$), 2) AS max_ltv_R$
FROM CohortRevenue
GROUP BY cohort_month
ORDER BY cohort_month;

-- ============================================================================
-- STAGE 4 - STEP 3: COHORT QUALITY RANKING
-- ============================================================================

SELECT '=== TOP 10 BEST COHORTS BY CUSTOMER VALUE ===' AS section_title;

WITH FirstPurchase AS (
    SELECT 
        c.customer_id,
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS cohort_month
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
),
CohortRevenue AS (
    SELECT 
        fp.cohort_month,
        fp.customer_id,
        ROUND(SUM(py.payment_value), 2) AS customer_ltv_R$
    FROM FirstPurchase fp
    JOIN olist_orders_dataset o ON fp.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY fp.cohort_month, fp.customer_id
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size,
    ROUND(SUM(customer_ltv_R$), 2) AS total_revenue_R$,
    ROUND(AVG(customer_ltv_R$), 2) AS avg_customer_value_R$
FROM CohortRevenue
GROUP BY cohort_month
HAVING cohort_size >= 100
ORDER BY avg_customer_value_R$ DESC
LIMIT 10;

-- ============================================================================
-- STAGE 4 - STEP 4: COHORT GROWTH TREND
-- ============================================================================

SELECT '=== CUSTOMER ACQUISITION GROWTH TREND ===' AS section_title;

WITH FirstPurchase AS (
    SELECT 
        c.customer_id,
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS cohort_month
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) AS new_customers,
    SUM(COUNT(DISTINCT customer_id)) OVER (ORDER BY cohort_month) AS cumulative_customers
FROM FirstPurchase
GROUP BY cohort_month
ORDER BY cohort_month;

-- ============================================================================
-- STAGE 5 - STEP 1: TOP PRODUCT CATEGORIES OVERALL
-- ============================================================================

SELECT '=== TOP 20 PRODUCT CATEGORIES BY REVENUE ===' AS section_title;

SELECT 
    p.product_category_name AS category_portuguese,
    COALESCE(t.product_category_name_english, p.product_category_name) AS category_english,
    COUNT(DISTINCT oi.order_id) AS orders,
    COUNT(oi.order_item_id) AS items_sold,
    ROUND(SUM(oi.price), 2) AS product_revenue_R$,
    ROUND(SUM(oi.freight_value), 2) AS freight_revenue_R$,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue_R$,
    ROUND(AVG(oi.price), 2) AS avg_item_price_R$
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o ON oi.order_id = o.order_id
JOIN olist_products_dataset p ON oi.product_id = p.product_id
LEFT JOIN olist_db.product_category_name_translation t 
    ON p.product_category_name = t.product_category_name
WHERE o.order_status = 'delivered'
  AND p.product_category_name IS NOT NULL
GROUP BY p.product_category_name, t.product_category_name_english
ORDER BY total_revenue_R$ DESC
LIMIT 20;

-- ============================================================================
-- STAGE 5 - STEP 2: CATEGORY PREFERENCES BY CUSTOMER VALUE TIER
-- ============================================================================

SELECT '=== TOP 5 CATEGORIES PER CUSTOMER TIER ===' AS section_title;

WITH CustomerValue AS (
    SELECT 
        c.customer_id,
        CASE 
            WHEN SUM(py.payment_value) >= 1000 THEN 'VIP'
            WHEN SUM(py.payment_value) >= 500 THEN 'High Value'
            WHEN SUM(py.payment_value) >= 200 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_tier
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
),
CategoryByTier AS (
    SELECT 
        cv.value_tier,
        COALESCE(t.product_category_name_english, p.product_category_name) AS category,
        COUNT(oi.order_item_id) AS items_sold,
        ROUND(SUM(oi.price), 2) AS category_revenue_R$,
        ROW_NUMBER() OVER (PARTITION BY cv.value_tier ORDER BY SUM(oi.price) DESC) AS rank_in_tier
    FROM olist_order_items_dataset oi
    JOIN olist_orders_dataset o ON oi.order_id = o.order_id
    JOIN olist_products_dataset p ON oi.product_id = p.product_id
    LEFT JOIN olist_db.product_category_name_translation t 
        ON p.product_category_name = t.product_category_name
    JOIN CustomerValue cv ON o.customer_id = cv.customer_id
    WHERE o.order_status = 'delivered'
      AND p.product_category_name IS NOT NULL
    GROUP BY cv.value_tier, category
)
SELECT 
    value_tier,
    category,
    items_sold,
    category_revenue_R$
FROM CategoryByTier
WHERE rank_in_tier <= 5
ORDER BY value_tier, rank_in_tier;

-- ============================================================================
-- STAGE 5 - STEP 3: CATEGORY PERFORMANCE SUMMARY
-- ============================================================================

SELECT '=== CATEGORY PERFORMANCE OVERVIEW ===' AS section_title;

SELECT 
    COUNT(DISTINCT p.product_category_name) AS total_categories,
    COUNT(DISTINCT oi.product_id) AS total_products,
    COUNT(oi.order_item_id) AS total_items_sold,
    ROUND(SUM(oi.price), 2) AS total_product_revenue_R$,
    ROUND(SUM(oi.freight_value), 2) AS total_freight_revenue_R$,
    ROUND(AVG(oi.price), 2) AS avg_item_price_R$,
    ROUND(AVG(oi.freight_value), 2) AS avg_freight_per_item_R$
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o ON oi.order_id = o.order_id
JOIN olist_products_dataset p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'
  AND p.product_category_name IS NOT NULL;
  
  -- ============================================================================
-- STAGE 5 - STEP 4: CATEGORIES POPULAR ACROSS ALL TIERS
-- ============================================================================

SELECT '=== CATEGORIES WITH BROAD APPEAL (ALL TIERS) ===' AS section_title;

WITH CustomerValue AS (
    SELECT 
        c.customer_id,
        CASE 
            WHEN SUM(py.payment_value) >= 1000 THEN 'VIP'
            WHEN SUM(py.payment_value) >= 500 THEN 'High Value'
            WHEN SUM(py.payment_value) >= 200 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_tier
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
),
CategoryTierCount AS (
    SELECT 
        COALESCE(t.product_category_name_english, p.product_category_name) AS category,
        COUNT(DISTINCT cv.value_tier) AS tiers_buying,
        ROUND(SUM(oi.price), 2) AS total_revenue_R$,
        COUNT(DISTINCT cv.customer_id) AS unique_customers
    FROM olist_order_items_dataset oi
    JOIN olist_orders_dataset o ON oi.order_id = o.order_id
    JOIN olist_products_dataset p ON oi.product_id = p.product_id
    LEFT JOIN olist_db.product_category_name_translation t 
        ON p.product_category_name = t.product_category_name
    JOIN CustomerValue cv ON o.customer_id = cv.customer_id
    WHERE o.order_status = 'delivered'
      AND p.product_category_name IS NOT NULL
    GROUP BY category
)
SELECT 
    category,
    tiers_buying,
    total_revenue_R$,
    unique_customers,
    ROUND(total_revenue_R$ / unique_customers, 2) AS revenue_per_customer_R$
FROM CategoryTierCount
WHERE tiers_buying = 4
ORDER BY total_revenue_R$ DESC
LIMIT 15;

-- ============================================================================
-- STAGE 6 - STEP 1: CUSTOMER RECENCY SEGMENTATION
-- ============================================================================

SELECT '=== CUSTOMER CHURN RISK SEGMENTS ===' AS section_title;

WITH CustomerRecency AS (
    SELECT 
        c.customer_id,
        c.customer_state,
        c.customer_city,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$,
        MAX(o.order_purchase_timestamp) AS last_purchase_date,
        DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                 MAX(o.order_purchase_timestamp)) AS days_since_last_purchase
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_state, c.customer_city
)
SELECT 
    CASE 
        WHEN days_since_last_purchase <= 90 THEN 'Active (0-90 days)'
        WHEN days_since_last_purchase <= 180 THEN 'At Risk (91-180 days)'
        WHEN days_since_last_purchase <= 365 THEN 'Churning (181-365 days)'
        ELSE 'Churned (365+ days)'
    END AS recency_segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(total_spent_R$), 2) AS avg_ltv_R$,
    ROUND(SUM(total_spent_R$), 2) AS segment_revenue_R$,
    ROUND(AVG(days_since_last_purchase), 0) AS avg_days_inactive,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM CustomerRecency), 2) AS pct_of_customers
FROM CustomerRecency
GROUP BY recency_segment
ORDER BY avg_days_inactive;

-- ============================================================================
-- STAGE 6 - STEP 2: HIGH-VALUE CUSTOMERS AT RISK
-- ============================================================================

SELECT '=== HIGH-VALUE CUSTOMERS AT RISK (VIP + HIGH) ===' AS section_title;

WITH CustomerRecency AS (
    SELECT 
        c.customer_id,
        c.customer_state,
        c.customer_city,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$,
        MAX(o.order_purchase_timestamp) AS last_purchase_date,
        DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                 MAX(o.order_purchase_timestamp)) AS days_since_last_purchase
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_state, c.customer_city
)
SELECT 
    customer_id,
    customer_city,
    customer_state,
    total_spent_R$ AS lifetime_value_R$,
    days_since_last_purchase AS days_inactive,
    last_purchase_date,
    CASE 
        WHEN total_spent_R$ >= 1000 THEN 'VIP'
        ELSE 'High Value'
    END AS customer_tier
FROM CustomerRecency
WHERE total_spent_R$ >= 500
  AND days_since_last_purchase BETWEEN 91 AND 365
ORDER BY total_spent_R$ DESC
LIMIT 50;

-- ============================================================================
-- STAGE 6 - STEP 3: CHURN RISK BY STATE (TOP 10 STATES)
-- ============================================================================

SELECT '=== CHURN RISK DISTRIBUTION BY STATE ===' AS section_title;

WITH CustomerRecency AS (
    SELECT 
        c.customer_id,
        c.customer_state,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$,
        DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                 MAX(o.order_purchase_timestamp)) AS days_since_last_purchase
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_state
)
SELECT 
    customer_state,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN days_since_last_purchase <= 90 THEN 1 ELSE 0 END) AS active,
    SUM(CASE WHEN days_since_last_purchase BETWEEN 91 AND 180 THEN 1 ELSE 0 END) AS at_risk,
    SUM(CASE WHEN days_since_last_purchase BETWEEN 181 AND 365 THEN 1 ELSE 0 END) AS churning,
    SUM(CASE WHEN days_since_last_purchase > 365 THEN 1 ELSE 0 END) AS churned,
    ROUND(100.0 * SUM(CASE WHEN days_since_last_purchase BETWEEN 91 AND 365 THEN 1 ELSE 0 END) / COUNT(*), 2) AS at_risk_pct
FROM CustomerRecency
GROUP BY customer_state
HAVING total_customers >= 100
ORDER BY total_customers DESC
LIMIT 10;

-- ============================================================================
-- STAGE 6 - STEP 4: CHURN SUMMARY & PRIORITY ACTIONS
-- ============================================================================

SELECT '=== CHURN RISK SUMMARY - PORTFOLIO READY ===' AS section_title;

WITH CustomerRecency AS (
    SELECT 
        c.customer_id,
        ROUND(SUM(py.payment_value), 2) AS total_spent_R$,
        DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                 MAX(o.order_purchase_timestamp)) AS days_since_last_purchase,
        CASE 
            WHEN SUM(py.payment_value) >= 1000 THEN 'VIP'
            WHEN SUM(py.payment_value) >= 500 THEN 'High Value'
            WHEN SUM(py.payment_value) >= 200 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_tier
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
)
SELECT 
    value_tier,
    COUNT(*) AS total_customers,
    SUM(CASE WHEN days_since_last_purchase <= 90 THEN 1 ELSE 0 END) AS active,
    SUM(CASE WHEN days_since_last_purchase BETWEEN 91 AND 180 THEN 1 ELSE 0 END) AS at_risk,
    SUM(CASE WHEN days_since_last_purchase BETWEEN 181 AND 365 THEN 1 ELSE 0 END) AS churning,
    ROUND(100.0 * SUM(CASE WHEN days_since_last_purchase BETWEEN 91 AND 180 THEN 1 ELSE 0 END) / COUNT(*), 2) AS at_risk_pct,
    ROUND(SUM(CASE WHEN days_since_last_purchase BETWEEN 91 AND 180 THEN total_spent_R$ ELSE 0 END), 2) AS revenue_at_risk_R$
FROM CustomerRecency
GROUP BY value_tier
ORDER BY revenue_at_risk_R$ DESC;

-- ============================================================================
-- FINAL LAYER - PART 1: ENHANCED CLV CALCULATION
-- ============================================================================

SELECT '=== CUSTOMER LIFETIME VALUE - COMPLETE METRICS ===' AS section_title;

WITH CustomerMetrics AS (
    SELECT 
        c.customer_id,
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        c.customer_zip_code_prefix,
        
        -- Revenue Metrics
        COUNT(o.order_id) AS total_orders,
        ROUND(SUM(py.payment_value), 2) AS total_revenue_R$,
        ROUND(AVG(py.payment_value), 2) AS avg_order_value_R$,
        ROUND(MIN(py.payment_value), 2) AS min_order_value_R$,
        ROUND(MAX(py.payment_value), 2) AS max_order_value_R$,
        
        -- Cost Metrics
        ROUND(SUM(oi.freight_value), 2) AS total_freight_cost_R$,
        ROUND(AVG(oi.freight_value), 2) AS avg_freight_per_order_R$,
        
        -- Gross Profit Proxy (Revenue - Freight)
        ROUND(SUM(py.payment_value) - SUM(oi.freight_value), 2) AS gross_profit_proxy_R$,
        ROUND((SUM(py.payment_value) - SUM(oi.freight_value)) / SUM(py.payment_value) * 100, 2) AS gross_margin_pct,
        
        -- Time Metrics
        MIN(o.order_purchase_timestamp) AS first_purchase_date,
        MAX(o.order_purchase_timestamp) AS last_purchase_date,
        DATEDIFF(MAX(o.order_purchase_timestamp), MIN(o.order_purchase_timestamp)) AS customer_lifespan_days,
        DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                 MAX(o.order_purchase_timestamp)) AS days_since_last_purchase
        
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_unique_id, c.customer_state, c.customer_city, c.customer_zip_code_prefix
)
SELECT 
    customer_id,
    customer_state,
    customer_city,
    total_orders,
    total_revenue_R$,
    avg_order_value_R$,
    total_freight_cost_R$,
    gross_profit_proxy_R$,
    gross_margin_pct,
    customer_lifespan_days,
    days_since_last_purchase,
    first_purchase_date,
    last_purchase_date
FROM CustomerMetrics
ORDER BY total_revenue_R$ DESC
LIMIT 100;

-- ============================================================================
-- FINAL LAYER - PART 2: CLV STATISTICAL DISTRIBUTION 
-- ============================================================================

SELECT '=== CLV DISTRIBUTION - PERCENTILES & QUARTILES ===' AS section_title;

WITH CustomerCLV AS (
    SELECT 
        c.customer_id,
        ROUND(SUM(py.payment_value), 2) AS clv_R$
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
),
Percentiles AS (
    SELECT 
        clv_R$,
        PERCENT_RANK() OVER (ORDER BY clv_R$) AS percentile_rank
    FROM CustomerCLV
)
SELECT 
    'Total Customers' AS metric,
    CAST(COUNT(*) AS CHAR) AS value
FROM CustomerCLV

UNION ALL

SELECT 'Mean CLV (R$)', CAST(ROUND(AVG(clv_R$), 2) AS CHAR)
FROM CustomerCLV

UNION ALL

SELECT 'Median CLV (R$)', 
    CAST(ROUND(AVG(clv_R$), 2) AS CHAR)
FROM (
    SELECT clv_R$, 
           ROW_NUMBER() OVER (ORDER BY clv_R$) AS row_num,
           COUNT(*) OVER () AS total_rows
    FROM CustomerCLV
) AS ordered
WHERE row_num IN (FLOOR((total_rows + 1) / 2), CEIL((total_rows + 1) / 2))

UNION ALL

SELECT 'P25 (25th Percentile)', CAST(ROUND(MIN(clv_R$), 2) AS CHAR)
FROM (
    SELECT clv_R$, percentile_rank
    FROM Percentiles
    WHERE percentile_rank >= 0.25
    ORDER BY percentile_rank
    LIMIT 1
) p25

UNION ALL

SELECT 'P75 (75th Percentile)', CAST(ROUND(MIN(clv_R$), 2) AS CHAR)
FROM (
    SELECT clv_R$, percentile_rank
    FROM Percentiles
    WHERE percentile_rank >= 0.75
    ORDER BY percentile_rank
    LIMIT 1
) p75

UNION ALL

SELECT 'P90 (90th Percentile)', CAST(ROUND(MIN(clv_R$), 2) AS CHAR)
FROM (
    SELECT clv_R$, percentile_rank
    FROM Percentiles
    WHERE percentile_rank >= 0.90
    ORDER BY percentile_rank
    LIMIT 1
) p90

UNION ALL

SELECT 'P95 (95th Percentile)', CAST(ROUND(MIN(clv_R$), 2) AS CHAR)
FROM (
    SELECT clv_R$, percentile_rank
    FROM Percentiles
    WHERE percentile_rank >= 0.95
    ORDER BY percentile_rank
    LIMIT 1
) p95

UNION ALL

SELECT 'P99 (99th Percentile)', CAST(ROUND(MIN(clv_R$), 2) AS CHAR)
FROM (
    SELECT clv_R$, percentile_rank
    FROM Percentiles
    WHERE percentile_rank >= 0.99
    ORDER BY percentile_rank
    LIMIT 1
) p99;

-- ============================================================================
-- FINAL LAYER - PART 3: CAC PROXY METRICS
-- ============================================================================

SELECT '=== CAC PROXY - ACQUISITION EFFICIENCY METRICS ===' AS section_title;

WITH CohortMetrics AS (
    SELECT 
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS cohort_month,
        c.customer_id,
        ROUND(SUM(py.payment_value), 2) AS customer_ltv_R$,
        MIN(o.order_purchase_timestamp) AS first_purchase_date
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) AS customers_acquired,
    ROUND(SUM(customer_ltv_R$), 2) AS total_cohort_revenue_R$,
    ROUND(AVG(customer_ltv_R$), 2) AS avg_ltv_R$,
    
    -- CAC Proxy: Assume 20% of first-month revenue goes to acquisition
    ROUND(SUM(customer_ltv_R$) * 0.20, 2) AS estimated_acquisition_cost_R$,
    ROUND((SUM(customer_ltv_R$) * 0.20) / COUNT(DISTINCT customer_id), 2) AS estimated_cac_per_customer_R$,
    
    -- LTV:CAC Ratio (using proxy CAC)
    ROUND(AVG(customer_ltv_R$) / ((SUM(customer_ltv_R$) * 0.20) / COUNT(DISTINCT customer_id)), 2) AS ltv_to_cac_ratio,
    
    -- Payback Period (months to recover CAC at avg monthly revenue)
    ROUND(((SUM(customer_ltv_R$) * 0.20) / COUNT(DISTINCT customer_id)) / (AVG(customer_ltv_R$) / 1), 2) AS payback_months
    
FROM CohortMetrics
GROUP BY cohort_month
ORDER BY cohort_month;

-- ============================================================================
-- FINAL LAYER - PART 4: TABLEAU EXPORT TABLE - CUSTOMER MASTER
-- ============================================================================

SELECT '=== TABLEAU EXPORT: CUSTOMER MASTER TABLE ===' AS section_title;

WITH CustomerMaster AS (
    SELECT 
        c.customer_id,
        c.customer_unique_id,
        c.customer_state,
        c.customer_city,
        c.customer_zip_code_prefix,
        
        -- Revenue Metrics
        ROUND(SUM(py.payment_value), 2) AS clv_R$,
        ROUND(AVG(py.payment_value), 2) AS avg_order_value_R$,
        COUNT(o.order_id) AS total_orders,
        
        -- Cost Metrics
        ROUND(SUM(oi.freight_value), 2) AS total_freight_R$,
        ROUND(SUM(py.payment_value) - SUM(oi.freight_value), 2) AS gross_profit_R$,
        
        -- Time Metrics
        MIN(o.order_purchase_timestamp) AS first_purchase_date,
        MAX(o.order_purchase_timestamp) AS last_purchase_date,
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS acquisition_cohort,
        DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                 MAX(o.order_purchase_timestamp)) AS days_since_last_purchase,
        
        -- Segmentation
        CASE 
            WHEN SUM(py.payment_value) >= 1000 THEN 'VIP'
            WHEN SUM(py.payment_value) >= 500 THEN 'High Value'
            WHEN SUM(py.payment_value) >= 200 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_tier,
        
        CASE 
            WHEN DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                         MAX(o.order_purchase_timestamp)) <= 90 THEN 'Active'
            WHEN DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                         MAX(o.order_purchase_timestamp)) <= 180 THEN 'At Risk'
            WHEN DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
                         MAX(o.order_purchase_timestamp)) <= 365 THEN 'Churning'
            ELSE 'Churned'
        END AS churn_risk_segment
        
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id, c.customer_unique_id, c.customer_state, c.customer_city, c.customer_zip_code_prefix
)
SELECT * FROM CustomerMaster
ORDER BY clv_R$ DESC;

  -- ============================================================================
-- FINAL LAYER - PART 5: TABLEAU EXPORT - PRODUCT PERFORMANCE
-- ============================================================================

SELECT '=== TABLEAU EXPORT: PRODUCT CATEGORY PERFORMANCE ===' AS section_title;

SELECT 
    p.product_category_name AS category_portuguese,
    COALESCE(t.product_category_name_english, p.product_category_name) AS category_english,
    
    -- Volume Metrics
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS total_items_sold,
    COUNT(DISTINCT oi.product_id) AS unique_products,
    
    -- Revenue Metrics
    ROUND(SUM(oi.price), 2) AS product_revenue_R$,
    ROUND(SUM(oi.freight_value), 2) AS freight_revenue_R$,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS total_revenue_R$,
    ROUND(AVG(oi.price), 2) AS avg_item_price_R$,
    
    -- Profitability Proxy
    ROUND(SUM(oi.price) - SUM(oi.freight_value), 2) AS gross_profit_proxy_R$,
    ROUND((SUM(oi.price) - SUM(oi.freight_value)) / SUM(oi.price) * 100, 2) AS gross_margin_pct,
    
    -- Market Share
    ROUND(100.0 * SUM(oi.price) / (SELECT SUM(price) FROM olist_order_items_dataset 
                                    JOIN olist_orders_dataset o2 ON olist_order_items_dataset.order_id = o2.order_id
                                    WHERE o2.order_status = 'delivered'), 2) AS pct_of_total_revenue
    
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o ON oi.order_id = o.order_id
JOIN olist_products_dataset p ON oi.product_id = p.product_id
LEFT JOIN olist_db.product_category_name_translation t 
    ON p.product_category_name = t.product_category_name
WHERE o.order_status = 'delivered'
  AND p.product_category_name IS NOT NULL
GROUP BY p.product_category_name, t.product_category_name_english
ORDER BY total_revenue_R$ DESC;

-- ============================================================================
-- PYTHON EXPORT 1: CUSTOMER MASTER
-- ============================================================================

SELECT 
    c.customer_id,
    c.customer_state,
    c.customer_city,
    ROUND(SUM(py.payment_value), 2) AS clv,
    ROUND(AVG(py.payment_value), 2) AS avg_order_value,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(oi.freight_value), 2) AS total_freight,
    DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS acquisition_cohort,
    DATEDIFF((SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset), 
             MAX(o.order_purchase_timestamp)) AS days_since_last_purchase,
    CASE 
        WHEN SUM(py.payment_value) >= 1000 THEN 'VIP'
        WHEN SUM(py.payment_value) >= 500 THEN 'High Value'
        WHEN SUM(py.payment_value) >= 200 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS value_tier
FROM olist_customers_dataset c
JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
JOIN olist_order_items_dataset oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_id, c.customer_state, c.customer_city;

-- ============================================================================
-- PYTHON EXPORT 2: PRODUCT CATEGORY PERFORMANCE 
-- ============================================================================

SELECT 
    COALESCE(t.product_category_name_english, p.product_category_name) AS category,
    COUNT(DISTINCT oi.order_id) AS total_orders,
    COUNT(oi.order_item_id) AS items_sold,
    ROUND(SUM(oi.price), 2) AS revenue,
    ROUND(AVG(oi.price), 2) AS avg_price,
    ROUND(SUM(oi.freight_value), 2) AS freight_cost
FROM olist_order_items_dataset oi
JOIN olist_orders_dataset o ON oi.order_id = o.order_id
JOIN olist_products_dataset p ON oi.product_id = p.product_id
LEFT JOIN olist_db.product_category_name_translation t 
    ON p.product_category_name = t.product_category_name
WHERE o.order_status = 'delivered'
  AND p.product_category_name IS NOT NULL
GROUP BY p.product_category_name, t.product_category_name_english
ORDER BY revenue DESC;

-- ============================================================================
-- PYTHON EXPORT 3: MONTHLY COHORT DATA 
-- ============================================================================

WITH FirstPurchase AS (
    SELECT 
        c.customer_id,
        DATE_FORMAT(MIN(o.order_purchase_timestamp), '%Y-%m') AS cohort_month,
        SUM(py.payment_value) AS customer_revenue
    FROM olist_customers_dataset c
    JOIN olist_orders_dataset o ON c.customer_id = o.customer_id
    JOIN olist_order_payments_dataset py ON o.order_id = py.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_id
)
SELECT 
    cohort_month,
    COUNT(DISTINCT customer_id) AS cohort_size,
    ROUND(SUM(customer_revenue), 2) AS cohort_revenue,
    ROUND(AVG(customer_revenue), 2) AS avg_customer_value
FROM FirstPurchase
GROUP BY cohort_month
ORDER BY cohort_month;


