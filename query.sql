
CREATE INDEX idx_transactions_shop_date ON outlet_transactions (SHOP_ID, DATE);

CREATE VIEW status_periods AS
WITH min_date AS (
    SELECT
        SHOP_ID,
        DATE AS start_date,
        LEAD(DATE) OVER (PARTITION BY SHOP_ID ORDER BY DATE) AS end_date
    FROM outlet_transactions
),
status_calculation AS (
    SELECT
        SHOP_ID,
        start_date,
        end_date,
        JULIANDAY(end_date) - JULIANDAY(start_date) AS date_diff,
        CASE
            WHEN JULIANDAY(end_date) - JULIANDAY(start_date) >= 30 THEN 'clsd'
            ELSE 'open'
        END AS status
    FROM min_date
    WHERE end_date IS NOT NULL
),
status_with_lag AS (
    SELECT
        SHOP_ID,
        start_date,
        end_date,
        status,
        LAG(status) OVER (PARTITION BY SHOP_ID ORDER BY start_date) AS prev_status
    FROM status_calculation
),
grouped_status AS (
    SELECT
        SHOP_ID,
        start_date,
        end_date,
        status,
        CASE
            WHEN status = prev_status THEN 0
            ELSE 1
        END AS status_change_flag
    FROM status_with_lag
),
grouping AS (
    SELECT
        SHOP_ID,
        start_date,
        end_date,
        status,
        SUM(status_change_flag) OVER (PARTITION BY SHOP_ID ORDER BY start_date) AS period_group
    FROM grouped_status
)
SELECT
    SHOP_ID,
    MIN(start_date) AS lower_range,
    MAX(end_date) AS upper_range,
    status
FROM grouping
GROUP BY SHOP_ID, status, period_group
ORDER BY SHOP_ID, lower_range;
