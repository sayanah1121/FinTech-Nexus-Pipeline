-- Check the Reward Engine results
SELECT * FROM user_rewards LIMIT 10;

-- Check the Fraud Alerts
SELECT * FROM fraud_alerts LIMIT 10;

-- Check Vendor KPI Aggregations
SELECT * FROM vendor_kpis LIMIT 10;

SELECT 
    user_id_hash, 
    SUM(earned_points) as total_points, 
    COUNT(*) as transaction_count
FROM user_rewards
GROUP BY user_id_hash
ORDER BY total_points DESC
LIMIT 5;

SELECT 
    vendor, 
    SUM(total_volume) as total_volume, 
    SUM(txn_count) as total_transactions
FROM vendor_kpis
GROUP BY vendor
ORDER BY total_volume DESC;

SELECT * FROM user_rewards
WHERE amount > 500
ORDER BY txn_date DESC;