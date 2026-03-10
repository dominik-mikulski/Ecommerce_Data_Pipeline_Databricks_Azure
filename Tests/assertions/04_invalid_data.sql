SELECT
  CASE
    WHEN COUNT(*) = 0 THEN 'PASS'
    ELSE 'FAIL'
  END
FROM test_catalog.silver.orders
WHERE order_id IS NULL