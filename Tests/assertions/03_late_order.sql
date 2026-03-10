SELECT
  CASE
    WHEN COUNT(*) = 1 THEN 'PASS'
    ELSE 'FAIL'
  END
FROM test_catalog.silver.orders
WHERE order_id = 200