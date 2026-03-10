SELECT
  CASE
    WHEN COUNT(*) = 2 THEN 'PASS'
    ELSE 'FAIL'
  END
FROM test_catalog.golden.dim_customer
WHERE customer_id = 100