SELECT
  CASE
    WHEN COUNT(*) > 0 THEN 'PASS'
    ELSE 'FAIL'
  END
FROM test_catalog.bronze.orders
WHERE _rescued_data IS NOT NULL
  AND length(_rescued_data) > 2