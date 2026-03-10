SELECT
  CASE
    WHEN COUNT(*) = 3 THEN 'PASS'
    ELSE 'FAIL'
  END AS result
FROM prod_catalog.silver.orders