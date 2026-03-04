#Integrity rules for joins 

fact_order_rules = {
    "customer_sk_not_null": "customer_sk IS NOT NULL",
    "status_valid": "status IN ('CANCELLED','NEW', 'DELIVERED', 'SHIPPED')",
    "price_positive": "amount > 0"
}

fact_order_items_rules = {
    "customer_sk_not_null": "customer_sk IS NOT NULL",
    "product_sk_not_null": "product_sk IS NOT NULL",
    "quantity_positive": "quantity > 0",
    "unit_price_positive": "unit_price > 0"
}

