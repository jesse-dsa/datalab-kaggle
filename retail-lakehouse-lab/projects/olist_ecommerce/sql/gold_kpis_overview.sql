-- Consulta de referência para KPIs iniciais do projeto Olist
-- Ajuste os nomes de catálogo, schema e tabelas conforme o ambiente.

SELECT
    order_purchase_date,
    COUNT(DISTINCT order_id)                         AS total_orders,
    SUM(order_total_amount)                          AS gross_revenue,
    AVG(order_total_amount)                          AS average_order_value,
    SUM(CASE WHEN delivered_flag = 1 THEN 1 ELSE 0 END) AS delivered_orders
FROM gold.fact_orders
GROUP BY order_purchase_date
ORDER BY order_purchase_date;
