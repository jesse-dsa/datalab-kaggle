# Olist e-commerce

## Status atual do projeto

O projeto já concluiu a preparação do ambiente, a extração da base da Olist a partir do Kaggle, a camada Bronze principal, a camada Silver principal, a exploração analítica e a camada Gold principal. Neste momento, o projeto já possui produtos analíticos finais prontos para consumo executivo, dashboard e storytelling.

## Fonte de dados

- Origem: Kaggle
- Dataset: Olist Brazilian E-commerce
- Fluxo adotado: Kaggle → raw zone → Bronze → Silver → Gold

## Camada Bronze concluída

Tabelas criadas:

- `bronze_orders`
- `bronze_order_items`
- `bronze_customers`
- `bronze_products`
- `bronze_sellers`
- `bronze_order_reviews`
- `bronze_order_payments`
- `bronze_geolocation`
- `bronze_category_translation`

## Camada Silver concluída

Tabelas criadas:

- `silver_orders`
- `silver_order_items`
- `silver_order_payments`
- `silver_customers`
- `silver_products`
- `silver_sellers`
- `silver_order_reviews`
- `silver_orders_enriched`

## Exploração analítica concluída

Foi executada uma rodada de análise exploratória orientada pelas perguntas de negócio centrais, permitindo confirmar os principais padrões e definir os produtos finais da Gold.

## Camada Gold concluída

Tabelas criadas:

- `gold_fact_orders`
- `gold_dim_customers`
- `gold_dim_products`
- `gold_dim_sellers`
- `gold_kpi_delivery_performance`
- `gold_kpi_review_logistics`
- `gold_kpi_payment_consistency`

## O que a Gold já entrega

A Gold já entrega um fato principal pronto para exploração e dashboard, dimensões limpas para contexto analítico e KPIs executivos que mostram atraso, fricção, experiência do cliente e consistência de pagamento.
