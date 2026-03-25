# KPIs da Gold

## `gold_kpi_delivery_performance`

Esta tabela agrega os principais indicadores de performance logística. Seu objetivo é oferecer uma visão executiva, pronta para consumo, da relação entre volume de pedidos, atraso, fricção e comportamento temporal.

### Granularidade

- `order_purchase_month`
- `seller_state`
- `product_category_name`

### Indicadores principais

- `total_orders`
- `delayed_orders`
- `delay_rate_orders`
- `orders_with_negative_review`
- `friction_rate_orders`
- `high_friction_rate_orders`
- `avg_delay_days`
- `p50_delay_days`
- `p90_delay_days`
- métricas médias de jornada de entrega

### Valor de negócio

Esta tabela permite enxergar concentração de atraso e fricção por estado do seller e categoria do produto, facilitando priorização operacional.

## `gold_kpi_review_logistics`

Esta tabela conecta logística e experiência. Seu objetivo é mostrar como atraso e score de review se comportam juntos.

### Granularidade

- `order_purchase_month`
- `seller_state`
- `product_category_name`

### Indicadores principais

- `orders_with_negative_review`
- `negative_review_rate_orders`
- `delayed_orders`
- `delayed_orders_with_negative_review`
- `delayed_negative_review_rate`
- `avg_review_score`
- `avg_review_score_delayed_orders`
- `avg_review_score_on_time_orders`
- `review_logistics_risk_flag`

### Valor de negócio

Esta tabela aproxima a operação logística da percepção do cliente. Ela é uma das peças mais importantes do projeto porque materializa a hipótese central do estudo.

## `gold_kpi_payment_consistency`

Esta tabela agrega indicadores de consistência entre valor pago e soma dos itens do pedido.

### Granularidade

- `order_purchase_month`
- `seller_state`
- `product_category_name`

### Indicadores principais

- `orders_with_payment_gap`
- `payment_gap_rate_orders`
- `orders_with_credit_card`
- `orders_with_boleto`
- `orders_with_voucher`
- `avg_payment_gap_amount`
- `p50_payment_gap_amount`
- `p90_payment_gap_amount`
- `payment_consistency_risk_flag`

### Valor de negócio

Esta tabela ajuda a revelar ruídos entre pagamento e estrutura de itens, o que pode indicar inconsistências analíticas, operacionais ou financeiras.
