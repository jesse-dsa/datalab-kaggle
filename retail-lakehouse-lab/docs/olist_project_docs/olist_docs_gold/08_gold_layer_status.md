# Status da camada Gold

## Visão geral

A camada Gold do projeto Olist representa o ponto em que a arquitetura deixa de ser apenas preparação analítica e passa a se tornar produto de dados consumível. Nesta etapa, o objetivo não é mais organizar ou integrar entidades isoladas, mas entregar estruturas finais orientadas a consumo executivo, leitura de negócio e suporte à decisão.

A Gold foi construída a partir da `silver_orders_enriched`, das dimensões tratadas e da exploração analítica realizada na etapa anterior. Isso significa que os objetos finais da Gold não nasceram de forma arbitrária. Eles são resultado direto das perguntas de negócio priorizadas ao longo do projeto.

## Tabelas Gold concluídas

As tabelas concluídas até aqui são:

- `gold_fact_orders`
- `gold_dim_customers`
- `gold_dim_products`
- `gold_dim_sellers`
- `gold_kpi_delivery_performance`
- `gold_kpi_review_logistics`
- `gold_kpi_payment_consistency`

## Papel da Gold no projeto

A Gold cumpre quatro papéis principais. O primeiro é consolidar um fato analítico principal pronto para exploração, dashboards e storytelling. O segundo é oferecer dimensões limpas para contextualização geográfica, categórica e operacional. O terceiro é entregar KPIs agregados em nível executivo, reduzindo a necessidade de joins e cálculos repetitivos no consumo final. O quarto é servir como ponte definitiva entre a engenharia do pipeline e a narrativa de valor do projeto.

## Decisão arquitetural importante

O fato principal `gold_fact_orders` foi mantido em granularidade de item do pedido. Essa escolha preserva seller, produto, categoria e sinais operacionais relevantes sem empobrecer a leitura da jornada do pedido. Os KPIs agregados foram modelados separadamente para reduzir custo analítico e melhorar o consumo executivo.

## Resultado desta etapa

Ao final da Gold, o projeto passa a contar com um conjunto coerente de produtos analíticos prontos para responder, com muito mais clareza, onde o atraso se concentra, quais sellers e categorias estão mais associados à fricção, como a experiência do cliente reage ao desempenho logístico e onde existem inconsistências entre pagamento e itens.
