# Contratos do fato e das dimensões Gold

## `gold_fact_orders`

Esta é a tabela fato principal do projeto. Ela foi desenhada para concentrar, em granularidade de item do pedido, os principais atributos da jornada do pedido, do item, do cliente, do seller, do produto, do pagamento e da experiência.

### Papel analítico

A `gold_fact_orders` é a base mais importante para perguntas de negócio exploratórias, dashboards e storytelling. Ela permite observar, na mesma linha, o item do pedido, o seller, a categoria do produto, os sinais de atraso, os sinais de review e os sinais de fricção operacional.

### Principais grupos de colunas

- identificadores analíticos, como `order_item_key`, `order_id` e `order_item_id_int`
- atributos de pedido, cliente, seller e produto
- métricas de valor, pagamento e consistência
- métricas de jornada logística
- score e sinais de experiência do cliente
- agrupamentos executivos, como `delivery_status_group`, `review_sentiment_group` e `delay_days_bucket`
- flags compostas, como `friction_flag` e `high_friction_flag`

## `gold_dim_customers`

Esta dimensão organiza os clientes para consumo analítico, com foco em identificadores, localização, bandas de CEP e qualidade dos dados.

### Papel analítico

A `gold_dim_customers` permite análises de comportamento e distribuição geográfica com menor acoplamento ao fato principal. Ela também facilita filtros e segmentações em dashboards.

### Principais grupos de colunas

- chaves de cliente
- atributos geográficos
- agrupamentos de localização
- banda de CEP
- status de qualidade dos dados

## `gold_dim_products`

Esta dimensão organiza os produtos para consumo analítico, com foco em categoria, atributos físicos e qualidade de cadastro.

### Papel analítico

A `gold_dim_products` sustenta leituras sobre categoria, complexidade física, peso, volume e qualidade do cadastro. É especialmente importante para hipóteses ligadas a logística e fricção operacional.

### Principais grupos de colunas

- chaves de produto
- categoria
- atributos textuais e de catálogo
- peso e dimensões
- agrupamentos de tamanho, peso e fotos
- status de qualidade dos dados

## `gold_dim_sellers`

Esta dimensão organiza os sellers para consumo analítico, com foco em localização, agrupamentos geográficos e qualidade dos dados.

### Papel analítico

A `gold_dim_sellers` sustenta análises geográficas de seller e serve como apoio importante para leituras de fricção, atraso e concentração operacional.

### Principais grupos de colunas

- chaves de seller
- atributos geográficos
- agrupamentos de localização
- banda de CEP
- status de qualidade dos dados
