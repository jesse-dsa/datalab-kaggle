# Status da camada Bronze

## Objetivo da Bronze

A camada Bronze foi definida para preservar a origem com rastreabilidade técnica, mantendo os arquivos da Olist o mais próximo possível da fonte bruta e adicionando apenas metadados técnicos de ingestão.

## Origem e fluxo

Fluxo de entrada adotado:

**Kaggle → raw zone do projeto → Bronze**

A raw zone foi preparada em volume controlado e os arquivos foram baixados da base pública da Olist para uma área de aterrissagem dedicada.

## Padrão técnico adotado

Cada notebook Bronze:
- valida a existência da raw zone e do arquivo esperado;
- lê o CSV a partir de `/Volumes/...`;
- preserva as colunas originais da fonte;
- adiciona metadados técnicos por meio de `_metadata`;
- adiciona `_ingestion_timestamp`;
- persiste o resultado em Delta.

## Tabelas Bronze concluídas

- `bronze_orders`
- `bronze_order_items`
- `bronze_customers`
- `bronze_products`
- `bronze_sellers`
- `bronze_order_reviews`
- `bronze_order_payments`
- `bronze_geolocation`
- `bronze_category_translation`

## Resultado da etapa

A camada Bronze principal da Olist foi concluída com sucesso. Neste ponto, o projeto já possui a base bruta oficial do pipeline estruturada em Delta e pronta para o início da Silver.

## Próximo passo

Início da Silver, começando por `02_01_silver_orders`.
