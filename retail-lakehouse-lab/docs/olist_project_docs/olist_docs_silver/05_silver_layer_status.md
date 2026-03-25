# Status da camada Silver

## Visão geral

A camada Silver do projeto Olist foi construída para transformar as tabelas brutas da Bronze em estruturas confiáveis, tipadas e orientadas à análise. Nesta etapa, os dados deixam de ser apenas registros preservados da origem e passam a carregar significado operacional, consistência mínima e variáveis derivadas úteis para leitura do negócio.

A Silver não existe para entregar o produto final ao cliente. O seu papel é preparar a base para isso. Em termos práticos, ela organiza a jornada do pedido, corrige tipos, padroniza campos, cria flags de consistência e reduz o esforço analítico das etapas seguintes. É aqui que o projeto passa a ter uma base pronta para integração entre domínios como pedido, item, cliente, seller, produto, pagamento e review.

## Tabelas Silver concluídas

As tabelas concluídas até aqui são:

- `silver_orders`
- `silver_order_items`
- `silver_order_payments`
- `silver_customers`
- `silver_products`
- `silver_sellers`
- `silver_order_reviews`
- `silver_orders_enriched`

## Papel da camada Silver no projeto

A Silver cumpre quatro funções principais. A primeira é padronizar e tipar os campos essenciais. A segunda é criar variáveis derivadas úteis para análise de jornada e qualidade operacional. A terceira é sinalizar inconsistências e ausências de dados por meio de flags específicas. A quarta é preparar uma visão integrada do pedido por item, capaz de sustentar a futura modelagem Gold.

## Decisão arquitetural importante

A tabela `silver_orders_enriched` foi construída em granularidade de item do pedido. Isso significa que um mesmo `order_id` pode aparecer em múltiplas linhas, uma para cada item. Essa decisão foi adotada para preservar seller, produto e categoria sem empobrecer a análise.

## Resultado desta etapa

Ao final da Silver, o projeto passa a contar com uma base analítica intermediária que já consegue responder perguntas operacionais relevantes e que está pronta para alimentar fatos, dimensões e KPIs na camada Gold.
