# `silver_orders_enriched`

## O que é

A tabela `silver_orders_enriched` é a visão integrada da camada Silver. Ela junta pedidos, itens, clientes, produtos, sellers, pagamentos e reviews em uma única estrutura analítica intermediária. O seu papel é reduzir o custo de análise e preparar a transição para a Gold.

## Granularidade

A granularidade desta tabela é de item do pedido. Isso significa que cada linha representa um item associado a um `order_id`. Um pedido com múltiplos itens pode aparecer em várias linhas.

Essa escolha é importante porque preserva o contexto de seller, produto e categoria sem perder a conexão com a jornada do pedido.

## O que entra na integração

A tabela integra as seguintes origens Silver:

- `silver_orders`
- `silver_order_items`
- `silver_order_payments` (agregado por pedido)
- `silver_customers`
- `silver_products`
- `silver_sellers`
- `silver_order_reviews` (agregado por pedido)

## Métricas e sinais adicionados

Entre os campos derivados mais importantes, esta tabela entrega:

- contagem de itens por pedido
- valor total dos itens do pedido
- participação do item no valor total do pedido
- valor total de pagamento no pedido
- diferença entre valor pago e soma dos itens
- consistência entre pagamento e itens
- presença ou ausência de dados de item, pagamento, review, cliente, produto e seller

## Papel no projeto

A `silver_orders_enriched` é a ponte entre a Silver e a Gold. Ela já oferece uma visão muito rica da jornada do pedido, mas ainda não é a camada final de consumo executivo. O objetivo dela é concentrar a integração e deixar a Gold focada na entrega de fatos, dimensões e KPIs de negócio.

## Por que ela é importante

Sem essa tabela, a análise ficaria mais cara e repetitiva, exigindo joins frequentes em várias entidades. Com ela, o projeto passa a ter uma visão consolidada da operação, útil para investigar atraso, fricção logística, comportamento por seller, categoria e experiência do cliente.
