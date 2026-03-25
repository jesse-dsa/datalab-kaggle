# Contratos das principais tabelas Silver

## `silver_orders`

Esta tabela representa a jornada do pedido em nível de pedido. Nela ficam organizadas as datas principais do fluxo operacional, os tempos entre etapas, o atraso em relação ao prazo prometido e flags de inconsistência temporal. É a espinha dorsal temporal do projeto.

Principais grupos de colunas:
- identificação do pedido e cliente
- timestamps e datas da jornada
- métricas de tempo entre etapas
- indicadores de status e atraso
- flags de inconsistência temporal
- metadados mínimos de processamento

## `silver_order_items`

Esta tabela representa os itens do pedido em granularidade de item. Aqui são tipados preço, frete, identificador do item e data limite de envio. Também são criadas flags básicas de ausência e inconsistência.

Principais grupos de colunas:
- identificação do pedido, item, produto e seller
- valor do item, frete e total do item
- indicador de frete grátis
- flags de ausência de produto e seller
- flags de problemas em preço, frete e identificador do item

## `silver_order_payments`

Esta tabela organiza os pagamentos do pedido, com tipagem de valor, parcelas e sequência. Também cria sinalizadores para tipo de pagamento e qualidade do valor.

Principais grupos de colunas:
- identificação do pedido
- tipo de pagamento
- número de parcelas
- valor do pagamento
- flags para cartão, boleto e voucher
- flags de inconsistência em valor e estrutura do pagamento

## `silver_customers`

Esta tabela organiza a dimensão de cliente necessária para o estudo, com foco em localização e identificadores. Os campos de cidade e estado são padronizados e o CEP prefixado é tipado.

Principais grupos de colunas:
- identificadores do cliente
- CEP prefixado
- cidade e estado
- flags de ausência
- validação simples de UF brasileira

## `silver_products`

Esta tabela organiza a dimensão de produto com foco em categoria, atributos de catálogo e características físicas. Também cria o volume do produto em centímetros cúbicos, útil para leituras futuras de logística e complexidade operacional.

Principais grupos de colunas:
- identificador do produto
- categoria
- comprimento do nome e da descrição
- quantidade de fotos
- peso e dimensões
- volume calculado
- flags de ausência e inconsistência

## `silver_sellers`

Esta tabela organiza a dimensão de seller com foco em localização. Os campos de cidade e estado são padronizados e o CEP prefixado é tipado. Também há flags de qualidade simples.

Principais grupos de colunas:
- identificador do seller
- CEP prefixado
- cidade e estado
- flags de ausência
- validação simples de UF brasileira

## `silver_order_reviews`

Esta tabela organiza a experiência do pedido no nível do review. O score é tipado, as datas do review são tratadas e são criadas classificações simples de review positivo, neutro e negativo.

Principais grupos de colunas:
- identificadores de review e pedido
- score do review
- comentários
- datas de criação e resposta
- tempo de resposta
- classificação do review
- flags de inconsistência
