# Mapeamento das entidades principais

## Propósito do documento

Este documento registra as entidades principais da base Olist que participarão da construção do projeto e esclarece o papel analítico de cada uma dentro do problema escolhido. Sua função é transformar um conjunto disperso de tabelas em uma leitura organizada da jornada do pedido, permitindo que a arquitetura futura seja construída com consciência de granularidade, relação e finalidade de negócio.

## Entidade central de leitura

A entidade central do projeto será o **pedido**, pois é nele que a jornada comercial e operacional do e-commerce se materializa. O pedido liga compra, aprovação, expedição, entrega e percepção final do cliente, funcionando como eixo narrativo da análise. No entanto, o projeto não ficará preso exclusivamente a esse nível. Em diversos momentos, a granularidade correta será o **item do pedido**, já que sellers, produtos e categorias podem variar dentro do mesmo pedido e precisam ser observados com maior precisão.

Essa combinação entre pedido e item do pedido forma a base correta para o problema selecionado. Ela permite preservar a visão da jornada completa sem perder a capacidade de enxergar composição comercial, distribuição por vendedor e comportamento por categoria.

## Entidade orders

A tabela **orders** é a espinha dorsal do projeto. Ela contém o identificador principal do pedido e os marcos temporais que permitirão reconstruir a jornada operacional, incluindo criação, aprovação, envio, entrega e prazo estimado. Para o problema central do projeto, esta é a entidade mais crítica, porque é a partir dela que serão derivados indicadores de lead time, atraso, cumprimento de prazo e comportamento do fluxo pós-compra.

Seu papel não se limita a registrar um evento transacional. A tabela orders será a principal fonte para criação de variáveis temporais e de desempenho logístico, servindo como base de consolidação para a leitura integrada do pedido ao longo de sua trajetória.

## Entidade order_items

A tabela **order_items** representa os itens que compõem cada pedido. Sua importância é estrutural porque ela leva a análise a um nível mais detalhado, permitindo observar quais sellers, produtos e preços estão associados a cada ordem. Sem essa entidade, o projeto conseguiria medir a jornada do pedido, mas não teria base suficiente para identificar concentrações de fricção por vendedor, categoria ou composição comercial.

Dentro da arquitetura, order_items será fundamental para enriquecer a leitura do pedido e permitir a construção de fatos ou visões analíticas em granularidade mais fina. Ela é a entidade que conecta o fenômeno logístico ao contexto comercial efetivo da venda.

## Entidade order_reviews

A tabela **order_reviews** insere a voz do cliente no projeto. Como a pergunta central relaciona desempenho logístico e experiência, essa entidade é indispensável para observar o desfecho perceptivo da jornada. Ela permitirá avaliar se contextos de pior desempenho operacional estão associados a avaliações mais baixas e se certas combinações de seller, categoria, tempo ou geografia tendem a concentrar maior insatisfação.

Do ponto de vista analítico, é importante tratá-la com maturidade. Review não representa exclusivamente logística, pois a nota do cliente pode refletir diversos elementos da experiência. Ainda assim, combinada com os eventos operacionais do pedido, a tabela torna-se um indicador muito valioso para leitura do pós-compra.

## Entidade customers

A tabela **customers** representa o comprador e seu contexto territorial. Seu papel dentro do projeto está menos ligado à identidade do cliente em si e mais à dimensão geográfica da análise. Em operações digitais, desempenho logístico raramente se distribui de forma homogênea no território, e por isso a localização do cliente é fundamental para compreender diferenças regionais de prazo, fricção e experiência.

Essa entidade permitirá enriquecer a análise com perspectiva de estado, cidade e área de destino, contribuindo para diagnósticos mais maduros e evitando interpretações simplistas sobre sellers ou categorias sem considerar o peso do contexto geográfico.

## Entidade products

A tabela **products** representa o produto e a classificação de categoria. Sua função é permitir que o projeto observe o desempenho da jornada também por tipo de item vendido, reconhecendo que certas categorias podem apresentar maior complexidade operacional, maior sensibilidade à entrega ou maior propensão a avaliações negativas quando a experiência não corresponde à expectativa.

Na camada Gold, essa entidade deverá apoiar leituras por categoria e composição de mix, ampliando a capacidade do projeto de explicar onde a fricção logística se concentra e como ela pode variar conforme o perfil do produto comercializado.

## Entidade sellers

A tabela **sellers** possui papel central dentro da leitura de marketplace. Em um ecossistema desse tipo, o seller influencia diretamente a operação, especialmente em aspectos ligados a processamento, expedição e consistência do serviço. Por isso, esta entidade será uma das dimensões mais relevantes do projeto quando a análise buscar identificar onde o risco operacional se concentra.

O seller não será tratado como simples dimensão cadastral. Ele será lido como eixo de performance, permitindo observar diferenças de comportamento logístico, concentração de atraso e associação com a experiência final do cliente.

## Entidade order_payments

A tabela **order_payments** terá papel complementar. Embora não esteja no centro da dor escolhida, ela pode enriquecer a compreensão do contexto transacional do pedido, especialmente em situações em que o tempo de aprovação ou a composição financeira ajudem a explicar parte do comportamento operacional. Em outras palavras, trata-se de uma entidade de suporte, relevante para enriquecer, mas não para conduzir a narrativa principal.

## Entidade geolocation

A tabela **geolocation** amplia a capacidade de análise territorial. Ela não precisa necessariamente compor o primeiro núcleo mínimo viável da Gold, mas deve ser mapeada desde já porque possui valor para aprofundamentos geográficos futuros. Seu uso poderá fortalecer análises de dispersão regional, agrupamento territorial ou observação mais refinada do contexto de destino.

## Entidade product_category_name_translation

A tabela **product_category_name_translation** tem função de apoio editorial e analítico. Seu papel é melhorar a legibilidade da camada final, traduzindo nomes de categorias e tornando os outputs mais compreensíveis para consumo humano. Embora não altere a lógica estrutural do problema, sua presença eleva a qualidade de apresentação e reforça o cuidado do projeto com a camada de uso.

## Relações principais entre as entidades

Do ponto de vista relacional, a tabela **orders** se conecta a **order_items**, **order_reviews** e **order_payments** pelo identificador do pedido. A tabela **order_items** conecta o pedido a **products** e **sellers**, enquanto **orders** se conecta a **customers** para fornecer o contexto do comprador. A tabela **customers** pode ser enriquecida pela **geolocation** em análises territoriais mais avançadas, e **products** pode ser enriquecida pela **product_category_name_translation** para melhorar a camada final de consumo.

Esse desenho mostra que a jornada do pedido não está concentrada em um único lugar. Ela é distribuída entre entidades complementares, e caberá à engenharia costurar essas relações sem perder a granularidade correta nem introduzir duplicidades indevidas.

## Observação sobre granularidade

A granularidade de cada entidade deve ser respeitada desde o início. **Orders** opera no nível de um registro por pedido. **Order_items** opera no nível de item do pedido. **Order_payments** pode apresentar múltiplos registros por pedido. **Order_reviews** exige validação cuidadosa de cardinalidade. **Customers**, **products** e **sellers** funcionam como dimensões complementares. Esse ponto é decisivo, porque muitos erros analíticos surgem justamente quando joins são executados sem consciência do nível de detalhe de cada tabela.

## Priorização para a fase inicial

Na fase inicial do projeto, as entidades prioritárias serão **orders**, **order_items**, **order_reviews**, **customers**, **products** e **sellers**, pois elas sustentam diretamente a investigação do problema central. **Order_payments** entrará como enriquecimento secundário, **geolocation** como expansão territorial e **product_category_name_translation** como apoio de legibilidade e apresentação.

## Decisão formal deste documento

Fica registrado que a estrutura analítica do projeto será construída sobre o eixo **pedido + item do pedido**, enriquecido por contexto de cliente, seller, produto, review, pagamento e geografia. As entidades principais da base Olist foram mapeadas de acordo com seu papel no problema de negócio selecionado, e sua utilização futura deverá respeitar a granularidade correta, a lógica relacional e a necessidade de produzir uma camada Gold orientada à interpretação do desempenho logístico e da experiência do cliente.
