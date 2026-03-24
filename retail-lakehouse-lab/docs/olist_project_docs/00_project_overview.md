# Visão geral do projeto

## Propósito do documento

Este documento apresenta a visão executiva do projeto **Olist e-commerce data platform**, estabelecendo a identidade da iniciativa, seu propósito analítico, o problema de negócio que justifica sua existência e a lógica arquitetural que sustentará sua construção. A intenção aqui não é detalhar ainda cada transformação técnica, mas registrar com clareza o que o projeto é, por que ele existe e qual valor pretende gerar dentro de um contexto de e-commerce orientado por dados.

## Identidade do projeto

O **Olist e-commerce data platform** é um projeto de engenharia de dados aplicado ao varejo digital, estruturado a partir da base pública da Olist e desenhado para demonstrar, de forma profissional, como dados operacionais podem ser transformados em ativos analíticos confiáveis. O projeto nasce com foco em arquitetura, rastreabilidade, qualidade de dados e utilidade prática, buscando organizar a jornada do pedido em uma estrutura que permita leituras técnicas e de negócio com consistência.

Mais do que um exercício de ETL, este projeto foi concebido como uma plataforma compacta de inteligência analítica para e-commerce. Sua função é mostrar, na prática, como uma arquitetura bem definida consegue sair do nível bruto de arquivos transacionais e chegar a uma camada de decisão capaz de apoiar diagnósticos operacionais, leitura de desempenho e entendimento mais profundo da experiência do cliente no pós-compra.

## Problema central que orienta o projeto

O problema central escolhido para conduzir toda a modelagem é o impacto do desempenho logístico sobre a experiência do cliente no pós-compra. Em operações digitais, a venda não termina na aprovação do pagamento. Ela continua na expedição, no transporte, na entrega e na percepção final do cliente. Quando há atraso, fricção operacional ou inconsistência na jornada, o efeito recai sobre satisfação, reputação, confiança na compra e eficiência da operação.

Este projeto existe para estruturar uma visão integrada desse fenômeno. A proposta é organizar os dados de pedidos, itens, sellers, produtos, clientes e reviews de maneira que a arquitetura consiga responder onde os atrasos se concentram, em quais contextos operacionais eles ocorrem com maior frequência e como se relacionam com a percepção final do cliente. A engenharia, portanto, não será tratada como fim em si mesma, mas como instrumento para leitura de valor e risco no e-commerce.

## Objetivo central

O objetivo central do projeto é construir um repositório profissional de engenharia de dados capaz de ingerir, tratar, modelar e disponibilizar os dados da base Olist em arquitetura Medallion, de forma que o fluxo resultante responda perguntas relevantes de negócio no contexto do varejo digital. A ênfase está na criação de uma base confiável, reproduzível e inteligível, apta a sustentar tanto análises descritivas quanto evolução futura para produtos analíticos mais sofisticados.

## Arquitetura esperada

O projeto será organizado na lógica **Bronze, Silver e Gold**, respeitando o papel conceitual de cada camada. A Bronze terá a função de preservar a origem, garantindo rastreabilidade, mínima interferência e captura disciplinada dos dados de entrada. A Silver terá a responsabilidade de padronizar, limpar, tipar, enriquecer e preparar as entidades para integração analítica. A Gold, por sua vez, materializará fatos, dimensões e indicadores orientados a negócio, oferecendo uma leitura consolidada do comportamento operacional e comercial da jornada do pedido.

Essa arquitetura não foi escolhida apenas por aderência a boas práticas modernas de dados. Ela foi escolhida porque permite organizar a progressão natural do valor analítico. Em vez de misturar ingestão, tratamento e consumo em uma única camada confusa, o projeto separa claramente preservação, refinamento e entrega analítica. Isso melhora a manutenção, a legibilidade do repositório e a capacidade de explicar o projeto a públicos técnicos e executivos.

## Entidade de leitura dominante

A entidade dominante do projeto será o **pedido**, complementada pelo nível de **item do pedido** quando a análise exigir leitura por seller, produto ou categoria. Essa decisão é estrutural, porque o pedido representa a materialização da jornada de compra, enquanto o item permite granularidade suficiente para entender composição comercial, distribuição por vendedor e padrões de fricção associados a diferentes contextos operacionais.

A partir desse eixo, o projeto conectará informações de aprovação, entrega, prazo estimado, avaliações, sellers, clientes e produtos, compondo uma visão integrada da experiência pós-compra. Essa escolha assegura que a arquitetura fique ancorada em uma unidade de negócio clara e evite modelagens desconectadas da jornada real do e-commerce.

## Resultado esperado

Ao final, o projeto deverá entregar um repositório em que a navegação entre documentação, notebooks, SQL, metadados, testes e outputs faça sentido técnico e narrativo. Espera-se que qualquer pessoa que abra o projeto consiga compreender, sem esforço excessivo, qual problema está sendo investigado, quais entidades sustentam a análise, como a arquitetura foi organizada e quais produtos analíticos emergem da camada Gold.

O resultado esperado não é apenas a existência de tabelas transformadas. O objetivo é que o projeto revele maturidade de engenharia e clareza de raciocínio de negócio, servindo ao mesmo tempo como laboratório técnico, ativo de portfólio e base para expansões futuras em analytics e ciência de dados.

## Decisão formal deste documento

Fica registrado que o **Olist e-commerce data platform** será tratado como um projeto de engenharia de dados orientado a valor analítico, com foco na relação entre desempenho logístico e experiência do cliente no pós-compra. Sua construção será guiada por arquitetura Medallion, com o pedido como entidade central de leitura e com a camada Gold desenhada para responder perguntas relevantes do e-commerce de forma confiável e profissional.
