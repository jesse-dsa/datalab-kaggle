# Definição do problema de negócio

## Propósito do documento

Este documento formaliza o problema de negócio que orientará a arquitetura, a modelagem e as decisões analíticas do projeto. Seu papel é registrar, com clareza, qual dor do e-commerce será investigada, por que essa dor foi priorizada, qual hipótese sustenta a construção e que tipo de valor o negócio poderia extrair caso a análise seja bem conduzida.

## Problema central selecionado

O problema de negócio escolhido para guiar o projeto é o **atraso logístico e a degradação da experiência do cliente no pós-compra**. Essa escolha se apoia em um princípio essencial do varejo digital: a venda não termina quando o pedido é criado ou quando o pagamento é aprovado. A jornada comercial só se consolida de fato quando a operação entrega o que prometeu dentro de um nível aceitável de previsibilidade, confiança e experiência.

Quando o desempenho logístico falha, o impacto não fica restrito a um indicador operacional isolado. Ele se espalha para a percepção do cliente, para o desgaste do atendimento, para o risco reputacional da marca e para a eficiência da própria operação. Em termos de valor, atrasos e fricções no pós-compra representam deterioração de confiança, aumento de custo implícito e possibilidade de comprometimento de futuras compras.

## Formulação executiva do problema

A formulação executiva do problema pode ser descrita assim: a operação de e-commerce precisa entender como o desempenho logístico afeta a experiência do cliente e quais fatores operacionais, comerciais e contextuais estão mais associados a atrasos e fricções no pós-compra. A pergunta não é apenas se houve atraso, mas onde esse atraso se concentra, sob quais circunstâncias ele ocorre com maior frequência e como ele se relaciona com a percepção final da compra.

Essa formulação é importante porque desloca a análise de uma contagem simples de eventos para um raciocínio mais maduro de negócio. O objetivo deixa de ser apenas medir diferenças entre datas e passa a ser interpretar a jornada do pedido como sistema operacional com consequências perceptíveis no resultado final.

## Justificativa da escolha

Esse problema foi escolhido porque atende simultaneamente a relevância de negócio, aderência ao dataset e força narrativa para portfólio técnico. Do ponto de vista do negócio, atraso logístico é uma dor recorrente e financeiramente sensível no e-commerce. Do ponto de vista metodológico, a base Olist oferece atributos suficientes para observar a jornada do pedido, o prazo prometido, a entrega realizada e a avaliação do cliente. Do ponto de vista de posicionamento profissional, trata-se de um problema que evidencia como engenharia de dados pode servir diretamente à compreensão de performance operacional e experiência.

Poderíamos ter iniciado por temas como cancelamento, ruptura de estoque ou mix de pagamento, todos relevantes. No entanto, o problema escolhido apresenta melhor encaixe com a estrutura da base disponível. A Olist traz elementos que permitem relacionar pedido, item, seller, produto, cliente, timestamps operacionais e review final, formando um terreno mais consistente para investigar a qualidade do pós-compra.

## Hipótese de trabalho

A hipótese orientadora do projeto será a seguinte: **quanto pior o desempenho logístico do pedido, maior a probabilidade de deterioração da experiência percebida pelo cliente, e esse efeito não se distribui de forma homogênea entre sellers, categorias, regiões e contextos operacionais**. Essa hipótese não é uma conclusão antecipada, mas um ponto de partida útil para a modelagem. Ela exige que a arquitetura produza variáveis e relações capazes de testar esse comportamento de forma disciplinada.

A força dessa hipótese está em sua capacidade de orientar a camada Gold. Para investigá-la, o projeto precisará construir visão por pedido, visão por item, leitura temporal, contexto territorial, agrupamento por seller e categoria, além de uma forma consistente de conectar eventos logísticos à expressão final de experiência do cliente.

## O que o negócio ganha ao responder essa pergunta

Se o projeto responder bem esse problema, o negócio ganha capacidade de agir com mais precisão. Em vez de tratar a logística como um bloco único, passa a ser possível identificar concentrações de fricção, sellers com desempenho inferior, categorias mais sensíveis, regiões com comportamento mais crítico e combinações de contexto associadas a pior experiência. Isso melhora a qualidade da decisão porque substitui reações genéricas por diagnósticos direcionados.

Em linguagem de operação e valor, a análise pode apoiar revisão de SLA, avaliação de sellers, ajuste de promessa de prazo, priorização de melhorias de atendimento, reavaliação de categorias críticas e aprofundamento em contextos de maior risco operacional. O benefício final é uma leitura mais clara de onde a jornada do pós-compra está perdendo eficiência e confiança.

## Necessidade analítica derivada do problema

Para responder esse problema, a arquitetura precisará tratar o pedido como entidade central e conectá-lo a itens, sellers, produtos, clientes, reviews e marcos temporais do processo. Isso exige uma engenharia que preserve a origem, limpe e padronize os dados, crie variáveis derivadas de prazo e organize uma camada Gold apta a sustentar tanto análise descritiva quanto exploração de relações entre operação e experiência.

Em termos de necessidade de dados, o problema já antecipa o que será exigido da arquitetura. A Bronze terá de capturar e preservar a matéria-prima da jornada. A Silver precisará integrar e enriquecer os eventos relevantes. A Gold deverá apresentar o fenômeno em formato consumível, permitindo cortes por seller, categoria, geografia, tempo e desfecho de experiência.

## Decisão formal deste documento

Fica registrado que o problema de negócio orientador do projeto será a análise dos fatores associados ao atraso logístico e à degradação da experiência do cliente no pós-compra, utilizando os dados da Olist para estruturar uma visão integrada entre pedidos, itens, sellers, produtos, clientes e avaliações. Toda decisão posterior de modelagem, enriquecimento e construção da camada Gold deverá ser compatível com essa direção analítica.
