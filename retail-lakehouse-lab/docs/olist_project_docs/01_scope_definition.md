# Definição do escopo

## Propósito do documento

Este documento formaliza o escopo do projeto, estabelecendo com precisão o que será construído, qual fronteira será respeitada e que tipo de entrega se espera ao final da iniciativa. A intenção deste registro é impedir que o projeto se dilua em possibilidades excessivas e, ao mesmo tempo, garantir que a construção técnica permaneça alinhada a um propósito claro de negócio e de portfólio.

## Declaração de escopo

O projeto tem como escopo a construção de uma solução de engenharia de dados aplicada à base Olist, organizada em arquitetura Medallion, com foco em ingestão estruturada, transformação confiável, modelagem analítica e produção de insumos úteis para leitura de negócio no e-commerce. A solução será materializada em um repositório profissional, com separação explícita entre documentação, notebooks, SQL, testes, metadados e outputs analíticos.

A natureza do escopo é deliberadamente prática. Isso significa que o projeto não será conduzido como exercício acadêmico abstrato nem como simples coleção de scripts. Ele será tratado como uma plataforma compacta de dados, suficientemente estruturada para demonstrar lógica arquitetural, disciplina de transformação e capacidade de traduzir dados operacionais em visão analítica para decisão.

## Objetivo central do escopo

O objetivo central deste escopo é permitir que o projeto organize os dados da Olist em camadas coerentes e reprodutíveis, de forma que a base resultante responda perguntas relevantes sobre o funcionamento do e-commerce. Em termos concretos, o escopo existe para viabilizar a passagem entre a matéria-prima transacional e um modelo analítico legível, confiável e útil para diagnóstico operacional e interpretação de performance.

## O que está dentro do escopo

Está dentro do escopo a definição da arquitetura do projeto, a criação da lógica Bronze, Silver e Gold, a ingestão dos arquivos de origem, o tratamento estrutural das entidades principais, a criação de variáveis derivadas, a integração entre tabelas relevantes, a modelagem analítica final e a documentação técnica da jornada de dados. Também faz parte do escopo a definição de critérios de qualidade, rastreabilidade de origem, convenções de nomenclatura e organização do repositório em padrão profissional.

Do ponto de vista do uso analítico, o escopo também inclui a preparação de fatos, dimensões e indicadores capazes de sustentar perguntas de negócio ligadas à jornada do pedido, especialmente no contexto de desempenho logístico e experiência do cliente. Isso quer dizer que a camada Gold não será tratada como mero produto técnico, mas como camada de consumo voltada a interpretação operacional e comercial.

## O que está fora do escopo neste momento

Neste estágio inicial, não faz parte do escopo transformar o projeto em uma solução enterprise completa com todos os componentes de governança corporativa, monitoramento avançado, ingestão contínua em tempo real, esteiras completas de CI/CD produtivo ou controles multiambiente típicos de uma plataforma madura de produção. O objetivo aqui é construir um projeto tecnicamente sólido e profissional, mas em escala compatível com laboratório e portfólio.

Também está fora do escopo, por ora, a implantação de um produto final de machine learning em produção. Embora a estrutura do repositório reserve espaço para evolução futura nessa direção, a prioridade inicial será consolidar a engenharia de dados e a camada analítica. Da mesma forma, não é objetivo replicar integralmente um contexto corporativo específico, mas demonstrar domínio arquitetural e capacidade de construção aplicada.

## Público de interesse

O projeto foi desenhado para ser inteligível e valioso a três públicos principais. O primeiro é o público técnico, composto por engenheiros de dados, analytics engineers, arquitetos de dados e recrutadores, que observarão a qualidade da organização, da modelagem e da disciplina estrutural do repositório. O segundo é o público de negócio, como gestores de e-commerce, BI, operações e performance, que precisam enxergar utilidade prática na camada Gold e nas perguntas analíticas respondidas. O terceiro é o próprio autor do projeto, que utilizará o repositório como instrumento de consolidação técnica e base de evolução futura.

## Critérios de sucesso do escopo

O escopo será considerado corretamente definido quando não houver ambiguidade sobre o propósito do projeto, as fronteiras da construção, as entidades principais envolvidas e os produtos analíticos esperados ao final. Em termos práticos, o sucesso deste documento será percebido quando a codificação puder começar sem dúvidas estruturais sobre o que entra, como será tratado, o que sairá e por que isso importa para o problema central.

Mais do que fechar uma lista de entregas, o sucesso do escopo está em alinhar intenção, limite e direção. Um escopo bem definido reduz retrabalho, evita expansão prematura e melhora a qualidade de cada decisão posterior de modelagem, testes e documentação.

## Resultado esperado ao final do projeto

Ao término do projeto, espera-se um repositório em que a arquitetura e a documentação caminhem juntas, permitindo que a jornada dos dados seja compreendida desde a origem até os produtos analíticos finais. O resultado esperado inclui uma camada Gold forte o suficiente para sustentar análises sobre comportamento do pedido, desempenho logístico, sellers, categorias, geografia e experiência final do cliente, sem perder coerência com a lógica das camadas anteriores.

## Decisão formal deste documento

Fica registrado que o escopo do projeto consiste em construir uma solução de engenharia de dados aplicada à base Olist, organizada em arquitetura Medallion, orientada a valor analítico e materializada em um repositório profissional. O foco inicial recairá sobre ingestão, tratamento, modelagem e consumo analítico, mantendo fora do escopo, neste momento, expansões enterprise completas e implantação produtiva de machine learning.
