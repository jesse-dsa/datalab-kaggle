# olist_ecommerce

Projeto de engenharia de dados orientado ao domínio de e-commerce, utilizando a base Olist como caso central para organização de pipelines, transformação analítica e construção de ativos confiáveis sobre arquitetura medalhão. A proposta deste projeto é servir como base profissional para estudo, portfólio e evolução prática de um fluxo de dados moderno inspirado em Databricks, PySpark e Delta Lake.

## objetivo do projeto

Estruturar um caso técnico completo de dados para o contexto de marketplace e operações de pedidos, cobrindo desde a preparação do ambiente até a geração de entidades analíticas e a abertura para modelos preditivos. A base Olist oferece um cenário muito útil para isso, pois reúne informações de clientes, pedidos, pagamentos, itens, avaliações, vendedores e geolocalização, permitindo trabalhar problemas reais de engenharia e analytics em e-commerce.

## jornada prevista do dado

A organização do projeto segue a lógica da arquitetura medalhão, com separação clara entre as etapas de persistência, refinamento e consumo analítico.

### Bronze

A camada Bronze recebe os dados brutos e preserva a relação com a origem. O objetivo aqui é garantir rastreabilidade, repetibilidade de ingestão e um ponto de partida confiável para as etapas seguintes. Nessa fase, o foco está em leitura, ingestão, marcação de origem, padronização mínima estrutural e armazenamento inicial.

### Silver

A camada Silver transforma o dado bruto em estruturas mais consistentes e utilizáveis. É onde acontecem limpeza, tratamento de nulos, correções de tipos, deduplicação, regras de negócio intermediárias, junções relevantes e enriquecimentos necessários para formar tabelas confiáveis. Essa camada sustenta a qualidade do projeto e reduz a complexidade de uso para consumo analítico.

### Gold

A camada Gold organiza o dado em ativos finais voltados a análise e decisão. Aqui entram fatos, dimensões, métricas de negócio, agregações e visões de consumo voltadas a perguntas relevantes do mercado de e-commerce. O objetivo é deixar o dado pronto para painéis, indicadores, exploração executiva e futuras aplicações de ciência de dados.

## estrutura interna

### `notebooks/`

Trilha principal do projeto. A numeração foi desenhada para comunicar a sequência lógica da engenharia de dados.

- `00_setup/`: preparação do ambiente, definição de caminhos, parâmetros, catálogos e configurações iniciais.
- `01_bronze/`: ingestão e persistência do dado bruto.
- `02_silver/`: limpeza, padronização, enriquecimento e modelagem intermediária.
- `03_gold/`: construção de tabelas analíticas e métricas finais.
- `04_analytics/`: exploração orientada a negócio, consultas analíticas e validação de hipóteses.
- `05_ml/`: espaço reservado para experimentos futuros de machine learning, como previsão de cancelamento, atraso, churn ou segmentação.

### `sql/`

Área para consultas analíticas, validações, explorações complementares e scripts auxiliares de apoio. Essa pasta existe como componente funcional do projeto e ajuda a separar lógica SQL reutilizável da narrativa operacional dos notebooks.

### `tests/`

Espaço destinado a verificações de qualidade, regras de consistência, checagens de completude, duplicidade, conformidade de chaves e critérios de confiança sobre os dados tratados. A camada de testes reforça que o projeto não é apenas exploratório, mas orientado a disciplina de engenharia.

### `outputs/`

Destino para saídas geradas pelo projeto, como exportações, snapshots, relatórios, tabelas auxiliares, imagens e resultados produzidos durante as análises. Essa separação evita misturar código com artefatos derivados.

### `metadata/`

Reúne definições auxiliares do projeto, como manifestos, contratos de camada, convenções, mapeamentos de datasets e descrições estruturais. É a área que ajuda a tornar o projeto legível, governável e mais fácil de evoluir.

## papel da base Olist neste contexto

A base Olist é tratada aqui como um caso de engenharia analítica aplicada. O interesse não está apenas em carregar arquivos, mas em estruturar uma jornada do dado capaz de responder perguntas relevantes sobre operação, fulfillment, comportamento de compra, pagamentos, qualidade de entrega e performance comercial. Isso torna o projeto especialmente útil para demonstrar domínio técnico com aderência ao mundo real de e-commerce.

## direção de evolução

A estrutura atual já nasce preparada para crescer sem perda de clareza. O caminho natural de evolução inclui ingestões mais robustas, versionamento de dados em Delta, testes automatizados, documentação técnica complementar, indicadores executivos e experimentos de machine learning apoiados em features derivadas das camadas analíticas.

Em outras palavras, este não é um repositório montado apenas para armazenar arquivos. É uma base de trabalho organizada para sustentar evolução técnica real, comunicação profissional e demonstração consistente de método em engenharia de dados.
