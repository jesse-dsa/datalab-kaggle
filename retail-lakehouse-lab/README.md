# retail-lakehouse-lab

Repositório estruturado para projetos aplicados de engenharia de dados com foco em arquitetura medalhão, organização escalável de ativos analíticos e evolução disciplinada de soluções orientadas a dados. A proposta deste laboratório é servir como base profissional para estudos, portfólio, demonstrações técnicas e construção progressiva de pipelines modernos em ambientes como Databricks, PySpark e Delta Lake.

A raiz do repositório foi desenhada para comunicar claramente a separação entre ativos compartilhados, documentação e projetos específicos. Em vez de tratar cada iniciativa como um conjunto isolado de notebooks e consultas, a estrutura organiza o trabalho como um laboratório de dados que pode crescer com consistência. Isso favorece reaproveitamento, manutenção, padronização e leitura rápida por qualquer pessoa que precise navegar pelo ambiente.

## visão da organização

- `shared/`: área transversal para utilitários, padrões e componentes reutilizáveis entre projetos.
- `projects/`: concentra os casos de uso do laboratório. Cada projeto possui estrutura própria e identidade técnica.
- `docs/`: espaço reservado para documentação complementar, notas arquiteturais, decisões técnicas e materiais de apoio.

O projeto principal neste estágio é `projects/olist_ecommerce`, um caso aplicado sobre a base Olist, organizado para refletir a jornada completa do dado em uma arquitetura Bronze, Silver e Gold.

## racional arquitetural

A estrutura foi pensada para representar um fluxo real de engenharia de dados. O dado nasce em preparação e setup do ambiente, passa por ingestão bruta, refinamento, enriquecimento, modelagem analítica e, por fim, abre espaço para exploração de machine learning. A navegação do repositório permite entender essa sequência mesmo antes da leitura dos notebooks, reforçando a disciplina de organização e a intencionalidade técnica do projeto.

A arquitetura medalhão é o eixo central dessa organização:

- **Bronze** para captura e persistência do dado bruto, com foco em rastreabilidade e fidelidade à origem.
- **Silver** para limpeza, padronização, enriquecimento e modelagem intermediária, preparando o dado para consumo confiável.
- **Gold** para entidades analíticas, métricas de negócio, tabelas finais e ativos orientados à decisão.

Essa divisão não é apenas didática. Ela ajuda a reduzir acoplamento, melhora auditabilidade, facilita testes e cria um caminho sólido para manutenção e escala.

## projeto em destaque: Olist e-commerce

O projeto Olist foi preparado como caso técnico de engenharia analítica aplicada ao contexto de e-commerce. A estrutura interna contempla notebooks por etapa do pipeline, consultas SQL, testes, saídas geradas e metadados do projeto. O objetivo é oferecer uma base pronta para expansão com tabelas Delta, validações de qualidade, indicadores de negócio e futuras camadas preditivas.

## como navegar

1. Comece por `projects/olist_ecommerce/README.md` para entender o objetivo do caso e a lógica da estrutura interna.
2. Em `notebooks/`, siga a progressão numérica para acompanhar a jornada do dado.
3. Use `sql/` para consultas e explorações analíticas complementares.
4. Consulte `tests/` e `metadata/` para enxergar os controles e definições que sustentam o projeto.
5. Verifique `outputs/` para resultados exportados, artefatos e saídas geradas ao longo da evolução do caso.

## princípio de uso

Este laboratório foi organizado para parecer e funcionar como uma base real de trabalho. A intenção não é apenas armazenar arquivos, mas estruturar um ambiente onde cada pasta tenha papel claro dentro do ciclo de engenharia de dados. O repositório já nasce preparado para portfólio, estudo técnico, documentação de arquitetura e evolução contínua.
