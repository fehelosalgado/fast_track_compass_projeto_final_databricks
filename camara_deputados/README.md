# Plataforma Analítica Legislativa — Câmara dos Deputados (Databricks Lakehouse)

## Visão Geral

Este projeto implementa uma plataforma analítica ponta a ponta utilizando arquitetura Lakehouse Medalhão no Databricks para ingestão, processamento, modelagem dimensional e análise de dados públicos da Câmara dos Deputados.

A solução foi construída utilizando PySpark, Delta Lake e Databricks Workflows, com foco em escalabilidade, governança, rastreabilidade, reprocessamento e análises analíticas avançadas.

Fonte oficial dos dados:

https://dadosabertos.camara.leg.br/swagger/api.html

---

# Objetivos do Projeto

A solução contempla pipelines especializados para:

- Atlas de Frentes Parlamentares
- Calendário Analítico de Eventos Legislativos
- Correlação entre Frentes e Votações
- Raio-X de Gastos da CEAP
- Pipeline de Auditoria de CPIs
- Monitor de Presença e Engajamento Parlamentar

---

# Arquitetura da Solução

O projeto segue arquitetura medalhão.

## Bronze

Camada de ingestão bruta dos dados da API da Câmara.

Características:
- ingestão incremental
- paginação
- persistência em Delta Lake
- replay e reprocessamento

## Silver

Camada de padronização e transformação.

Características:
- normalização de schemas
- remoção de duplicidades
- serialização de estruturas complexas
- enriquecimento de dados

## Gold

Camada analítica e dimensional.

Características:
- modelagem fato/dimensão
- tabelas bridge
- métricas analíticas
- detecção de anomalias
- indicadores legislativos

---

# Tecnologias Utilizadas

| Tecnologia | Finalidade |
|---|---|
| Databricks | Plataforma principal |
| PySpark | Processamento distribuído |
| Delta Lake | Armazenamento Lakehouse |
| GitHub | Versionamento |
| Databricks Workflows | Orquestração |
| REST API | Ingestão dos dados da Câmara |

---

# Estrutura do Repositório

```text
bronze/
silver/
gold/
utils/
docs/
```

---

# Principais Entregas Implementadas

## Atlas de Frentes Parlamentares

- relacionamento entre frentes e deputados
- diversidade partidária
- cruzamento entre partidos e UFs
- analytics de participação parlamentar

## Eventos Legislativos

- calendário consolidado de eventos
- modelagem dimensional de eventos
- cruzamento entre presença e votações

## CEAP — Gastos Parlamentares

- ingestão incremental
- modelagem dimensional
- score de anomalia via z-score
- ranking de fornecedores

## Pipeline de Auditoria de CPIs

- identificação de CPIs via órgãos legislativos
- timeline de eventos
- analytics de duração e produtividade

## Engajamento Parlamentar

- score composto de presença e votação
- análise temporal de atividade parlamentar

---

# Orquestração

Os pipelines foram automatizados utilizando Databricks Workflows.

Estrutura:

```text
job_master_pipeline
    ├── job_setting_environment
    ├── job_bronze_pipeline
    ├── job_silver_pipeline
    └── job_gold_pipeline
```

---

# Governança e Auditoria

A solução implementa:

- dt_ingestao
- dt_processamento
- dt_criacao_gold
- controle de execução
- logs estruturados
- metadata de processamento

---

# Atendimento aos Requisitos da Ementa

| Requisito | Status |
|---|---|
| Arquitetura Medalhão | ✅ |
| Modelagem Dimensional | ✅ |
| Ingestão Incremental | ✅ |
| Detecção de Anomalias | ✅ |
| Pipelines Automatizados | ✅ |
| Governança e Auditoria | ✅ |
| Resiliência e Reprocessamento | ✅ |
| Relacionamento entre Tabelas | ✅ |
| Pipeline de CPIs | ✅ |
| Analytics Legislativos | ✅ |

---

# Considerações Finais

A solução foi desenvolvida com foco em boas práticas de Engenharia de Dados, utilizando recursos nativos do PySpark e arquitetura escalável baseada em Lakehouse.

O projeto contempla ingestão, transformação, modelagem analítica, observabilidade, versionamento e automação operacional de pipelines legislativos.