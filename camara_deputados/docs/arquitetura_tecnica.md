# Arquitetura Técnica da Solução

# Arquitetura Geral

A solução foi construída utilizando arquitetura medalhão (Bronze, Silver e Gold), organizada sobre Delta Lake no Databricks.

Fluxo principal:

```text
API Câmara
    ↓
Bronze
    ↓
Silver
    ↓
Gold
    ↓
Analytics
```

---

# Camada Bronze

Responsável pela ingestão dos dados brutos da API.

Características implementadas:

- paginação automática
- ingestão incremental
- armazenamento em Delta
- replay de processamento
- persistência em Volumes

Exemplo de endpoints utilizados:

- deputados
- partidos
- eventos
- votacoes
- frentes
- despesas
- membros de frentes

---

# Camada Silver

Responsável pela padronização e transformação dos dados.

Transformações aplicadas:

- serialização JSON
- normalização de estruturas complexas
- tratamento de tipos
- deduplicação
- enriquecimento temporal

Também foram implementados:

- dt_processamento
- validações de consistência
- padronização de schemas

---

# Camada Gold

Camada analítica e dimensional.

Modelagem implementada:

## Dimensões

- dim_deputado
- dim_partido
- dim_frente
- dim_tempo
- dim_cpi

## Fatos

- fato_eventos
- fato_votacoes
- fato_despesas

## Bridges

- bridge_frente_deputado
- bridge_evento_votacao
- bridge_cpi_eventos

## Analytics

- analytics_frentes
- analytics_engajamento
- analytics_alinhamento_votacoes
- analytics_despesas_anomalias
- analytics_cpi_auditoria

---

# Estratégia de Ingestão Incremental

Foi implementada ingestão incremental para endpoints com alta volumetria.

Estratégias utilizadas:

- controle por data
- controle por ID
- paginação incremental
- replay completo

A metadata de execução é persistida em tabela dedicada.

---

# Governança e Auditoria

A solução implementa rastreabilidade ponta a ponta utilizando:

- dt_ingestao
- dt_processamento
- dt_criacao_gold
- metadata_ingestion_control
- logs estruturados

---

# Detecção de Anomalias

Foi implementado pipeline analítico de anomalias para despesas parlamentares utilizando:

- z-score por categoria
- agrupamento por UF
- score estatístico
- classificação de outliers

---

# Resiliência e Recuperação

Mecanismos implementados:

- try/except
- retries em workflows
- replay completo de ingestão
- reprocessamento por camada
- overwrite controlado

---

# Orquestração

A automação foi implementada utilizando Databricks Workflows.

Estrutura:

```text
job_master_pipeline
    ↓
job_bronze_pipeline
    ↓
job_silver_pipeline
    ↓
job_gold_pipeline
```

---

# Versionamento

Todo o código foi versionado em GitHub integrado ao Databricks Repos.

Benefícios:
- controle de versão
- rastreabilidade
- recuperação
- organização modular