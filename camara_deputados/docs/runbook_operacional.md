# Runbook Operacional

# Objetivo

Este documento descreve procedimentos operacionais, recuperação de falhas e estratégias de reprocessamento da plataforma analítica legislativa.

---

# Estrutura Operacional

A solução é executada via Databricks Workflows.

Jobs:

- job_bronze_pipeline
- job_silver_pipeline
- job_gold_pipeline
- job_master_pipeline

---

# Fluxo Operacional

```text
Bronze
    ↓
Silver
    ↓
Gold
```

---

# Estratégias de Recuperação

## Falha na Bronze

Ações:
- validar disponibilidade da API
- verificar paginação
- executar replay do endpoint
- reprocessar somente endpoint afetado

---

## Falha na Silver

Ações:
- validar schema da Bronze
- validar transformação JSON
- reprocessar tabela específica

---

## Falha na Gold

Ações:
- validar dimensões dependentes
- validar joins
- validar consistência temporal
- reprocessar analytics específicos

---

# Estratégia de Replay

A arquitetura permite replay completo das cargas.

Estratégias:

- overwrite controlado
- reload por endpoint
- reprocessamento isolado por camada

---

# Logs e Auditoria

Os pipelines registram:

- horário de execução
- status
- quantidade de registros
- mensagens de erro

Tabela utilizada:

```text
workspace.default.metadata_ingestion_control
```

---

# Dependências

## Bronze

Sem dependências internas.

## Silver

Dependente da Bronze correspondente.

## Gold

Dependente de:
- Silver
- dimensões
- bridges

---

# Estratégia de Retry

Configuração recomendada:

- retries: 2
- retry interval: 5 minutos

---

# Estratégia de Escalabilidade

A solução foi construída utilizando:

- PySpark nativo
- Delta Lake
- processamento distribuído
- arquitetura modular

---

# Boas Práticas Implementadas

- separação por camadas
- modularização
- versionamento Git
- notebooks reutilizáveis
- parametrização por widgets
- controle de metadata
- observabilidade operacional