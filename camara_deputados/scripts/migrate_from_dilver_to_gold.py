# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.metadata;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.audit;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.default.metadata_ingestion_control
# MAGIC RENAME TO workspace.metadata.ingestion_control;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.default.silver_deputados
# MAGIC RENAME TO workspace.silver.deputados;
# MAGIC
# MAGIC ALTER TABLE workspace.default.silver_partidos
# MAGIC RENAME TO workspace.silver.partidos;
# MAGIC
# MAGIC ALTER TABLE workspace.default.silver_eventos
# MAGIC RENAME TO workspace.silver.eventos;
# MAGIC
# MAGIC ALTER TABLE workspace.default.silver_orgaos
# MAGIC RENAME TO workspace.silver.orgaos;
# MAGIC
# MAGIC ALTER TABLE workspace.default.silver_frentes
# MAGIC RENAME TO workspace.silver.frentes;
# MAGIC
# MAGIC ALTER TABLE workspace.default.silver_proposicoes
# MAGIC RENAME TO workspace.silver.proposicoes;
# MAGIC
# MAGIC ALTER TABLE workspace.default.silver_votacoes
# MAGIC RENAME TO workspace.silver.votacoes;
