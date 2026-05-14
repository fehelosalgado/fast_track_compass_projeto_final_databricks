# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.metadata;
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.audit;

# COMMAND ----------

# ==========================================
# WIDGETS
# ==========================================

dbutils.widgets.text(
    "endpoint",
    ""
)

endpoint = dbutils.widgets.get(
    "endpoint"
)

table_name = endpoint

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.types import *

from datetime import datetime

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager
# MAGIC

# COMMAND ----------

# ==========================================
# CAMINHOS
# ==========================================

bronze_path = (
    f"/Volumes/workspace/default/"
    f"camara_deputados/bronze/{endpoint}"
)

silver_table = (
    f"workspace.silver.{table_name}"
)

print(f"Bronze path: {bronze_path}")
print(f"Silver table: {silver_table}")

# COMMAND ----------

# ==========================================
# LEITURA BRONZE
# ==========================================

df = (
    spark.read
    .format("delta")
    .load(bronze_path)
)

display(df)

# COMMAND ----------

# ==========================================
# TRANSFORMAÇÕES SILVER
# ==========================================

# converte colunas complexas para string
for field in df.schema.fields:

    field_type = str(field.dataType)

    if (
        "MapType" in field_type
        or "ArrayType" in field_type
        or "StructType" in field_type
    ):

        df = df.withColumn(
            field.name,
            F.to_json(
                F.col(field.name)
            )
        )

# remove duplicados
df = df.dropDuplicates()

# adiciona data processamento
df = (
    df
    .withColumn(
        "dt_processamento",
        F.current_timestamp()
    )
)

# COMMAND ----------

# ==========================================
# VALIDAÇÃO DATAFRAME VAZIO
# ==========================================

record_count = df.count()

print(
    f"Registros processados: {record_count}"
)

if record_count == 0:

    log_info(
        f"Nenhum dado para processar: {endpoint}"
    )

    dbutils.notebook.exit(
        "Sem dados"
    )

# COMMAND ----------

# ==========================================
# ESCRITA SILVER
# ==========================================

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(
    f"Tabela Silver salva: {silver_table}"
)


# COMMAND ----------

# ==========================================
# REGISTRO METADATA
# ==========================================

register_execution(
    table_name=f"silver.{table_name}",
    endpoint=endpoint,
    status="SUCCESS",
    record_count=record_count,
    error_message=None
)


# COMMAND ----------

# ==========================================
# FINALIZAÇÃO
# ==========================================

log_info(
    f"Processamento Silver finalizado: {endpoint}"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC min(dataHoraFim),
# MAGIC min(dataHoraInicio)
# MAGIC from
# MAGIC workspace.silver.eventos
