# Databricks notebook source
endpoint = "frentes_detalhes"

table_name = endpoint

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

from datetime import datetime

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

# MAGIC %run ../utils/delta_writer

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

# ------------------------------------------
# CONVERSÃO COLUNAS COMPLEXAS
# ------------------------------------------

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

# ------------------------------------------
# TIMESTAMP PROCESSAMENTO
# ------------------------------------------

df = (
    df
    .withColumn(
        "dt_processamento",
        F.current_timestamp()
    )
)

# ------------------------------------------
# REMOVE DUPLICADOS
# ------------------------------------------

df = df.dropDuplicates()

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
