# Databricks notebook source
# ==========================================
# WIDGETS
# ==========================================

endpoint = "frentes_membros"

table_name = endpoint

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager

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

print(bronze_path)
print(silver_table)

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
# TRANSFORMAÇÕES
# ==========================================

df = (

    df

    .dropDuplicates()

    .withColumn(
        "dt_processamento",
        F.current_timestamp()
    )

)

# COMMAND ----------

# ==========================================
# CONTAGEM
# ==========================================

record_count = df.count()

print(
    f"Registros processados: {record_count}"
)

if record_count == 0:

    raise Exception(
        "Nenhum dado para processar."
    )

# COMMAND ----------

# ==========================================
# ESCRITA SILVER
# ==========================================

(

    df.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(silver_table)

)

print(
    f"Tabela criada: {silver_table}"
)

# COMMAND ----------

# ==========================================
# METADATA
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
    f"Silver finalizada: {table_name}"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.silver.frentes_membros
# MAGIC LIMIT 100
