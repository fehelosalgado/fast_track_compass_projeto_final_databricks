# Databricks notebook source
# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Leitura Silver
df = spark.table(
    "workspace.silver.orgaos"
)

display(df)

# COMMAND ----------

# Transformações Gold
df_gold = (

    df

    .select(
        F.col("id").alias("id_orgao"),
        F.col("sigla").alias("sigla_orgao"),
        F.col("nome").alias("nome_orgao"),
        F.col("uri").alias("uri_orgao"),
        F.col("codTipoOrgao")
    )

    .dropDuplicates()

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

# Validação
display(df_gold)

# COMMAND ----------

# Escrita Gold
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.dim_orgao"
    )
)

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=f"gold.dim_orgao",
    endpoint=None,
    status="SUCCESS",
    record_count=df.count(),
    error_message=None
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize
# MAGIC OPTIMIZE workspace.gold.dim_orgao;

# COMMAND ----------

# Validação

display(

    spark.table(
        "workspace.gold.dim_orgao"
    )

)
