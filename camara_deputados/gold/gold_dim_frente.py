# Databricks notebook source
# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

df = spark.table(
    "workspace.silver.frentes"
)

display(df)

# COMMAND ----------

from pyspark.sql import functions as F

df_gold = (

    df

    .select(
        F.col("id").alias("sk_frente"),
        F.col("titulo").alias("nm_frente"),
        F.col("idLegislatura").alias("id_legislatura"),
        F.col("uri").alias("url_api"),
        F.col("dt_ingestao"),
        F.col("dt_processamento")
    )

    .dropDuplicates()

)

# COMMAND ----------

(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.dim_frente"
    )
)

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=f"gold.dim_frente",
    endpoint=None,
    status="SUCCESS",
    record_count=df_gold.count(),
    error_message=None
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.gold.dim_frente
# MAGIC LIMIT 20
