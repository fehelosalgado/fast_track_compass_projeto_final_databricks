# Databricks notebook source
from pyspark.sql import functions as F

df_votacoes = spark.table(
    "workspace.gold.fato_votacoes"
)

df_eventos = spark.table(
    "workspace.gold.fato_eventos"
)

# COMMAND ----------

df_bridge = (

    df_votacoes

    .filter(
        F.col("id_evento").isNotNull()
    )

    .select(
        F.col("id_evento"),
        F.col("sk_votacao"),
        F.col("dt_votacao"),
        F.col("id_orgao")
    )

    .dropDuplicates()

)

# COMMAND ----------

display(df_bridge)

# COMMAND ----------

(
    df_bridge
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.bridge_evento_votacao"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.gold.bridge_evento_votacao
# MAGIC ZORDER BY (
# MAGIC     id_evento,
# MAGIC     sk_votacao
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.gold.bridge_evento_votacao
# MAGIC LIMIT 20
