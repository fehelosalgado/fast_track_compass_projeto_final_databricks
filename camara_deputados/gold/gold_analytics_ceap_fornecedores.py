# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("workspace.gold.fato_despesas")

# ==========================================
# BASE FORNECEDORES
# ==========================================

stats_forn = (

    df.groupBy("nm_fornecedor")

    .agg(

        F.count("*").alias("qtd"),
        F.sum("vl_liquido").alias("total"),
        F.avg("vl_liquido").alias("ticket_medio")

    )

)

# ==========================================
# SCORE SIMPLES DE SUSPEITA
# ==========================================

stats_forn = stats_forn.withColumn(
    "score_suspeita",
    F.col("qtd") * F.col("ticket_medio")
)

# ==========================================
# RANKING
# ==========================================

df_forn_final = stats_forn.orderBy(
    F.desc("score_suspeita")
)

df_forn_final.write.mode("overwrite").saveAsTable(
    "workspace.gold.analytics_ceap_fornecedores"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.analytics_ceap_fornecedores
