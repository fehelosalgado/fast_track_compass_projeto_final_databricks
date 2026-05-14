# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("workspace.gold.fato_despesas")

# ==========================================
# AGREGADO MENSAL
# ==========================================

df_mensal = (

    df.groupBy(
        "sg_partido",
        "ano",
        "mes"
    )

    .agg(

        F.sum("vl_liquido").alias("total_gasto"),
        F.count("*").alias("qtd_despesas"),
        F.avg("vl_liquido").alias("ticket_medio")

    )

)

# ==========================================
# RANKING
# ==========================================

df_mensal = df_mensal.orderBy(
    F.desc("total_gasto")
)

df_mensal.write.mode("overwrite").saveAsTable(
    "workspace.gold.analytics_ceap_mensal_partido"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.analytics_ceap_mensal_partido
