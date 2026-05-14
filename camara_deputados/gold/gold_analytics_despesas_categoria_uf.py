# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("workspace.gold.fato_despesas")

# ==========================================
# BASE ESTATÍSTICA
# ==========================================

stats = (

    df.groupBy(
        "tipo_despesa",
        "sigla_uf"
    )

    .agg(

        F.avg("vl_liquido").alias("media"),
        F.stddev("vl_liquido").alias("desvio")

    )

)

# ==========================================
# JOIN
# ==========================================

df_cat = df.join(
    stats,
    ["tipo_despesa", "sigla_uf"],
    "left"
)

# ==========================================
# Z-SCORE
# ==========================================

df_cat = df_cat.withColumn(
    "zscore_categoria_uf",
    F.when(
        (F.col("desvio").isNull()) | (F.col("desvio") == 0),
        0
    ).otherwise(
        (F.col("vl_liquido") - F.col("media")) / F.col("desvio")
    )
)

# ==========================================
# FLAG
# ==========================================

df_cat = df_cat.withColumn(
    "flag_anomalia",
    F.when(F.abs(F.col("zscore_categoria_uf")) >= 3, 1).otherwise(0)
)

# ==========================================
# AGREGADO FINAL
# ==========================================

df_cat_final = (

    df_cat.groupBy(
        "tipo_despesa",
        "sigla_uf"
    )

    .agg(

        F.sum("flag_anomalia").alias("qtd_anomalias"),
        F.avg("zscore_categoria_uf").alias("score_medio")

    )

    .orderBy(F.desc("qtd_anomalias"))

)

df_cat_final.write.mode("overwrite").saveAsTable(
    "workspace.gold.analytics_ceap_categoria_uf"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.analytics_ceap_categoria_uf
