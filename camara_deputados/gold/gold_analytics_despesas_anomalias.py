# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F

# COMMAND ----------

# ==========================================
# LEITURA FATOS
# ==========================================

df = spark.table(
    "workspace.gold.fato_despesas"
)

display(df)

# COMMAND ----------

# ==========================================
# ESTATÍSTICAS POR DEPUTADO
# ==========================================

stats_dep = (

    df.groupBy(
        "id_deputado",
        "sg_partido",
        "sigla_uf"
    )

    .agg(

        F.avg("vl_liquido").alias("media_dep"),
        F.stddev("vl_liquido").alias("desvio_dep"),
        F.count("*").alias("qtd_despesas")

    )

)

# COMMAND ----------

# ==========================================
# JOIN (SEM AMBIGUIDADE)
# ==========================================

df_anom = (

    df.alias("f")

    .join(
        stats_dep.alias("s"),
        "id_deputado",
        "left"
    )

    .select(

        F.col("f.id_deputado"),
        F.col("f.sg_partido"),
        F.col("f.sigla_uf"),
        F.col("f.vl_liquido"),
        F.col("f.tipo_despesa"),
        F.col("s.media_dep"),
        F.col("s.desvio_dep"),
        F.col("s.qtd_despesas")

    )

)

# COMMAND ----------

# ==========================================
# Z-SCORE POR DEPUTADO
# ==========================================

df_anom = df_anom.withColumn(
    "zscore_dep",
    F.when(
        (F.col("desvio_dep").isNull()) |
        (F.col("desvio_dep") == 0),
        0
    ).otherwise(
        (F.col("vl_liquido") - F.col("media_dep")) /
        F.col("desvio_dep")
    )
)

# COMMAND ----------

# ==========================================
# FLAG ANOMALIA
# ==========================================

df_anom = df_anom.withColumn(
    "flag_anomalia",
    F.when(F.abs(F.col("zscore_dep")) >= 3, 1).otherwise(0)
)

# COMMAND ----------

# ==========================================
# SCORE AGREGADO POR DEPUTADO
# ==========================================

score_dep = (

    df_anom.groupBy(
        "id_deputado",
        "sg_partido",
        "sigla_uf"
    )

    .agg(

        F.sum("flag_anomalia").alias("qtd_anomalias"),
        F.avg("zscore_dep").alias("zscore_medio"),
        F.sum("vl_liquido").alias("total_gasto"),
        F.first("qtd_despesas").alias("qtd_despesas")

    )

)

# COMMAND ----------

# ==========================================
# SCORE FINAL
# ==========================================

score_dep = score_dep.withColumn(
    "score_anomalia",
    F.col("qtd_anomalias") * F.abs(F.col("zscore_medio"))
)

# COMMAND ----------

# ==========================================
# ORDENAÇÃO FINAL (CORRIGIDA)
# ==========================================

df_final = score_dep.orderBy(
    F.desc("score_anomalia")
)

display(df_final)

# COMMAND ----------

# ==========================================
# ESCRITA GOLD
# ==========================================

(
    df_final.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(
        "workspace.gold.analytics_despesas_anomalias"
    )
)

# COMMAND ----------

# ==========================================
# VALIDAÇÃO RÁPIDA
# ==========================================

display(

    df_final.select(
        "id_deputado",
        "sg_partido",
        "sigla_uf",
        "total_gasto",
        "qtd_anomalias",
        "score_anomalia"
    )
    .orderBy(F.desc("score_anomalia"))

)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.analytics_despesas_anomalias
