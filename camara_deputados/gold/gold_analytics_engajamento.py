# Databricks notebook source
# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

from pyspark.sql import functions as F

df_eventos = spark.table(
    "workspace.gold.fato_eventos"
)

df_votacoes = spark.table(
    "workspace.gold.fato_votacoes"
)

df_orgaos = spark.table(
    "workspace.gold.dim_orgao"
)

# COMMAND ----------

eventos_orgao = (

    df_eventos

    .groupBy("id_orgao")

    .agg(

        F.countDistinct(
            "sk_evento"
        ).alias("qt_eventos"),

        F.countDistinct(
            F.to_date("dt_inicio")
        ).alias("qt_dias_evento")

    )

)

# COMMAND ----------

votacoes_orgao = (

    df_votacoes

    .groupBy("id_orgao")

    .agg(

        F.countDistinct(
            "sk_votacao"
        ).alias("qt_votacoes")

    )

)

# COMMAND ----------

metricas_eventos = (

    df_eventos

    .agg(

        F.countDistinct(
            "id_evento"
        ).alias("qt_eventos"),

        F.countDistinct(
            F.to_date("dt_inicio")
        ).alias("qt_dias_evento")

    )

)

metricas_votacoes = (

    df_votacoes

    .agg(

        F.countDistinct(
            "sk_votacao"
        ).alias("qt_votacoes")

    )

)

df_analytics = (

    metricas_eventos

    .crossJoin(
        metricas_votacoes
    )

    .withColumn(

        "score_engajamento",

        (
            F.col("qt_eventos") * 1.0
            +
            F.col("qt_votacoes") * 2.0
            +
            F.col("qt_dias_evento") * 0.5
        )

    )

    .withColumn(
        "dt_processamento",
        F.current_timestamp()
    )

)

# COMMAND ----------

display(df_analytics)

# COMMAND ----------

(
    df_analytics
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.analytics_engajamento"
    )
)

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=f"gold.analytics_engajamento",
    endpoint=None,
    status="SUCCESS",
    record_count=df_analytics.count(),
    error_message=None
)
