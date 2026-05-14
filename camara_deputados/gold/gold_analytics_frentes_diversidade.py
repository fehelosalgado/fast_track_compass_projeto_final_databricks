# Databricks notebook source
# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

from pyspark.sql import functions as F

df_frente = (
    spark.table("workspace.gold.dim_frente")
)

df_dep = (
    spark.table("workspace.gold.dim_deputado")
)

# COMMAND ----------

df_base = (

    df_frente.alias("f")

    .join(
        df_dep.alias("d"),
        F.col("f.id_deputado") == F.col("d.id_deputado"),
        "left"
    )

    .groupBy(
        "id_frente",
        "titulo",
        "sigla_partido"
    )

    .agg(
        F.countDistinct(
            "f.id_deputado"
        ).alias("qtd_deputados")
    )

)

display(df_base)

# COMMAND ----------

df_total = (

    df_base

    .groupBy(
        "id_frente"
    )

    .agg(
        F.sum(
            "qtd_deputados"
        ).alias("total_frente")
    )

)

# COMMAND ----------

df_share = (

    df_base.alias("b")

    .join(
        df_total.alias("t"),
        "id_frente"
    )

    .withColumn(
        "share_partido",
        F.col("qtd_deputados") /
        F.col("total_frente")
    )

    .withColumn(
        "share_quadrado",
        F.pow(
            F.col("share_partido"),
            2
        )
    )

)

display(df_share)

# COMMAND ----------

df_hhi = (

    df_share

    .groupBy(
        "id_frente",
        "titulo"
    )

    .agg(

        F.sum(
            "share_quadrado"
        ).alias("indice_hhi"),

        F.countDistinct(
            "sigla_partido"
        ).alias("qtd_partidos"),

        F.sum(
            "qtd_deputados"
        ).alias("qtd_deputados")

    )

    .withColumn(

        "nivel_diversidade",

        F.when(
            F.col("indice_hhi") >= 0.25,
            "BAIXA"
        )

        .when(
            F.col("indice_hhi") >= 0.15,
            "MEDIA"
        )

        .otherwise("ALTA")

    )

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

display(
    df_hhi.orderBy("indice_hhi")
)

# COMMAND ----------

(
    df_hhi

    .write

    .format("delta")

    .mode("overwrite")

    .saveAsTable(
        "workspace.gold.analytics_frentes_diversidade"
    )

)

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=f"gold.analytics_frentes_diversidade",
    endpoint=None,
    status="SUCCESS",
    record_count=df_hhi.count(),
    error_message=None
)
