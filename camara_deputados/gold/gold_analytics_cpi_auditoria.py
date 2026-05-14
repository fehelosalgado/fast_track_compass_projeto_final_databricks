# Databricks notebook source
# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

df = spark.table(
    "workspace.gold.dim_cpi"
)

# COMMAND ----------

# ==========================================
# CLASSIFICAÇÃO EFICIÊNCIA
# ==========================================

df = (

    df

    .withColumn(

        "classificacao_cpi",

        F.when(
            F.col("score_produtividade") >= 1,
            "Alta"
        )

        .when(
            F.col("score_produtividade") >= 0.3,
            "Média"
        )

        .otherwise("Baixa")

    )

)

# COMMAND ----------

# ==========================================
# INDICADOR CONCLUSÃO
# ==========================================

df = (

    df

    .withColumn(

        "flag_concluida",

        F.when(
            F.col("dt_fim_cpi").isNotNull(),
            1
        ).otherwise(0)

    )

)

# COMMAND ----------

# ==========================================
# SCORE FINAL
# ==========================================

df = (

    df

    .withColumn(

        "score_final",

        F.round(

            (
                F.col("score_produtividade") * 0.7
            )
            +
            (
                (1 - F.col("flag_excedeu_prazo")) * 0.3
            ),

            4

        )

    )

)

# COMMAND ----------

# ==========================================
# ORDENAÇÃO FINAL
# ==========================================

df_final = df.orderBy(
    F.desc("score_final")
)

# COMMAND ----------

# ==========================================
# AUDITORIA
# ==========================================

df_final = (

    df_final

    .withColumn(
        "dt_criacao_analytics",
        F.current_timestamp()
    )

)

# COMMAND ----------

# ==========================================
# ESCRITA GOLD
# ==========================================

(

    df_final.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        "workspace.gold.analytics_cpi_auditoria"
    )

)

print(
    "Tabela criada: workspace.gold.analytics_cpi_auditoria"
)

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=f"gold.analytics_cpi_auditoria",
    endpoint=None,
    status="SUCCESS",
    record_count=df_final.count(),
    error_message=None
)

# COMMAND ----------

display(df_final)
