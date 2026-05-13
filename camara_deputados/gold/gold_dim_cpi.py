# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F

# COMMAND ----------

# ==========================================
# LEITURA SILVER CPI
# ==========================================

df = spark.table(
    "workspace.silver.cpi_eventos"
)

display(df)

# COMMAND ----------

# ==========================================
# DIMENSÃO CPI
# ==========================================

dim_cpi = (

    df

    .groupBy(

        "id_orgao",
        "nome_orgao",
        "sigla_orgao",
        "cod_tipo_orgao"

    )

    .agg(

        F.min("dt_inicio").alias("dt_inicio_cpi"),

        F.max("dt_fim").alias("dt_fim_cpi"),

        F.countDistinct(
            "id_evento"
        ).alias(
            "qtd_eventos"
        )

    )

)

# COMMAND ----------

# ==========================================
# DURAÇÃO CPI
# ==========================================

dim_cpi = (

    dim_cpi

    .withColumn(

        "duracao_dias",

        F.datediff(
            "dt_fim_cpi",
            "dt_inicio_cpi"
        )

    )

)

# COMMAND ----------

# ==========================================
# REGRA PRAZO REGIMENTAL
# ==========================================
# referência simplificada:
# >120 dias = excedeu prazo

dim_cpi = (

    dim_cpi

    .withColumn(

        "flag_excedeu_prazo",

        F.when(
            F.col("duracao_dias") > 120,
            1
        ).otherwise(0)

    )

)

# COMMAND ----------

# ==========================================
# TIPO CPI
# ==========================================

dim_cpi = (

    dim_cpi

    .withColumn(

        "tipo_cpi",

        F.when(
            F.col("cod_tipo_orgao") == "20",
            "CPMI"
        ).otherwise("CPI")

    )

)

# COMMAND ----------

# ==========================================
# SCORE PRODUTIVIDADE
# ==========================================

dim_cpi = (

    dim_cpi

    .withColumn(

        "score_produtividade",

        F.round(

            F.col("qtd_eventos")
            /
            F.when(
                F.col("duracao_dias") <= 0,
                1
            ).otherwise(
                F.col("duracao_dias")
            ),

            4

        )

    )

)

# COMMAND ----------

# ==========================================
# AUDITORIA
# ==========================================

dim_cpi = (

    dim_cpi

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

# ==========================================
# DEDUPLICAÇÃO
# ==========================================

dim_cpi = dim_cpi.dropDuplicates()

# COMMAND ----------

# ==========================================
# VALIDAÇÃO
# ==========================================

record_count = dim_cpi.count()

print(
    f"Registros processados: {record_count}"
)

# COMMAND ----------

# ==========================================
# ESCRITA GOLD
# ==========================================

(

    dim_cpi.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        "workspace.gold.dim_cpi"
    )

)

print(
    "Tabela criada: workspace.gold.dim_cpi"
)

# COMMAND ----------

# ==========================================
# VALIDAÇÃO ANALÍTICA
# ==========================================

display(

    dim_cpi.orderBy(
        F.desc("duracao_dias")
    )

)
