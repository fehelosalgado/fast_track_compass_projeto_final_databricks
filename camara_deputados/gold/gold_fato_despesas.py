# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F

# COMMAND ----------

# ==========================================
# LEITURA SILVER
# ==========================================

df = spark.table(
    "workspace.silver.deputados_despesas"
)

display(df)

# COMMAND ----------

# ==========================================
# LEITURA DIM_DEPUTADO
# ==========================================

dim_deputado = spark.table(
    "workspace.gold.dim_deputado"
)

# COMMAND ----------

# ==========================================
# LEITURA DIM_PARTIDO
# ==========================================

dim_partido = spark.table(
    "workspace.gold.dim_partido"
)

# COMMAND ----------

# ==========================================
# LEITURA DIM_TEMPO
# ==========================================

dim_tempo = spark.table(
    "workspace.gold.dim_tempo"
)

# COMMAND ----------

# ==========================================
# JOIN DIM_DEPUTADO
# ==========================================

df_fato = (

    df.alias("f")

    .join(

        dim_deputado.alias("d"),

        F.col("f.id_deputado") == F.col("d.id_deputado"),

        "left"

    )

    .select(

        F.col("f.*"),

        F.col("d.sk_deputado"),

        F.col("d.sigla_partido").alias("sg_partido"),

        F.col("d.sigla_uf")

    )

)

display(df_fato)

# COMMAND ----------

# ==========================================
# JOIN DIM_PARTIDO
# ==========================================

df_fato = (

    df_fato.alias("f")

    .join(

        dim_partido.alias("p"),

        F.col("f.sg_partido")
        ==
        F.col("p.sigla"),

        "left"

    )

    .select(

        F.col("f.*"),

        F.col("p.id_partido")

    )

)

# COMMAND ----------

# ==========================================
# DATA REFERÊNCIA
# ==========================================

df_fato = (

    df_fato

    .withColumn(

        "dt_referencia",

        F.to_date(

            F.concat_ws(

                "-",

                F.col("ano"),

                F.lpad(
                    F.col("mes"),
                    2,
                    "0"
                ),

                F.lit("01")

            )

        )

    )

)

# COMMAND ----------

# ==========================================
# JOIN DIM_TEMPO
# ==========================================

df_fato = (

    df_fato.alias("f")

    .join(

        dim_tempo.alias("t"),

        F.col("f.dt_referencia")
        ==
        F.col("t.data"),

        "left"

    )

    .select(

        F.col("f.*"),

        F.col("t.sk_data")

    )

)

# COMMAND ----------

# ==========================================
# COLUNAS AUDITORIA
# ==========================================

df_fato = (

    df_fato

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

# ==========================================
# DEDUPLICAÇÃO
# ==========================================

df_fato = df_fato.dropDuplicates()

# COMMAND ----------

# ==========================================
# VALIDAÇÃO
# ==========================================

record_count = df_fato.count()

print(
    f"Registros processados: {record_count}"
)

# COMMAND ----------

# ==========================================
# ESCRITA GOLD
# ==========================================

(

    df_fato.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        "workspace.gold.fato_despesas"
    )

)

print(
    "Tabela criada: workspace.gold.fato_despesas"
)

# COMMAND ----------

# ==========================================
# VALIDAÇÕES ANALÍTICAS
# ==========================================

display(

    df_fato

    .groupBy(
        "tipo_despesa"
    )

    .agg(

        F.round(

            F.sum(
                "vl_liquido"
            ),

            2

        ).alias(
            "vl_total"
        )

    )

    .orderBy(
        F.desc("vl_total")
    )

)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.fato_despesas
# MAGIC LIMIT 100
