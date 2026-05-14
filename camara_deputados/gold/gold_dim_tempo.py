# Databricks notebook source
from pyspark.sql import functions as F

df_tempo = (

    spark.range(1)

    .select(

        F.explode(

            F.sequence(

                F.to_date(
                    F.lit("2010-01-01")
                ),

                F.to_date(
                    F.lit("2035-12-31")
                ),

                F.expr("interval 1 day")

            )

        ).alias("data")

    )

)

# COMMAND ----------

df_tempo = (

    df_tempo

    .withColumn(
        "sk_data",
        F.date_format(
            "data",
            "yyyyMMdd"
        ).cast("int")
    )

    .withColumn(
        "ano",
        F.year("data")
    )

    .withColumn(
        "mes",
        F.month("data")
    )

    .withColumn(
        "nome_mes",
        F.date_format(
            "data",
            "MMMM"
        )
    )

    .withColumn(
        "trimestre",
        F.quarter("data")
    )

    .withColumn(
        "semana_ano",
        F.weekofyear("data")
    )

    .withColumn(
        "dia_mes",
        F.dayofmonth("data")
    )

    .withColumn(
        "dia_semana",
        F.dayofweek("data")
    )

    .withColumn(
        "nome_dia_semana",
        F.date_format(
            "data",
            "EEEE"
        )
    )

    .withColumn(

        "flag_final_semana",

        F.when(
            F.dayofweek("data").isin([1,7]),
            1
        ).otherwise(0)

    )

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

display(df_tempo)

# COMMAND ----------

(
    df_tempo
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.dim_tempo"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.gold.dim_tempo
# MAGIC ZORDER BY (
# MAGIC     data,
# MAGIC     ano,
# MAGIC     mes
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.gold.dim_tempo
# MAGIC LIMIT 20
