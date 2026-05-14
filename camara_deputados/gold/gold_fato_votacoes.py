# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table(
    "workspace.silver.votacoes"
)

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

df_gold = (

    df

    .withColumn(
        "sk_votacao",
        F.col("id")
    )

    .withColumn(
        "dt_votacao",
        F.to_date("data")
    )

    .withColumn(
        "dt_hora_registro",
        F.to_timestamp("dataHoraRegistro")
    )

    .withColumn(
        "id_evento",
        F.regexp_extract(
            F.col("uriEvento"),
            r'(\d+)$',
            1
        ).cast("long")
    )

    .withColumn(
        "id_orgao",
        F.regexp_extract(
            F.col("uriOrgao"),
            r'(\d+)$',
            1
        ).cast("long")
    )

    .select(

        "sk_votacao",

        "dt_votacao",

        "dt_hora_registro",

        "descricao",

        "aprovacao",

        "proposicaoObjeto",

        "siglaOrgao",

        "id_evento",

        "id_orgao",

        "uri",

        "uriEvento",

        "uriOrgao",

        "uriProposicaoObjeto",

        "dt_ingestao",

        "dt_processamento"

    )

    .dropDuplicates()

)

display(df_gold)

# COMMAND ----------

(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.fato_votacoes"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE workspace.gold.fato_votacoes
# MAGIC ZORDER BY (
# MAGIC     dt_votacao,
# MAGIC     id_orgao
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.gold.fato_votacoes
# MAGIC LIMIT 20
