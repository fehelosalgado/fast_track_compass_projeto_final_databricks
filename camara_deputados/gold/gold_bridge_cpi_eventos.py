# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

df = spark.table(
    "workspace.silver.cpi_eventos"
)

# COMMAND ----------

bridge = (

    df.select(

        "id_evento",

        "id_orgao",
        "nome_orgao",

        "descricao",
        "descricaoTipo",

        "dt_inicio",
        "dt_fim",

        "situacao",

        "localCamara",
        "localExterno",

        "urlRegistro"

    )

)

# COMMAND ----------

bridge = (

    bridge

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

bridge = bridge.dropDuplicates()

# COMMAND ----------

(

    bridge.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        "workspace.gold.bridge_cpi_eventos"
    )

)

print(
    "Tabela criada: workspace.gold.bridge_cpi_eventos"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC *
# MAGIC from
# MAGIC workspace.gold.bridge_cpi_eventos
