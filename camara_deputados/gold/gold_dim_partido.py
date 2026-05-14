# Databricks notebook source
# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Leitura Silver
df = spark.table(
    "workspace.silver.partidos"
)

display(df)

# COMMAND ----------

# Transformações Gold
df_gold = (

    df

    .select(
        F.col("id").alias("id_partido"),
        F.col("sigla"),
        F.col("nome"),
        F.col("uri")
    )

    .dropDuplicates()

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

# Validação
display(df_gold)

# COMMAND ----------

# Escrita Gold
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.dim_partido"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize
# MAGIC OPTIMIZE workspace.gold.dim_partido;
# MAGIC

# COMMAND ----------

# Validação

display(

    spark.table(
        "workspace.gold.dim_partido"
    )

)
