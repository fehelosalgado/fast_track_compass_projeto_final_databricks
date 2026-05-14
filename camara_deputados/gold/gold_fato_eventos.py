# Databricks notebook source
# Imports
from pyspark.sql import functions as F

# COMMAND ----------

# Leitura Silver
df_eventos = spark.table(
    "workspace.silver.eventos"
)

display(df_eventos)

# COMMAND ----------

# Tratamento colunas complexas
# O endpoint eventos possui campos JSON/map.
# Precisamos converter alguns campos.
# Verifique primeiro:
df_eventos.printSchema()

# COMMAND ----------

# Transformações Gold
df_gold = (

    df_eventos

    .select(

        F.col("id").alias("id_evento"),

        F.to_timestamp(
            "dataHoraInicio"
        ).alias("dt_inicio"),

        F.to_timestamp(
            "dataHoraFim"
        ).alias("dt_fim"),

        F.col("situacao"),
        F.col("descricaoTipo"),
        F.col("descricao"),

        F.col("localCamara"),

        F.to_date(
            "dataHoraInicio"
        ).alias("data_evento")

    )

    .withColumn(
        "ano",
        F.year("data_evento")
    )

    .withColumn(
        "mes",
        F.month("data_evento")
    )

    .withColumn(
        "semana_ano",
        F.weekofyear("data_evento")
    )

    .withColumn(
        "flag_evento_futuro",
        F.when(
            F.col("data_evento") > F.current_date(),
            1
        ).otherwise(0)
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

# Contagem
print(
    f"Total registros: {df_gold.count()}"
)

# COMMAND ----------

# Escrita Gold
(
    df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(
        "workspace.gold.fato_eventos"
    )
)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Optimize
# MAGIC OPTIMIZE workspace.gold.fato_eventos;

# COMMAND ----------

# Validação

display(

    spark.table(
        "workspace.gold.fato_eventos"
    )

)
