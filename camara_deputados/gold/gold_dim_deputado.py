# Databricks notebook source
# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

# Imports

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from datetime import datetime

# COMMAND ----------

# Leitura Silver

df = spark.table(
    "workspace.silver.deputados"
)

display(df)

# COMMAND ----------

# Transformações

df_gold = (

    df

    .select(
        F.col("id").alias("id_deputado"),

        F.col("nome"),

        F.col("siglaPartido")
            .alias("sigla_partido"),

        F.col("siglaUf")
            .alias("sigla_uf"),

        F.col("email"),

        F.col("urlFoto")
            .alias("url_foto"),

        F.col("idLegislatura")
            .alias("id_legislatura")
    )

    .dropDuplicates()

)

# COMMAND ----------

# Surrogate Key

window_spec = Window.orderBy(
    "id_deputado"
)

df_gold = (

    df_gold

    .withColumn(
        "sk_deputado",
        F.row_number().over(window_spec)
    )

)

# COMMAND ----------

# Colunas SCD2

df_gold = (

    df_gold

    .withColumn(
        "dt_inicio_vigencia",
        F.current_timestamp()
    )

    .withColumn(
        "dt_fim_vigencia",
        F.lit(None).cast("timestamp")
    )

    .withColumn(
        "registro_ativo",
        F.lit(True)
    )

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

# Reordenação

df_gold = df_gold.select(

    "sk_deputado",

    "id_deputado",

    "nome",

    "sigla_partido",

    "sigla_uf",

    "email",

    "url_foto",

    "id_legislatura",

    "dt_inicio_vigencia",

    "dt_fim_vigencia",

    "registro_ativo",

    "dt_criacao_gold"

)

# COMMAND ----------

# Escrita Gold

(
    df_gold.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        "workspace.gold.dim_deputado"
    )
)

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=f"gold.dim_deputado",
    endpoint=None,
    status="SUCCESS",
    record_count=df_gold.count(),
    error_message=None
)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE
# MAGIC
# MAGIC OPTIMIZE workspace.gold.dim_deputado;

# COMMAND ----------

# Validação

display(

    spark.table(
        "workspace.gold.dim_deputado"
    )

)
