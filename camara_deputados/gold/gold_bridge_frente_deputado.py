# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# ==========================================
# LEITURA SILVER
# ==========================================

df_frentes = spark.table(
    "workspace.silver.frentes"
)

df_membros = spark.table(
    "workspace.silver.frentes_membros"
)

# COMMAND ----------

# ==========================================
# SCHEMA MEMBRO
# ==========================================

schema_membro = StructType([

    StructField(
        "id",
        LongType(),
        True
    ),

    StructField(
        "uri",
        StringType(),
        True
    ),

    StructField(
        "nome",
        StringType(),
        True
    ),

    StructField(
        "siglaPartido",
        StringType(),
        True
    ),

    StructField(
        "uriPartido",
        StringType(),
        True
    ),

    StructField(
        "siglaUf",
        StringType(),
        True
    ),

    StructField(
        "idLegislatura",
        LongType(),
        True
    ),

    StructField(
        "urlFoto",
        StringType(),
        True
    ),

    StructField(
        "email",
        StringType(),
        True
    )

])

# COMMAND ----------

# ==========================================
# PARSE JSON MEMBROS
# ==========================================

df_membros_parse = (

    df_membros

    .withColumn(

        "json_data",

        F.from_json(
            F.col("payload_json"),
            schema_membro
        )

    )

)

display(df_membros_parse)

# COMMAND ----------

# ==========================================
# NORMALIZAÇÃO
# ==========================================

df_bridge = (

    df_membros_parse

    .select(

        F.col("id_frente"),

        F.col("json_data.id").alias(
            "id_deputado"
        ),

        F.col("json_data.nome").alias(
            "nm_deputado"
        ),

        F.col("json_data.siglaPartido").alias(
            "sg_partido"
        ),

        F.col("json_data.siglaUf").alias(
            "sg_uf"
        ),

        F.col("json_data.idLegislatura").alias(
            "id_legislatura"
        ),

        F.col("json_data.email").alias(
            "email"
        )

    )

)

display(df_bridge)

# COMMAND ----------

# ==========================================
# JOIN FRENTES
# ==========================================

df_bridge = (

    df_bridge.alias("m")

    .join(

        df_frentes.alias("f"),

        F.col("m.id_frente")
        ==
        F.col("f.id"),

        "left"

    )

    .select(

        F.col("m.*"),

        F.col("f.titulo").alias(
            "nm_frente"
        )

    )

)

# COMMAND ----------

# ==========================================
# JOIN DIM_DEPUTADO
# ==========================================

dim_deputado = spark.table(
    "workspace.gold.dim_deputado"
)

df_bridge = (

    df_bridge.alias("f")

    .join(

        dim_deputado.alias("d"),

        F.col("f.id_deputado")
        ==
        F.col("d.id_deputado"),

        "left"

    )

    .select(

        F.col("f.*"),

        F.col("d.sk_deputado")

    )

)

# COMMAND ----------

# ==========================================
# JOIN DIM_PARTIDO
# ==========================================

dim_partido = spark.table(
    "workspace.gold.dim_partido"
)

df_bridge = (

    df_bridge.alias("f")

    .join(

        dim_partido.alias("p"),

        F.col("f.sg_partido")
        ==
        F.col("p.sigla"),

        "left"

    )

    .select(

        F.col("f.*"),

        F.col("p.id_partido").alias(
            "id_partido"
        )

    )

)

# COMMAND ----------

# ==========================================
# AUDITORIA
# ==========================================

df_bridge = (

    df_bridge

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

    .dropDuplicates()

)

# COMMAND ----------

# ==========================================
# CONTAGEM
# ==========================================

record_count = df_bridge.count()

print(
    f"Registros processados: {record_count}"
)

# COMMAND ----------

# ==========================================
# ESCRITA GOLD
# ==========================================

(

    df_bridge.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        "workspace.gold.bridge_frente_deputado"
    )

)

print(
    "Tabela criada: workspace.gold.bridge_frente_deputado"
)

# COMMAND ----------

# ==========================================
# VALIDAÇÕES ANALÍTICAS
# ==========================================

display(

    df_bridge

    .groupBy(
        "nm_deputado"
    )

    .agg(

        F.countDistinct(
            "id_frente"
        ).alias(
            "qtd_frentes"
        )

    )

    .orderBy(
        F.desc("qtd_frentes")
    )

)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.bridge_frente_deputado
# MAGIC LIMIT 100
