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

df_votos = spark.table(
    "workspace.silver.votacoes_votos"
)

df_frentes = spark.table(
    "workspace.gold.bridge_frente_deputado"
)

# COMMAND ----------

# ==========================================
# SCHEMA VOTO
# ==========================================

schema_voto = StructType([

    StructField(
        "id",
        StringType(),
        True
    ),

    StructField(
        "uriVotacao",
        StringType(),
        True
    ),

    StructField(
        "deputado_",
        StructType([

            StructField(
                "id",
                LongType(),
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
                "siglaUf",
                StringType(),
                True
            )

        ]),
        True
    ),

    StructField(
        "tipoVoto",
        StringType(),
        True
    )

])

# COMMAND ----------

# ==========================================
# PARSE JSON
# ==========================================

df_parse = (

    df_votos

    .withColumn(

        "json_data",

        F.from_json(
            F.col("payload_json"),
            schema_voto
        )

    )

)

# COMMAND ----------

# ==========================================
# NORMALIZAÇÃO VOTOS
# ==========================================

df_votos_norm = (

    df_parse

    .select(

        F.col("id_votacao"),

        F.col("json_data.deputado_.id").alias(
            "id_deputado"
        ),

        F.col("json_data.deputado_.nome").alias(
            "nm_deputado"
        ),

        F.col("json_data.tipoVoto").alias(
            "tipo_voto"
        )

    )

)

display(df_votos_norm)

# COMMAND ----------

# ==========================================
# JOIN FRENTES
# ==========================================

df_base = (

    df_votos_norm.alias("v")

    .join(

        df_frentes.alias("f"),

        F.col("v.id_deputado")
        ==
        F.col("f.id_deputado"),

        "inner"

    )

    .select(

        F.col("v.id_votacao"),

        F.col("v.id_deputado"),

        F.col("v.nm_deputado"),

        F.col("v.tipo_voto"),

        F.col("f.id_frente"),

        F.col("f.nm_frente")

    )

)

display(df_base)

# COMMAND ----------

# ==========================================
# SELF JOIN COMPARAÇÃO VOTOS
# ==========================================

df_compare = (

    df_base.alias("a")

    .join(

        df_base.alias("b"),

        (

            F.col("a.id_votacao")
            ==
            F.col("b.id_votacao")

        )

        &

        (

            F.col("a.id_frente")
            ==
            F.col("b.id_frente")

        )

        &

        (

            F.col("a.id_deputado")
            <
            F.col("b.id_deputado")

        ),

        "inner"

    )

)

# COMMAND ----------

# ==========================================
# FLAG ALINHAMENTO
# ==========================================

df_compare = (

    df_compare

    .withColumn(

        "flag_voto_igual",

        F.when(

            F.col("a.tipo_voto")
            ==
            F.col("b.tipo_voto"),

            1

        ).otherwise(0)

    )

)

display(df_compare)

# COMMAND ----------

# ==========================================
# AGREGAÇÃO ANALÍTICA
# ==========================================

df_analytics = (

    df_compare

    .groupBy(

        F.col("a.id_frente").alias(
            "id_frente"
        ),

        F.col("a.nm_frente").alias(
            "nm_frente"
        )

    )

    .agg(

        F.sum(
            "flag_voto_igual"
        ).alias(
            "votos_iguais"
        ),

        F.count("*").alias(
            "votos_comparados"
        )

    )

)

# COMMAND ----------

# ==========================================
# SCORE ALINHAMENTO
# ==========================================

df_analytics = (

    df_analytics

    .withColumn(

        "percentual_alinhamento",

        F.round(

            (
                F.col("votos_iguais")
                /
                F.col("votos_comparados")
            ) * 100,

            2

        )

    )

)

# COMMAND ----------

# ==========================================
# CLASSIFICAÇÃO
# ==========================================

df_analytics = (

    df_analytics

    .withColumn(

        "classificacao_alinhamento",

        F.when(

            F.col("percentual_alinhamento") >= 80,

            "Alto alinhamento"

        )

        .when(

            F.col("percentual_alinhamento") >= 60,

            "Médio alinhamento"

        )

        .otherwise(

            "Baixo alinhamento"

        )

    )

)

# COMMAND ----------

# ==========================================
# AUDITORIA
# ==========================================

df_analytics = (

    df_analytics

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

# COMMAND ----------

# ==========================================
# CONTAGEM
# ==========================================

record_count = df_analytics.count()

print(
    f"Registros processados: {record_count}"
)

# COMMAND ----------

# ==========================================
# ESCRITA GOLD
# ==========================================

(

    df_analytics.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        "workspace.gold.analytics_alinhamento_votacoes"
    )

)

print(
    "Tabela criada: workspace.gold.analytics_alinhamento_votacoes"
)

# COMMAND ----------

# ==========================================
# VALIDAÇÕES
# ==========================================

display(

    df_analytics.orderBy(
        F.desc("percentual_alinhamento")
    )

)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.analytics_alinhamento_votacoes
# MAGIC ORDER BY percentual_alinhamento DESC
