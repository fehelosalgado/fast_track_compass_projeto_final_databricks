# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# ==========================================
# LEITURA GOLD
# ==========================================

df = spark.table(
    "workspace.gold.bridge_frente_deputado"
)

display(df)

# COMMAND ----------

# ==========================================
# TOTAL DE DEPUTADOS POR FRENTE
# ==========================================

df_total = (

    df

    .groupBy(

        "id_frente",
        "nm_frente"

    )

    .agg(

        F.countDistinct(
            "id_deputado"
        ).alias(
            "qtd_deputados"
        ),

        F.countDistinct(
            "sg_partido"
        ).alias(
            "qtd_partidos"
        )

    )

)

display(df_total)

# COMMAND ----------

# ==========================================
# DISTRIBUIÇÃO PARTIDÁRIA
# ==========================================

df_partidos = (

    df

    .groupBy(

        "id_frente",
        "sg_partido"

    )

    .agg(

        F.countDistinct(
            "id_deputado"
        ).alias(
            "qtd_dep_partido"
        )

    )

)

display(df_partidos)

# COMMAND ----------

# ==========================================
# JOIN TOTAL
# ==========================================

df_hhi = (

    df_partidos.alias("p")

    .join(

        df_total.alias("t"),

        F.col("p.id_frente")
        ==
        F.col("t.id_frente"),

        "left"

    )

    .select(

        F.col("p.id_frente"),

        F.col("t.nm_frente"),

        F.col("p.sg_partido"),

        F.col("p.qtd_dep_partido"),

        F.col("t.qtd_deputados"),

        F.col("t.qtd_partidos")

    )

)

display(df_hhi)

# COMMAND ----------

# ==========================================
# CÁLCULO PARTICIPAÇÃO
# ==========================================

df_hhi = (

    df_hhi

    .withColumn(

        "share_partido",

        F.col("qtd_dep_partido")
        /
        F.col("qtd_deputados")

    )

    .withColumn(

        "share_quadrado",

        F.pow(
            F.col("share_partido"),
            2
        )

    )

)

display(df_hhi)

# COMMAND ----------

# ==========================================
# ÍNDICE HERFINDAHL
# ==========================================

df_hhi_final = (

    df_hhi

    .groupBy(

        "id_frente",
        "nm_frente",
        "qtd_deputados",
        "qtd_partidos"

    )

    .agg(

        F.round(

            F.sum(
                "share_quadrado"
            ),

            4

        ).alias(
            "indice_herfindahl"
        )

    )

)

display(df_hhi_final)

# COMMAND ----------

# ==========================================
# CLASSIFICAÇÃO DIVERSIDADE
# ==========================================

df_hhi_final = (

    df_hhi_final

    .withColumn(

        "classificacao_diversidade",

        F.when(

            F.col("indice_herfindahl") >= 0.25,

            "Baixa diversidade"

        )

        .when(

            F.col("indice_herfindahl") >= 0.15,

            "Diversidade moderada"

        )

        .otherwise(

            "Alta diversidade"

        )

    )

)

display(df_hhi_final)

# COMMAND ----------

# ==========================================
# DEPUTADOS EM MAIS FRENTES
# ==========================================

df_deputados = (

    df

    .groupBy(

        "id_deputado",
        "nm_deputado",
        "sg_partido",
        "sg_uf"

    )

    .agg(

        F.countDistinct(
            "id_frente"
        ).alias(
            "qtd_frentes"
        )

    )

)

display(

    df_deputados.orderBy(
        F.desc("qtd_frentes")
    )

)

# COMMAND ----------

# ==========================================
# OVERLAP ENTRE FRENTES
# ==========================================

df_overlap = (

    df.alias("a")

    .join(

        df.alias("b"),

        (

            F.col("a.id_deputado")
            ==
            F.col("b.id_deputado")

        )

        &

        (

            F.col("a.id_frente")
            <
            F.col("b.id_frente")

        ),

        "inner"

    )

    .groupBy(

        F.col("a.id_frente").alias(
            "id_frente_a"
        ),

        F.col("a.nm_frente").alias(
            "nm_frente_a"
        ),

        F.col("b.id_frente").alias(
            "id_frente_b"
        ),

        F.col("b.nm_frente").alias(
            "nm_frente_b"
        )

    )

    .agg(

        F.countDistinct(
            F.col("a.id_deputado")
        ).alias(
            "qtd_deputados_comuns"
        )

    )

)

display(

    df_overlap.orderBy(
        F.desc("qtd_deputados_comuns")
    )

)

# COMMAND ----------

# ==========================================
# DATASET ANALÍTICO FINAL
# ==========================================

df_analytics = (

    df_hhi_final

    .withColumn(
        "dt_criacao_gold",
        F.current_timestamp()
    )

)

display(df_analytics)

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
        "workspace.gold.analytics_frentes"
    )

)

print(
    "Tabela criada: workspace.gold.analytics_frentes"
)

# COMMAND ----------

# ==========================================
# VALIDAÇÕES FINAIS
# ==========================================

display(

    spark.table(
        "workspace.gold.analytics_frentes"
    )

    .orderBy(
        F.desc("qtd_deputados")
    )

)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.gold.analytics_frentes
# MAGIC ORDER BY indice_herfindahl DESC
