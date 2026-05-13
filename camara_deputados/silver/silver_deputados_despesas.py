# Databricks notebook source
# ==========================================
# CONFIG
# ==========================================

endpoint = "deputados_despesas"

table_name = endpoint

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

# ==========================================
# IMPORTS
# ==========================================

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

# ==========================================
# CAMINHOS
# ==========================================

bronze_path = (
    f"/Volumes/workspace/default/"
    f"camara_deputados/bronze/{endpoint}"
)

silver_table = (
    f"workspace.silver.{table_name}"
)

print(bronze_path)
print(silver_table)

# COMMAND ----------

# ==========================================
# LEITURA BRONZE
# ==========================================

df = (

    spark.read

    .format("delta")

    .load(bronze_path)

)

display(df)

# COMMAND ----------

# ==========================================
# SCHEMA DESPESA
# ==========================================

schema_despesa = StructType([

    StructField(
        "ano",
        IntegerType(),
        True
    ),

    StructField(
        "mes",
        IntegerType(),
        True
    ),

    StructField(
        "tipoDespesa",
        StringType(),
        True
    ),

    StructField(
        "codDocumento",
        #LongType(),
        StringType(),
        True
    ),

    StructField(
        "tipoDocumento",
        StringType(),
        True
    ),

    StructField(
        "codTipoDocumento",
        LongType(),
        True
    ),

    StructField(
        "dataDocumento",
        StringType(),
        True
    ),

    StructField(
        "numDocumento",
        StringType(),
        True
    ),

    StructField(
        "valorDocumento",
        DoubleType(),
        True
    ),

    StructField(
        "urlDocumento",
        StringType(),
        True
    ),

    StructField(
        "nomeFornecedor",
        StringType(),
        True
    ),

    StructField(
        "cnpjCpfFornecedor",
        StringType(),
        True
    ),

    StructField(
        "valorLiquido",
        DoubleType(),
        True
    ),

    StructField(
        "valorGlosa",
        DoubleType(),
        True
    ),

    StructField(
        "numRessarcimento",
        StringType(),
        True
    ),

    StructField(
        "codLote",
        LongType(),
        True
    ),

    StructField(
        "parcela",
        LongType(),
        True
    )

])

# COMMAND ----------

# ==========================================
# PARSE JSON
# ==========================================

df_parse = (

    df

    .withColumn(

        "json_data",

        F.from_json(
            F.col("payload_json"),
            schema_despesa
        )

    )

)

display(df_parse)

# COMMAND ----------

# ==========================================
# NORMALIZAÇÃO
# ==========================================

df_silver = (

    df_parse

    .select(

        F.col("id_deputado"),

        F.col("json_data.ano").alias(
            "ano"
        ),

        F.col("json_data.mes").alias(
            "mes"
        ),

        F.col("json_data.tipoDespesa").alias(
            "tipo_despesa"
        ),

        F.col("json_data.codDocumento").alias(
            "cod_documento"
        ),

        F.col("json_data.tipoDocumento").alias(
            "tipo_documento"
        ),

        F.col("json_data.codTipoDocumento").alias(
            "cod_tipo_documento"
        ),

        F.to_date(
            F.col("json_data.dataDocumento")
        ).alias(
            "dt_documento"
        ),

        F.col("json_data.numDocumento").alias(
            "num_documento"
        ),

        F.col("json_data.valorDocumento").alias(
            "vl_documento"
        ),

        F.col("json_data.valorLiquido").alias(
            "vl_liquido"
        ),

        F.col("json_data.valorGlosa").alias(
            "vl_glosa"
        ),

        F.col("json_data.nomeFornecedor").alias(
            "nm_fornecedor"
        ),

        F.col("json_data.cnpjCpfFornecedor").alias(
            "nr_cnpj_cpf"
        ),

        F.col("json_data.urlDocumento").alias(
            "url_documento"
        ),

        F.col("json_data.codLote").alias(
            "cod_lote"
        ),

        F.col("json_data.parcela").alias(
            "parcela"
        ),

        F.col("dt_ingestao")

    )

)

# COMMAND ----------

# ==========================================
# DEDUPLICAÇÃO
# ==========================================

df_silver = (

    df_silver

    .dropDuplicates()

    .withColumn(
        "dt_processamento",
        F.current_timestamp()
    )

)

# COMMAND ----------

# ==========================================
# VALIDAÇÃO
# ==========================================

record_count = df_silver.count()

print(
    f"Registros processados: {record_count}"
)

if record_count == 0:

    raise Exception(
        "Nenhuma despesa processada."
    )

# COMMAND ----------

# ==========================================
# ESCRITA SILVER
# ==========================================

(

    df_silver.write

    .format("delta")

    .mode("overwrite")

    .option(
        "overwriteSchema",
        "true"
    )

    .saveAsTable(
        silver_table
    )

)

print(
    f"Tabela criada: {silver_table}"
)

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=f"silver.{table_name}",
    endpoint=endpoint,
    status="SUCCESS",
    record_count=record_count,
    error_message=None
)

# COMMAND ----------

# ==========================================
# FINALIZAÇÃO
# ==========================================

log_info(
    f"Silver finalizada: {table_name}"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.silver.deputados_despesas
# MAGIC where id_deputado = 74171
# MAGIC --and tipo_documento = 4
# MAGIC and num_documento = '466998'
# MAGIC LIMIT 100
