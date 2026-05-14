# Databricks notebook source
# ==========================================
# CONFIGURAÇÃO
# ==========================================

endpoint = "cpi_eventos"
table_name = "cpi_eventos"

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

# ==========================================
# LEITURA SILVER EVENTOS
# ==========================================

df = spark.table("workspace.silver.eventos")

# COMMAND ----------

# ==========================================
# CONVERSÃO ORGAOS (STRING → ARRAY STRUCT)
# ==========================================

df = df.withColumn(
    "orgaos_struct",
    F.from_json(
        F.col("orgaos"),
        "array<struct<id:string,uri:string,sigla:string,nome:string,codTipoOrgao:string,tipoOrgao:string>>"
    )
)

# COMMAND ----------

# ==========================================
# EXPLODE ORGAOS
# ==========================================

df = df.withColumn(
    "orgao",
    F.explode_outer("orgaos_struct")
)

# COMMAND ----------

# ==========================================
# FILTRO CPI (REGRA DE NEGÓCIO)
# ==========================================
# CPI identificada via codTipoOrgao

df_cpi = df.filter(
    F.col("orgao.codTipoOrgao").isin("20", "4")
)

# COMMAND ----------

# ==========================================
# SELEÇÃO FINAL CPI
# ==========================================

df_cpi = df_cpi.select(

    F.col("id").alias("id_evento"),

    "descricao",
    "descricaoTipo",

    # período do evento (novo schema correto)
    F.to_timestamp("dataHoraInicio").alias("dt_inicio"),
    F.to_timestamp("dataHoraFim").alias("dt_fim"),

    "situacao",
    "localCamara",
    "localExterno",

    "uri",
    "urlRegistro",

    # órgão
    F.col("orgao.id").alias("id_orgao"),
    F.col("orgao.nome").alias("nome_orgao"),
    F.col("orgao.sigla").alias("sigla_orgao"),
    F.col("orgao.codTipoOrgao").alias("cod_tipo_orgao"),

    "dt_ingestao"

)

# COMMAND ----------

# ==========================================
# TIMESTAMP PROCESSAMENTO
# ==========================================

df_cpi = df_cpi.withColumn(
    "dt_processamento",
    F.current_timestamp()
)

# COMMAND ----------

# ==========================================
# DEDUPLICAÇÃO
# ==========================================

df_cpi = df_cpi.dropDuplicates()

# COMMAND ----------

# ==========================================
# VALIDAÇÃO
# ==========================================

record_count = df_cpi.count()

print(f"Registros CPI: {record_count}")

if record_count == 0:
    log_info("Nenhum dado CPI encontrado")
    dbutils.notebook.exit("Sem dados")

# COMMAND ----------

# ==========================================
# ESCRITA SILVER
# ==========================================

(
    df_cpi.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"workspace.silver.{table_name}")
)

print(f"Tabela criada: workspace.silver.{table_name}")

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

log_info("Silver CPI finalizada com schema correto")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM workspace.silver.cpi_eventos
# MAGIC where 0=0
