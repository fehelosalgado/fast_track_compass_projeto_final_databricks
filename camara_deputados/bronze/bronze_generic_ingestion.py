# Databricks notebook source
# MAGIC %run ../utils/api_client

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/delta_writer

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

dbutils.widgets.text(
    "endpoint",
    ""
)

# COMMAND ----------

endpoint = dbutils.widgets.get("endpoint")

# COMMAND ----------

table_name = f"bronze_{endpoint}"

# COMMAND ----------

last_execution = get_last_execution(
    table_name
)

print(
    f"Última execução: {last_execution}"
)

# COMMAND ----------

try:

    log_info(
        f"Iniciando ingestão endpoint: {endpoint}"
    )

    params = build_incremental_params(
        endpoint=endpoint,
        last_execution=last_execution
    )    

    dados = get_all_pages(
        endpoint,
        params=params
    )

    # ==========================================
    # TRATAMENTO SEM NOVOS DADOS
    # ==========================================

    if len(dados) == 0:

        log_info(
            f"Nenhum dado novo encontrado para {endpoint}"
        )

        register_execution(
            table_name=table_name,
            endpoint=endpoint,
            status="SUCCESS",
            record_count=0,
            error_message=None
        )

        dbutils.notebook.exit(
            "Sem novos dados."
        )

    # ==========================================
    # DATAFRAME
    # ==========================================

    df = spark.createDataFrame(dados)

    df = (
        df
        .withColumn(
            "dt_ingestao",
            F.current_timestamp()
        )
    )

    display(df)

    # ==========================================
    # PATH BRONZE
    # ==========================================

    bronze_path = (
        f"/Volumes/workspace/default/"
        f"camara_deputados/bronze/{endpoint}"
    )

    # ==========================================
    # WRITE DELTA
    # ==========================================

    write_delta(
        df=df,
        path=bronze_path,
        mode="overwrite"
    )

    # ==========================================
    # REGISTRO EXECUÇÃO
    # ==========================================

    register_execution(
        table_name=table_name,
        endpoint=endpoint,
        status="SUCCESS",
        record_count=df.count(),
        error_message=None
    )

    log_info(
        f"Ingestão finalizada tabela: {table_name}"
    )

except Exception as e:

    register_execution(
        table_name=table_name,
        endpoint=endpoint,
        status="ERROR",
        record_count=0,
        error_message=str(e)
    )

    log_error(str(e))

    raise

# COMMAND ----------

# MAGIC %skip
# MAGIC spark.sql("""
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.default.metadata_ingestion_control
# MAGIC ORDER BY execution_time DESC
# MAGIC
# MAGIC """).display()

# COMMAND ----------

# MAGIC %skip
# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM workspace.default.bronze_deputados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM delta.`/Volumes/workspace/default/camara_deputados/bronze/orgaos`
