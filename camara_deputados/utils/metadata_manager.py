# Databricks notebook source
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType
)

from datetime import datetime

METADATA_TABLE = (
    "workspace.metadata.ingestion_control"
)

# ==========================================
# CRIAR TABELA METADATA
# ==========================================

def create_metadata_table():

    spark.sql(f"""

    CREATE TABLE IF NOT EXISTS
    {METADATA_TABLE}

    (
        execution_id STRING,
        table_name STRING,
        endpoint STRING,
        execution_time TIMESTAMP,
        status STRING,
        record_count INT,
        error_message STRING
    )

    USING DELTA

    """)

    print(
        "Tabela metadata criada/verificada."
    )

# ==========================================
# REGISTRAR EXECUÇÃO
# ==========================================

def register_execution(
    table_name,
    endpoint,
    status,
    record_count,
    error_message
):

    schema = StructType([

        StructField(
            "execution_id",
            StringType(),
            True
        ),

        StructField(
            "table_name",
            StringType(),
            True
        ),

        StructField(
            "endpoint",
            StringType(),
            True
        ),

        StructField(
            "execution_time",
            TimestampType(),
            True
        ),

        StructField(
            "status",
            StringType(),
            True
        ),

        StructField(
            "record_count",
            IntegerType(),
            True
        ),

        StructField(
            "error_message",
            StringType(),
            True
        )

    ])

    data = [(
        str(datetime.now().timestamp()),
        table_name,
        endpoint,
        datetime.now(),
        status,
        int(record_count),
        error_message
    )]

    df = spark.createDataFrame(
        data,
        schema=schema
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(METADATA_TABLE)
    )

    print(
        f"Execução registrada: {table_name}"
    )

# ==========================================
# ÚLTIMA EXECUÇÃO
# ==========================================

def get_last_execution(
    table_name
):

    df = spark.sql(f"""

        SELECT
            MAX(execution_time) as last_execution

        FROM {METADATA_TABLE}

        WHERE table_name = '{table_name}'
        AND status = 'SUCCESS'

    """)

    result = df.collect()[0]

    return result["last_execution"]

# COMMAND ----------

# MAGIC %skip
# MAGIC dbutils.fs.rm(
# MAGIC     "/Volumes/workspace/default/camara_deputados/logs/metadata_ingestion_control",
# MAGIC     recurse=True
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC initialize_metadata_table()

# COMMAND ----------

# MAGIC %skip
# MAGIC display(
# MAGIC     spark.read.format("delta").load(
# MAGIC         "/Volumes/workspace/default/camara_deputados/logs/metadata_ingestion_control"
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %skip
# MAGIC create_metadata_table()
