# Databricks notebook source
# MAGIC %run ../utils/api_client

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/delta_writer

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

import requests
import json
import time

from pyspark.sql import functions as F

# COMMAND ----------

endpoint = "frentes_detalhes"

table_name = f"bronze_{endpoint}"

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

last_execution = get_last_execution(
    table_name
)

print(
    f"Última execução: {last_execution}"
)

# COMMAND ----------

try:

    # ==========================================
    # LOG INÍCIO
    # ==========================================

    log_info(
        f"Iniciando ingestão endpoint: {endpoint}"
    )

    # ==========================================
    # LEITURA IDs FRENTES
    # ==========================================

    df_frentes = spark.table(
        "workspace.silver.frentes"
    )

    ids_frentes = [

        row["id"]

        for row in (
            df_frentes
            .select("id")
            .collect()
        )

    ]

    print(
        f"Quantidade frentes: {len(ids_frentes)}"
    )

    # ==========================================
    # COLETA DETALHES
    # ==========================================

    dados = []

    cont = 0

    for i, id_frente in enumerate(ids_frentes):

        print(
            f"[{i+1}/{len(ids_frentes)}] "
            f"Consultando frente {id_frente}"
        )

        try:

            url = (
                f"{BASE_URL}/frentes/{id_frente}"
            )

            response = requests.get(
                url,
                headers=HEADERS,
                timeout=20
            )

            response.raise_for_status()

            payload = response.json()["dados"]

            dados.append({

                "id_frente": payload.get("id"),

                "payload_json": json.dumps(
                    payload,
                    ensure_ascii=False
                )

            })

            time.sleep(0.2)

            cont = cont + 1

            if cont > 10:

                break

        except Exception as e:

            print(
                f"Erro frente {id_frente}: {e}"
            )

    # ==========================================
    # SEM NOVOS DADOS
    # ==========================================

    if len(dados) == 0:

        log_info(
            f"Nenhum dado encontrado para {endpoint}"
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

    print(bronze_path)

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

    # ==========================================
    # LOG FINAL
    # ==========================================

    log_info(
        f"Ingestão finalizada tabela: {table_name}"
    )

except Exception as e:

    # ==========================================
    # REGISTRO ERRO
    # ==========================================

    register_execution(
        table_name=table_name,
        endpoint=endpoint,
        status="ERROR",
        record_count=0,
        error_message=str(e)
    )

    log_error(str(e))

    raise
