# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

import requests
import json
import time

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %run ../utils/logger

# COMMAND ----------

# MAGIC %run ../utils/metadata_manager

# COMMAND ----------

# ==========================================
# CONFIG API
# ==========================================

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

HEADERS = {
    "accept": "application/json"
}

# COMMAND ----------

# ==========================================
# CONFIG
# ==========================================

endpoint = "deputados_despesas"

table_name = f"bronze_{endpoint}"

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

# ==========================================
# LEITURA DEPUTADOS
# ==========================================

df_deputados = spark.table(
    "workspace.silver.deputados"
)

lista_deputados = [

    row["id"]

    for row in df_deputados.select(
        "id"
    ).distinct().collect()

]

print(
    f"Deputados encontrados: {len(lista_deputados)}"
)

# COMMAND ----------

# ==========================================
# DOWNLOAD DESPESAS
# ==========================================

dados = []

cont = 0

for idx, id_deputado in enumerate(lista_deputados):

    cont = cont + 1

    if cont > 10:
        break

    print(
        f"[{idx+1}/{len(lista_deputados)}] "
        f"Deputado {id_deputado}"
    )

    pagina = 1

    while True:

        try:

            url = (
                f"{BASE_URL}/deputados/"
                f"{id_deputado}/despesas"
            )

            params = {

                "pagina": pagina,
                "itens": 100

            }

            response = requests.get(
                url,
                headers=HEADERS,
                params=params,
                timeout=60
            )

            response.raise_for_status()

            response_json = response.json()

            despesas = response_json.get(
                "dados",
                []
            )

            if len(despesas) == 0:

                break

            for despesa in despesas:

                dados.append({

                    "id_deputado": id_deputado,

                    "payload_json": json.dumps(
                        despesa,
                        ensure_ascii=False
                    )

                })

            print(
                f"Página {pagina} - "
                f"{len(despesas)} registros"
            )

            pagina += 1

            time.sleep(0.2)

        except Exception as e:

            print(
                f"Erro deputado "
                f"{id_deputado}: {str(e)}"
            )

            break

# COMMAND ----------

# ==========================================
# VALIDAÇÃO
# ==========================================

print(
    f"Total despesas: {len(dados)}"
)

# COMMAND ----------

# ==========================================
# DATAFRAME
# ==========================================

if len(dados) == 0:

    raise Exception(
        "Nenhuma despesa retornada."
    )

df = spark.createDataFrame(dados)

df = (

    df

    .withColumn(
        "dt_ingestao",
        F.current_timestamp()
    )

)

display(df)

# COMMAND ----------

# ==========================================
# PATH BRONZE
# ==========================================

bronze_path = (
    f"/Volumes/workspace/default/"
    f"camara_deputados/bronze/{endpoint}"
)

print(bronze_path)

# COMMAND ----------

# ==========================================
# WRITE DELTA
# ==========================================

(
    df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(bronze_path)
)

print(f"Tabela salva em: {bronze_path}")

# COMMAND ----------

# ==========================================
# METADATA
# ==========================================

register_execution(
    table_name=table_name,
    endpoint=endpoint,
    status="SUCCESS",
    record_count=df.count(),
    error_message=None
)

# COMMAND ----------

# ==========================================
# FINALIZAÇÃO
# ==========================================

log_info(
    f"Ingestão finalizada: {table_name}"
)

# COMMAND ----------

display(

    spark.read

    .format("delta")

    .load(
        "/Volumes/workspace/default/camara_deputados/bronze/deputados_despesas"
    )

)
