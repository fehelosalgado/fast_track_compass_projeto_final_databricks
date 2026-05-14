# Databricks notebook source
# ==========================================
# IMPORTS
# ==========================================

import requests
import json

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
# TABELA METADATA
# ==========================================

endpoint = "frentes_membros"

table_name = f"bronze_{endpoint}"

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

# ==========================================
# LEITURA FRENTES
# ==========================================

df_frentes = spark.table(
    "workspace.silver.frentes"
)

display(df_frentes)

# COMMAND ----------

# ==========================================
# LISTA IDS FRENTES
# ==========================================

lista_frentes = [

    row["id"]

    for row in df_frentes.select(
        "id"
    ).distinct().collect()

]

print(
    f"Quantidade frentes: {len(lista_frentes)}"
)

# COMMAND ----------

# ==========================================
# DOWNLOAD MEMBROS
# ==========================================

dados = []

cont = 0

for idx, id_frente in enumerate(lista_frentes):

    print(
        f"[{idx+1}/{len(lista_frentes)}] "
        f"Consultando membros frente {id_frente}"
    )

    try:

        url = (
            f"{BASE_URL}/frentes/"
            f"{id_frente}/membros"
        )

        response = requests.get(
            url,
            headers=HEADERS,
            timeout=30
        )

        response.raise_for_status()

        response_json = response.json()

        membros = response_json.get(
            "dados",
            []
        )

        for membro in membros:

            dados.append({

                "id_frente": id_frente,

                "payload_json": json.dumps(
                    membro,
                    ensure_ascii=False
                )

            })
        
        cont = cont + 1
        
        if cont > 10:

            break            

    except Exception as e:

        print(
            f"Erro frente {id_frente}: {str(e)}"
        )

# COMMAND ----------

# ==========================================
# VALIDAÇÃO
# ==========================================

print(
    f"Total registros: {len(dados)}"
)

# COMMAND ----------

# ==========================================
# DATAFRAME
# ==========================================

if len(dados) == 0:

    raise Exception(
        "Nenhum dado retornado."
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
# REGISTRO EXECUÇÃO
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
        "/Volumes/workspace/default/camara_deputados/bronze/frentes_membros"
    )
)
