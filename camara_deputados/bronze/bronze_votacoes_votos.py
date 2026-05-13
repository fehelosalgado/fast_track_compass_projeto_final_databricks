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

# MAGIC %run ../utils/delta_writer

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
# CONFIG TABELA
# ==========================================

endpoint = "votacoes_votos"

table_name = f"bronze_{endpoint}"

print(f"Endpoint: {endpoint}")
print(f"Tabela: {table_name}")

# COMMAND ----------

# ==========================================
# LEITURA VOTAÇÕES
# ==========================================

df_votacoes = spark.table(
    "workspace.silver.votacoes"
)

display(df_votacoes)

# COMMAND ----------

# ==========================================
# LISTA IDs VOTAÇÕES
# ==========================================

lista_votacoes = [

    row["id"]

    for row in df_votacoes.select(
        "id"
    ).distinct().collect()

]

print(
    f"Quantidade votações: {len(lista_votacoes)}"
)

# COMMAND ----------

# ==========================================
# DOWNLOAD VOTOS
# ==========================================

dados = []

cont = 0

for idx, id_votacao in enumerate(lista_votacoes):

    print(
        f"[{idx+1}/{len(lista_votacoes)}] "
        f"Consultando votação {id_votacao}"
    )

    try:

        url = (
            f"{BASE_URL}/votacoes/"
            f"{id_votacao}/votos"
        )

        response = requests.get(
            url,
            headers=HEADERS,
            timeout=30
        )

        response.raise_for_status()

        response_json = response.json()

        votos = response_json.get(
            "dados",
            []
        )

        for voto in votos:

            dados.append({

                "id_votacao": id_votacao,

                "payload_json": json.dumps(
                    voto,
                    ensure_ascii=False
                )

            })

        cont = cont + 1
        
        if cont > 10:
            break

    except Exception as e:

        print(
            f"Erro votação {id_votacao}: {str(e)}"
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
        "Nenhum voto retornado."
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

write_delta(
    df=df,
    path=bronze_path,
    mode="overwrite"
)

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
        "/Volumes/workspace/default/camara_deputados/bronze/votacoes_votos"
    )

)
