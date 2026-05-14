# Databricks notebook source
import requests
import time

# COMMAND ----------

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

# COMMAND ----------

def build_incremental_params(
    endpoint,
    last_execution=None
):

    params = {}

    # endpoints com grande volume
    endpoints_incrementais = [
        "eventos",
        "proposicoes",
        "votacoes",
    ]

    # carga incremental
    if (
        endpoint in endpoints_incrementais
        and last_execution is not None
    ):

        params["dataInicio"] = (
            last_execution.strftime("%Y-%m-%d")
        )
        #params["dataInicio"] = "2023-05-01"
        #params["dataFim"] = "2024-01-01"

    # primeira carga
    elif endpoint in endpoints_incrementais:

        params["dataInicio"] = "2026-01-01"

    return params

# COMMAND ----------

def get_all_pages(
    endpoint,
    params=None
):

    all_data = []

    url = f"{BASE_URL}/{endpoint}"

    while url:

        response = requests.get(
            url,
            params=params
        )

        response.raise_for_status()

        data = response.json()

        registros = data.get("dados", [])

        all_data.extend(registros)

        print(
            f"Página carregada - {len(registros)} registros"
        )

        next_url = None

        for link in data.get("links", []):

            if link.get("rel") == "next":

                next_url = link.get("href")

        url = next_url

        params = None

        time.sleep(0.2)

    return all_data

# COMMAND ----------

# MAGIC %skip
# MAGIC from datetime import datetime
# MAGIC
# MAGIC params = build_incremental_params(
# MAGIC     datetime.now()
# MAGIC )
# MAGIC
# MAGIC print(params)

# COMMAND ----------

# MAGIC %skip
# MAGIC dados = get_all_pages(
# MAGIC     "eventos",
# MAGIC     params={
# MAGIC         "dataInicio": "2025-01-01"
# MAGIC     }
# MAGIC )
# MAGIC
# MAGIC print(len(dados))
