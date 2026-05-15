# Databricks notebook source
import requests
import time

# COMMAND ----------

BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

# COMMAND ----------

def build_incremental_params(
    endpoint,
    dataInicio=None,
    dataFim=None,
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
        and dataInicio is not None
        and dataFim is not None
        #and last_execution is not None        
    ):

        params["dataInicio"] = dataInicio
        params["dataFim"] = dataFim

        #params["dataInicio"] = (
            #last_execution.strftime("%Y-%m-%d")
        #)        

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
