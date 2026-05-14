# Databricks notebook source
# Databricks notebook source

def write_delta(
    df,
    path,
    mode="overwrite"
):

    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .save(path)
    )

    print(f"Tabela salva em: {path}")
