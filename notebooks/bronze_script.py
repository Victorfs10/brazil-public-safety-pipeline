# Databricks notebook source
# Instalar biblioteca para ler Excel
%pip install openpyxl

# Caminhos dos arquivos raw
RAW_PATH = "/Volumes/workspace/seguranca_publica/raw"
MUNICIPAL_FILE = f"{RAW_PATH}/indicadoressegurancapublicamunic.xlsx"
UF_FILE = f"{RAW_PATH}/indicadoressegurancapublicauf.xlsx"

print("Caminhos definidos com sucesso!")

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import lit, current_timestamp

# Ler todas as abas do arquivo municipal e empilhar
xl_munic = pd.ExcelFile(MUNICIPAL_FILE)
abas = xl_munic.sheet_names

print(f"Total de abas encontradas: {len(abas)}")
print(abas)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

RAW_PATH = "/Volumes/workspace/seguranca_publica/raw"
MUNICIPAL_FILE = f"{RAW_PATH}/indicadoressegurancapublicamunic.xlsx"
UF_FILE = f"{RAW_PATH}/indicadoressegurancapublicauf.xlsx"

print("Caminhos redefinidos com sucesso!")

# COMMAND ----------

import pandas as pd

xl_munic = pd.ExcelFile(MUNICIPAL_FILE)
abas = xl_munic.sheet_names

# Ler todas as abas e empilhar
dfs = []
for aba in abas:
    df = pd.read_excel(MUNICIPAL_FILE, sheet_name=aba)
    df["aba_origem"] = aba  # coluna extra para rastrear de qual aba veio
    dfs.append(df)

df_municipal = pd.concat(dfs, ignore_index=True)

print(f"Total de linhas: {len(df_municipal)}")
print(f"Colunas: {df_municipal.columns.tolist()}")
df_municipal.head()

# COMMAND ----------

# Renomear colunas para formato compatível com Delta
df_municipal = df_municipal.rename(columns={
    "Cód_IBGE": "cod_ibge",
    "Município": "municipio",
    "Sigla UF": "sigla_uf",
    "Região": "regiao",
    "Mês/Ano": "mes_ano",
    "Vítimas": "vitimas",
    "aba_origem": "aba_origem"
})

# Converter para Spark DataFrame
df_spark_municipal = spark.createDataFrame(df_municipal)

# Salvar como tabela Delta na camada Bronze
(df_spark_municipal
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.seguranca_publica.bronze_municipal")
)

print("Tabela bronze_municipal criada com sucesso!")
print(f"Total de registros: {df_spark_municipal.count()}")

# COMMAND ----------

xl_uf = pd.ExcelFile(UF_FILE)
abas_uf = xl_uf.sheet_names

print(f"Total de abas: {len(abas_uf)}")
print(abas_uf)

# COMMAND ----------

# Ler aba Ocorrências
df_ocorrencias = pd.read_excel(UF_FILE, sheet_name="Ocorrências")

print("=== OCORRÊNCIAS ===")
print(f"Total de linhas: {len(df_ocorrencias)}")
print(f"Colunas: {df_ocorrencias.columns.tolist()}")
df_ocorrencias.head()

# COMMAND ----------

# Ler aba Vítimas
df_vitimas_uf = pd.read_excel(UF_FILE, sheet_name="Vítimas")

print("=== VÍTIMAS ===")
print(f"Total de linhas: {len(df_vitimas_uf)}")
print(f"Colunas: {df_vitimas_uf.columns.tolist()}")
df_vitimas_uf.head()

# COMMAND ----------

# Renomear colunas - Ocorrências
df_ocorrencias = df_ocorrencias.rename(columns={
    "UF": "uf",
    "Tipo Crime": "tipo_crime",
    "Ano": "ano",
    "Mês": "mes",
    "Ocorrências": "ocorrencias"
})

# Salvar Bronze Ocorrências
df_spark_ocorrencias = spark.createDataFrame(df_ocorrencias)
(df_spark_ocorrencias
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.seguranca_publica.bronze_estado_ocorrencias")
)
print(f"Tabela bronze_estado_ocorrencias criada! Total de registros: {df_spark_ocorrencias.count()}")

# Renomear colunas - Vítimas
df_vitimas_uf = df_vitimas_uf.rename(columns={
    "UF": "uf",
    "Tipo Crime": "tipo_crime",
    "Ano": "ano",
    "Mês": "mes",
    "Sexo da Vítima": "sexo_vitima",
    "Vítimas": "vitimas"
})

# Salvar Bronze Vítimas
df_spark_vitimas_uf = spark.createDataFrame(df_vitimas_uf)
(df_spark_vitimas_uf
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.seguranca_publica.bronze_estado_vitimas")
)
print(f"Tabela bronze_estado_vitimas criada! Total de registros: {df_spark_vitimas_uf.count()}")
