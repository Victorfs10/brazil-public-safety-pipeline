# Databricks notebook source
from pyspark.sql.functions import col, quarter, month, year, dayofmonth, concat, lit, lpad, monotonically_increasing_id

# Leitura das tabelas Silver
df_silver_municipal = spark.table("workspace.seguranca_publica.silver_municipal")
df_silver_ocorrencias = spark.table("workspace.seguranca_publica.silver_estado_ocorrencias")
df_silver_vitimas_uf = spark.table("workspace.seguranca_publica.silver_estado_vitimas")

# DIM MUNICIPIO
dim_municipio = (df_silver_municipal
    .select("cod_ibge", "municipio", "sigla_uf", "regiao")
    .dropDuplicates()
    .orderBy("cod_ibge")
)

dim_municipio.write.format("delta").mode("overwrite").saveAsTable("workspace.seguranca_publica.dim_municipio")
print(f"✅ dim_municipio criada: {dim_municipio.count()} municípios")
dim_municipio.show(5)

# COMMAND ----------

from pyspark.sql.functions import when, col

# Corrigir nome_mes para português
dim_tempo_ptbr = dim_tempo.withColumn("nome_mes",
    when(col("mes") == 1, "Janeiro")
    .when(col("mes") == 2, "Fevereiro")
    .when(col("mes") == 3, "Março")
    .when(col("mes") == 4, "Abril")
    .when(col("mes") == 5, "Maio")
    .when(col("mes") == 6, "Junho")
    .when(col("mes") == 7, "Julho")
    .when(col("mes") == 8, "Agosto")
    .when(col("mes") == 9, "Setembro")
    .when(col("mes") == 10, "Outubro")
    .when(col("mes") == 11, "Novembro")
    .when(col("mes") == 12, "Dezembro")
)

dim_tempo_ptbr.write.format("delta").mode("overwrite").saveAsTable("workspace.seguranca_publica.dim_tempo")
print(f"✅ dim_tempo atualizada com nomes em português!")
dim_tempo_ptbr.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

# FATO HOMICIDIOS MUNICIPAL
fato_homicidios_municipal = (df_silver_municipal
    .select("cod_ibge", "mes_ano", "vitimas")
    .filter(col("vitimas") > 0)
)

fato_homicidios_municipal.write.format("delta").mode("overwrite").saveAsTable("workspace.seguranca_publica.fato_homicidios_municipal")
print(f"✅ fato_homicidios_municipal criada: {fato_homicidios_municipal.count()} registros")
fato_homicidios_municipal.show(5)

# COMMAND ----------

# FATO OCORRENCIAS UF
fato_ocorrencias_uf = (df_silver_ocorrencias
    .select("uf", "tipo_crime", "mes_ano", "ano", "mes", "ocorrencias")
)

fato_ocorrencias_uf.write.format("delta").mode("overwrite").saveAsTable("workspace.seguranca_publica.fato_ocorrencias_uf")
print(f"✅ fato_ocorrencias_uf criada: {fato_ocorrencias_uf.count()} registros")

# FATO VITIMAS UF
fato_vitimas_uf = (df_silver_vitimas_uf
    .select("uf", "tipo_crime", "sexo_vitima", "mes_ano", "ano", "mes", "vitimas")
)

fato_vitimas_uf.write.format("delta").mode("overwrite").saveAsTable("workspace.seguranca_publica.fato_vitimas_uf")
print(f"✅ fato_vitimas_uf criada: {fato_vitimas_uf.count()} registros")