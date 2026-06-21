# Databricks notebook source
# Célula 1 - Leitura das tabelas Bronze
df_bronze_municipal = spark.table("workspace.seguranca_publica.bronze_municipal")
df_bronze_ocorrencias = spark.table("workspace.seguranca_publica.bronze_estado_ocorrencias")
df_bronze_vitimas_uf = spark.table("workspace.seguranca_publica.bronze_estado_vitimas")

print(f"Municipal: {df_bronze_municipal.count()} registros")
print(f"Ocorrências UF: {df_bronze_ocorrencias.count()} registros")
print(f"Vítimas UF: {df_bronze_vitimas_uf.count()} registros")

# COMMAND ----------

from pyspark.sql.functions import col, to_date, year, month, upper, trim

# Tratamento Silver - Municipal
df_silver_municipal = (df_bronze_municipal
    .withColumn("cod_ibge", col("cod_ibge").cast("integer"))
    .withColumn("municipio", trim(col("municipio")))
    .withColumn("sigla_uf", upper(trim(col("sigla_uf"))))
    .withColumn("regiao", upper(trim(col("regiao"))))
    .withColumn("mes_ano", to_date(col("mes_ano")))
    .withColumn("ano", year(col("mes_ano")))
    .withColumn("mes", month(col("mes_ano")))
    .withColumn("vitimas", col("vitimas").cast("integer"))
    .drop("aba_origem")  # coluna de rastreamento, não precisa mais
    .dropDuplicates()
    .filter(col("vitimas").isNotNull())
)

print(f"Registros antes: {df_bronze_municipal.count()}")
print(f"Registros depois: {df_silver_municipal.count()}")
df_silver_municipal.show(5)

# COMMAND ----------

from pyspark.sql.functions import lpad, when, lit, concat, to_date, upper, trim, col

# Tratamento Silver - Ocorrências UF
df_silver_ocorrencias = df_bronze_ocorrencias

# Converter mês texto para número
meses_map = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8,
    "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12
}

# Criar coluna mes_num
df_silver_ocorrencias = df_silver_ocorrencias.withColumn("mes_num", lit(None).cast("integer"))

for mes_nome, mes_num in meses_map.items():
    df_silver_ocorrencias = df_silver_ocorrencias.withColumn(
        "mes_num",
        when(col("mes") == mes_nome, mes_num).otherwise(col("mes_num"))
    )

df_silver_ocorrencias = (df_silver_ocorrencias
    .withColumn("uf", upper(trim(col("uf"))))
    .withColumn("tipo_crime", trim(col("tipo_crime")))
    .withColumn("ano", col("ano").cast("integer"))
    .withColumn("ocorrencias", col("ocorrencias").cast("integer"))
    .withColumn("mes_ano", to_date(
        concat(col("ano").cast("string"), lit("-"), lpad(col("mes_num").cast("string"), 2, "0"), lit("-01")),
        "yyyy-MM-dd"
    ))
    .drop("mes")
    .withColumnRenamed("mes_num", "mes")
    .dropDuplicates()
    .filter(col("ocorrencias").isNotNull())
)

print(f"Registros antes: {df_bronze_ocorrencias.count()}")
print(f"Registros depois: {df_silver_ocorrencias.count()}")
df_silver_ocorrencias.show(5)

# COMMAND ----------

# Tratamento Silver - Vítimas UF
df_silver_vitimas_uf = df_bronze_vitimas_uf

# Criar coluna mes_num
df_silver_vitimas_uf = df_silver_vitimas_uf.withColumn("mes_num", lit(None).cast("integer"))

for mes_nome, mes_num in meses_map.items():
    df_silver_vitimas_uf = df_silver_vitimas_uf.withColumn(
        "mes_num",
        when(col("mes") == mes_nome, mes_num).otherwise(col("mes_num"))
    )

df_silver_vitimas_uf = (df_silver_vitimas_uf
    .withColumn("uf", upper(trim(col("uf"))))
    .withColumn("tipo_crime", trim(col("tipo_crime")))
    .withColumn("sexo_vitima", trim(col("sexo_vitima")))
    .withColumn("ano", col("ano").cast("integer"))
    .withColumn("vitimas", col("vitimas").cast("integer"))
    .withColumn("mes_ano", to_date(
        concat(col("ano").cast("string"), lit("-"), lpad(col("mes_num").cast("string"), 2, "0"), lit("-01")),
        "yyyy-MM-dd"
    ))
    .drop("mes")
    .withColumnRenamed("mes_num", "mes")
    .dropDuplicates()
    .filter(col("vitimas").isNotNull())
)

print(f"Registros antes: {df_bronze_vitimas_uf.count()}")
print(f"Registros depois: {df_silver_vitimas_uf.count()}")
df_silver_vitimas_uf.show(5)

# COMMAND ----------

# Salvar Silver Municipal
(df_silver_municipal
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.seguranca_publica.silver_municipal")
)
print(f"✅ silver_municipal salva: {df_silver_municipal.count()} registros")

# Salvar Silver Ocorrências
(df_silver_ocorrencias
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.seguranca_publica.silver_estado_ocorrencias")
)
print(f"✅ silver_estado_ocorrencias salva: {df_silver_ocorrencias.count()} registros")

# Salvar Silver Vítimas UF
(df_silver_vitimas_uf
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.seguranca_publica.silver_estado_vitimas")
)
print(f"✅ silver_estado_vitimas salva: {df_silver_vitimas_uf.count()} registros")