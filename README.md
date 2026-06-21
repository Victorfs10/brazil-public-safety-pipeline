# 🔍 Brazil Public Safety Pipeline

A end-to-end data engineering project that collects, processes, and visualizes Brazilian public safety data from **SINESP** (National Public Security Information System), built on **Databricks** with a **Medallion Architecture** and a public **Metabase** dashboard.

📊 **[Live Dashboard](https://dash.arzatech.online/public/dashboard/8ccad566-20ad-47b8-885b-a4a56b7e0265)**

---

## 🏗️ Architecture

```
SINESP (Raw Excel files)
        │
        ▼
┌───────────────────────────────────────────────┐
│                  DATABRICKS                   │
│                                               │
│  RAW → BRONZE → SILVER → GOLD                │
│         (Delta Lake + Unity Catalog)          │
│                                               │
│  Dimensions:  dim_municipio, dim_tempo        │
│  Facts:       fato_homicidios_municipal       │
│               fato_ocorrencias_uf             │
│               fato_vitimas_uf                 │
└───────────────────────────────────────────────┘
        │
        ▼
  Metabase (Docker on VPS)
        │
        ▼
  Public Dashboard (HTTPS)
```

---

## 📦 Data Sources

All data is publicly available from the Brazilian Ministry of Justice via [dados.gov.br](https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/dados-nacionais-1).

| File | Period | Metric | Granularity |
|------|--------|--------|-------------|
| `indicadoressegurancapublicamunic.xlsx` | 2018–2022 | Victims | Municipality |
| `indicadoressegurancapublicauf.xlsx` | 2015–2022 | Occurrences + Victims | State (UF) |

---

## 🗂️ Medallion Architecture

### RAW
Original `.xlsx` files stored as-is in a Databricks Volume:
```
/Volumes/workspace/seguranca_publica/raw/
├── indicadoressegurancapublicamunic.xlsx
└── indicadoressegurancapublicauf.xlsx
```

### BRONZE
Raw data ingested into Delta tables with no transformations:
- `bronze_municipal` — 294,706 records
- `bronze_estado_ocorrencias` — 23,020 records
- `bronze_estado_vitimas` — 19,748 records

### SILVER
Cleaned and standardized data:
- Renamed columns to snake_case
- Standardized date formats (`mes_ano` as date type)
- Converted Portuguese month names to numbers
- Removed duplicates and null values
- Uppercased and trimmed string fields

### GOLD
Dimensional model (Star Schema) ready for BI consumption:

| Table | Type | Records | Description |
|-------|------|---------|-------------|
| `dim_municipio` | Dimension | 5,606 | Brazilian municipalities with IBGE code |
| `dim_tempo` | Dimension | 133 | Time dimension with month, quarter, semester |
| `fato_homicidios_municipal` | Fact | 75,609 | Intentional homicide victims by municipality |
| `fato_ocorrencias_uf` | Fact | 23,020 | Crime occurrences by state and type |
| `fato_vitimas_uf` | Fact | 19,748 | Victims by state, crime type and gender |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Processing | Databricks (Serverless) |
| Storage | Delta Lake + Unity Catalog |
| Language | Python (PySpark + Pandas) |
| BI Tool | Metabase |
| Infrastructure | Docker on Ubuntu VPS |
| SSL/Proxy | Nginx + Certbot (Let's Encrypt) |

---

## 📊 Dashboard Visualizations

- **Intentional homicide victims per year** — Brazil (2018–2022)
- **Ranking of states by homicide victims** — Brazil (2018–2022)
- **Top 10 municipalities with most victims** — homicide (2018–2022)
- **Vehicle theft and robbery trends** — Brazil (2015–2022)
- **Total occurrences by crime type** — Brazil (2015–2022)

---

## 📁 Repository Structure

```
brazil-public-safety-pipeline/
├── notebooks/
│   ├── bronze_script.py      # Raw ingestion into Delta Bronze tables
│   ├── silver_script.py      # Data cleaning and standardization
│   └── gold_script.py        # Dimensional modeling (Star Schema)
├── metabase/
│   └── docker-compose.yml    # Metabase deployment
└── README.md
```

---

## 🚀 How to Reproduce

### Prerequisites
- Databricks account (Free Edition works)
- Docker + Docker Compose
- VPS or local machine for Metabase

### Steps

**1. Download the data**

Download the files from [SINESP](https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/seguranca-publica/estatistica/dados-nacionais-1) and upload them to your Databricks Volume:
```
/Volumes/workspace/seguranca_publica/raw/
```

**2. Run the notebooks in order**
```
bronze_script.py → silver_script.py → gold_script.py
```

**3. Deploy Metabase**
```bash
cd metabase
docker compose up -d
```

**4. Connect Metabase to Databricks**

Use the SQL Warehouse connection details from your Databricks workspace:
- Host: your Databricks hostname
- HTTP Path: your SQL Warehouse HTTP path
- Token: your Personal Access Token
- Default Catalog: `workspace`

---

## 📌 Key Insights from the Data

- Brazil recorded ~49,000 intentional homicide victims in 2018, dropping to ~40,000 in 2019
- **Bahia (BA)** leads in homicide victims (26,455), followed by Rio de Janeiro (RJ) and Pernambuco (PE)
- **Salvador** is the municipality with the most homicide victims over the period
- Vehicle theft and robbery both declined significantly between 2018 and 2020, likely influenced by the COVID-19 pandemic
- Homicide rates show **no significant seasonality** throughout the year in Brazil

---

## 📄 Data Dictionary

### Municipal File (Homicide victims by municipality)
| Column | Type | Description |
|--------|------|-------------|
| cod_ibge | integer | IBGE municipality code |
| municipio | string | Municipality name |
| sigla_uf | string | State abbreviation |
| regiao | string | Geographic region |
| mes_ano | date | Reference month/year |
| vitimas | integer | Number of homicide victims |

### State File — Occurrences
| Column | Type | Description |
|--------|------|-------------|
| uf | string | State name |
| tipo_crime | string | Crime type |
| ano | integer | Year |
| mes | integer | Month number |
| ocorrencias | integer | Number of registered occurrences |

### State File — Victims
| Column | Type | Description |
|--------|------|-------------|
| uf | string | State name |
| tipo_crime | string | Crime type |
| sexo_vitima | string | Victim gender |
| ano | integer | Year |
| mes | integer | Month number |
| vitimas | integer | Number of victims |

---

## ⚠️ Data Limitations

- Municipal data covers only **intentional homicides** (homicídio doloso) — other crime types are only available at state level
- Data collected before December 2018 (Portaria MJSP nº 229/2018) may not follow the current standardization
- Totals by state may not match the sum of municipalities due to different collection and validation processes by state managers

---

## 👤 Author

**Victor Silva**
- LinkedIn: [linkedin.com/in/victor-francasilva](https://linkedin.com/in/victor-franca-silva)
- Portfolio: [victorfs10.github.io](https://victorfs10.github.io)

---

## 📜 License

This project is licensed under the MIT License. The data is publicly available and provided by the Brazilian Ministry of Justice.
