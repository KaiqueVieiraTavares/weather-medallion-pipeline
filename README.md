# Pipeline Meteorológico Multicloud com Arquitetura Medallion

Pipeline batch para ingestão, processamento e análise de dados meteorológicos históricos.

O projeto utiliza a arquitetura Medallion para organizar os dados nas camadas Bronze, Silver e Gold, com armazenamento no Amazon S3 e disponibilização das tabelas analíticas no Google BigQuery.

## Tecnologias

- Python
- PySpark
- Open-Meteo API
- Amazon S3
- Google BigQuery
- SQL
- Streamlit ou Looker Studio

## Objetivo

Construir um pipeline modular e testável capaz de:

- consumir dados meteorológicos da Open-Meteo API;
- armazenar os dados brutos no Amazon S3;
- processar e validar os dados com PySpark;
- organizar os dados nas camadas Bronze, Silver e Gold;
- carregar as tabelas analíticas no BigQuery;
- disponibilizar os resultados para consultas SQL e dashboards.

Os dados possuem granularidade horária e incluem temperatura, sensação térmica, umidade, precipitação, pressão atmosférica, cobertura de nuvens, vento e condição meteorológica.

## Arquitetura

```text
Open-Meteo API
       ↓
     Python
       ↓
Amazon S3 - Bronze
   JSON original
       ↓
     PySpark
       ↓
Amazon S3 - Silver
Parquet limpo e validado
       ↓
     PySpark
       ↓
 Amazon S3 - Gold
Métricas meteorológicas
       ↓
Google BigQuery
       ↓
 SQL + Dashboard
```

## Camadas

### Bronze

Preserva os dados próximos ao formato original recebido da API.

- Armazenamento do JSON original
- Registro da cidade, coordenadas e horário de ingestão
- Persistência local ou no Amazon S3

### Silver

Transforma os dados brutos em registros tabulares confiáveis.

- Normalização do JSON
- Conversão de timestamps e tipos numéricos
- Remoção de registros nulos e duplicados
- Validação de umidade, precipitação, vento e coordenadas
- Inclusão de metadados
- Particionamento por `year`, `month` e `day`
- Armazenamento em Parquet

Chave de deduplicação:

```text
location_id + event_timestamp + source
```

### Gold

Cria tabelas direcionadas a consultas analíticas e dashboards.

- `daily_weather`: métricas diárias por cidade
- `monthly_weather`: métricas mensais por cidade
- `weather_anomalies`: possíveis eventos meteorológicos anormais

Entre as métricas estão temperatura mínima, média e máxima, umidade média, precipitação total, velocidade máxima do vento e períodos com chuva.

## Estrutura do projeto

```text
weather-medallion-pipeline/
├── data/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── sql/
├── src/
│   ├── config.py
│   ├── spark_session.py
│   ├── bronze/
│   │   ├── api_client.py
│   │   ├── writer.py
│   │   └── job.py
│   ├── silver/
│   │   ├── reader.py
│   │   ├── transformations.py
│   │   ├── quality.py
│   │   ├── writer.py
│   │   └── job.py
│   ├── gold/
│   │   ├── reader.py
│   │   ├── transformations.py
│   │   ├── writer.py
│   │   └── job.py
│   └── warehouse/
│       ├── bigquery_client.py
│       └── job.py
├── dashboard/
│   └── app.py
├── tests/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── .env.example
├── .gitignore
├── requirements.txt
├── README.md
└── main.py
```

## Fluxo de processamento

```text
OpenMeteoClient
       ↓
BronzeIngestionJob
       ↓
Amazon S3 Bronze
       ↓
BronzeToSilverJob
       ↓
Transformações e validações
       ↓
Amazon S3 Silver
       ↓
SilverToGoldJob
       ↓
Métricas meteorológicas
       ↓
Amazon S3 Gold
       ↓
GoldToBigQueryJob
       ↓
Google BigQuery
       ↓
Dashboard
```
