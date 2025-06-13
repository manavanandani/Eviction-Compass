# Eviction-Compass
The Eviction Tracker is a data analytics platform that helps with real-time eviction pattern analysis used in the interpretation of the entire state of California.

 
Housing instability and evictions are current dilemmas in urban areas, which in general, reflect underlying socio-economic risks. San Francisco, due to its high cost of living and its dynamic housing market, has attracted considerable interest around the topic of eviction data and the implications it has for public policy and community well-being. In an effort to help data-informed decision-making to meet the challenges, the dissemination and analysis of open eviction data has become a major concern. The goal of this project is to use the current data warehousing and visualization tools and knowledge in order to derive practical and actionable knowledge from eviction data collected from the San Francisco Government Open Data Portal.
 
The main goal of this work is to develop and implement a complete data pipeline and analysis framework able to automate data ingestion, processing, storage, and visualization. By building a scalable and efficient architecture, the system ensures that eviction data can be systematically analyzed for trends, anomalies, and correlations, offering valuable insights to stakeholders, including policymakers, researchers, and community organizations.
 
For this, we repackaged some of the state-of-the-art tools and technologies into the workflow
 
Data Collection and Storage: The eviction information is retrieved through an API of SF Gov Open Data Portal and is stored securely in Amazon S3. This guarantees a robust and scalable repository for the unprocessed data.
 
Automation with Apache Airflow: Task orchestration and process automation are managed using Apache Airflow, which schedules and monitors the extraction and loading of data.
 
Data Warehousing with Snowflake: Snowflake is used to build scalable structured dimension and fact tables, enabling fast querying and aggregation of data. In this stage unprocessed data are transformed into a format suitable for analytical processing.
 
Data Visualization with MongoDB Atlas: Most important insights are represented by using MongoDB Atlas, providing eviction patterns and trends in a user-friendly and interactive mode for end users.
 
Not only this project illustrates the capability of combining cloud services and automating tools but also highlight the significance of open data in solving social challenges. Through the transduction of raw eviction data into actionable knowledge, the system not only creates a better insight of the trends underlying them, but also provides a basis for evidence-based interventions.


# Eviction-Compass 🔍

> A Scalable, Automated Eviction Analytics Pipeline for the State of California using Open Government Data

---

## 📄 Project Structure
```
Eviction-Compass/
├── README.md                        # Project overview and documentation
├── .env.example                     # Environment variable template
├── airflow/                         # Airflow DAGs and config
│   └── dags/
│       └── eviction_pipeline.py     # Airflow DAG definition
├── scripts/                         # Python scripts for ingestion and upload
│   ├── fetch_evictions.py           # Fetches data from SF Open Data API
│   └── upload_s3.py                 # Uploads data to Amazon S3
├── snowflake/                       # DDLs for data warehouse
│   └── ddl/
│       ├── fact_eviction.sql        # Fact table for eviction records
│       ├── dim_reason.sql           # Dimension table for reasons
│       └── dim_building.sql         # Dimension table for building info
├── visualizations/                  # MongoDB Atlas Charts templates
│   └── mongo_templates.json         # Chart configurations for dashboards
├── data/
│   └── raw/                         # Local storage for raw JSONs fetched
└── LICENSE                          # MIT License
```

---

## 🚀 Overview
**Eviction-Compass** is an end-to-end data engineering platform built to analyze real-time eviction trends in California using open data from the [San Francisco Government Open Data Portal](https://data.sfgov.org/).

Housing instability is a pressing issue in urban America, and through this project, we aim to empower policymakers, community organizations, and researchers by delivering actionable insights via automation, cloud-based storage, and rich visualizations.

---

## 📊 Architecture
```
+------------------+
| SF Gov Open Data |
+--------+---------+
         |
         |  API Fetch (JSON)
         v
+--------+--------+
|  Apache Airflow  |
|  DAG Scheduler   |
+--------+--------+
         |
         | Raw Data
         v
+--------+--------+
|   Amazon S3      |
|  (Data Lake)     |
+--------+--------+
         |
         | Snowpipe Loader
         v
+--------+--------+
|   Snowflake DB   |
| (Data Warehouse) |
+--------+--------+
         |
         | Mongo Connector
         v
+--------+--------+
| MongoDB Atlas    |
| (Visual Dash UI) |
+------------------+
```

---

## 📚 Table of Contents
- [Project Structure](#project-structure)
- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Getting Started](#getting-started)
- [Airflow DAG Design](#airflow-dag-design)
- [Data Model - Snowflake](#data-model---snowflake)
- [MongoDB Atlas Visuals](#mongodb-atlas-visuals)
- [Use Cases](#use-cases)
- [Sample Outputs](#sample-outputs)
- [Future Work](#future-work)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## ✨ Features
- Automated eviction data ingestion (daily refresh)
- ETL pipeline built using **Apache Airflow**
- Raw data stored in **Amazon S3** as a data lake
- Structured data warehousing using **Snowflake** with star schema (Fact + Dimension)
- Dashboard and map-based visuals with **MongoDB Atlas Charts**
- Scalable, modular, and production-grade architecture

---

## ⚙️ Technologies Used
| Layer | Tool/Tech |
|------|------------|
| Data Source | SF Gov Open Data Portal (REST API) |
| Ingestion | Apache Airflow, Python |
| Storage | Amazon S3 |
| Processing & Warehouse | Snowflake, SQL |
| Visualization | MongoDB Atlas Charts |
| Infrastructure | AWS, Python 3.11 |

---

## 🚧 Getting Started
### 1. Clone the repo
```bash
git clone https://github.com/your-username/eviction-compass.git
cd eviction-compass
```

### 2. Setup `.env`
Create a `.env` file with your keys:
```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
SF_API_URL=https://data.sfgov.org/resource/evictions.json
SNOWFLAKE_ACCOUNT=...
MONGODB_URI=...
```

### 3. Run Airflow Locally
```bash
cd airflow
docker-compose up -d
```
DAG will fetch data daily and load it into S3 & Snowflake.

### 4. Connect Snowflake to S3
Use Snowpipe or COPY INTO from `staged_raw_data` bucket.

### 5. MongoDB Atlas Charts
Link `eviction_summary` collection to your Atlas dashboard. Use the templates provided in `visualizations/`.

---

## 🌐 API Extraction Code
```python
# scripts/fetch_evictions.py
import requests, json, os
from datetime import datetime

def fetch_evictions():
    url = os.getenv("SF_API_URL")
    response = requests.get(url)
    data = response.json()
    today = datetime.now().strftime('%Y-%m-%d')
    with open(f"data/raw/evictions_{today}.json", "w") as f:
        json.dump(data, f)
```

---

## ⏰ Airflow DAG Design
```python
# dags/eviction_pipeline.py
with DAG(dag_id='eviction_ingestion', ...) as dag:
    fetch_data = PythonOperator(..., python_callable=fetch_evictions)
    upload_to_s3 = PythonOperator(...)
    transform_load_snowflake = PythonOperator(...)

    fetch_data >> upload_to_s3 >> transform_load_snowflake
```

---

## 📈 Data Model - Snowflake
### Fact Table: `eviction_facts`
| eviction_id | date | zip | reason_code | building_id | tenant_id | ... |

### Dimension Tables:
- `dim_building`
- `dim_tenant`
- `dim_reason`
- `dim_date`

---

## 📊 MongoDB Atlas Visuals
Visuals created:
- Choropleth map of evictions by zip
- Trendline: Evictions vs Month
- Breakdown: Reasons for eviction
- Alert flags on abnormal spike detection

Visual template configs are saved in `/visualizations/mongo_templates.json`

---

## 🤝 Use Cases
- **Policy Design**: Identify areas with eviction surges to guide rent control actions.
- **Nonprofits**: Support vulnerable communities with data-driven eviction defense.
- **Academia**: Leverage historical trends for socioeconomic studies.

---

## 📅 Sample Outputs
```
> eviction_facts
+-------------+------------+-------+------------+
| eviction_id | date       | zip   | reason     |
+-------------+------------+-------+------------+
| 187292      | 2023-11-01 | 94110 | Breach     |
| 187293      | 2023-11-01 | 94103 | Nonpayment |
```

---

## 🚪 Future Work
- Real-time anomaly alerts with AWS Lambda & SNS
- Forecasting models for eviction risk
- Integration with census demographic datasets

---
## 🎨 Acknowledgements
- San Francisco Open Data Portal
- Airflow, Snowflake, MongoDB Atlas teams
- AWS S3 and Lambda documentation
  
---

## 👨‍📋 Contributing
We welcome community contributions. Please read our [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

---


## ✉️ License
This repository is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---


