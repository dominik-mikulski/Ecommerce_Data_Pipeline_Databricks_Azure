# Azure Databricks Incremental Data Pipeline

## Overview

Production-inspired **incremental data pipeline** built on **Azure
Databricks** using **Azure Data Lake Storage**, **Spark Declarative
Pipelines**, and **Unity Catalog**.

The project simulates an e-commerce data engineering workload and
demonstrates how to build a **medallion data pipeline** with:

-   Bronze → Silver → Gold data layers
-   Incremental ingestion and processing
-   Environment separation for development and production
-   Data quality checks implemented directly in the pipeline

The goal of the project is to show how modern data pipelines ingest raw
data, transform it into analytical models, and monitor data quality
using Databricks native capabilities.

------------------------------------------------------------------------

## Project Structure

    Data                                                        # Data generation and ready json files
    
    Notebooks/
      Pipeline_Validation_and_Data_Quality_Observability        # Table inspection and data quality metrics
    
    Pipelines/ Retail_Azure_Medallion_ETL/
    Transformations/
    
      retail_pipeline.py                                        # Declarative Spark pipeline definition

      dq_rules.py                                               # Data quality expectations used in the pipeline

      bronze_to_silver_transformations                          # Pipeline helper functions

    scripts/
      setup_infrastructure.py                                   # Creates catalogs and schemas

------------------------------------------------------------------------

## Architecture

The pipeline creates streaming tables organized in a **medallion
architecture** and governed by **Unity Catalog**.

### Bronze Layer

Raw JSON files are ingested from **Azure Data Lake Storage** using
**Auto Loader**.

The Bronze table stores:

-   raw payload
-   file metadata (`file_path`, `ingested_at`)

This layer preserves the original data for traceability.

### Silver Layer

The Silver layer transforms raw data into structured tables.

The pipeline:

-   unpacks JSON payloads
-   deduplicates records
-   upserts data into structured tables

Resulting tables:

-   `orders`
-   `order_items`

### Gold Layer

The Gold layer prepares data for analytics using a **star schema**.

The pipeline builds:

**Dimensions**

-   `dim_customer` (SCD Type 2 history)\
-   `dim_product` (SCD Type 2 history)

**Facts**

-   `fact_orders`\
-   `fact_order_items`

Fact tables are connected to dimensions using **deterministic surrogate
keys**.

------------------------------------------------------------------------

## Environment Separation

Unity Catalog is used to separate development and production
environments.

**Catalogs**

-   `PROD_CATALOG` -- production data used for analytics\
-   `DEV_CATALOG` -- development and testing environment

This setup allows safe pipeline development without affecting production
data.

------------------------------------------------------------------------

## Source Control

The project is stored in a **Git repository** and connected to
Databricks through **Git integration**.

This enables:

-   version control for pipeline code and notebooks\
-   branch-based development and testing\
-   reproducible deployment of pipeline logic

------------------------------------------------------------------------

## Key Concepts Demonstrated

-   **Cloud-native ingestion** using Auto Loader\
-   **Incremental data processing** with declarative Spark pipelines\
-   **Medallion architecture** (Bronze → Silver → Gold)\
-   **Star schema modeling** with fact and dimension tables\
-   **Slowly Changing Dimensions (SCD Type 2)** for history tracking\
-   **Environment isolation** using Unity Catalog\
-   **Data quality monitoring** using pipeline expectations\
-   **Pipeline observability** through event log inspection and data
    quality metrics\
-   **Secure storage access** using Azure Managed Identity

------------------------------------------------------------------------

## How to Replicate

### 1. Azure Infrastructure Setup
1.  **Workspace:** Create an Azure Databricks Workspace (Premium Tier) with Unity Catalog enabled.
2.  **Storage:** Create an **ADLS Gen2 Account** with **Hierarchical Namespace** enabled and a container named `container-databricks-azure`.
3.  **Permissions:** Locate the **Access Connector for Azure Databricks** (Managed Identity) created with your workspace. Assign these roles at the **Storage Account** scope:
    - `Storage Blob Data Contributor`
    - `Storage Account Contributor`
    - `Storage Queue Data Contributor`
    - `EventGrid EventSubscription Contributor`

### 2. Unity Catalog Configuration (In Databricks)
1.  **Storage Credential:** Create a new credential using the **Access Connector ID** from Step 1.
2.  **External Location:** Create a location named `azure_blob` pointing to:
    `abfss://container-databricks-azure@<account_name>.dfs.core.windows.net/`
3.  **Permissions:** Grant yourself `READ FILES` and `WRITE FILES`.


### 3. Clone & Deploy
```bash
git clone [https://github.com/dominik-mikulski/Retail_Azure_Medallion_Project](https://github.com/dominik-mikulski/Retail_Azure_Medallion_Project)
```
### 4. Run 
1. Provide Path and run scripts/setup_infrastructure.py to provision catalogs and schemas.
2. Pipeline Creation: - In Databricks, go to Delta Live Tables and click Create Pipeline.
3. Source Code: Point to pipelines/retail_medallion_pipeline.py within your cloned repo.
4. Pipeline Destination Catalog: prod_catalog, Target_schema: gold
5. Variables: In Settings > JSON, add your pipeline.raw_path variable pointing to your Azure /raw folder.

Scope
Focused architecture demonstration. Not included: Terraform/IaC, external orchestration, or production monitoring.

**Dominik Mikulski**  
Expanding hands-on data engineering capabilities alongside 12 years of analytics leadership
[LinkedIn](https://www.linkedin.com/in/dominikmikulski/) | [dominik.mikulski@gmail.com](mailto:dominik.mikulski@gmail.com)

