# Azure Databricks Incremental Data Pipeline (Unity Catalog)

## Overview

Production insprired project of a event-driven, incremental data pipeline on Databricks and Azure using ** Azure blob and notificaitons **, **Delta Live Tables (DLT)**, **Auto Loader**, and **Unity Catalog**.

It simulates an e-commerce data engineering workload and implements a medallion architecture, Unity Catalog for schema management and prod, dev environment separation.

------------------------------------------------------------------------

## Architecture



The data platform is divided into three functional layers within Unity Catalog:

-   **BRONZE** — Auto Loader ingests JSON CDC files from ADLS Gen2, capturing raw payloads with enrichment metadata (`file_path`, `ingested_at`).
-   **SILVER** — Declarative DLT transformations unpack JSON into structured `orders` (deduplicated) and `order_items` (state store) tables via `apply_changes`.
-   **GOLD** — Analytical star schema featuring **SCD Type 2** history for `dim_customer` and `dim_product`, and fact tables joined via deterministic surrogate keys (`xxhash64`).

### Environment Separation

Separate catalogs isolate production and development workloads:

**Catalogs**
- `PROD_CATALOG` — Data and code for production BI consumption.
- `DEV_CATALOG` — development environment.

**Databricks Compute Configuration**
For development and cost control, the pipeline runs on a minimal single-user cluster:
- **Policy:** Personal Compute 
- **Runtime:** 17.3 LTS (Spark 4.0, Scala 2.13)
- **Node Type:** Standard_D4ds_v4 (16 GB, 4 cores) 
- **Auto-termination:** 15 minutes

------------------------------------------------------------------------

## Data Flow

1.  JSON CDC files land in Azure Data Lake Storage (ADLS Gen2).
2.  **Auto Loader** ingests files incrementally into `bronze.orders_raw`.
3.  A declarative DLT pipeline processes the bronze stream to build Silver deduplicated tables.
4.  A second transformation stage builds the Gold analytical baseline:
    -   **Facts:** `fact_orders` (Order grain) and `fact_order_items` (Line-item grain).
    -   **Dimensions:** `dim_customer` and `dim_product` (SCD Type 2 history).
5.  DLT automatically manages checkpoints and event logs for full observability.

------------------------------------------------------------------------

## Key Concepts Demonstrated

-   **Cloud-Native Ingestion** (Auto Loader with Schema Evolution).
-   **Declarative ETL** (DLT for automated state and dependency management).
-   **CDC Handling** (Incremental upserts and SCD2 via `apply_changes`).
-   **Star Schema Modeling** (History tracking and surrogate key integrity).
-   **Environment Isolation** (Catalog-level separation for Dev/Prod).
-   **Managed Identity Access** (Secret-less authentication via Azure Access Connector).

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
1. Provide Path and run scripts/01_setup_infrastructure.py to provision catalogs and schemas.
2. Pipeline Creation: - In Databricks, go to Delta Live Tables and click Create Pipeline.
3. Source Code: Point to pipelines/retail_medallion_pipeline.py within your cloned repo.
4. Pipeline Destination Catalog: prod_catalog, Target_schema: gold
5. Variables: In Settings > JSON, add your pipeline.raw_path variable pointing to your Azure /raw folder.

Scope
Focused architecture demonstration. Not included: Terraform/IaC, external orchestration, or production monitoring.

**Dominik Mikulski**  
Expanding hands-on data engineering capabilities alongside 12 years of analytics leadership
[LinkedIn](https://www.linkedin.com/in/dominikmikulski/) | [dominik.mikulski@gmail.com](mailto:dominik.mikulski@gmail.com)

