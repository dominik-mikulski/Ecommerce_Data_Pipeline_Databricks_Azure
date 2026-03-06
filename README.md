  # Azure Databricks Incremental Data Pipeline

  ## Overview

  This project demonstrates a **modern cloud-based data pipeline** for
  processing e-commerce order data using a **Medallion architecture**.

  The pipeline ingests raw events into a Bronze layer, cleans and
  validates them in the Silver layer, and produces analytical datasets
  in the Gold layer for downstream reporting and analytics.

  **Databricks Declarative Pipelines and Delta Lake** enable incremental
  processing, deterministic updates, and idempotent pipeline execution.
  **Auto Loader** provides scalable ingestion of source data.

  **Data quality expectations** monitor key constraints and ensure that
  datasets meet the quality standards required by downstream
  stakeholders, while observability components expose pipeline
  metrics and validation results.

  All pipeline code is **version-controlled in Git** using a branching
  workflow, supporting traceability, collaboration, and controlled
  releases.

  Although the project operates on synthetic data, the architecture
  reflects patterns commonly used in production data platforms.

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

  The pipeline is implemented using **Databricks Declarative Pipelines and
  follows a Medallion architecture**. Data flows through three layers that
  progressively improve structure and usability while preserving the
  original raw data.

  Streaming tables are used throughout the pipeline, enabling incremental
  processing and consistent state management across pipeline runs.

  ### Bronze Layer

  The Bronze layer ingests raw JSON order data from **Azure Data Lake
  Storage** using **Auto Loader**. Data is written into an append-only
  orders_bronze table together with file-level ingestion metadata.

  No business transformations are applied at this stage. The Bronze layer
  **preserves the original structure** of incoming data and acts as the
  system's immutable ingestion log.

  Key characteristics:

  - Raw data is appended together with file metadata (file path, file
  name, file size, modification timestamp, ingestion timestamp).
  - **Auto Loader incrementally processes** only new files, avoiding
  reprocessing of already ingested data.
  - **Schema evolution is isolated** in this layer. Unknown fields are
  captured in the`_rescued_data` column without interrupting pipeline
  execution.

  Because Bronze stores the raw ingestion history, **the pipeline can be
  safely replayed or reprocessed if required**.

  ### Silver Layer

  The Silver layer transforms raw Bronze data into structured,
  analytics-ready datasets.

  At this stage the pipeline parses JSON payloads, normalizes the schema,
  and organizes the data into relational tables representing orders and
  their associated line items.

  **Change data is applied using databricks declartive pipeline** to ensure
  that repeated processing or replay does not introduce duplicate
  records.

  Resulting tables:

  - orders
  - order_items

  ### Gold Layer

  The Gold layer models the data into an **analytical star schema** optimized
  for reporting and downstream analytics.

  **Dimension tables maintain historical changes** using Slowly Changing
  Dimensions (Type 2), while fact tables represent transactional order
  data.

  Dimensions:

  - dim_customer (SCD Type 2)
  - dim_product (SCD Type 2)

  Facts:

  - fact_orders (SCD Type 1)
  - fact_order_items (SCD Type 1)

  Fact tables reference dimension tables using deterministic surrogate
  keys to maintain stable analytical relationships.

  ------------------------------------------------------------------------

  ## Environment Separation

  The pipeline uses Unity Catalog to **separate development, testing and production
  environments**.

  Three catalogs are defined:

  - DEV_CATALOG – used for development and experimentation
  - TEST_CATALOG - to be used for testing and validation
  - PROD_CATALOG – used for production datasets consumed by analytics

  This separation allows safe iteration on pipeline logic without
  affecting production data or downstream analytical workloads.

  ------------------------------------------------------------------------

  ## Source Control

  The project is **maintained in a Git repository** and integrated with the
  Databricks workspace through Git integration.

  All pipeline code, transformations, and configuration are version
  controlled, enabling:

  - branch-based development and experimentation
  - traceability of changes to pipeline logic
  - controlled promotion of updates into production

  This workflow supports **reproducible and auditable pipeline releases**.

  ------------------------------------------------------------------------

  ## Key Concepts Demonstrated

  - **Cloud-native data ingestion using Auto Loader** for scalable file
  processing from Azure Data Lake Storage
  - **Incremental data processing** using Databricks Declarative Pipelines
  and streaming tables
  - **Medallion architecture (Bronze → Silver → Gold)** for progressive
  data refinement and separation of concerns
  - **Schema evolution handling** using rescued data columns to prevent
  pipeline failures when new fields appear in source data
  - **Change data processing** with deterministic merge semantics to
  ensure idempotent updates
  - **Star schema modeling** with fact and dimension tables for
  analytical workloads
  - **Slowly Changing Dimensions** (Type 2) to preserve historical
  dimension changes
  - **Data quality monitoring** using expectations
  - **Pipeline observability** through event log inspection and data
  quality metrics
  - **Environment isolation** using Unity Catalog
  - **Version-controlled pipeline development** using Git and
  branch-based workflows
  - **Secure data access** using Azure Managed Identity

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

