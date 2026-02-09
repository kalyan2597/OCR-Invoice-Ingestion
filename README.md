# Invoice Ingestion & OCR Data Pipeline

A Spark-based data pipeline for processing invoices of OCR, extracting structured data, and building a medallion architecture data warehouse using Delta Lake.

## Overview

This project implements an automated invoice processing pipeline that:
- Downloads invoice csv from Kaggle
- Applies OCR to extract text and structured JSON data
- Processes data through a medallion architecture (Bronze → Silver → Gold)
- Builds aggregated business-ready fact tables using Apache Spark and Delta Lake

## Architecture

The project follows the **Medallion Architecture** pattern:

```
Raw Data (Kaggle)
    ↓
[BRONZE LAYER]  - invoices_raw (raw OCR extraction)
    ↓ (Cleansing & Validation)
[SILVER LAYER]  - invoice_ocr (cleaned, normalized data)
    ↓ (Aggregation & Enrichment)
[GOLD LAYER]    - invoicefact (business-ready facts)
```

### Key Components

- **Apache Spark 3.5.1**: Distributed data processing and SQL execution
- **Delta Lake**: ACID transactions, time travel, and optimization
- **Jupyter Lab**: Interactive notebook environment
- **Kaggle API**: Dataset ingestion
- **Docker**: Containerized environment management

## Project Structure

```
invoice_ingestion/
├── docker-compose.yml          # Docker container orchestration
├── kaggle_login.json           # Kaggle API credentials
├── notebooks/                  # Jupyter notebooks for pipeline stages
│   ├── ingest_raw_data.ipynb   # Bronze layer ingestion
│   ├── creating_silver_layer.ipynb  # Silver layer transformation
│   └── creating_gold_layer.ipynb    # Gold layer aggregation
├── data/                       # Data warehouse
│   ├── raw/                    # Raw downloaded datasets
│   ├── bronze/                 # Delta tables - raw OCR data
│   ├── silver/                 # Delta tables - cleaned data
│   ├── gold/                   # Delta tables - business facts
│   └── processed/              # Processed CSV outputs
└── README.md
```

## Prerequisites

- Docker & Docker Compose
- Kaggle account with API credentials
- Python 3.10+ (if running outside container)

## Getting Started

### 1. Setup Kaggle Credentials

Create `kaggle_login.json` in the project root:

```json
{
    "kaggle_username": "your_kaggle_username",
    "kaggle_key": "your_kaggle_api_key"
}
```

> Get your credentials from [kaggle.com/account](https://kaggle.com/account)

### 2. Start the Docker Container

```bash
docker-compose up -d
```

This will:
- Pull the Apache Spark 3.5.1 image
- Install dependencies (delta-spark, kaggle, jupyter)
- Start Jupyter Lab on `http://localhost:8888`

### 3. Access Jupyter Lab

Open your browser and navigate to:
```
http://localhost:8888/lab
```

## Pipeline Notebooks

### 1. **ingest_raw_data.ipynb** - Bronze Layer
**Purpose**: Download and ingest raw invoice data

**Steps**:
- Download invoice dataset from Kaggle using API credentials
- Read CSV files containing invoice images, OCR text, and JSON data
- Add metadata columns: source file name, ingestion timestamp
- Write to Delta table: `invoices_raw`

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| file_name | STRING | Original file identifier |
| json_data | STRING | Structured JSON with invoice details |
| ocred_text | STRING | Raw OCR extracted text |
| source_file | STRING | Source CSV file name |
| ingestion_ts | TIMESTAMP | Data ingestion timestamp |

### 2. **creating_silver_layer.ipynb** - Silver Layer
**Purpose**: Clean, validate, and normalize invoice data

**Steps**:
- Read incremental data from Bronze layer (since last run)
- Parse JSON schema into structured columns:
  - Invoice details (client, seller, dates, numbers)
  - Line items (description, quantity, price)
  - Totals (tax, discount, invoice total)
  - Payment instructions
- Explode line items for atomic-level granularity
- Apply data quality rules and type conversions
- Normalize numeric fields and date formats
- Write to Delta table: `invoice_ocr`

**Key Transformations**:
- Extract nested JSON structures
- Parse numeric fields (handle currency symbols, commas, spaces)
- Standardize date formats
- Remove duplicates and null values

### 3. **creating_gold_layer.ipynb** - Gold Layer
**Purpose**: Create business-ready fact tables for analysis

**Steps**:
- Read incremental data from Silver layer
- Aggregate by invoice level:
  - Count line items per invoice
  - Sum quantities and amounts
  - Extract tax and discount
  - Get total invoice amount
- Upsert into Gold fact table using Delta merge
- Write to Delta table: `invoicefact`

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| invoice_number | STRING | Unique invoice ID |
| invoice_date | DATE | Invoice date |
| client_name | STRING | Customer name |
| seller_name | STRING | Seller/vendor name |
| num_items | INT | Number of line items |
| total_quantity | INT | Sum of all quantities |
| total_tax | DOUBLE | Total tax amount |
| total_discount | DOUBLE | Total discount |
| total_invoice_amount | DOUBLE | Final invoice total |
| ingestion_ts | TIMESTAMP | Processing timestamp |

## Configuration

### Docker Environment Variables

The `docker-compose.yml` configures:
- **SPARK_MODE**: Master node
- **SPARK_OPTS**: Includes Delta Lake packages
- **SPARK_RPC_ENCRYPTION**: Disabled for development
- **Jupyter**: No authentication token for easy access

### Spark Configuration

Each notebook includes:
```python
spark_builder = (
    SparkSession.builder
    .appName("PipelineName")
    .master("local[*]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
```

## Data Flow & Processing

### Incremental Processing

Each layer implements incremental processing:
1. Check if Delta table exists
2. Get last ingestion timestamp
3. Filter source data: `WHERE ingestion_ts > last_ingestion_ts`
4. Append/Merge new records only

This enables:
- Efficient batch processing
- Support for complex schedules
- Easy failure recovery

### Delta Lake Benefits

- **ACID Transactions**: Guaranteed data consistency
- **Schema Evolution**: Auto-schema merging with `autoMerge.enabled`
- **Merge Operations**: Upsert with `MATCHED` and `NOT MATCHED` clauses
- **Time Travel**: Query historical data states

## Running the Pipeline

### Execute in Sequence

```bash
# 1. Open Jupyter and run notebooks in order:
# 1. ingest_raw_data.ipynb          (Bronze)
# 2. creating_silver_layer.ipynb    (Silver)
# 3. creating_gold_layer.ipynb      (Gold)
```

Each notebook can be re-run safely; incremental logic prevents duplicates.

### Query Results

Access data with Spark SQL in any notebook:

```python
# Query Gold layer
spark.sql("SELECT * FROM invoicefact LIMIT 10").show()

# Check table info
spark.sql("DESCRIBE DETAIL invoicefact").show()
```

## Troubleshooting

### Kaggle Dataset Download Failed
- Verify `kaggle_login.json` credentials are correct
- Check Kaggle API key hasn't expired
- Ensure dataset name in code matches Kaggle

### Spark Memory Issues
- Increase Docker memory allocation
- Check for large broadcast joins in Silver layer
- Monitor Spark UI on `http://localhost:8080`

### Delta Table Corruption
```python
# Repair table
spark.sql("FSCK TABLE table_name REPAIR")

# Check table history
spark.sql("SELECT * FROM table_name@v0")  # Time travel
```

## Performance Optimization

### Current Optimizations
- Incremental processing (only new data)
- Partition pruning on `ingestion_ts`
- Schema inference limits with explicit types
- Merge operations for upsert efficiency

### Future Improvements
- Partitioning by `invoice_date` or `client_name`
- Z-ordering on frequently filtered columns
- Caching hot tables in memory
- Vacuum old Delta files for storage cleanup

## Data Validation

### Quality Checks Implemented
- JSON schema validation (Silver layer)
- Numeric field parsing with null handling
- Duplicate detection and removal
- Invoice amount reconciliation (tax + subtotal = total)

### Adding New Validations

```python
# Example: Validate total amount
df_validated = df.filter(
    col("total_invoice_amount") >= 0
).filter(
    col("total_tax").isNotNull()
)
```

## Extension Points

### Add New Data Sources

1. Create new notebook: `ingest_new_source.ipynb`
2. Read source data (CSV, Parquet, API, etc.)
3. Add metadata columns (`source_file`, `ingestion_ts`)
4. Write to appropriate Bronze table

### Add New Processing Steps

1. Create intermediate Silver table
2. Apply transformations and validations
3. Merge into existing Gold tables
4. Update downstream dependencies

## Production Deployment

### Recommendations

- **Scheduling**: Use Apache Airflow, Prefect, or cloud schedulers (Azure Data Factory, AWS Glue)
- **Monitoring**: Add logging and alerting; track dataset lineage
- **Testing**: Unit tests for transformation logic; data quality checks
- **Access Control**: Implement row-level security; audit logging
- **Backup**: Configure Delta table retention policies; external backups

## Azure Databricks & ADLS Gen 2 Deployment

This pipeline can be easily deployed to **Azure Databricks** with minimal modifications, leveraging **Azure Data Lake Storage (ADLS) Gen 2** for scalable, secure data management.

### Prerequisites

- Azure subscription
- Azure Databricks workspace
- Azure Storage Account with ADLS Gen 2 enabled
- Service Principal credentials (for authentication)

### Setup Steps

#### 1. Create Azure Storage Account & Container

```bash
# Create resource group
az group create --name invoice-rg --location eastus

# Create storage account
az storage account create \
  --name invoicestg \
  --resource-group invoice-rg \
  --location eastus \
  --kind StorageV2 \
  --access-tier Hot

# Create ADLS Gen 2 filesystem
az storage fs create \
  --name invoice-data \
  --account-name invoicestg
```

#### 2. Create Service Principal for Authentication

```bash
az ad sp create-for-rbac \
  --name invoice-databricks-sp \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/{subscription-id}/resourceGroups/invoice-rg/providers/Microsoft.Storage/storageAccounts/invoicestg
```

Save the output:
- `appId`: Client ID
- `password`: Client Secret
- `tenant`: Tenant ID

#### 3. Deploy to Azure Databricks

**Create a cluster** with:
- **Databricks Runtime**: 14.0 or higher (Python 3.11+)
- **Spark Config**:
  ```
  spark.hadoop.fs.azure.account.auth.type.invoicestg.dfs.core.windows.net OAuth
  spark.hadoop.fs.azure.account.oauth.provider.type.invoicestg.dfs.core.windows.net org.apache.hadoop.fs.azureblobfs.oauth2.ClientCredsTokenProvider
  spark.hadoop.fs.azure.account.oauth2.client.id.invoicestg.dfs.core.windows.net {SP_CLIENT_ID}
  spark.hadoop.fs.azure.account.oauth2.client.secret.invoicestg.dfs.core.windows.net {SP_CLIENT_SECRET}
  spark.hadoop.fs.azure.account.oauth2.client.endpoint.invoicestg.dfs.core.windows.net https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token
  ```

#### 4. Modify Notebooks for Azure Paths

Replace local paths with ADLS Gen 2 paths:

**Before (Docker/Local)**:
```python
bronze_path = '/opt/spark/data/bronze/invoices_raw'
silver_path = '/opt/spark/data/silver/invoice_ocr'
gold_path = '/opt/spark/data/gold/invoicefact'
```

**After (Azure ADLS Gen 2)**:
```python
# ADLS Gen 2 paths
bronze_path = 'abfss://invoice-data@invoicestg.dfs.core.windows.net/bronze/invoices_raw'
silver_path = 'abfss://invoice-data@invoicestg.dfs.core.windows.net/silver/invoice_ocr'
gold_path = 'abfss://invoice-data@invoicestg.dfs.core.windows.net/gold/invoicefact'

# Alternative: using catalog (Unity Catalog)
bronze_path = 'catalog.schema.invoices_raw'
silver_path = 'catalog.schema.invoice_ocr'
gold_path = 'catalog.schema.invoicefact'
```

#### 5. Kaggle Credentials in Databricks Secrets

Instead of `kaggle_login.json`:

```python
# Store credentials in Databricks Secrets
# In terminal: databricks secrets put-secret --scope kaggle --key username
# databricks secrets put-secret --scope kaggle --key api-key

# In notebook:
kaggle_username = dbutils.secrets.get(scope="kaggle", key="username")
kaggle_key = dbutils.secrets.get(scope="kaggle", key="api-key")

os.environ["KAGGLE_USERNAME"] = kaggle_username
os.environ["KAGGLE_KEY"] = kaggle_key
```

### Configuration Comparison

| Feature | Docker/Local | Azure Databricks |
|---------|--------------|------------------|
| **Storage** | Local filesystem | ADLS Gen 2 |
| **Compute** | Single node | Auto-scaling clusters |
| **Secrets** | JSON files | Azure Key Vault / Databricks Secrets |
| **Scheduling** | Manual / Cron | Databricks Workflows, Azure Data Factory |
| **Cost** | Compute only | Storage + Compute (elastic) |
| **Scalability** | Limited by machine | Unlimited (petabyte-scale) |
| **Compliance** | Local only | Enterprise (SOC2, HIPAA-ready) |

### Modified Spark Session (Azure Databricks)

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Databricks provides pre-configured Spark session
spark_builder = (
    SparkSession.builder
    .appName("InvoicePipelineAzure")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Azure-specific: ADLS Gen 2 optimization
    .config("spark.hadoop.fs.azure.file.partition.size", "256M")
)

spark = configure_spark_with_delta_pip(spark_builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
```

### File Path Helper Function

```python
def get_adls_path(layer, table_name):
    """Generate ADLS Gen 2 path for Delta tables"""
    base_path = "abfss://invoice-data@invoicestg.dfs.core.windows.net"
    return f"{base_path}/{layer}/{table_name}"

# Usage:
bronze_path = get_adls_path("bronze", "invoices_raw")
silver_path = get_adls_path("silver", "invoice_ocr")
gold_path = get_adls_path("gold", "invoicefact")
```

### Scheduling with Databricks Workflows

Create a workflow in Databricks UI or define in YAML:

```yaml
name: Invoice Pipeline
clusters:
  - cluster_label: invoke-cluster
    node_type_id: i3.xlarge
    spark_version: 14.0.x-scala2.12
    num_workers: 2

tasks:
  - task_key: bronze_layer
    notebook_task:
      notebook_path: /Users/user@company.com/notebooks/ingest_raw_data
    cluster_label: invoke-cluster
    
  - task_key: silver_layer
    depends_on:
      - task_key: bronze_layer
    notebook_task:
      notebook_path: /Users/user@company.com/notebooks/creating_silver_layer
    cluster_label: invoke-cluster
    
  - task_key: gold_layer
    depends_on:
      - task_key: silver_layer
    notebook_task:
      notebook_path: /Users/user@company.com/notebooks/creating_gold_layer
    cluster_label: invoke-cluster

schedule:
  quartz_cron_expression: "0 0 * * * ?" # Daily at midnight
  timezone_id: America/New_York
```

### Monitoring & Logging (Azure)

**Enable Azure Monitor integration**:

```python
# Add to notebook
from pyspark.sql.functions import current_timestamp, lit

# Log processing to a monitoring table
log_df = spark.createDataFrame([{
    "pipeline": "invoice_ingestion",
    "layer": "bronze",
    "status": "success",
    "record_count": df.count(),
    "timestamp": current_timestamp()
}])

log_df.write.mode("append").option("mergeSchema", "true") \
    .save("abfss://invoice-data@invoicestg.dfs.core.windows.net/logs/pipeline_runs")
```

### Cost Optimization Tips (Azure)

- **Use Spot VMs**: Enable in cluster config for 60-70% cost savings
- **Auto-termination**: Set cluster timeout to 30 mins of inactivity
- **Partition Data**: Use `PARTITION BY invoice_date` to reduce scan costs
- **Vacuum Old Files**: Run `VACUUM invoices_raw RETAIN 7 DAYS` weekly
- **Reserved Capacity**: Use Databricks Capacity for predictable workloads

### Security Best Practices (Azure)

- **Network**: Use Private Link to connect to ADLS Gen 2
- **Encryption**: Enable transparent data encryption (TDE) on storage account
- **RBAC**: Assign minimal permissions via Azure roles
- **Audit Logging**: Enable Azure Monitor & Log Analytics
- **Compliance**: Mark sensitive data with labels; use Column-level security in Unity Catalog

### Migration Path

**Note**: Modern Azure Databricks workspaces have **Unity Catalog enabled by default**, streamlining governance

1. **Phase 1**: Run notebooks in Databricks workspace (test compatibility)
2. **Phase 2**: Create Unity Catalog, database, and tables
   - Create catalog: `CREATE CATALOG invoice_catalog`
   - Create schema: `CREATE SCHEMA invoice_catalog.invoice_db`
   - Tables automatically registered with 3-level namespace
3. **Phase 3**: Update all paths to ADLS Gen 2 with Unity Catalog references
   ```python
   # Use 3-level naming from day 1
   bronze_table = "invoice_catalog.invoice_db.invoices_raw"
   silver_table = "invoice_catalog.invoice_db.invoice_ocr"
   gold_table = "invoice_catalog.invoice_db.invoicefact"
   ```
4. **Phase 4**: Deploy Databricks Workflows for scheduling
5. **Phase 5**: Integrate with Azure Data Factory for orchestration

#### Unity Catalog Integration

Since Unity Catalog is auto-enabled, leverage these features immediately:

**Create Managed Tables** (stored in ADLS Gen 2):
```python
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {bronze_table}
(
    file_name string,
    json_data string,
    ocred_text string,
    source_file string,
    ingestion_ts timestamp
)
USING DELTA
CLUSTER BY (ingestion_ts)
""")
```

**Grant Access Control**:
```sql
-- Grant read access to analysts
GRANT SELECT ON CATALOG invoice_catalog TO `analyst-group@company.com`

-- Grant write access to data engineers
GRANT MODIFY ON SCHEMA invoice_catalog.invoice_db TO `engineering@company.com`

-- Enable row-level filtering
ALTER TABLE invoice_catalog.invoice_db.invoicefact 
SET ROW FILTER user_partition_column = current_user()
```

**Data Lineage & Governance**:
- Unity Catalog automatically tracks data lineage through Databricks Lineage
- Access control enforced at catalog/schema/table levels
- Audit logs available in Azure Monitor

---

**Last Updated**: February 2026
**Spark Version**: 3.5.8
**Delta Lake Version**: 3.1.0
