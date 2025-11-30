import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from delta import configure_spark_with_delta_pip
import shutil

# --- Infrastructure Layer (Windows Fix) ---
def setup_environment():
    """Sets up Windows environment variables for Hadoop/Spark."""
    project_root = os.getcwd()
    hadoop_home = os.path.join(project_root, 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    # Reduce logging noise
    os.environ['pyspark.log.level'] = "ERROR"

def create_spark_session(app_name: str) -> SparkSession:
    """Initializes Spark with Delta Lake support."""
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "localhost") \
        .config("spark.ui.showConsoleProgress", "false")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# --- Service Layer (ETL Logic) ---
class BronzeIngestion:
    """
    Handles the raw ingestion from CSV (Landing Zone) to Delta Lake (Bronze Layer).
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def ingest_table(self, source_path: str, table_name: str, primary_key: str):
        """
        Reads CSV, adds metadata, and saves as Delta Table.
        Mode: Overwrite (for this simulation). In real streaming, we use Append.
        """
        print(f"--> Processing: {table_name}...")
        
        # 1. Read Raw Data
        try:
            df_raw = self.spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(source_path)
        except Exception as e:
            print(f"Error reading {source_path}: {e}")
            return

        # 2. Add Audit Columns (Lineage)
        df_enriched = df_raw \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name())

        # 3. Write to Bronze (Delta Format)
        # Path: ./bronze/customers, ./bronze/transactions, etc.
        output_path = f"./bronze/{table_name}"
        
        (df_enriched.write
            .format("delta")
            .mode("overwrite")  # Reset simulation each time
            .option("overwriteSchema", "true")
            .save(output_path))
            
        print(f"✅ Ingested {df_enriched.count()} records to Bronze: {output_path}")

# --- Entry Point ---
if __name__ == "__main__":
    setup_environment()
    spark = create_spark_session("BronzeIngestionJob")
    
    ingestor = BronzeIngestion(spark)
    
    # Define jobs: (Source File, Table Name, Key)
    jobs = [
        ("./landing_zone/customers.csv", "customers", "customer_id"),
        ("./landing_zone/accounts.csv", "accounts", "account_id"),
        ("./landing_zone/transactions.csv", "transactions", "transaction_id")
    ]
    
    print("--- Starting Bronze Layer Ingestion ---")
    for source, name, key in jobs:
        ingestor.ingest_table(source, name, key)
        
    print("--- Bronze Ingestion Complete ---")
    spark.stop()
