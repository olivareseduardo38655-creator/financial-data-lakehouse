import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, sum as _sum, count, lit, current_date, date_format
from delta import configure_spark_with_delta_pip

# --- Infrastructure ---
def setup_environment():
    """Environment configuration for Windows/Hadoop."""
    project_root = os.getcwd()
    hadoop_home = os.path.join(project_root, 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['pyspark.log.level'] = "ERROR"

def create_spark_session(app_name: str) -> SparkSession:
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "localhost") \
        .config("spark.ui.showConsoleProgress", "false")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

# --- Domain Logic (Aggregations) ---
class GoldTransformer:
    """Contains pure business logic for creating analytical tables."""

    @staticmethod
    def create_customer_360(df_customers: DataFrame, df_accounts: DataFrame) -> DataFrame:
        """
        Joins Customers and Accounts to create a Master View.
        Metrics: Total Balance, Number of Accounts.
        """
        print("   -> Calculating Customer 360 metrics...")
        
        # 1. Aggregate Accounts at Customer Level
        account_stats = df_accounts.groupBy("customer_id").agg(
            _sum("current_balance").alias("total_balance"),
            count("account_id").alias("number_of_accounts")
        )

        # 2. Join with Customer Details
        # Left join ensures we keep customers even if they don't have accounts yet (rare but possible)
        df_360 = df_customers.join(account_stats, on="customer_id", how="left")
        
        # 3. Fill Nulls for cleaner reporting (if a customer has no accounts, balance is 0)
        return df_360.na.fill({"total_balance": 0.0, "number_of_accounts": 0})

    @staticmethod
    def create_daily_movements(df_transactions: DataFrame) -> DataFrame:
        """
        Aggregates transactions by date and type to see daily cash flow.
        """
        print("   -> Calculating Daily Financial movements...")
        
        # Extract Date from Timestamp for grouping
        return df_transactions \
            .withColumn("movement_date", date_format(col("transaction_date"), "yyyy-MM-dd")) \
            .groupBy("movement_date", "transaction_type") \
            .agg(
                _sum("amount").alias("daily_total_amount"),
                count("transaction_id").alias("transaction_count")
            ) \
            .orderBy("movement_date")

# --- Service Layer ---
class GoldPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self):
        print("--- Starting Gold Layer Processing ---")

        # 1. Load Silver Data (Dimensions & Facts)
        try:
            df_cust = self.spark.read.format("delta").load("./silver/customers")
            df_acc = self.spark.read.format("delta").load("./silver/accounts")
            df_tx = self.spark.read.format("delta").load("./silver/transactions")
        except Exception as e:
            print(f"CRITICAL ERROR: Could not load Silver tables. Run Bronze/Silver first. Details: {e}")
            return

        # 2. Compute: Customer 360
        df_360 = GoldTransformer.create_customer_360(df_cust, df_acc)
        self._save_to_gold(df_360, "gold_customer_360")

        # 3. Compute: Daily Movements
        df_movements = GoldTransformer.create_daily_movements(df_tx)
        self._save_to_gold(df_movements, "gold_daily_movements")

        print("--- Gold Processing Complete ---")

    def _save_to_gold(self, df: DataFrame, table_name: str):
        output_path = f"./gold/{table_name}"
        (df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(output_path))
        print(f"✅ Gold Table Created: {table_name} | Records: {df.count()}")

if __name__ == "__main__":
    setup_environment()
    spark = create_spark_session("GoldLayerJob")
    
    pipeline = GoldPipeline(spark)
    pipeline.run()
    
    spark.stop()
