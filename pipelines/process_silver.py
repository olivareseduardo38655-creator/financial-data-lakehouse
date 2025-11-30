import os
import sys
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lower, trim, when
from delta import configure_spark_with_delta_pip

# --- Infrastructure / Config ---
def setup_environment():
    """Configuración de entorno para Windows (Boilerplate)."""
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

# --- Domain Logic (Business Rules) ---
class SilverTransformer:
    """Contiene las reglas de limpieza pura. No sabe de archivos, solo transforma DataFrames."""
    
    @staticmethod
    def clean_customers(df: DataFrame) -> DataFrame:
        print("   -> Aplicando reglas de calidad a Customers...")
        return df \
            .dropDuplicates(["customer_id"]) \
            .withColumn("email", lower(trim(col("email")))) \
            .filter(col("email").contains("@")) # Regla básica de validación

    @staticmethod
    def clean_accounts(df: DataFrame) -> DataFrame:
        print("   -> Aplicando reglas de calidad a Accounts...")
        return df \
            .dropDuplicates(["account_id"]) \
            .withColumn("current_balance", col("current_balance").cast("double")) \
            .filter(col("current_balance") >= 0) # Regla: No permitimos cuentas en negativo en este reporte

    @staticmethod
    def clean_transactions(df: DataFrame) -> DataFrame:
        print("   -> Aplicando reglas de calidad a Transactions...")
        return df \
            .dropDuplicates(["transaction_id"]) \
            .withColumn("amount", col("amount").cast("double")) \
            .filter(col("amount") > 0) # Regla: Transacciones deben tener valor

# --- Service Layer (ETL Flow) ---
class SilverPipeline:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self):
        # 1. Procesar Customers
        self._process_table(
            source_table="customers",
            transform_func=SilverTransformer.clean_customers
        )

        # 2. Procesar Accounts
        self._process_table(
            source_table="accounts",
            transform_func=SilverTransformer.clean_accounts
        )

        # 3. Procesar Transactions
        self._process_table(
            source_table="transactions",
            transform_func=SilverTransformer.clean_transactions
        )

    def _process_table(self, source_table: str, transform_func):
        bronze_path = f"./bronze/{source_table}"
        silver_path = f"./silver/{source_table}"
        
        print(f"🔄 Procesando: {source_table}")
        
        # READ (Bronze)
        try:
            df_bronze = self.spark.read.format("delta").load(bronze_path)
        except Exception:
            print(f"⚠️ No se encontró la tabla Bronze: {source_table}")
            return

        # TRANSFORM
        df_silver = transform_func(df_bronze)

        # WRITE (Silver - Upsert Strategy simulated with Overwrite for MVP)
        (df_silver.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path))
            
        print(f"✅ Silver actualizado: {silver_path} | Registros: {df_silver.count()}\n")

if __name__ == "__main__":
    setup_environment()
    spark = create_spark_session("SilverLayerJob")
    
    pipeline = SilverPipeline(spark)
    pipeline.run()
    
    spark.stop()
