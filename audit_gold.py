import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def setup_environment():
    project_root = os.getcwd()
    os.environ['HADOOP_HOME'] = os.path.join(project_root, 'hadoop')
    os.environ['PATH'] += os.pathsep + os.path.join(project_root, 'hadoop', 'bin')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['pyspark.log.level'] = "ERROR"

if __name__ == "__main__":
    setup_environment()
    builder = SparkSession.builder \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.showConsoleProgress", "false")
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- 💰 TOP 5 CLIENTES MÁS RICOS (Customer 360) ---")
    df_360 = spark.read.format("delta").load("./gold/gold_customer_360")
    df_360.select("first_name", "last_name", "total_balance", "number_of_accounts") \
          .orderBy("total_balance", ascending=False) \
          .show(5, truncate=False)

    print("\n--- 📅 MOVIMIENTO FINANCIERO DIARIO ---")
    df_daily = spark.read.format("delta").load("./gold/gold_daily_movements")
    df_daily.show(5)
    
    spark.stop()
