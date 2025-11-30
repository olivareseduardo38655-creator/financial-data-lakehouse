import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import shutil

# --- Configuration Layer ---
def setup_environment():
    """Configura las variables de entorno para Windows."""
    project_root = os.getcwd()
    hadoop_home = os.path.join(project_root, 'hadoop')
    
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    
    # Reducir ruido de logs en consola (WARN/ERROR solamente)
    os.environ['pyspark.log.level'] = "ERROR"

def create_spark_session() -> SparkSession:
    """Inicializa la sesión de Spark."""
    builder = SparkSession.builder \
        .appName("LakehouseEnvironmentCheck") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.driver.host", "localhost")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR") # Menos ruido en consola
    return spark

# --- Service Layer ---
def run_smoke_test(spark: SparkSession):
    output_path = "./bronze/test_delta_table"
    
    # Limpieza previa
    if os.path.exists(output_path):
        try:
            shutil.rmtree(output_path)
        except OSError:
            pass

    data = [(1, "System Operational"), (2, "Ready for Phase 1")]
    df = spark.createDataFrame(data, ["id", "message"])
    
    print("\n--- 🚀 [TEST] Escritura Delta ---")
    df.write.format("delta").mode("overwrite").save(output_path)
    print("✅ Escritura exitosa.")
    
    print("--- 🔍 [TEST] Lectura Delta ---")
    # CORRECCIÓN AQUÍ: Usamos 'spark.read', no 'df.read'
    df_read = spark.read.format("delta").load(output_path)
    df_read.show(truncate=False)
    print("✅ Lectura exitosa.\n")

if __name__ == "__main__":
    setup_environment()
    spark = create_spark_session()
    try:
        run_smoke_test(spark)
    finally:
        spark.stop()
