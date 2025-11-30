import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable
from delta import configure_spark_with_delta_pip

# --- Infrastructure ---
def setup_environment():
    project_root = os.getcwd()
    hadoop_home = os.path.join(project_root, 'hadoop')
    os.environ['HADOOP_HOME'] = hadoop_home
    os.environ['PATH'] += os.pathsep + os.path.join(hadoop_home, 'bin')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ['pyspark.log.level'] = "ERROR"

def create_spark_session() -> SparkSession:
    builder = SparkSession.builder \
        .appName("TimeTravelDemo") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "localhost") \
        .config("spark.ui.showConsoleProgress", "false")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def demonstrate_time_travel(spark: SparkSession):
    table_path = "./silver/customers"
    
    print("\n--- 🕰️ INICIANDO DEMOSTRACIÓN DE TIME TRAVEL ---\n")

    # 1. Leer la versión actual (Original)
    print("1️⃣  Versión Original (v0):")
    df_v0 = spark.read.format("delta").load(table_path)
    # Tomamos un cliente de ejemplo
    target_customer = df_v0.select("customer_id", "email").limit(1).collect()[0]
    cust_id = target_customer["customer_id"]
    print(f"    Cliente: {cust_id}")
    print(f"    Email Original: {target_customer['email']}")

    # 2. Simular una modificación (UPDATE)
    print("\n2️⃣  Realizando cambios (Generando v1)...")
    deltaTable = DeltaTable.forPath(spark, table_path)
    
    # Cambiamos el email a algo erróneo
    deltaTable.update(
        condition = col("customer_id") == cust_id,
        set = { "email": lit("ERROR_HACKED_EMAIL@bad.com") }
    )
    print("    ✅ Cambio realizado.")

    # 3. Leer la versión actual (La 'dañada')
    print("\n3️⃣  Leyendo datos actuales (v1):")
    df_v1 = spark.read.format("delta").load(table_path)
    new_email = df_v1.filter(col("customer_id") == cust_id).select("email").collect()[0][0]
    print(f"    Email Actual: {new_email}  <-- ¡DATO CORRUPTO!")

    # 4. VIAJE EN EL TIEMPO (Time Travel)
    print("\n4️⃣  ⏳ VIAJANDO AL PASADO (Restaurando v0)...")
    # La magia ocurre aquí: option("versionAsOf", 0)
    df_old = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
    
    old_email = df_old.filter(col("customer_id") == cust_id).select("email").collect()[0][0]
    print(f"    Email Recuperado del Pasado: {old_email} <-- ¡DATO CORRECTO!")

    print("\n--- DEMO EXITOSA: Historial preservado ---")

if __name__ == "__main__":
    setup_environment()
    spark = create_spark_session()
    try:
        demonstrate_time_travel(spark)
    finally:
        spark.stop()
