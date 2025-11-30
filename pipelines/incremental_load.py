import os
import sys
import random
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, col, lit
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
        .appName("IncrementalMergeJob") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "localhost") \
        .config("spark.ui.showConsoleProgress", "false")
    return configure_spark_with_delta_pip(builder).getOrCreate()

# --- Data Generation (Simulating Day 2) ---
def generate_day_2_data(spark: SparkSession):
    """Crea un DataFrame en memoria simulando nuevos datos entrantes."""
    print("   -> Generando datos del 'Día 2' (Nuevos + Actualizaciones)...")
    
    data = [
        # CASO 1: Cliente NUEVO (Insert)
        (str(uuid.uuid4()), "new_guy@test.com", "Elon", "Musk", "2025-01-02"),
        
        # CASO 2: Cliente EXISTENTE que actualiza su email (Update)
        # Nota: Usamos un ID genérico aquí para el ejemplo, 
        # en producción usaríamos IDs reales. Para esta demo, forzaremos un match
        # buscando un ID existente en Silver.
    ]
    
    # Buscamos un ID real de Silver para simular la actualización
    existing_id = spark.read.format("delta").load("./silver/customers") \
        .limit(1).select("customer_id").collect()[0][0]
    
    data.append((existing_id, "updated_email@tesla.com", "OldName", "OldLast", "2025-01-02"))

    return spark.createDataFrame(data, ["customer_id", "email", "first_name", "last_name", "created_at"])

# --- Core Logic: The MERGE (Upsert) ---
def perform_upsert(spark: SparkSession, df_incoming, target_path):
    """
    La Joya de la Corona: El Upsert.
    Si existe -> Actualiza.
    Si no existe -> Inserta.
    """
    print(f"   -> Ejecutando MERGE en {target_path}...")
    
    # 1. Definir la tabla destino (Silver)
    target_table = DeltaTable.forPath(spark, target_path)
    
    # 2. Ejecutar el Merge
    (target_table.alias("target")
        .merge(
            df_incoming.alias("source"),
            "target.customer_id = source.customer_id" # Llave de cruce
        )
        .whenMatchedUpdate(set={
            "email": col("source.email"),
            "first_name": col("source.first_name")
        })
        .whenNotMatchedInsert(values={
            "customer_id": col("source.customer_id"),
            "email": col("source.email"),
            "first_name": col("source.first_name"),
            "last_name": col("source.last_name"),
            "created_at": col("source.created_at")
        })
        .execute()
    )
    print("   ✅ Merge completado exitosamente.")

if __name__ == "__main__":
    setup_environment()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print("\n--- 🔄 INICIO DE CARGA INCREMENTAL (Día 2) ---")

    # 1. Obtener datos "frescos"
    df_new_data = generate_day_2_data(spark)
    
    # 2. (Opcional) Ingesta a Bronze - Saltamos este paso por brevedad y vamos directo al Merge
    
    # 3. Aplicar Merge en Silver
    perform_upsert(spark, df_new_data, "./silver/customers")

    # 4. Verificación
    print("\n--- 🔍 VERIFICACIÓN DE RESULTADOS ---")
    print("1. Buscando al Nuevo Cliente (Elon):")
    spark.read.format("delta").load("./silver/customers") \
        .filter(col("first_name") == "Elon").show()
        
    print("2. Buscando al Cliente Actualizado (@tesla.com):")
    spark.read.format("delta").load("./silver/customers") \
        .filter(col("email") == "updated_email@tesla.com").show()

    spark.stop()
