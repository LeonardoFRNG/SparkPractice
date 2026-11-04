from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round as _round

#Inicializamos el cluster localmente
spark = SparkSession.builder \
    .appName("PracticaSpark") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

#Ingesta de los datos
csv_path = Path(__file__).parent / "ventas.csv"
df = spark.read.csv(str(csv_path), header=True, inferSchema=True)

print("\n--- DATOS ORIGINALES ---")
df.show()

#Aca se crea una nueva columna pero ahora agregamos el total: precio * cantidad
df_total = df.withColumn("total_venta", _round(col("precio") * col("cantidad"), 2))

#Filtrado de registros
df_filtrado = df_total.filter(col("precio") > 30)

print("\n--- PRODUCTOS CON PRECIO > 30 ---")

df_filtrado.show()

#Resumen por categoria

resumen = df_total.groupBy("categoria").agg(
    _sum("total_venta").alias("ingresos_totales"),
    _sum("cantidad").alias("unidades_vendidas")
).orderBy(col("ingresos_totales").desc())

print("\n--- RESUMEN POR CATEGORIA ---")
resumen.show()

#Cerramos la sesion de spark
spark.stop()    
