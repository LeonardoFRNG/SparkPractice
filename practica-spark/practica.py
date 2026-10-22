from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round as _round

#1 iniciamos spark con compatibilidad para jdk 21

spark = SparkSession.builder \
    .appName("PracticaSpark") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .getOrCreate()

# Ocultar mensajes de logs innecesarios ya que spark suele ser verboso
spark.sparkContext.setLogLevel("WARN")

#2 Leer el .CSV

df = spark.read.csv("ventas.csv", header = True, inferSchema = True)

print("\n--- DATOS ORIGINALES ---")
df.show()

#3 Creamos una nueva columna: total = precio * cantidad
df_total = df.withColumn("total_venta", _round(col("precio") * col("cantidad"), 2))

#4 Filtrar: solo productos con precio mayor a 30
df_filtrado = df_total.filter(col("precio") > 30)

print("\n--- PRODUCTOS CON PRECIO > 30 ---")
df_filtrado.show()

# 5. Agrupación y totales por categoría
resumen = df_total.groupBy("categoria").agg(
    _sum("total_venta").alias("ingresos_totales"),
    _sum("cantidad").alias("unidades_vendidas")
).orderBy(col("ingresos_totales").desc())

print("\n--- RESUMEN POR CATEGORIA ---")
resumen.show()

#6 cerrar sesion de Spark
spark.stop()