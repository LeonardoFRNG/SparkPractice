from practica import csv_path
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round as _round, avg as _avg, count as _count, when

spark = SparkSession.builder \
    .appName("PracticaSpark2") \
    .master("local[*]") \
    .config("spark.driver.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .config("spark.executor.extraJavaOptions", "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

csv_path = Path(__file__).parent / "pedidos.csv"
df_raw = spark.read.csv(str(csv_path), header=True, inferSchema=True)

print("---DATOS NORMALES---")
df_raw.show()

#eliminar filas con nulos en precio_unitario, cantidad.df_raw
df_limpio = df_raw.dropna(subset=["precio_unitario", "cantidad"])

print("---DATOS LIMPIOS---")
df_limpio.show()

df_monto_total = df_limpio.withColumn("monto_total", _round(col("precio_unitario") * col("cantidad"), 2))

df_segmentado = df_monto_total.withColumn(
    "tipo_compra",
    when(col("monto_total") >= 150, "premium").otherwise("Estandar")
)

print("---DATOS CON MONTO Y SEGMENTO---")
df_segmentado.show()

resumen_ciudades = df_segmentado.groupBy("ciudad").agg(
    _round(_sum("monto_total"), 2).alias("total_recaudado"),
    _round(_avg("monto_total"), 2).alias("promedio_por_pedido"),
    _count("id_pedido").alias("total_pedidos")
).filter(col("total_recaudado") > 100 ) \
 .orderBy(col("total_recaudado").desc()) 

print("\n--- RESUMEN POR CIUDAD (RECAUDO > 100) ---")
resumen_ciudades.show()

spark.stop()
