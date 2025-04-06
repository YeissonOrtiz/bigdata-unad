from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, when, regexp_replace, length, isnull, count, isnan, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import os

# Inicializar SparkSession
spark = SparkSession.builder \
    .appName("US Election Tweets Processing") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

# Definir las rutas a los archivos CSV
biden_file_path = "hashtag_joebiden.csv"
trump_file_path = "hashtag_donaldtrump.csv"

# Definir el esquema explícito para los CSV
# Esto evitará el warning de discrepancia entre cabecera y esquema
tweet_schema = StructType([
    StructField("tweet_id", StringType(), True),
    StructField("tweet", StringType(), True),
    StructField("user_screen_name", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("user_join_date", StringType(), True),
    StructField("user_followers_count", StringType(), True),
    StructField("user_location", StringType(), True),
    StructField("lat", StringType(), True),
    StructField("long", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("continent", StringType(), True),
    StructField("state", StringType(), True),
    StructField("date_time", StringType(), True),
    StructField("timezone", StringType(), True),
    StructField("language", StringType(), True),
    StructField("retweet_count", StringType(), True),
    StructField("likes", StringType(), True),
    StructField("source", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("collected_at", StringType(), True)
])

print(f"Leyendo datos de Biden desde: {os.path.abspath(biden_file_path)}")
print(f"Leyendo datos de Trump desde: {os.path.abspath(trump_file_path)}")

# Cargar los datasets con el esquema explícito
print("Cargando datasets...")

# Cargar tweets de Biden con esquema explícito
biden_df = spark.read.csv(
    biden_file_path, 
    header=True,
    schema=tweet_schema,
    multiLine=True, 
    escape='"',
    mode="DROPMALFORMED"  # Ignorar filas que no se ajusten al esquema
)
biden_df = biden_df.withColumn("candidate", lit("Biden"))  # Añadir columna para identificar

# Cargar tweets de Trump con esquema explícito
trump_df = spark.read.csv(
    trump_file_path, 
    header=True,
    schema=tweet_schema,
    multiLine=True, 
    escape='"',
    mode="DROPMALFORMED"  # Ignorar filas que no se ajusten al esquema
)
trump_df = trump_df.withColumn("candidate", lit("Trump"))  # Añadir columna para identificar

# Combinar ambos dataframes
tweets_df = biden_df.union(trump_df)

# Mostrar el esquema para entender la estructura de datos
print("Esquema del dataset combinado:")
tweets_df.printSchema()

# Mostrar información básica sobre el dataset
print(f"Número total de registros: {tweets_df.count()}")
print(f"Registros de Biden: {biden_df.count()}")
print(f"Registros de Trump: {trump_df.count()}")
print("Muestra de los datos combinados:")
tweets_df.show(5, truncate=False)

# Análisis inicial de datos faltantes por columna
print("Conteo de valores nulos por columna:")
for column in tweets_df.columns:
    null_count = tweets_df.filter(col(column).isNull() | isnan(col(column)) | (col(column) == "")).count()
    print(f"{column}: {null_count} valores nulos ({(null_count/tweets_df.count())*100:.2f}%)")

# Limpieza de datos
print("Limpiando datos...")

# 1. Convertir fechas a formato timestamp
cleaned_df = tweets_df.withColumn("created_at", to_timestamp(col("created_at")))
cleaned_df = cleaned_df.withColumn("user_join_date", to_timestamp(col("user_join_date")))
cleaned_df = cleaned_df.withColumn("collected_at", to_timestamp(col("collected_at")))

# 2. Convertir columnas numéricas al tipo adecuado
cleaned_df = cleaned_df.withColumn("likes", col("likes").cast(IntegerType()))
cleaned_df = cleaned_df.withColumn("retweet_count", col("retweet_count").cast(IntegerType()))
cleaned_df = cleaned_df.withColumn("user_followers_count", col("user_followers_count").cast(IntegerType()))
cleaned_df = cleaned_df.withColumn("lat", col("lat").cast(FloatType()))
cleaned_df = cleaned_df.withColumn("long", col("long").cast(FloatType()))

# 3. Limpiar texto de tweets (eliminar caracteres especiales, etc.)
cleaned_df = cleaned_df.withColumn("tweet", regexp_replace(col("tweet"), "[\\r\\n]", " "))

# 4. Filtrar tweets vacíos o demasiado cortos
cleaned_df = cleaned_df.filter(length(col("tweet")) > 5)

# 5. Manejar valores nulos en campos importantes
cleaned_df = cleaned_df.na.fill({
    "likes": 0,
    "retweet_count": 0
})

# Guardar el dataset limpio en formato parquet (más eficiente para Spark)
output_path = "cleaned_tweets_parquet"
print(f"Guardando datos limpios en formato parquet en: {output_path}")
cleaned_df.write.mode("overwrite").parquet(output_path)

# Guardar también una versión en CSV si es necesario
csv_output_path = "cleaned_tweets.csv"
print(f"Guardando datos limpios en formato CSV en: {csv_output_path}")
cleaned_df.write.mode("overwrite").option("header", "true").csv(csv_output_path)

# Mostrar información sobre el dataset limpio
print(f"Número de registros después de la limpieza: {cleaned_df.count()}")
print("Distribución por candidato después de la limpieza:")
cleaned_df.groupBy("candidate").count().show()
print("Muestra de los datos limpios:")
cleaned_df.show(5, truncate=False)

# Crear directorio para el procesamiento en streaming
os.makedirs("processed_data", exist_ok=True)
print("Creando copia de los datos procesados para el streaming...")
cleaned_df.write.mode("overwrite").parquet("processed_data/cleaned_tweets_parquet")

# Cerrar la sesión de Spark
spark.stop()