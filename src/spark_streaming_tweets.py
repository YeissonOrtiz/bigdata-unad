from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum, avg, explode, split, to_timestamp, length
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType
import os
import json
import sys
from datetime import datetime

# Definir el esquema para los tweets entrantes
tweet_schema = StructType([
    StructField("tweet_id", StringType(), True),
    StructField("text", StringType(), True),
    StructField("user", StringType(), True),
    StructField("candidate", StringType(), True),
    StructField("likes", IntegerType(), True),
    StructField("retweets", IntegerType(), True),
    StructField("followers", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", StringType(), True)  # Cambiado a StringType para recibir cadenas de texto
])

def create_spark_session():
    """Crear y configurar la sesión de Spark para streaming"""
    return SparkSession.builder \
        .appName("Twitter Election Stream Analysis") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.default.parallelism", "8") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
        .getOrCreate()

def read_kafka_stream(spark, kafka_bootstrap_servers, topic_name):
    """Leer el stream de datos desde Kafka"""
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topic_name) \
        .option("startingOffsets", "latest") \
        .load()

def process_tweets(kafka_df, schema):
    """Procesar el stream de tweets"""
    # Extraer el valor del mensaje como string y convertirlo a JSON
    json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_value")
    
    # Parsear JSON a columnas estructuradas
    parsed_df = json_df.select(
        from_json(col("json_value"), schema).alias("tweet_data")
    ).select("tweet_data.*")
    
    # Convertir el timestamp a formato adecuado
    tweets_df = parsed_df.withColumn(
        "timestamp", 
        to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
    )
    
    return tweets_df

def analyze_stream(tweets_df):
    """Realizar diferentes análisis sobre el stream de tweets"""
    # Directorio para guardar los resultados
    output_dir = "streaming_results"
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Conteo de tweets por candidato en ventanas de tiempo
    candidate_counts = tweets_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute", "30 seconds"),
            col("candidate")
        ) \
        .count() \
        .orderBy(col("window").desc(), col("count").desc())
    
    # Guardar los resultados en archivos
    candidate_query = candidate_counts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # 2. Engagement en tiempo real (likes y retweets)
    engagement_stream = tweets_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute", "30 seconds"),
            col("candidate")
        ) \
        .agg(
            avg("likes").alias("avg_likes"),
            avg("retweets").alias("avg_retweets"),
            sum("likes").alias("total_likes"),
            sum("retweets").alias("total_retweets"),
            count("*").alias("tweet_count")
        ) \
        .orderBy(col("window").desc())
    
    engagement_query = engagement_stream \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    # 3. Análisis de palabras clave más mencionadas
    words_stream = tweets_df \
        .select(
            col("timestamp"),
            explode(split(col("text"), "\\s+")).alias("word"),
            col("candidate")
        ) \
        .filter(col("word").rlike("^[A-Za-z]+$") & (length(col("word")) > 3)) \
        .filter(~col("word").isin(["https", "http", "the", "and", "this", "that", "with", "for"])) \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), "1 minute", "30 seconds"),
            col("candidate"),
            col("word")
        ) \
        .count() \
        .orderBy(col("window").desc(), col("count").desc())
    
    words_query = words_stream \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()
        
    # 4. Guardar datos procesados en CSV para análisis posterior
    output_path = f"{output_dir}/processed_tweets"
    
    # Utiliza foreachBatch para guardar los resultados
    def save_to_csv(batch_df, batch_id):
        # Solo guardar si hay datos
        if not batch_df.isEmpty():
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            # Guardar en formato CSV
            batch_df.coalesce(1).write.mode("append") \
                .option("header", "true") \
                .csv(f"{output_path}")
    
    # Configurar el escritor para guardar los tweets procesados
    save_query = tweets_df \
        .writeStream \
        .foreachBatch(save_to_csv) \
        .outputMode("append") \
        .start()
    
    # Retorna todos los queries para controlarlos más tarde
    return [candidate_query, engagement_query, words_query, save_query]

def run_streaming_analysis(kafka_bootstrap_servers="localhost:9092", topic_name="election-tweets"):
    """Ejecuta el análisis completo de streaming"""
    try:
        # Crear la sesión de Spark
        spark = create_spark_session()
        
        # Leer el stream desde Kafka
        kafka_stream = read_kafka_stream(spark, kafka_bootstrap_servers, topic_name)
        
        # Procesar y analizar los tweets
        tweets_df = process_tweets(kafka_stream, tweet_schema)
        
        # Realizar los análisis
        queries = analyze_stream(tweets_df)
        
        print(f"Análisis de streaming iniciado. Procesando tweets desde el topic '{topic_name}'...")
        print("Presiona Ctrl+C para detener.")
        
        # Esperar a que los queries terminen
        for query in queries:
            query.awaitTermination()
            
    except KeyboardInterrupt:
        print("\nDetención manual del streaming...")
        # Las consultas se detendrán automáticamente debido a la opción stopGracefullyOnShutdown
    except Exception as e:
        print(f"Error durante el análisis de streaming: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        try:
            # Asegurarse de que Spark se cierre adecuadamente
            if 'spark' in locals():
                spark.stop()
                print("Spark session terminada.")
        except:
            pass

if __name__ == "__main__":
    # Obtener parámetros de configuración desde línea de comandos
    kafka_server = "localhost:9092"
    topic = "election-tweets"
    
    # Permitir configuración desde argumentos
    if len(sys.argv) > 1:
        kafka_server = sys.argv[1]
    if len(sys.argv) > 2:
        topic = sys.argv[2]
    
    print(f"Iniciando análisis de streaming con Kafka en {kafka_server} y topic {topic}")
    run_streaming_analysis(kafka_server, topic) 