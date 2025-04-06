import json
import time
import random
import sys
import os
from datetime import datetime, timedelta
from kafka import KafkaProducer
from pyspark.sql import SparkSession

def load_sample_tweets(sample_size=1000):
    """Carga una muestra de tweets desde los archivos procesados para simular el stream"""
    try:
        # Inicializar SparkSession temporalmente para cargar datos
        spark = SparkSession.builder \
            .appName("Tweet Sample Loader") \
            .master("local[*]") \
            .getOrCreate()
            
        # Verificar si existe el archivo de datos procesados
        data_path = "cleaned_tweets_parquet"
        if not os.path.exists(data_path):
            # Intentar con la ruta alternativa 
            data_path = "processed_data/cleaned_tweets_parquet"
            if not os.path.exists(data_path):
                print(f"Error: No se encontró el directorio de datos procesados.")
                print("Por favor, ejecute primero el script process_tweets_batch.py")
                return []
            
        # Cargar los datos y obtener una muestra
        tweets_df = spark.read.parquet(data_path)
        sample_tweets = tweets_df.sample(withReplacement=False, fraction=min(1.0, sample_size/tweets_df.count())) \
                                .limit(sample_size) \
                                .select("tweet_id", "tweet", "user_screen_name", "candidate", 
                                        "likes", "retweet_count", "user_followers_count", "country") \
                                .collect()
                                
        # Convertir a lista de diccionarios
        tweets_list = [{
            "tweet_id": row["tweet_id"],
            "text": row["tweet"],
            "user": row["user_screen_name"],
            "candidate": row["candidate"],
            "likes": row["likes"] if row["likes"] is not None else 0,
            "retweets": row["retweet_count"] if row["retweet_count"] is not None else 0,
            "followers": row["user_followers_count"] if row["user_followers_count"] is not None else 0,
            "country": row["country"] if row["country"] is not None else "Unknown",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        } for row in sample_tweets]
        
        # Cerrar la sesión de Spark
        spark.stop()
        
        print(f"Cargados {len(tweets_list)} tweets de muestra para simulación")
        return tweets_list
        
    except Exception as e:
        print(f"Error al cargar tweets de muestra: {str(e)}")
        import traceback
        traceback.print_exc()
        return []

def create_kafka_producer():
    """Crea y retorna un productor de Kafka"""
    try:
        # Crear productor de Kafka (asegúrate de tener Kafka ejecutándose en este host/puerto)
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10)  # Especifica la versión de Kafka API
        )
        return producer
    except Exception as e:
        print(f"Error al crear productor Kafka: {str(e)}")
        return None

def simulate_live_tweets(producer, tweets, topic_name="election-tweets", duration_minutes=10):
    """Simula un flujo de tweets en tiempo real enviándolos a Kafka"""
    if not producer or not tweets:
        print("No se puede simular el flujo: productor o tweets no disponibles")
        return
        
    print(f"Iniciando simulación de stream por {duration_minutes} minutos")
    print(f"Enviando tweets al topic de Kafka: {topic_name}")
    
    # Hora de inicio y fin
    start_time = datetime.now()
    end_time = start_time + timedelta(minutes=duration_minutes)
    
    count = 0
    try:
        while datetime.now() < end_time:
            # Seleccionar un tweet aleatorio
            tweet = random.choice(tweets)
            
            # Actualizar timestamp para simular tiempo real
            tweet["timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Añadir algunas variaciones aleatorias para hacer más realista la simulación
            tweet["likes"] = max(0, tweet["likes"] + random.randint(-5, 10))
            tweet["retweets"] = max(0, tweet["retweets"] + random.randint(-3, 5))
            
            # Enviar a Kafka
            producer.send(topic_name, tweet)
            count += 1
            
            # Imprimir progreso
            if count % 10 == 0:
                print(f"Enviados {count} tweets a Kafka. Último: {tweet['user']} - {tweet['text'][:30]}...")
                
            # Pequeña pausa para simular stream (ajustar para más o menos intensidad)
            time.sleep(random.uniform(0.5, 2.0))
            
        # Asegurarse que todos los mensajes se envíen
        producer.flush()
        print(f"Simulación terminada. Total de tweets enviados: {count}")
        
    except KeyboardInterrupt:
        print("\nSimulación interrumpida por el usuario")
    except Exception as e:
        print(f"Error durante la simulación: {str(e)}")
    finally:
        if producer:
            producer.close()
            
if __name__ == "__main__":
    # Configurar duración de la simulación desde argumentos o valor por defecto
    duration = 10  # minutos por defecto
    if len(sys.argv) > 1:
        try:
            duration = int(sys.argv[1])
        except ValueError:
            print("Argumento inválido. Usando duración por defecto (10 minutos)")
    
    # Cargar tweets de muestra
    sample_tweets = load_sample_tweets(sample_size=300)
    
    if sample_tweets:
        # Crear productor Kafka
        kafka_producer = create_kafka_producer()
        
        if kafka_producer:
            # Iniciar simulación
            simulate_live_tweets(
                producer=kafka_producer,
                tweets=sample_tweets,
                topic_name="election-tweets",
                duration_minutes=duration
            )
        else:
            print("No se pudo iniciar el productor de Kafka. Verifique que Kafka esté en ejecución.")
    else:
        print("No se pudieron cargar los tweets de muestra. Verifique que los datos procesados estén disponibles.") 