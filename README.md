# Análisis de Tweets sobre Elecciones EE.UU. 2020 con Big Data

Este proyecto demuestra el uso de tecnologías Big Data para procesar y analizar tweets relacionados con las elecciones presidenciales de EE.UU. 2020, comparando menciones a los candidatos Biden y Trump.

## Requisitos

- Python 3.6+
- Apache Spark 3.0+
- Apache Kafka 2.6+
- ZooKeeper (requerido para Kafka)
- Java 8 o superior

## Instalación de dependencias

```bash
pip install pyspark kafka-python matplotlib pandas numpy textblob findspark nltk
```

## Descargar datasets

```bash
curl -L -o ./us-election-2020-tweets.zip https://www.kaggle.com/api/v1/datasets/download/manchunhui/us-election-2020-tweets
```

## Descomprimir dataset

```bash
unzip us-election-2020-tweets.zip
```

## Estructura del proyecto

- `src/` - Scripts de procesamiento y análisis
  - `process_tweets_batch.py` - Procesamiento en batch de los datasets originales
  - `exploratory_analysis.py` - Análisis exploratorio de los datos procesados
  - `tweet_stream_generator.py` - Generador de stream para simular tweets en tiempo real
  - `spark_streaming_tweets.py` - Análisis en tiempo real con Spark Streaming
- `bash/` - Scripts de configuración de Kafka
  - `start-zookeeper.sh` - Iniciar Zookeeper
  - `start-kafka.sh` - Iniciar Kafka
  - `create-topic.sh` - Crear el topic para los tweets
- `us-election-2020-tweets.zip` - Dataset original
- `cleaned_tweets_parquet/` - Datos procesados en formato Parquet
- `processed_data/` - Datos procesados en formato CSV
- `analysis_results/` - Resultados del análisis exploratorio
- `streaming_results/` - Resultados del procesamiento en tiempo real

## Flujo de trabajo

### 1. Procesamiento por lotes (Batch)

El primer paso es procesar los archivos CSV originales y generar un conjunto de datos limpio:

```bash
python3 ./src/process_tweets_batch.py
```

Este script:
- Carga los datasets de tweets relacionados con Biden y Trump
- Limpia los datos (convierte tipos, maneja valores nulos, etc.)
- Guarda los resultados en formato Parquet optimizado para Spark

### 2. Análisis exploratorio de datos

Una vez procesados los datos, podemos realizar análisis exploratorios:

```bash
python3 ./src/exploratory_analysis.py
```

Este script:
- Carga los datos procesados en el paso anterior
- Realiza diversos análisis (distribución temporal, engagement, ubicación geográfica)
- Genera visualizaciones y archivos CSV con los resultados en la carpeta `analysis_results/`

### 3. Configuración de Kafka para procesamiento en tiempo real

Antes de ejecutar el análisis en tiempo real, necesitas configurar Kafka. Ejecuta estos pasos una sola vez para la instalación:

```bash
# Descargar e instalar Kafka (si no está instalado)
mkdir -p ~/kafka
cd ~/kafka
wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz
tar -xzf kafka_2.13-3.4.0.tgz
ln -s kafka_2.13-3.4.0 current

# Crear directorios para los datos
cd ~/kafka/current
mkdir -p data/zookeeper data/kafka

# Configurar Zookeeper y Kafka
echo "dataDir=$(pwd)/data/zookeeper" > config/zookeeper.properties.new
cat config/zookeeper.properties | grep -v dataDir >> config/zookeeper.properties.new
mv config/zookeeper.properties.new config/zookeeper.properties

sed -i "s|log.dirs=.*|log.dirs=$(pwd)/data/kafka|" config/server.properties
```

Una vez instalado, puedes usar los scripts proporcionados en este proyecto para iniciar los servicios:

```bash
# En la primera terminal, inicia Zookeeper
./bash/start-zookeeper.sh

# En la segunda terminal, inicia el servidor Kafka
./bash/start-kafka.sh

# En la tercera terminal, crea el topic para los tweets
./bash/create-topic.sh
```

### 4. Simulación de stream de tweets en tiempo real

Para simular un flujo de tweets en tiempo real:

```bash
python3 ./src/tweet_stream_generator.py [duración_en_minutos]
```

Este script:
- Carga una muestra de los tweets procesados
- Simula un flujo continuo enviando tweets a Kafka con variaciones aleatorias
- Por defecto, la simulación dura 10 minutos

### 5. Análisis en tiempo real con Spark Streaming

Finalmente, ejecutamos el análisis en tiempo real:

```bash
python3 ./src/spark_streaming_tweets.py
```

Este script:
- Se conecta al topic de Kafka y consume los tweets en tiempo real
- Realiza múltiples análisis en ventanas temporales deslizantes
- Muestra resultados en consola y guarda datos procesados para análisis posterior

## Resultados

Los resultados del análisis se guardan en:
- `processed_data/` - Datos procesados en formato Parquet
- `analysis_results/` - Resultados del análisis exploratorio en CSV y visualizaciones
- `streaming_results/` - Resultados del procesamiento en tiempo real

## Notas importantes

- Los archivos CSV originales deben estar en la raíz del proyecto
- Asegúrese de tener suficiente memoria disponible para procesar grandes volúmenes de datos
- Para datasets muy grandes, puede ser necesario ajustar la configuración de Spark en los scripts

## Autor

Grupo: 202016911_41
Curso: Big Data
