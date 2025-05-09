# Proyecto HBase - Análisis de Tweets de Elecciones 2020

Este proyecto implementa una solución para almacenar, consultar y manipular datos de tweets relacionados con las elecciones presidenciales de EE.UU. 2020 utilizando Apache HBase.

## Requisitos Previos

1. Apache HBase instalado y configurado
2. Python 3.6+
3. Biblioteca happybase para Python

## Instalación y Configuración

### 1. Configuración de HBase

Asegúrate de que HBase esté instalado y funcionando:

```bash
# Verificar si HBase está en ejecución
jps
```

Deberías ver procesos como `HMaster` en la lista.

### 2. Iniciar el servicio Thrift de HBase

El servicio Thrift es necesario para que Python pueda comunicarse con HBase:

```bash
# Método 1: Usando el comando hbase thrift start
hbase thrift start

# Método 2: Usando el daemon script (recomendado)
cd $HBASE_HOME && bin/hbase-daemon.sh start thrift
```

### 3. Instalación de Dependencias Python

```bash
pip install happybase
```

### 4. Descarga y extracción del Dataset

```bash
# Descargar el dataset de Kaggle
curl -L -o ~/Downloads/us-election-2020-tweets.zip \
  https://www.kaggle.com/api/v1/datasets/download/manchunhui/us-election-2020-tweets

# Crear directorio para los datos (si no existe)
mkdir -p data

# Extraer archivos CSV al directorio del proyecto
unzip ~/Downloads/us-election-2020-tweets.zip -d ./
```

## Estructura del Proyecto

- `hbase_twitter_operations.py`: Script principal que implementa todas las operaciones en HBase
- `documentacion_hbase.md`: Documentación detallada sobre la estructura y operaciones
- `hashtag_joebiden.csv`: Datos de tweets con el hashtag #JoeBiden
- `hashtag_donaldtrump.csv`: Datos de tweets con el hashtag #DonaldTrump

## Ejecución

1. Asegúrate de que HBase y el servicio Thrift estén en ejecución:

```bash
# Verificar procesos HBase
jps

# Verificar que el servidor Thrift esté escuchando en el puerto 9090
netstat -tuln | grep 9090
```

2. Ejecuta el script Python:

```bash
python hbase_twitter_operations.py
```

## Parámetros Configurables

El script tiene varios parámetros que puedes modificar según tus necesidades:

- `THRIFT_HOST`: Host donde se ejecuta el servicio Thrift (por defecto: 'localhost')
- `THRIFT_PORT`: Puerto del servicio Thrift (por defecto: 9090)
- `BATCH_SIZE`: Número de filas procesadas en cada lote (por defecto: 500)
- `max_rows_per_file`: Número máximo de filas a cargar de cada archivo (por defecto: 10000, se puede cambiar en la llamada a `load_data()`)

## Descripción de la Implementación

El script realiza las siguientes operaciones:

1. **Creación de la tabla**: Crea una tabla en HBase con cuatro familias de columnas: tweet_info, user_info, location y meta.

2. **Carga de datos**: Lee los archivos CSV y carga los datos en la tabla de HBase, procesando los datos en lotes.

3. **Operaciones de consulta**:
   - Consulta un tweet específico
   - Filtra tweets con más de 50 retweets
   - Consulta tweets de Estados Unidos

4. **Operaciones de escritura**:
   - Inserta un nuevo tweet
   - Actualiza un tweet existente
   - Elimina un tweet

## Salida Esperada

El script mostrará información detallada sobre cada operación, incluyendo:
- Confirmación de la creación de la tabla
- Progreso de la carga de datos
- Resultados de las consultas
- Detalles de las operaciones de escritura

## Solución de Problemas

- **Error de conexión**: Si el script no puede conectarse a HBase, verifica que el servicio Thrift esté activo y escuchando en el puerto correcto.

- **Problemas al cargar datos**: Reduce el valor de `max_rows_per_file` o `BATCH_SIZE` si experimentas problemas de memoria o tiempo de espera.

- **Errores de valores nulos**: El script está configurado para manejar valores nulos en los archivos CSV. Si encuentras errores relacionados, verifica el formato de los archivos.

- **Tiempo de espera en operaciones**: Algunas operaciones de HBase, como la eliminación de tablas, pueden tomar tiempo con conjuntos de datos grandes. Ten paciencia o ajusta tu implementación.

## Para Más Información

Consulta el archivo `documentacion_hbase.md` para obtener información detallada sobre:
- Estructura de la tabla en HBase
- Familias de columnas y su diseño
- Descripción detallada de las operaciones
- Ventajas de usar HBase para este caso de uso 