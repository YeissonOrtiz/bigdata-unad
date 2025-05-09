# Documentación HBase - Análisis de Tweets de Elecciones 2020

## 1. Estructura de la Tabla en HBase

La tabla para almacenar los datos de Twitter se ha diseñado con las siguientes familias de columnas:

### Familias de Columnas

1. **tweet_info**: Almacena la información básica del tweet
   - created_at: Fecha y hora de creación del tweet
   - text: Contenido del tweet
   - likes: Número de "me gusta" del tweet
   - retweet_count: Número de retweets
   - source: Plataforma o aplicación desde donde se publicó el tweet

2. **user_info**: Almacena información sobre el autor del tweet
   - id: ID del usuario
   - name: Nombre del usuario
   - screen_name: Nombre de usuario en Twitter
   - description: Descripción del perfil del usuario
   - join_date: Fecha en que el usuario se unió a Twitter
   - followers_count: Número de seguidores
   - location: Ubicación indicada en el perfil del usuario

3. **location**: Información geográfica del tweet (cuando está disponible)
   - lat: Latitud
   - long: Longitud
   - city: Ciudad
   - country: País
   - continent: Continente
   - state: Estado/Provincia
   - state_code: Código del estado/provincia

4. **meta**: Metadatos adicionales sobre el tweet
   - collected_at: Fecha y hora en que se recopiló el tweet
   - file_source: Archivo de origen del tweet
   - updated: Indica si el tweet ha sido actualizado (solo presente en tweets modificados)
   - update_time: Hora de la última actualización (solo presente en tweets modificados)

### Clave de Fila (Row Key)

- Para los tweets originales: Se utiliza el ID del tweet como clave de fila.
- Para tweets con ID ausente: Se genera una clave basada en timestamp y un contador: `gen_tweet_{timestamp}_{counter}`.
- Para tweets insertados manualmente: Se utiliza un prefijo "custom_tweet_" seguido de un timestamp.

## 2. Operaciones Realizadas en HBase

### 2.1 Creación de la Tabla

El script crea una tabla con un nombre único basado en timestamp (formato: `twitter_data_{timestamp}`). Esta estrategia evita conflictos con tablas existentes y elimina la necesidad de borrar tablas previas.

Cada tabla se crea con las cuatro familias de columnas mencionadas anteriormente. Cada familia tiene configurada una versión máxima de 1, lo que significa que HBase solo mantendrá la versión más reciente de cada columna.

### 2.2 Carga de Datos

Los datos se cargan desde dos archivos CSV:
- hashtag_joebiden.csv
- hashtag_donaldtrump.csv

El proceso de carga implementa las siguientes optimizaciones:

1. **Procesamiento por lotes**: Se utiliza un enfoque de batch (por defecto: 500 registros por lote) para optimizar el rendimiento de escritura.
2. **Limitación de filas**: Se puede definir un límite máximo de filas a procesar por archivo (por defecto: 10,000) para facilitar pruebas y reducir tiempos de procesamiento.
3. **Gestión de valores nulos**: El script maneja correctamente los valores nulos en los archivos CSV, convirtiéndolos a cadenas vacías al almacenarlos en HBase.
4. **Generación de claves para registros incompletos**: Si un tweet no tiene ID, se genera automáticamente una clave única basada en timestamp.
5. **Manejo de errores**: El proceso incluye manejo de excepciones para continuar la carga incluso si hay problemas con registros individuales.

### 2.3 Operaciones de Consulta

#### 2.3.1 Consulta Básica
Se implementó la capacidad de recuperar un tweet específico utilizando su ID como clave de fila. Esto demuestra la funcionalidad de obtención de datos punto a punto de HBase.

#### 2.3.2 Consulta con Filtros
Se implementaron consultas con filtros para:
1. **Tweets con alta interacción**: Se filtran tweets con más de 50 retweets.
2. **Tweets por ubicación geográfica**: Se filtran tweets originados en Estados Unidos.

Estas consultas utilizan el método `scan()` de HBase con filtros de tipo `SingleColumnValueFilter`.

### 2.4 Operaciones de Escritura

#### 2.4.1 Inserción
Se demostró la capacidad de insertar un nuevo tweet en la tabla utilizando el método `put()` de HBase. El tweet insertado incluye datos en las cuatro familias de columnas.

#### 2.4.2 Actualización
Se implementó la actualización de un tweet existente, modificando su número de likes y retweets, y agregando metadatos adicionales sobre la actualización (meta:updated y meta:update_time).

#### 2.4.3 Eliminación
Se demostró la eliminación de un tweet utilizando el método `delete()` de HBase, con verificación previa y posterior para confirmar el éxito de la operación.

## 3. Resultados Obtenidos

### 3.1 Rendimiento de Carga

La carga de datos desde los archivos CSV se optimizó mediante:
- Procesamiento por lotes para reducir el número de operaciones de red
- Envío periódico de lotes para evitar saturación de memoria
- Manejo de errores para garantizar la continuidad del proceso

El tiempo de carga varía según el volumen de datos procesado, pero con las optimizaciones implementadas, el proceso ha demostrado ser eficiente incluso con grandes volúmenes de datos.

### 3.2 Efectividad de las Consultas

Las consultas demostraron ser efectivas para:
- Recuperar información específica de tweets individuales.
- Filtrar tweets según criterios numéricos (como el número de retweets).
- Filtrar tweets por ubicación geográfica.

### 3.3 Operaciones de Escritura

Las operaciones de escritura (inserción, actualización y eliminación) funcionaron correctamente, demostrando la capacidad de HBase para:
- Almacenar nuevos datos.
- Actualizar datos existentes.
- Eliminar datos que ya no son necesarios.

## 4. Ventajas de Utilizar HBase para este Caso

1. **Escalabilidad**: HBase puede manejar grandes volúmenes de datos, lo que es ideal para el análisis de tweets durante eventos importantes.

2. **Modelo de datos flexible**: La estructura de familias de columnas permite organizar los datos de manera lógica y eficiente.

3. **Alto rendimiento en consultas por clave**: HBase es muy eficiente para recuperar datos utilizando las claves de fila.

4. **Capacidad para filtrar datos**: HBase permite aplicar filtros para seleccionar subconjuntos específicos de datos.

5. **Escritura eficiente**: El procesamiento por lotes permite escrituras rápidas y eficientes.

6. **Manejo de datos dispersos**: HBase es eficiente para almacenar datos donde muchos valores pueden estar ausentes.

## 5. Consideraciones y Mejoras Implementadas

1. **Nombres de tabla únicos**: Utilizamos nombres de tabla basados en timestamp para evitar conflictos y operaciones de eliminación costosas.

2. **Tamaño de lote optimizado**: Se ajustó el tamaño de lote (BATCH_SIZE) para equilibrar rendimiento y uso de memoria.

3. **Manejo de valores nulos**: Se implementó detección y manejo de valores nulos para aumentar la robustez del procesamiento.

4. **División del procesamiento**: Las operaciones se dividieron claramente (creación, carga, consulta, escritura) con manejo de errores independiente.

5. **Limitación de datos procesados**: Se implementó la capacidad de limitar el número de filas procesadas para facilitar pruebas y desarrollo.

## 6. Mejoras Futuras Posibles

1. **Diseño de clave de fila**: Para mejorar la distribución de datos, se podría considerar un diseño más sofisticado de la clave de fila, como agregar un prefijo basado en la fecha o el hash del usuario.

2. **Compresión de datos**: Para conjuntos de datos más grandes, se podría habilitar la compresión para reducir el espacio de almacenamiento.

3. **Índices secundarios**: Para mejorar el rendimiento de consultas no basadas en la clave de fila, se podrían implementar índices secundarios.

4. **Particionamiento de tablas**: Para mejorar aún más la escalabilidad, se podría considerar el particionamiento de la tabla basado en criterios específicos.

5. **Integración con herramientas de análisis**: Conectar HBase con herramientas como Spark o Hadoop para realizar análisis más complejos sobre los datos almacenados. 