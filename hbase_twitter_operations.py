#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para operaciones con HBase usando datos de Twitter de las elecciones 2020
"""

import csv
import time
import happybase
import datetime

# Configuración
THRIFT_HOST = 'localhost'
THRIFT_PORT = 9090
BATCH_SIZE = 500
CSV_FILES = ['hashtag_joebiden.csv', 'hashtag_donaldtrump.csv']

class HBaseTwitterOperations:
    def __init__(self, host=THRIFT_HOST, port=THRIFT_PORT):
        """Inicializar conexión con HBase"""
        self.connection = happybase.Connection(host=host, port=port)
        
    def create_table(self, table_name=None):
        """Crear tabla en HBase con las familias de columnas apropiadas"""
        # Si no se proporciona nombre, crear uno único basado en timestamp
        if table_name is None:
            table_name = f'twitter_data_{int(time.time())}'
            
        print(f"\n{'='*80}")
        print(f"OPERACIÓN: CREACIÓN DE TABLA EN HBASE")
        print(f"{'='*80}")
        print(f"» Nombre de la tabla: '{table_name}'")
        
        # Verificar si la tabla ya existe
        if table_name.encode() in self.connection.tables():
            print(f"» Estado: La tabla '{table_name}' ya existe. Se usará la tabla existente.")
            return table_name
        
        # Definir familias de columnas:
        # 1. tweet_info: información básica sobre el tweet
        # 2. user_info: información sobre el usuario
        # 3. location: información de ubicación
        # 4. meta: metadatos adicionales
        families = {
            'tweet_info': dict(max_versions=1),
            'user_info': dict(max_versions=1),
            'location': dict(max_versions=1),
            'meta': dict(max_versions=1)
        }
        
        print(f"» Creando tabla con las siguientes familias de columnas:")
        for family in families:
            print(f"  - {family}: max_versions={families[family]['max_versions']}")
        
        self.connection.create_table(table_name, families)
        print(f"» Estado: Tabla '{table_name}' creada con éxito")
        print(f"» Timestamp de creación: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Mostrar información sobre todas las tablas en HBase
        all_tables = self.connection.tables()
        print(f"» Total de tablas en HBase: {len(all_tables)}")
        
        return table_name
    
    def load_data(self, table_name='twitter_data', max_rows_per_file=10000):
        """Cargar datos de los archivos CSV a la tabla de HBase, limitando a max_rows_per_file por archivo"""
        print(f"\n{'='*80}")
        print(f"OPERACIÓN: CARGA DE DATOS EN HBASE")
        print(f"{'='*80}")
        print(f"» Tabla destino: {table_name}")
        print(f"» Archivos de origen: {', '.join(CSV_FILES)}")
        print(f"» Configuración: BATCH_SIZE={BATCH_SIZE}, max_rows_per_file={max_rows_per_file}")
        
        start_time = time.time()
        table = self.connection.table(table_name)
        
        total_rows = 0
        
        for csv_file in CSV_FILES:
            print(f"\n» Procesando archivo: {csv_file}")
            file_start_time = time.time()
            
            # Contador de filas procesadas para este archivo
            file_rows = 0
            
            try:
                with open(csv_file, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    
                    # Información de columnas
                    print(f"  - Columnas detectadas: {', '.join(reader.fieldnames)}")
                    
                    # Procesar filas en lotes más pequeños para evitar bloqueos
                    batch = table.batch(batch_size=BATCH_SIZE)
                    
                    for row in reader:
                        # Limitar a max_rows_per_file filas por archivo
                        if file_rows >= max_rows_per_file:
                            print(f"  - Alcanzado el límite de {max_rows_per_file} filas para {csv_file}")
                            break
                            
                        # Usar tweet_id como clave de fila, o generar uno basado en timestamp si es None
                        if row['tweet_id']:
                            row_key = f"{row['tweet_id']}".encode()
                        else:
                            # Generar una clave única basada en timestamp y un contador
                            row_key = f"gen_tweet_{int(time.time())}_{file_rows}".encode()
                        
                        # Agrupar datos por familia de columnas
                        batch.put(row_key, {
                            # Información del tweet
                            'tweet_info:created_at': row['created_at'].encode() if row['created_at'] else b'',
                            'tweet_info:text': row['tweet'].encode() if row['tweet'] else b'',
                            'tweet_info:likes': row['likes'].encode() if row['likes'] else b'',
                            'tweet_info:retweet_count': row['retweet_count'].encode() if row['retweet_count'] else b'',
                            'tweet_info:source': row['source'].encode() if row['source'] else b'',
                            
                            # Información del usuario
                            'user_info:id': row['user_id'].encode() if row['user_id'] else b'',
                            'user_info:name': row['user_name'].encode() if row['user_name'] else b'',
                            'user_info:screen_name': row['user_screen_name'].encode() if row['user_screen_name'] else b'',
                            'user_info:description': row['user_description'].encode() if row['user_description'] else b'',
                            'user_info:join_date': row['user_join_date'].encode() if row['user_join_date'] else b'',
                            'user_info:followers_count': row['user_followers_count'].encode() if row['user_followers_count'] else b'',
                            'user_info:location': row['user_location'].encode() if row['user_location'] else b'',
                            
                            # Información de ubicación
                            'location:lat': row['lat'].encode() if row['lat'] else b'',
                            'location:long': row['long'].encode() if row['long'] else b'',
                            'location:city': row['city'].encode() if row['city'] else b'',
                            'location:country': row['country'].encode() if row['country'] else b'',
                            'location:continent': row['continent'].encode() if row['continent'] else b'',
                            'location:state': row['state'].encode() if row['state'] else b'',
                            'location:state_code': row['state_code'].encode() if row['state_code'] else b'',
                            
                            # Metadatos
                            'meta:collected_at': row['collected_at'].encode() if row['collected_at'] else b'',
                            'meta:file_source': csv_file.encode()
                        })
                        
                        file_rows += 1
                        
                        # Enviar el lote y crear uno nuevo cada BATCH_SIZE filas
                        if file_rows % BATCH_SIZE == 0:
                            print(f"  - Progreso: {file_rows} filas procesadas ({file_rows/max_rows_per_file*100:.1f}%) - Enviando lote...")
                            try:
                                batch.send()
                                # Crear un nuevo lote
                                batch = table.batch(batch_size=BATCH_SIZE)
                            except Exception as e:
                                print(f"  ! Error al enviar lote: {e}")
                                # Intentar continuar con un nuevo lote
                                batch = table.batch(batch_size=BATCH_SIZE)
                            
                # Enviar los datos restantes si quedan
                if file_rows % BATCH_SIZE != 0:
                    print(f"  - Enviando lote final con {file_rows % BATCH_SIZE} filas...")
                    try:
                        batch.send()
                    except Exception as e:
                        print(f"  ! Error al enviar lote final: {e}")
                
                file_elapsed_time = time.time() - file_start_time
                total_rows += file_rows
                print(f"  - Completado: {file_rows} filas cargadas de {csv_file}")
                print(f"  - Tiempo: {file_elapsed_time:.2f} segundos ({file_rows/file_elapsed_time:.1f} filas/segundo)")
                
            except Exception as e:
                print(f"  ! Error al cargar datos desde {csv_file}: {e}")
        
        elapsed_time = time.time() - start_time
        print(f"\n» Resumen de carga de datos:")
        print(f"  - Total de filas cargadas: {total_rows}")
        print(f"  - Tiempo total: {elapsed_time:.2f} segundos")
        print(f"  - Velocidad promedio: {total_rows/elapsed_time:.1f} filas/segundo")
    
    def query_operations(self, table_name='twitter_data'):
        """Realizar consultas sobre los datos"""
        print(f"\n{'='*80}")
        print(f"OPERACIÓN: CONSULTAS DE DATOS EN HBASE")
        print(f"{'='*80}")
        print(f"» Tabla de origen: {table_name}")
        
        table = self.connection.table(table_name)
        example_key = None
        
        # 1. Consulta básica: obtener un tweet específico
        print(f"\n» CONSULTA 1: Recuperación de un tweet específico (get)")
        print(f"  - Descripción: Obtener información detallada de un tweet usando su clave de fila")
        print(f"  - Tipo: Consulta punto a punto (row key)")
        
        # Tomar la primera fila como ejemplo
        start_time = time.time()
        for key, data in table.scan(limit=1):
            example_key = key
            
            # Formatear el texto del tweet (limitar a 100 caracteres)
            tweet_text = data.get(b'tweet_info:text', b'').decode()
            if len(tweet_text) > 100:
                tweet_text = tweet_text[:100] + "..."
            
            # Mostrar información del tweet
            print(f"  - Resultados:")
            print(f"    * Row key: {key.decode()}")
            print(f"    * Tweet: {tweet_text}")
            print(f"    * Usuario: {data.get(b'user_info:name', b'').decode()}")
            print(f"    * Fecha: {data.get(b'tweet_info:created_at', b'').decode()}")
            print(f"    * Ubicación: {data.get(b'user_info:location', b'').decode()}")
            print(f"    * Likes: {data.get(b'tweet_info:likes', b'').decode()}")
            print(f"    * Retweets: {data.get(b'tweet_info:retweet_count', b'').decode()}")
            
        elapsed_time = time.time() - start_time
        print(f"  - Tiempo de consulta: {elapsed_time*1000:.2f} ms")
        
        # 2. Filtrar tweets con más de X retweets
        print(f"\n» CONSULTA 2: Filtrado por número de retweets (scan + filter)")
        print(f"  - Descripción: Obtener tweets con más de 50 retweets")
        print(f"  - Tipo: Consulta con filtro SingleColumnValueFilter")
        print(f"  - Filtro: SingleColumnValueFilter('tweet_info', 'retweet_count', >=, 'binary:50')")
        
        count = 0
        retweet_counts = []
        start_time = time.time()
        
        scan_filter = b"SingleColumnValueFilter('tweet_info', 'retweet_count', >=, 'binary:50')"
        for key, data in table.scan(filter=scan_filter):
            retweet_count = int(data.get(b'tweet_info:retweet_count', b'0').decode() or '0')
            retweet_counts.append(retweet_count)
            
            if count < 5:  # Limitamos la salida a 5 ejemplos
                tweet_text = data.get(b'tweet_info:text', b'').decode()
                if len(tweet_text) > 50:
                    tweet_text = tweet_text[:50] + "..."
                    
                print(f"  - Ejemplo {count+1}:")
                print(f"    * Tweet: {tweet_text}")
                print(f"    * Retweets: {retweet_count}")
                print(f"    * Usuario: {data.get(b'user_info:name', b'').decode()}")
            count += 1
        
        elapsed_time = time.time() - start_time
        
        # Calcular estadísticas si hay resultados
        if retweet_counts:
            avg_retweets = sum(retweet_counts) / len(retweet_counts)
            max_retweets = max(retweet_counts)
            print(f"  - Estadísticas:")
            print(f"    * Total de tweets: {count}")
            print(f"    * Promedio de retweets: {avg_retweets:.1f}")
            print(f"    * Máximo de retweets: {max_retweets}")
        else:
            print(f"  - No se encontraron tweets con más de 50 retweets")
            
        print(f"  - Tiempo de consulta: {elapsed_time:.2f} segundos")
        
        # 3. Consultar tweets por país
        print(f"\n» CONSULTA 3: Filtrado por ubicación geográfica (scan + filter)")
        print(f"  - Descripción: Obtener tweets de Estados Unidos")
        print(f"  - Tipo: Consulta con filtro SingleColumnValueFilter")
        print(f"  - Filtro: SingleColumnValueFilter('location', 'country', =, 'binary:United States of America')")
        
        count = 0
        states = {}
        start_time = time.time()
        
        scan_filter = b"SingleColumnValueFilter('location', 'country', =, 'binary:United States of America')"
        for key, data in table.scan(filter=scan_filter):
            state = data.get(b'location:state', b'').decode()
            if state:
                states[state] = states.get(state, 0) + 1
                
            if count < 5:  # Limitamos la salida a 5 ejemplos
                tweet_text = data.get(b'tweet_info:text', b'').decode()
                if len(tweet_text) > 50:
                    tweet_text = tweet_text[:50] + "..."
                    
                print(f"  - Ejemplo {count+1}:")
                print(f"    * Tweet: {tweet_text}")
                print(f"    * Estado: {state}")
                print(f"    * Ciudad: {data.get(b'location:city', b'').decode()}")
            count += 1
        
        elapsed_time = time.time() - start_time
        
        # Mostrar estadísticas por estado
        if states:
            print(f"  - Estadísticas por estado (top 5):")
            for state, count in sorted(states.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"    * {state}: {count} tweets")
        
        print(f"  - Total de tweets de Estados Unidos: {count}")
        print(f"  - Tiempo de consulta: {elapsed_time:.2f} segundos")
        
        return example_key
    
    def write_operations(self, table_name='twitter_data', example_key=None):
        """Realizar operaciones de escritura (inserción, actualización, eliminación)"""
        print(f"\n{'='*80}")
        print(f"OPERACIÓN: ESCRITURA DE DATOS EN HBASE")
        print(f"{'='*80}")
        print(f"» Tabla objetivo: {table_name}")
        
        table = self.connection.table(table_name)
        
        # 1. Inserción de un nuevo tweet
        print(f"\n» OPERACIÓN 1: Inserción de un nuevo tweet (put)")
        print(f"  - Descripción: Insertar un tweet creado manualmente")
        print(f"  - Tipo: Operación de escritura - put()")
        
        # Generar clave única para el nuevo tweet
        start_time = time.time()
        new_row_key = f"custom_tweet_{int(time.time())}".encode()
        
        # Preparar datos a insertar
        tweet_data = {
            'tweet_info:created_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode(),
            'tweet_info:text': b'Este es un tweet de prueba insertado manualmente #HBase #BigData',
            'tweet_info:likes': b'0',
            'tweet_info:retweet_count': b'0',
            'tweet_info:source': b'HBase Python Client',
            
            'user_info:name': b'Usuario de Prueba',
            'user_info:screen_name': b'test_user',
            
            'location:country': b'Colombia',
            
            'meta:collected_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S').encode(),
            'meta:file_source': b'manual_insertion'
        }
        
        # Realizar la operación de inserción
        print(f"  - Insertando datos con row key: {new_row_key.decode()}")
        table.put(new_row_key, tweet_data)
        
        # Verificar la inserción
        row = table.row(new_row_key)
        
        elapsed_time = time.time() - start_time
        
        # Mostrar resultados
        print(f"  - Resultados:")
        print(f"    * Row key: {new_row_key.decode()}")
        print(f"    * Tweet: {row.get(b'tweet_info:text', b'').decode()}")
        print(f"    * Fecha: {row.get(b'tweet_info:created_at', b'').decode()}")
        print(f"    * Origen: {row.get(b'meta:file_source', b'').decode()}")
        print(f"  - Tiempo de operación: {elapsed_time*1000:.2f} ms")
        
        # 2. Actualización de un tweet existente
        if example_key:
            print(f"\n» OPERACIÓN 2: Actualización de un tweet existente (put)")
            print(f"  - Descripción: Modificar datos de un tweet ya almacenado")
            print(f"  - Tipo: Operación de escritura - put() sobre clave existente")
            print(f"  - Row key objetivo: {example_key.decode()}")
            
            start_time = time.time()
            
            # Mostrar información antes de actualizar
            row_before = table.row(example_key)
            likes_before = row_before.get(b'tweet_info:likes', b'0').decode()
            retweets_before = row_before.get(b'tweet_info:retweet_count', b'0').decode()
            
            print(f"  - Estado inicial:")
            print(f"    * Likes: {likes_before}")
            print(f"    * Retweets: {retweets_before}")
            
            # Preparar datos para actualización
            update_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            update_data = {
                'tweet_info:likes': b'999',
                'tweet_info:retweet_count': b'888',
                'meta:updated': b'True',
                'meta:update_time': update_time.encode()
            }
            
            # Ejecutar la actualización
            print(f"  - Actualizando datos...")
            table.put(example_key, update_data)
            
            # Mostrar información después de actualizar
            row_after = table.row(example_key)
            
            elapsed_time = time.time() - start_time
            
            print(f"  - Estado actualizado:")
            print(f"    * Likes: {row_after.get(b'tweet_info:likes', b'').decode()} (antes: {likes_before})")
            print(f"    * Retweets: {row_after.get(b'tweet_info:retweet_count', b'').decode()} (antes: {retweets_before})")
            print(f"    * Actualizado: {row_after.get(b'meta:updated', b'').decode()}")
            print(f"    * Hora de actualización: {row_after.get(b'meta:update_time', b'').decode()}")
            print(f"  - Tiempo de operación: {elapsed_time*1000:.2f} ms")
        else:
            print(f"\n» OPERACIÓN 2: Actualización de tweet (omitida)")
            print(f"  - No se pudo realizar la actualización porque no se encontró un tweet de ejemplo")
        
        # 3. Eliminar un tweet
        print(f"\n» OPERACIÓN 3: Eliminación de un tweet (delete)")
        print(f"  - Descripción: Eliminar un tweet de la tabla")
        print(f"  - Tipo: Operación de escritura - delete()")
        print(f"  - Row key a eliminar: {new_row_key.decode()}")
        
        start_time = time.time()
        
        # Primero comprobamos que existe
        row_before_delete = table.row(new_row_key)
        if row_before_delete:
            print(f"  - Estado inicial:")
            print(f"    * Tweet encontrado: {row_before_delete.get(b'tweet_info:text', b'').decode()}")
            
            # Eliminar el tweet
            table.delete(new_row_key)
            
            # Verificar que se eliminó
            row_after_delete = table.row(new_row_key)
            
            elapsed_time = time.time() - start_time
            
            if row_after_delete:
                print(f"  - Resultado: El tweet NO se eliminó correctamente (todavía existe)")
            else:
                print(f"  - Resultado: El tweet se eliminó correctamente (ya no existe en la tabla)")
            
            print(f"  - Tiempo de operación: {elapsed_time*1000:.2f} ms")
        else:
            print(f"  - Error: No se encontró el tweet con key {new_row_key.decode()}")
            
        # 4. Resumen de operaciones
        print(f"\n» RESUMEN DE OPERACIONES DE ESCRITURA:")
        print(f"  - Inserción: Se insertó 1 tweet con clave '{new_row_key.decode()}'")
        if example_key:
            print(f"  - Actualización: Se actualizó 1 tweet con clave '{example_key.decode()}'")
        else:
            print(f"  - Actualización: No se realizó (no se encontró tweet de ejemplo)")
        print(f"  - Eliminación: Se eliminó 1 tweet con clave '{new_row_key.decode()}'")
        print(f"  - Total de operaciones: {2 if example_key else 1} de 3 completadas con éxito")
    
    def run_all_operations(self):
        """Ejecutar todas las operaciones"""
        print(f"\n{'#'*80}")
        print(f"# INICIO DEL ANÁLISIS DE TWEETS CON HBASE")
        print(f"# Fecha y hora: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'#'*80}")
        
        table_name = None
        example_key = None
        
        try:
            # Crear tabla
            print("\n=== FASE 1: CREACIÓN DE TABLA EN HBASE ===")
            table_name = self.create_table()
        except Exception as e:
            print(f"! Error al crear la tabla: {e}")
            # Si no se pudo crear la tabla, intentar usar una existente
            table_name = 'twitter_data'
            print(f"! Intentando usar la tabla existente: {table_name}")
        
        try:
            # Cargar datos
            print("\n=== FASE 2: CARGA DE DATOS EN HBASE ===")
            if table_name:
                self.load_data(table_name, max_rows_per_file=1000)  # Limitar a 1000 filas por archivo para prueba
            else:
                print("! No se pudo cargar datos porque no hay una tabla disponible.")
        except Exception as e:
            print(f"! Error al cargar datos: {e}")
        
        try:
            # Consultar datos
            print("\n=== FASE 3: CONSULTAS DE DATOS EN HBASE ===")
            if table_name:
                example_key = self.query_operations(table_name)
            else:
                print("! No se pudieron realizar consultas porque no hay una tabla disponible.")
        except Exception as e:
            print(f"! Error al realizar consultas: {e}")
        
        try:
            # Operaciones de escritura
            print("\n=== FASE 4: OPERACIONES DE ESCRITURA EN HBASE ===")
            if table_name:
                self.write_operations(table_name, example_key)
            else:
                print("! No se pudieron realizar operaciones de escritura porque no hay una tabla disponible.")
        except Exception as e:
            print(f"! Error durante las operaciones de escritura: {e}")
        finally:
            # Cerrar conexión
            try:
                self.connection.close()
                print(f"\n{'#'*80}")
                print(f"# FIN DEL ANÁLISIS DE TWEETS CON HBASE")
                print(f"# Conexión a HBase cerrada correctamente.")
                print(f"{'#'*80}")
            except Exception as e:
                print(f"! Error al cerrar la conexión: {e}")

if __name__ == "__main__":
    # Ejecutar todas las operaciones
    ops = HBaseTwitterOperations()
    ops.run_all_operations() 