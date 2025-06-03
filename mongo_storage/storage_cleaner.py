from pymongo import MongoClient
from datetime import datetime, timezone 
import time
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_URI = "mongodb://mongo-storage:27017"
DB_NAME = "waze_db"
RAW_COLLECTION_NAME = "eventos_crudos" 
HOMOGENIZED_COLLECTION_NAME = "eventos_homogeneizados"

DEFAULT_COMUNA = "DESCONOCIDA" 

def esperar_mongo(cliente, intentos=10, intervalo=5):
    logging.info("Verificando conexión con MongoDB...")
    for i in range(intentos):
        try:
            cliente.admin.command("ping")
            logging.info("MongoDB operativo.")
            return True
        except ServerSelectionTimeoutError:
            logging.warning(f"Esperando MongoDB... intento {i+1}/{intentos}")
            time.sleep(intervalo)
    logging.error("MongoDB no respondió a tiempo.")
    return False

def validar_y_transformar_evento(evento_crudo):
    evento_homogeneizado = {}

    uuid = evento_crudo.get("uuid") 
    if not uuid and evento_crudo.get("evento_tipo_waze") == "user_location":
        uuid = evento_crudo.get("id")
    
    if not uuid:
        logging.debug(f"Evento descartado: Falta uuid o id. Evento: {evento_crudo.get('evento_tipo_waze')}")
        return None
    evento_homogeneizado["id_original"] = str(uuid)
    evento_homogeneizado["tipo_waze_original"] = evento_crudo.get("evento_tipo_waze")

    loc = evento_crudo.get("location")
    if not (isinstance(loc, dict) and "x" in loc and "y" in loc):
        logging.debug(f"Evento {uuid} descartado: Falta location.x o location.y.")
        return None
    try:
        evento_homogeneizado["longitud"] = float(loc["x"])
        evento_homogeneizado["latitud"] = float(loc["y"])
    except (ValueError, TypeError):
        logging.debug(f"Evento {uuid} descartado: location.x o location.y no son numéricos.")
        return None

    timestamp_evento_utc = None
    if "pubMillis" in evento_crudo and evento_crudo["pubMillis"] is not None:
        try:
            timestamp_evento_utc = datetime.fromtimestamp(evento_crudo["pubMillis"] / 1000.0, tz=timezone.utc)
        except (TypeError, ValueError) as e:
            logging.warning(f"Timestamp pubMillis ('{evento_crudo['pubMillis']}') inválido para evento {uuid}: {e}. Se intentará usar timestamp_scrape.")
            timestamp_evento_utc = None 

    if timestamp_evento_utc is None:
        raw_ts_scrape = evento_crudo.get("timestamp_scrape")
        if raw_ts_scrape:
            try:
                timestamp_evento_utc = datetime.fromisoformat(raw_ts_scrape.replace("Z", "+00:00"))
            except ValueError as e_iso:
                logging.error(f"Timestamp_scrape ('{raw_ts_scrape}') también inválido para evento {uuid}: {e_iso}")
                return None 
        else:
            logging.debug(f"Evento {uuid} descartado: Falta pubMillis válido y timestamp_scrape.")
            return None 
            
    evento_homogeneizado["timestamp_evento"] = timestamp_evento_utc
    
    ts_scrape_raw = evento_crudo.get("timestamp_scrape")
    if ts_scrape_raw:
        try:
            evento_homogeneizado["timestamp_scrape"] = datetime.fromisoformat(ts_scrape_raw.replace("Z", "+00:00"))
        except ValueError:
            logging.error(f"Timestamp_scrape ('{ts_scrape_raw}') para el scrapeo es inválido en evento {uuid}. Esto no debería pasar.")
            evento_homogeneizado["timestamp_scrape"] = None 
    else:
        logging.warning(f"Falta timestamp_scrape en evento {uuid}. Esto no debería pasar.")
        evento_homogeneizado["timestamp_scrape"] = None

   
    city_data = evento_crudo.get("city")
    if city_data and isinstance(city_data, str) and city_data.strip() != "" and city_data.lower().strip() != "n/a":
        evento_homogeneizado["comuna"] = city_data.strip()
    else:
        evento_homogeneizado["comuna"] = DEFAULT_COMUNA 

    tipo_waze = evento_crudo.get("evento_tipo_waze")
    
    if tipo_waze == "alerta":
        evento_homogeneizado["tipo_incidente_general"] = evento_crudo.get("type", "DESCONOCIDO").upper()
        evento_homogeneizado["subtipo_incidente"] = evento_crudo.get("subtype", "N/A").upper()
        evento_homogeneizado["descripcion"] = evento_crudo.get("street") if evento_crudo.get("street") else evento_crudo.get("reportDescription", "N/A")
        evento_homogeneizado["confianza"] = evento_crudo.get("confidence")
        evento_homogeneizado["fiabilidad"] = evento_crudo.get("reliability")
        
    elif tipo_waze == "jam": 
        evento_homogeneizado["tipo_incidente_general"] = "ATASCO"
        evento_homogeneizado["subtipo_incidente"] = f"NIVEL_{evento_crudo.get('level', 'N/A')}"
        evento_homogeneizado["descripcion"] = evento_crudo.get("street", "N/A")
        evento_homogeneizado["velocidad_kmh"] = evento_crudo.get("speedKMH")
        evento_homogeneizado["retraso_segundos"] = evento_crudo.get("delaySeconds") 
        if evento_homogeneizado["retraso_segundos"] is None and "delay" in evento_crudo: 
            evento_homogeneizado["retraso_segundos"] = evento_crudo.get("delay")
        
    elif tipo_waze == "user_location":
        evento_homogeneizado["tipo_incidente_general"] = "UBICACION_USUARIO"
        evento_homogeneizado["subtipo_incidente"] = "N/A"
        evento_homogeneizado["descripcion"] = "Posición de usuario"
        
    else:
        logging.warning(f"Tipo de evento Waze desconocido: {tipo_waze} para evento {uuid}")
        evento_homogeneizado["tipo_incidente_general"] = "DESCONOCIDO"
        

    if timestamp_evento_utc: 
        evento_homogeneizado["dia_semana_evento"] = timestamp_evento_utc.weekday() 
        evento_homogeneizado["hora_dia_evento"] = timestamp_evento_utc.hour
    else: 
        evento_homogeneizado["dia_semana_evento"] = None
        evento_homogeneizado["hora_dia_evento"] = None
    return evento_homogeneizado

def procesar_y_guardar_eventos():
    cliente_mongo = None
    cursor = None
    eventos_procesados_ok = 0
    eventos_descartados = 0
    count_final_homogeneizados_para_log = 0 

    try:
        cliente_mongo = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        if not esperar_mongo(cliente_mongo):
            return

        db = cliente_mongo[DB_NAME]
        coleccion_cruda = db[RAW_COLLECTION_NAME]
        coleccion_homogeneizada = db[HOMOGENIZED_COLLECTION_NAME]

        
        count_crudos_inicial = 0
        try:
            count_crudos_inicial = coleccion_cruda.count_documents({})
        except Exception as e_count_crudos:
            logging.error(f"Error al contar eventos crudos: {e_count_crudos}")
        logging.info(f"Total de eventos crudos a procesar: {count_crudos_inicial}")
        
        logging.info(f"Limpiando colección homogeneizada: {HOMOGENIZED_COLLECTION_NAME}")
        coleccion_homogeneizada.delete_many({})
        
        batch_size = 1000
        cursor = coleccion_cruda.find(no_cursor_timeout=False).batch_size(batch_size)
        eventos_para_insertar_lote = []

        for evento_crudo in cursor:
            evento_transformado = validar_y_transformar_evento(evento_crudo)
            if evento_transformado:
                eventos_para_insertar_lote.append(evento_transformado)
                eventos_procesados_ok += 1
            else:
                eventos_descartados += 1
            
            if len(eventos_para_insertar_lote) >= batch_size:
                if eventos_para_insertar_lote:
                    try:
                        coleccion_homogeneizada.insert_many(eventos_para_insertar_lote, ordered=False)
                        logging.info(f"Lote de {len(eventos_para_insertar_lote)} eventos insertado en '{HOMOGENIZED_COLLECTION_NAME}'.")
                    except Exception as e_batch: 
                        logging.error(f"Error al insertar lote: {e_batch}", exc_info=True)
                    finally:
                        eventos_para_insertar_lote = []

            if (eventos_procesados_ok + eventos_descartados) > 0 and (eventos_procesados_ok + eventos_descartados) % (batch_size * 2) == 0:
                 logging.info(f"Progreso: Procesados {eventos_procesados_ok + eventos_descartados} eventos crudos. Transformados OK: {eventos_procesados_ok}, Descartados: {eventos_descartados}")

        if eventos_para_insertar_lote:
            try:
                coleccion_homogeneizada.insert_many(eventos_para_insertar_lote, ordered=False)
                logging.info(f"Lote final de {len(eventos_para_insertar_lote)} eventos insertado.")
            except Exception as e_final: 
                logging.error(f"Error al insertar lote final: {e_final}", exc_info=True)
        
        if cliente_mongo is not None and coleccion_homogeneizada is not None: 
             count_final_homogeneizados_para_log = coleccion_homogeneizada.count_documents({})
        else:
            if cliente_mongo is None:
                logging.warning("cliente_mongo es None antes de contar documentos finales. Esto es inesperado.")
            if coleccion_homogeneizada is None: 
                logging.warning("coleccion_homogeneizada es None antes de contar documentos finales. Esto es inesperado.")
    except Exception as e_main:
        logging.error(f"Error principal durante el procesamiento: {e_main}", exc_info=True)
    finally:
        if cursor:
            try:
                cursor.close()
                logging.info("Cursor cerrado.")
            except Exception as e_cursor:
                logging.error(f"Error al cerrar el cursor: {e_cursor}")
        
        logging.info("--- Resumen del Filtrado y Homogeneización ---")
        logging.info(f"Total de eventos crudos leídos e intentados procesar: {eventos_procesados_ok + eventos_descartados}")
        logging.info(f"Eventos válidos y transformados (intentados guardar): {eventos_procesados_ok}")
        logging.info(f"Eventos descartados por invalidación: {eventos_descartados}")
        logging.info(f"Total de eventos en '{HOMOGENIZED_COLLECTION_NAME}' (conteo final): {count_final_homogeneizados_para_log}") 

        if cliente_mongo:
            try:
                cliente_mongo.close()
                logging.info("Conexión a MongoDB cerrada.")
            except Exception as e_close:
                logging.error(f"Error al cerrar la conexión a MongoDB: {e_close}")

if __name__ == "__main__":
    logging.info("Iniciando script de limpieza y homogeneización de datos...")
    procesar_y_guardar_eventos()
    logging.info("Script de limpieza y homogeneización finalizado.")