import time
import requests
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError, OperationFailure
from datetime import datetime
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


MONGO_URI = 'mongodb://mongo-storage:27017'
DB_NAME = 'waze_db'
RAW_EVENTS_COLLECTION_NAME = 'eventos_crudos'



ZONA_RM = {
    "norte": -33.35, 
    "sur": -33.45,
    "oeste": -70.6,
    "este": -70.5,
    "tipos": "alerts,traffic,users" 
}


SCRAPING_INTERVAL_SECONDS = 30 

MAX_TOTAL_EVENTS_TO_COLLECT = 10000 

API_URL_TEMPLATE = (
    "https://www.waze.com/live-map/api/georss"
    "?top={norte}&bottom={sur}&left={oeste}&right={este}"
    "&env=row&types={tipos}"
)

def get_api_url(zone_config):
    return API_URL_TEMPLATE.format(**zone_config)

def establecer_conexion_mongo():
    logging.info(f"Estableciendo conexión con MongoDB en {MONGO_URI}...")
    try:
        cliente = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        logging.info(f"Cliente MongoClient creado. Intentando ping a MongoDB...")
       
        resultado_ping = cliente.admin.command("ping")
        logging.info(f"Resultado del PING a MongoDB: {resultado_ping}") 
        if resultado_ping.get('ok') == 1.0:
            logging.info("PING a MongoDB exitoso. Conexión establecida.")
        else:
            logging.error("PING a MongoDB NO exitoso, aunque no lanzó excepción.")
        db = cliente[DB_NAME]
        return db[RAW_EVENTS_COLLECTION_NAME]
    except ServerSelectionTimeoutError as e:
        logging.error(f"TIMEOUT al conectar/ping a MongoDB: {e}", exc_info=True)
        exit(1)
    except OperationFailure as e: 
        logging.error(f"ERROR DE OPERACIÓN al conectar/ping a MongoDB: {e.details}", exc_info=True)
        exit(1)
    except Exception as e:
        logging.error(f"ERROR INESPERADO al conectar/ping a MongoDB: {e}", exc_info=True)
        exit(1)


def pedir_eventos_waze(url):
    logging.info(f"Solicitando datos desde Waze API: {url}")
    headers = { # Es bueno simular un navegador
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Referer': 'https://www.waze.com/live-map'
    }
    try:
        resultado = requests.get(url, headers=headers, timeout=20) 
        resultado.raise_for_status() 
        
        contenido_json = resultado.json()
        num_alerts = len(contenido_json.get('alerts', []))
        num_jams = len(contenido_json.get('jams', [])) 
        num_users = len(contenido_json.get('users', []))
        logging.info(f"Datos recibidos de Waze. Alerts: {num_alerts}, Jams: {num_jams}, Users: {num_users}")
        return contenido_json
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Error HTTP al hacer la petición a Waze: {http_err} - Status: {resultado.status_code if 'resultado' in locals() else 'N/A'}")
    except requests.exceptions.ConnectionError as conn_err:
        logging.error(f"Error de conexión al hacer la petición a Waze: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logging.error(f"Timeout al hacer la petición a Waze: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logging.error(f"Error genérico en la petición a Waze: {req_err}")
    except ValueError as json_err: 
        logging.error(f"Error al decodificar JSON de Waze: {json_err}")
    return None

def registrar_eventos_en_db(db_collection, datos_waze):
    if not datos_waze:
        return 0

    
    eventos_a_guardar = [] 
    timestamp_actual_iso = datetime.utcnow().isoformat() + "Z"

    # Procesar Alertas
    for alerta in datos_waze.get("alerts", []):
        alerta["timestamp_scrape"] = timestamp_actual_iso
        alerta["evento_tipo_waze"] = "alerta"
        eventos_a_guardar.append(alerta)

    # Procesar Atascos (Jams)
    for jam in datos_waze.get("jams", []):
        jam["timestamp_scrape"] = timestamp_actual_iso
        jam["evento_tipo_waze"] = "jam"
        eventos_a_guardar.append(jam)

    # Procesar Usuarios
    for usuario in datos_waze.get("users", []):
        usuario["timestamp_scrape"] = timestamp_actual_iso
        usuario["evento_tipo_waze"] = "user_location"
        eventos_a_guardar.append(usuario)

    
    if eventos_a_guardar:
        logging.info(f"Intentando guardar {len(eventos_a_guardar)} eventos en la colección '{db_collection.name}'.")
        logging.debug(f"Primer evento de muestra: {eventos_a_guardar[0] if eventos_a_guardar else 'N/A'}")
        try:
            resultado_insercion = db_collection.insert_many(eventos_a_guardar, ordered=False)
            num_insertados = len(resultado_insercion.inserted_ids)
            logging.info(f"PyMongo REPORTA ÉXITO: Guardados {num_insertados} eventos. IDs insertados (muestra): {resultado_insercion.inserted_ids[:3]}")
            return num_insertados
        except OperationFailure as op_fail:
            logging.error(f"ERROR DE OPERACIÓN de MongoDB durante insert_many: {op_fail.details}", exc_info=True)
            return 0
        except Exception as e:
            logging.error(f"ERROR GENERAL durante insert_many: {e}", exc_info=True)
            return 0
    else:
        logging.info("No se encontraron eventos (alertas, jams, usuarios) válidos para agregar a la lista de guardado en esta iteración.")
        return 0

if __name__ == "__main__":
    logging.info("Iniciando el scraper de Waze...")
    waze_api_url = get_api_url(ZONA_RM)
    
    coleccion_mongo = establecer_conexion_mongo()
    
    eventos_acumulados_total = 0
    try:
        
        eventos_acumulados_total = coleccion_mongo.count_documents({})
        logging.info(f"Eventos existentes en la colección '{RAW_EVENTS_COLLECTION_NAME}': {eventos_acumulados_total}")
    except Exception as e:
        logging.error(f"No se pudo contar documentos iniciales: {e}")

    try:
        while MAX_TOTAL_EVENTS_TO_COLLECT == 0 or eventos_acumulados_total < MAX_TOTAL_EVENTS_TO_COLLECT:
            datos_waze_actuales = pedir_eventos_waze(waze_api_url)
            
            if datos_waze_actuales:
                nuevos_eventos_guardados = registrar_eventos_en_db(coleccion_mongo, datos_waze_actuales)
                eventos_acumulados_total += nuevos_eventos_guardados
                logging.info(f"Total de eventos acumulados: {eventos_acumulados_total} de {MAX_TOTAL_EVENTS_TO_COLLECT if MAX_TOTAL_EVENTS_TO_COLLECT > 0 else 'infinito'}")
            else:
                logging.warning("No se recibieron datos de Waze en esta iteración o hubo un error al solicitarlos.")

            if MAX_TOTAL_EVENTS_TO_COLLECT > 0 and eventos_acumulados_total >= MAX_TOTAL_EVENTS_TO_COLLECT:
                logging.info(f"Se ha alcanzado el límite de {MAX_TOTAL_EVENTS_TO_COLLECT} eventos. Finalizando scraper.")
                break
            
            logging.info(f"Esperando {SCRAPING_INTERVAL_SECONDS} segundos para la próxima solicitud...")
            time.sleep(SCRAPING_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logging.info("Scraper detenido manualmente por el usuario.")
    except Exception as e:
        logging.error(f"Ha ocurrido un error inesperado en el bucle principal del scraper: {e}", exc_info=True)
    finally:
        
        try:
            conteo_final = coleccion_mongo.count_documents({})
            logging.info(f"Scraper finalizado. Total de eventos en la colección '{RAW_EVENTS_COLLECTION_NAME}' al finalizar: {conteo_final}")
        except Exception as e:
            logging.error(f"Error al obtener el conteo final de documentos: {e}")