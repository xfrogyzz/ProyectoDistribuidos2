# Proyecto Sistemas Distribuidos: Plataforma de Análisis de Tráfico (Entrega 2)

Este proyecto implementa un pipeline para la extracción, almacenamiento, homogeneización y procesamiento de datos de tráfico de Waze para la Región Metropolitana, utilizando Apache Pig para el análisis.

## Arquitectura y Flujo

Waze API → **Web Scraper** (Python) → **MongoDB** (`eventos_crudos`) → **Filtering/Homogeneización** (Python) → **MongoDB** (`eventos_homogeneizados`) → **Pig Processor** (Apache Pig) → **MongoDB** (Resultados: `analisis_por_comuna`, `analisis_por_tipo`, `analisis_por_hora`).

## Módulos Principales

1.  **Web Scraper (`web_scraper/scraper.py`):** Extrae datos de Waze (alertas, atascos, usuarios) y los guarda en `eventos_crudos` (MongoDB).
2.  **Filtering y Homogeneización (`mongo_storage/storage_cleaner.py`):** Limpia y transforma los datos de `eventos_crudos`, guardándolos en `eventos_homogeneizados` (MongoDB). Asigna "DESCONOCIDA" a comunas no disponibles.
3.  **Pig Processor (`pig_processor/`):** Utiliza Apache Pig para analizar `eventos_homogeneizados`.
    -   Agrupa incidentes por comuna.
    -   Cuenta la frecuencia de tipos de incidentes.
    -   Analiza incidentes por hora del día.
    -   Guarda los resultados en colecciones de análisis dedicadas en MongoDB.

## Requisitos Previos

-   Docker
-   Docker Compose
-   Conexión a Internet

## Instrucciones de Ejecución Completa del Pipeline

-Sigue estos pasos en orden desde la raíz del proyecto en tu terminal.(Lo ideal es tener 4 terminales abiertas).

**Paso 0: (Opcional) Limpieza Inicial Completa**
Si deseas empezar desde cero (elimina todos los datos y contenedores previos del proyecto):

docker-compose down --volumes 

**Paso 1: Levantar Servicio de Base de Datos**
  Asegúrate de que MongoDB esté corriendo en segundo plano. Si no lo está, o para asegurar:

  docker-compose up -d mongo-storage

  "Espera unos segundos para que el servicio se inicie y pase el healthcheck."

**Paso 2: Recolección de Datos Crudos (Scraper)**

docker-compose up --build scraper

  El scraper comenzará a recolectar datos. Por defecto, está configurado para 10,000 eventos.
  
  Monitorea los logs. Una vez que veas el mensaje "Finalizando scraper" o hayas recolectado suficientes datos, puedes detenerlo manualmente con Ctrl + C en esta terminal.
  
  Verificación rápida de datos crudos (en otra terminal):

docker exec -it mongo-storage mongo

use waze_db;

db.eventos_crudos.count(); 

exit;

**Paso 3: Filtrado y Homogeneización de Datos**
docker-compose run --rm filtering-homogenization

Este script procesará eventos_crudos y guardará en eventos_homogeneizados.

Verificación rápida de datos homogeneizados (en otra terminal):

docker exec -it mongo-storage mongo

use waze_db; 

db.eventos_homogeneizados.count(); 

exit;

**Paso 4: Procesamiento con Apache Pig**
Limpiar colecciones de análisis previas:

docker exec -it mongo-storage mongo

Dentro del shell mongo:

use waze_db;

db.analisis_por_comuna.deleteMany({});

db.analisis_por_tipo.deleteMany({});

db.analisis_por_hora.deleteMany({});

exit;

Ejecutar el procesador Pig:

docker-compose up --build pig-processor

**Paso 5: Verificación de Resultados del Análisis Final**

docker exec -it mongo-storage mongo

Dentro del shell mongo:

use waze_db;

print("--- Análisis por Comuna ---");

printjson(db.analisis_por_comuna.find().toArray());

print("Conteo: " + db.analisis_por_comuna.count());

print("--- Análisis por Tipo de Incidente ---");

printjson(db.analisis_por_tipo.find().toArray());

print("Conteo: " + db.analisis_por_tipo.count());

print("--- Análisis por Hora del Día ---");

printjson(db.analisis_por_hora.find().toArray());

print("Conteo: " + db.analisis_por_hora.count());

exit;

