#!/bin/bash
set -e 



if [ -z "$PIG_SCRIPT_TO_RUN" ]; then
  echo "Error: La variable de entorno PIG_SCRIPT_TO_RUN no está definida."
  exit 1
fi

SCRIPT_PATH="/opt/pig_scripts/${PIG_SCRIPT_TO_RUN}"

if [ ! -f "$SCRIPT_PATH" ]; then
  echo "Error: El script de Pig '$SCRIPT_PATH' no fue encontrado."
  exit 1
fi

echo "Ejecutando script de Pig: $SCRIPT_PATH"
echo "Conexión a MongoDB: Host=${MONGO_HOST}, DB=${MONGO_DATABASE}"
echo "Colección de entrada: ${MONGO_INPUT_COLLECTION}"
echo "Colecciones de salida: Comuna=${MONGO_OUTPUT_COLLECTION_COMUNA}, Tipo=${MONGO_OUTPUT_COLLECTION_TIPO}, Hora=${MONGO_OUTPUT_COLLECTION_HORA}"

# Ejecutar Pig en modo local
pig -x local \
    -param MONGO_HOST_PARAM="$MONGO_HOST" \
    -param MONGO_PORT_PARAM="27017" \
    -param MONGO_DB_PARAM="$MONGO_DATABASE" \
    -param MONGO_INPUT_COLLECTION_PARAM="$MONGO_INPUT_COLLECTION" \
    -param MONGO_OUTPUT_COLLECTION_COMUNA_PARAM="$MONGO_OUTPUT_COLLECTION_COMUNA" \
    -param MONGO_OUTPUT_COLLECTION_TIPO_PARAM="$MONGO_OUTPUT_COLLECTION_TIPO" \
    -param MONGO_OUTPUT_COLLECTION_HORA_PARAM="$MONGO_OUTPUT_COLLECTION_HORA" \
    "$SCRIPT_PATH"

echo "Ejecución de Pig finalizada."