/*
  Script Pig para procesar datos de Waze directamente desde MongoDB
  y guardar resultados en MongoDB.
*/




events_raw = LOAD 'mongodb://${MONGO_HOST_PARAM}:${MONGO_PORT_PARAM}/${MONGO_DB_PARAM}.${MONGO_INPUT_COLLECTION_PARAM}?readPreference=primary&mongo.input.split.use_chunks=false&mongo.input.splitter_class=com.mongodb.hadoop.splitter.SingleMongoSplitter'
    USING com.mongodb.hadoop.pig.MongoLoader  ( -- Paréntesis de apertura para MongoLoader
        -- Comilla simple INICIA la cadena del esquema
        'id_original:chararray, 
        tipo_waze_original:chararray, 
        longitud:double, 
        latitud:double, 
        timestamp_evento:datetime, 
        timestamp_scrape:datetime, 
        tipo_incidente_general:chararray, 
        subtipo_incidente:chararray, 
        descripcion:chararray, 
        confianza:int, 
        fiabilidad:int, 
        comuna:chararray, 
        velocidad_kmh:double, 
        retraso_segundos:int, 
        dia_semana_evento:int, 
        hora_dia_evento:int' 
        
    ); 


filtered_events = FILTER events_raw BY  
    (comuna IS NOT NULL AND TRIM(LOWER(comuna)) != 'n/a' AND TRIM(comuna) != '') AND
    (tipo_incidente_general IS NOT NULL AND TRIM(LOWER(tipo_incidente_general)) != 'n/a' AND TRIM(tipo_incidente_general) != '' AND TRIM(LOWER(tipo_incidente_general)) != 'desconocido');


grouped_by_comuna = GROUP filtered_events BY comuna;
incident_counts_by_comuna = FOREACH grouped_by_comuna GENERATE
    group AS comuna,
    COUNT(filtered_events) AS total_incidentes;
DUMP incident_counts_by_comuna;
STORE incident_counts_by_comuna INTO 'mongodb://${MONGO_HOST_PARAM}:${MONGO_PORT_PARAM}/${MONGO_DB_PARAM}.${MONGO_OUTPUT_COLLECTION_COMUNA_PARAM}'
    USING com.mongodb.hadoop.pig.MongoInsertStorage('comuna');


grouped_by_tipo_incidente = GROUP filtered_events BY tipo_incidente_general;
incident_counts_by_type = FOREACH grouped_by_tipo_incidente GENERATE
    group AS tipo_incidente,
    COUNT(filtered_events) AS frecuencia;
DUMP incident_counts_by_type;
STORE incident_counts_by_type INTO 'mongodb://${MONGO_HOST_PARAM}:${MONGO_PORT_PARAM}/${MONGO_DB_PARAM}.${MONGO_OUTPUT_COLLECTION_TIPO_PARAM}'
    USING com.mongodb.hadoop.pig.MongoInsertStorage('tipo_incidente');


grouped_by_hora = GROUP filtered_events BY hora_dia_evento;
incident_counts_by_hour = FOREACH grouped_by_hora GENERATE
    (chararray)group AS hora_del_dia, 
    COUNT(filtered_events) AS total_incidentes_en_hora;
DUMP incident_counts_by_hour;
STORE incident_counts_by_hour INTO 'mongodb://${MONGO_HOST_PARAM}:${MONGO_PORT_PARAM}/${MONGO_DB_PARAM}.${MONGO_OUTPUT_COLLECTION_HORA_PARAM}'
    USING com.mongodb.hadoop.pig.MongoInsertStorage('hora_del_dia');
grunt> quit