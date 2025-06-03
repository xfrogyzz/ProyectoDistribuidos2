
FROM openjdk:8-jdk-slim 

ARG PIG_VERSION=0.17.0
ENV PIG_HOME /opt/pig-${PIG_VERSION}
ENV PIG_CONF_DIR $PIG_HOME/conf # Definir PIG_CONF_DIR explícitamente
ENV PATH $PIG_HOME/bin:$PATH

ENV PIG_CLASSPATH $PIG_CONF_DIR


RUN apt-get update && apt-get install -y wget procps && rm -rf /var/lib/apt/lists/*


RUN wget https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz -O /tmp/pig.tar.gz && \
    tar -xzf /tmp/pig.tar.gz -C /opt && \
    rm /tmp/pig.tar.gz


ENV MONGO_JAVA_DRIVER_VERSION=3.12.14
ENV MONGO_HADOOP_VERSION=2.0.2


WORKDIR /tmp/mongo-hadoop-downloads 
ADD https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/${MONGO_JAVA_DRIVER_VERSION}/mongo-java-driver-${MONGO_JAVA_DRIVER_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/mongodb/mongo-hadoop/mongo-hadoop-core/${MONGO_HADOOP_VERSION}/mongo-hadoop-core-${MONGO_HADOOP_VERSION}.jar .
ADD https://repo1.maven.org/maven2/org/mongodb/mongo-hadoop/mongo-hadoop-pig/${MONGO_HADOOP_VERSION}/mongo-hadoop-pig-${MONGO_HADOOP_VERSION}.jar .


RUN cp /tmp/mongo-hadoop-downloads/*.jar $PIG_HOME/lib/ && \
    rm -rf /tmp/mongo-hadoop-downloads # Limpiar descargas temporales


WORKDIR /opt/pig_app
COPY ./scripts/ /opt/pig_scripts/
COPY run.sh /opt/pig_app/run.sh
RUN chmod +x /opt/pig_app/run.sh
ENTRYPOINT ["/opt/pig_app/run.sh"]