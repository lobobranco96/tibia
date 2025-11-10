# Imagem base oficial do Apache Spark 3.5.3
FROM apache/spark:3.5.3

USER root
WORKDIR /opt/spark/app

# Instala Python, utilitários e limpa cache
RUN apt-get update && \
    apt-get install -y curl wget python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Cria diretórios de jars e logs
RUN mkdir -p /opt/spark/jars /opt/spark/logs && chmod -R a+rw /opt/spark

# Dependências (Iceberg, Nessie, Hadoop AWS, SDK)
RUN wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.99.0/nessie-spark-extensions-3.5_2.12-0.99.0.jar && \
    wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar && \
    wget -P /opt/spark/jars \
    https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.13/bundle-2.28.13.jar

# Define variáveis para o Spark rodar em foreground (não-daemon)
ENV SPARK_NO_DAEMONIZE=true
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Copia o script de entrada
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

# Troca para o usuário padrão Spark
USER 185

# Executa o entrypoint customizado
ENTRYPOINT ["/opt/spark/entrypoint.sh"]