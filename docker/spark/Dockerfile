FROM bitnami/spark:3.5.3

WORKDIR /app

USER root

# Instala pacotes adicionais do sistema
RUN install_packages curl wget python3.11 python3.11-venv python3.11-dev

# Atualiza pip e instala great_expectations no Python nativo do Spark
# (Bitnami Spark usa /opt/bitnami/python/bin/python3 por padrão)
RUN /opt/bitnami/python/bin/pip install --upgrade pip \
 && /opt/bitnami/python/bin/pip install boto3
# Cria diretório de JARs extras
RUN mkdir -p /opt/spark/jars

# Baixa dependências para Iceberg, Nessie e MinIO (via Hadoop AWS)
RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.99.0/nessie-spark-extensions-3.5_2.12-0.99.0.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.13/bundle-2.28.13.jar

# Permissões e classpath
RUN chmod 644 /opt/spark/jars/*.jar
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

# Configura Python para PySpark
ENV PYSPARK_PYTHON=python3.11
ENV PYSPARK_DRIVER_PYTHON=python3.11

USER 185
