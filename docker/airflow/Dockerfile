FROM apache/airflow:2.7.4

# Variáveis de ambiente – Spark / Hadoop
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
ENV HADOOP_HOME=/opt/hadoop-3.3.4
ENV HADOOP_COMMON_LIB_NATIVE_DIR=/opt/hadoop-3.3.4/lib/native
ENV HADOOP_OPTS="-Djava.library.path=/opt/hadoop-3.3.4/lib/native"
ENV LD_LIBRARY_PATH=/opt/hadoop-3.3.4/lib/native

# Instalação de dependências do sistema
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    fonts-liberation \
    libnss3 \
    libxss1 \
    libasound2 \
    libx11-xcb1 \
    openjdk-17-jdk \
    python3.11 \
    python3.11-venv \
    python3.11-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


# JARs necessários – MinIO / Iceberg / Nessie
WORKDIR /opt

RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.99.0/nessie-spark-extensions-3.5_2.12-0.99.0.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.13/bundle-2.28.13.jar


USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
