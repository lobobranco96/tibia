FROM apache/airflow:3.0.6

# Para a utilização do pyspark
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/
ENV HADOOP_HOME /opt/hadoop-3.3.6
ENV HADOOP_COMMON_LIB_NATIVE_DIR /opt/hadoop-3.3.6/lib/native
ENV HADOOP_OPTS " -Djava.library.path=/opt/hadoop-3.3.6/lib/native"
ENV LD_LIBRARY_PATH /opt/hadoop-3.3.6/lib/native

USER root

RUN apt-get update && apt-get install -y wget unzip fonts-liberation libnss3 libxss1 libasound2 libx11-xcb1 openjdk-17-jdk python3.11 python3.11-venv python3.11-dev

# 
RUN wget -q https://storage.googleapis.com/chrome-for-testing-public/114.0.5735.90/linux64/chrome-linux64.zip && \
    wget -q https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip && \
    unzip -q chrome-linux64.zip && \
    unzip -q chromedriver_linux64.zip && \
    mkdir -p /usr/local/share/chrome && \
    mv chrome-linux64 /usr/local/share/chrome/ || mv chrome-* /usr/local/share/chrome/ || true && \
    mv chromedriver /usr/local/share/chrome/chromedriver || mv chromedriver-linux64/chromedriver /usr/local/share/chrome/chromedriver && \
    ln -sf /usr/local/share/chrome/chrome-linux64/chrome /usr/bin/chrome || ln -sf /usr/local/share/chrome/chrome*/chrome /usr/bin/chrome && \
    ln -sf /usr/local/share/chrome/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chrome /usr/bin/chromedriver && \
    rm -rf chrome-linux64.zip chromedriver_linux64.zip chromedriver-linux64

# Variáveis para Selenium saber onde está o Chromium e Chromedriver
ENV CHROME_BIN=/usr/bin/chrome
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
ENV PATH=$PATH:/usr/bin


WORKDIR /opt

# JARs necessários para acessar o MinIO via Hadoop AWS e Iceberg + Nessie
RUN wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.99.0/nessie-spark-extensions-3.5_2.12-0.99.0.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar \
 && wget -P /opt/spark/jars https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.13/bundle-2.28.13.jar

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt