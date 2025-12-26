FROM spark-custom:3.5.3

USER root
WORKDIR /app

# Dependências extras
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    git \
    curl \
    build-essential \
    libxml2-dev \
    libxslt1-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# Python libs
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install jupyterlab

    # JARs Iceberg + Nessie compatíveis com Spark 3.5 / Scala 2.12
RUN mkdir -p /opt/spark/jars && \
    # Hadoop S3A
    curl -L -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    \
    # Iceberg Spark Runtime
    curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    \
    # Nessie Spark Extensions
    curl -L -o /opt/spark/jars/nessie-spark-extensions-3.5_2.12-0.99.0.jar \
      https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.99.0/nessie-spark-extensions-3.5_2.12-0.99.0.jar && \
    \
    # Iceberg AWS + AWS SDK v2
    curl -L -o /opt/spark/jars/iceberg-aws-bundle-1.6.1.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar && \
    curl -L -o /opt/spark/jars/aws-sdk-bundle-2.28.13.jar \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.13/bundle-2.28.13.jar

EXPOSE 8888

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--NotebookApp.token=''"]
