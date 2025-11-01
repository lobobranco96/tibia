FROM apache/spark:3.5.3

USER root
WORKDIR /opt/spark/app

RUN apt-get update && \
    apt-get install -y curl wget python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars

#  DEPENDÊNCIAS PARA ICEBERG, NESSIE E MINIO

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

RUN chmod 644 /opt/spark/jars/*.jar
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit"]
