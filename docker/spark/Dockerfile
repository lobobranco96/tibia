FROM apache/spark:3.5.3

USER root
WORKDIR /opt/spark/app

RUN apt-get update && \
    apt-get install -y curl wget python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/spark/jars /opt/spark/logs && chmod -R a+rw /opt/spark

RUN set -eux; \
    wget -q --progress=dot:giga -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q --progress=dot:giga -P /opt/spark/jars \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    wget -q --progress=dot:giga -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && \
    wget -q --progress=dot:giga -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.99.0/nessie-spark-extensions-3.5_2.12-0.99.0.jar && \
    wget -q --progress=dot:giga -P /opt/spark/jars \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar && \
    wget -q --progress=dot:giga -P /opt/spark/jars \
      https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.28.13/bundle-2.28.13.jar

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV SPARK_NO_DAEMONIZE=true
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

RUN chown -R spark:spark /opt/spark
USER spark


#jars/
# ├─ iceberg-spark-runtime-3.5_2.12-1.5.2.jar
# ├─ nessie-spark-extensions-3.5_2.12-0.90.4.jar
# ├─ iceberg-aws-1.5.2.jar
# └─ hadoop-aws-3.3.4.jar