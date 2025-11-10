#!/bin/bash
set -e

echo "==== Iniciando container Spark com modo: $SPARK_MODE ===="

# Inicia conforme o modo definido
if [ "$SPARK_MODE" = "master" ]; then
    echo "-> Iniciando Spark Master..."
    /opt/spark/sbin/start-master.sh
elif [ "$SPARK_MODE" = "worker" ]; then
    echo "-> Iniciando Spark Worker e conectando a: $SPARK_MASTER_URL"
    /opt/spark/sbin/start-worker.sh "$SPARK_MASTER_URL"
else
    echo "-> Nenhum modo definido. Executando Bash interativo..."
    exec /bin/bash
fi

# Mantém o container ativo
tail -f /opt/spark/logs/*
