FROM python:3.8-slim

WORKDIR /app
USER root

# Instalar dependências de sistema
RUN apt-get update && apt-get install -y \
    curl \
    git \
    default-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Variáveis de ambiente do Java/Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3


# Instalar Jupyter e bibliotecas Python
#docker/notebook/
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Entrypoint
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root",  "--NotebookApp.token=''"]