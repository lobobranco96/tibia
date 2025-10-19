FROM bitnami/spark:3.5.3

WORKDIR /app
USER root
RUN install_packages curl

COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root",  "--NotebookApp.token=''"]