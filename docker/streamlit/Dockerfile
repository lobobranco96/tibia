FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit \
    trino \
    pandas \
    duckdb \
    pyarrow \
    boto3 \
    s3fs

# 🔥 instala a extensão httpfs do DuckDB
RUN python - << 'EOF'
import duckdb
con = duckdb.connect()
con.execute("INSTALL httpfs;")
EOF

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
