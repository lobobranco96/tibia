.PHONY: up down restart

# Inicia todos os serviços
up:
	docker compose -f services/lakehouse.yaml up -d
	docker compose -f services/orchestration.yaml up -d
	docker compose -f services/processing.yaml up -d

down:
	docker compose -f services/lakehouse.yaml down
	docker compose -f services/orchestration.yaml down
	docker compose -f services/processing.yaml down

build:
	docker compose -f services/orchestration.yaml build	
	docker compose -f services/processing.yaml build
	docker compose -f services/lakehouse.yaml build

# Reinicia o Airflow e os serviços de observabilidade
restart: down up docker compose -f services/observability.yaml up -d 	docker compose -f services/observability.yaml down
