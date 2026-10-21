# Atajos para operar el stack de la cátedra.
# Requiere Docker Desktop (o Docker Engine + compose plugin) instalado.

.PHONY: help up down logs ps restart clean work dbt-build dbt-test seed

help:
	@echo "Comandos disponibles:"
	@echo "  make up        - Levanta todo el stack (minio, postgres, airflow, metabase)"
	@echo "  make down      - Detiene el stack (conserva los datos)"
	@echo "  make clean     - Detiene el stack y BORRA los volúmenes (datos incluidos)"
	@echo "  make ps        - Estado de los servicios"
	@echo "  make logs      - Sigue los logs de todos los servicios"
	@echo "  make restart   - Reinicia el stack"
	@echo "  make work      - Abre una shell en el contenedor de trabajo (notebooks/dbt)"
	@echo "  make dbt-build - Ejecuta dbt build dentro del contenedor de trabajo"
	@echo "  make dbt-test  - Ejecuta dbt test dentro del contenedor de trabajo"
	@echo ""
	@echo "Interfaces web una vez levantado:"
	@echo "  MinIO consola : http://localhost:9001  (minioadmin / minioadmin)"
	@echo "  Airflow       : http://localhost:8080  (admin / admin)"
	@echo "  Metabase      : http://localhost:3000  (configurar en primer uso)"

up:
	docker compose up -d --build
	@echo "Stack levantado. Airflow tarda ~1-2 min en estar disponible en :8080"

down:
	docker compose down

clean:
	docker compose down -v
	@echo "Stack detenido y volúmenes eliminados."

ps:
	docker compose ps

logs:
	docker compose logs -f

restart:
	docker compose restart

work:
	docker compose -f docker-compose.work.yml run --rm --service-ports work bash

dbt-build:
	docker compose -f docker-compose.work.yml run --rm work \
	  bash -c "cd dbt && dbt build --profiles-dir ."

dbt-test:
	docker compose -f docker-compose.work.yml run --rm work \
	  bash -c "cd dbt && dbt test --profiles-dir ."
