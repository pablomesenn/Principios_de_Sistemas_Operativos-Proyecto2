.PHONY: build test clean run-master run-worker run-client demo test-fault-tolerance test-cache test-metrics
.PHONY: docker-build docker-up docker-down docker-demo docker-test docker-logs docker-clean

# ============ Compilación Local ============

build:
	cargo build --release

build-debug:
	cargo build

test:
	cargo test

clean:
	cargo clean

delete:
	rm -rf /tmp/minispark/*

# ============ Ejecución Local ============

run-master:
	cargo run --release --bin master

# Variables: PORT, FAIL, CACHE, LOG_LEVEL (DEBUG/INFO/WARN/ERROR), LOG_FORMAT (text/json)
run-worker:
	WORKER_PORT=$(or $(PORT),9000) \
	FAIL_PROBABILITY=$(or $(FAIL),0) \
	CACHE_MAX_MB=$(or $(CACHE),128) \
	LOG_LEVEL=$(or $(LOG_LEVEL),INFO) \
	LOG_FORMAT=$(or $(LOG_FORMAT),text) \
	cargo run --release --bin worker

run-client-submit:
	cargo run --release --bin client -- submit

run-client-join:
	cargo run --release --bin client -- submit-join

run-client-status:
	cargo run --release --bin client -- status $(ID)

run-client-results:
	cargo run --release --bin client -- results $(ID)

# ============ Docker ============

docker-build:
	docker-compose build

docker-up:
	docker-compose up -d master worker1 worker2

docker-up-full:
	docker-compose --profile full up -d

docker-down:
	docker-compose down

docker-clean:
	docker-compose down -v
	docker rmi minispark-master minispark-worker minispark-client 2>/dev/null || true

docker-logs:
	docker-compose logs -f

docker-ps:
	docker-compose ps

docker-demo:
	chmod +x scripts/docker-demo.sh
	./scripts/docker-demo.sh

docker-test:
	chmod +x scripts/docker-fault-test.sh
	./scripts/docker-fault-test.sh

docker-client-submit:
	docker-compose run --rm client submit --input /app/data/input.csv --parallelism $(or $(P),4)

docker-client-status:
	docker-compose run --rm client status $(ID)

docker-client-results:
	docker-compose run --rm client results $(ID)

docker-metrics-system:
	@curl -s http://localhost:8080/api/v1/metrics/system | python3 -m json.tool

docker-metrics-jobs:
	@curl -s http://localhost:8080/api/v1/metrics/jobs | python3 -m json.tool

# ============ Demo Local ============

demo: build
	@echo "=== Mini-Spark Demo ==="
	@echo ""
	@echo "1. Terminal 1: make run-master"
	@echo "2. Terminal 2: make run-worker"
	@echo "3. Terminal 3: make run-client-submit"
	@echo ""
	@echo "Opciones de worker:"
	@echo "  PORT=9001 make run-worker          # Puerto diferente"
	@echo "  CACHE=64 make run-worker           # Cache de 64MB"
	@echo "  FAIL=30 make run-worker            # 30% fallos simulados"
	@echo "  LOG_LEVEL=DEBUG make run-worker    # Logs verbose"
	@echo "  LOG_FORMAT=json make run-worker    # Logs en JSON"
	@echo ""
	@echo "=== Docker Demo ==="
	@echo ""
	@echo "  make docker-build                  # Construir imágenes"
	@echo "  make docker-up                     # Iniciar cluster (2 workers)"
	@echo "  make docker-demo                   # Demo completa automática"
	@echo "  make docker-test                   # Test de tolerancia a fallos"
	@echo "  make docker-down                   # Apagar cluster"

# ============ Tests Locales ============

test-fault-tolerance: build
	chmod +x scripts/test_fault_tolerance.sh
	./scripts/test_fault_tolerance.sh

test-cache: build
	chmod +x scripts/test_cache.sh
	./scripts/test_cache.sh

test-metrics: build
	chmod +x scripts/test_metrics.sh
	./scripts/test_metrics.sh

test-load-balancing: build
	chmod +x scripts/test_load_balancing.sh
	./scripts/test_load_balancing.sh

test-parallel-tasks: build
	chmod +x scripts/test_parallel_tasks.sh
	./scripts/test_parallel_tasks.sh

test-failures: build
	chmod +x scripts/test_failures.sh
	./scripts/test_failures.sh

test-client-metrics: build
	chmod +x scripts/test_client_metrics.sh
	./scripts/test_client_metrics.sh

# ============ Métricas - Master ============

metrics-system:
	@curl -s http://127.0.0.1:8080/api/v1/metrics/system | python3 -m json.tool

metrics-failures:
	@curl -s http://127.0.0.1:8080/api/v1/metrics/failures | python3 -m json.tool

metrics-jobs:
	@curl -s http://127.0.0.1:8080/api/v1/metrics/jobs | python3 -m json.tool

metrics-job:
	@curl -s http://127.0.0.1:8080/api/v1/jobs/$(ID)/metrics | python3 -m json.tool

metrics-stages:
	@curl -s http://127.0.0.1:8080/api/v1/jobs/$(ID)/stages | python3 -m json.tool

# Métricas - Worker (puerto por defecto 10000 = 9000 + 1000)
metrics-worker:
	@curl -s http://127.0.0.1:$(or $(PORT),10000)/metrics | python3 -m json.tool

# Estado persistido
state:
	@curl -s http://127.0.0.1:8080/api/v1/state | python3 -m json.tool

state-file:
	@cat /tmp/minispark/state/jobs.json | python3 -m json.tool

# ============ Utilidades ============

fmt:
	cargo fmt

check:
	cargo check
	cargo clippy

# Datos de prueba
create-test-data:
	mkdir -p data
	echo "text" > data/input.csv
	echo "hello world" >> data/input.csv
	echo "foo bar baz" >> data/input.csv
	echo "hello foo" >> data/input.csv
	echo "world bar" >> data/input.csv
	@echo "Datos creados en data/input.csv"

create-large-data:
	mkdir -p data
	echo "text" > data/large.csv
	@for i in $$(seq 1 10000); do \
		echo "line $$i with some text data for testing" >> data/large.csv; \
	done
	@echo "Datos grandes creados en data/large.csv (10000 líneas)"

create-1m-data:
	mkdir -p data
	echo "text" > data/benchmark_1m.csv
	@echo "Generando 1M de líneas (esto puede tardar)..."
	@for i in $$(seq 1 1000000); do \
		echo "line $$i hello world foo bar baz test benchmark data" >> data/benchmark_1m.csv; \
	done
	@echo "Datos de benchmark creados en data/benchmark_1m.csv (1M líneas)"

create-join-data:
	mkdir -p data
	echo "product_id,amount" > data/sales.csv
	@for i in $$(seq 1 1000); do \
		echo "$$((i % 100)),$$((RANDOM % 1000))" >> data/sales.csv; \
	done
	echo "product_id,name" > data/products.csv
	@for i in $$(seq 0 99); do \
		echo "$$i,Product_$$i" >> data/products.csv; \
	done
	@echo "Datos de join creados: data/sales.csv (1000 ventas), data/products.csv (100 productos)"

# ============ Ayuda ============

help:
	@echo "Mini-Spark - Motor de Procesamiento Distribuido"
	@echo ""
	@echo "Comandos principales:"
	@echo "  make build              Compilar proyecto"
	@echo "  make demo               Mostrar instrucciones de uso"
	@echo ""
	@echo "Ejecución local:"
	@echo "  make run-master         Iniciar master"
	@echo "  make run-worker         Iniciar worker"
	@echo "  make run-client-submit  Enviar job"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build       Construir imágenes"
	@echo "  make docker-up          Iniciar cluster"
	@echo "  make docker-demo        Demo completa"
	@echo "  make docker-test        Test tolerancia a fallos"
	@echo "  make docker-down        Apagar cluster"
	@echo ""
	@echo "Tests:"
	@echo "  make test               Ejecutar tests unitarios"
	@echo "  make test-fault-tolerance"
	@echo "  make test-cache"
	@echo "  make test-metrics"