.PHONY: build test clean run-master run-worker run-client demo test-fault-tolerance test-cache test-metrics

# Compilar todo
build:
	cargo build --release

# Compilar en modo debug
build-debug:
	cargo build

# Ejecutar tests
test:
	cargo test

# Limpiar artefactos
clean:
	cargo clean
	rm -rf /tmp/minispark

# Ejecutar master
run-master:
	cargo run --release --bin master

# Ejecutar worker
# Variables: PORT, FAIL, CACHE, LOG_LEVEL (DEBUG/INFO/WARN/ERROR), LOG_FORMAT (text/json)
run-worker:
	WORKER_PORT=$(or $(PORT),9000) \
	FAIL_PROBABILITY=$(or $(FAIL),0) \
	CACHE_MAX_MB=$(or $(CACHE),128) \
	LOG_LEVEL=$(or $(LOG_LEVEL),INFO) \
	LOG_FORMAT=$(or $(LOG_FORMAT),text) \
	cargo run --release --bin worker

# Ejecutar cliente
run-client-submit:
	cargo run --release --bin client -- submit

run-client-join:
	cargo run --release --bin client -- submit-join

run-client-status:
	cargo run --release --bin client -- status $(ID)

run-client-results:
	cargo run --release --bin client -- results $(ID)

# Demo
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

# Tests
test-fault-tolerance: build
	chmod +x scripts/test_fault_tolerance.sh
	./scripts/test_fault_tolerance.sh

test-cache: build
	chmod +x scripts/test_cache.sh
	./scripts/test_cache.sh

test-metrics: build
	chmod +x scripts/test_metrics.sh
	./scripts/test_metrics.sh

# Métricas - Master
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

# Formato de código
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
