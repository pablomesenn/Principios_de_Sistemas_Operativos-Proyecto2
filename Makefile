.PHONY: build test clean run-master run-worker run-client demo test-fault-tolerance test-cache

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

# Ejecutar worker (se puede especificar WORKER_PORT, FAIL_PROBABILITY, CACHE_MAX_MB)
run-worker:
	WORKER_PORT=$(or $(PORT),9000) \
	FAIL_PROBABILITY=$(or $(FAIL),0) \
	CACHE_MAX_MB=$(or $(CACHE),128) \
	cargo run --release --bin worker

# Ejecutar cliente - submit job
run-client-submit:
	cargo run --release --bin client -- submit

# Ejecutar cliente - submit join
run-client-join:
	cargo run --release --bin client -- submit-join

# Ejecutar cliente - status
run-client-status:
	cargo run --release --bin client -- status $(ID)

# Ejecutar cliente - results
run-client-results:
	cargo run --release --bin client -- results $(ID)

# Demo: compilar y mostrar ayuda
demo: build
	@echo "=== Mini-Spark Demo ==="
	@echo ""
	@echo "1. En terminal 1: make run-master"
	@echo "2. En terminal 2: make run-worker"
	@echo "3. En terminal 3: make run-client-submit"
	@echo ""
	@echo "Para más workers:"
	@echo "  PORT=9001 make run-worker"
	@echo "  PORT=9002 make run-worker"
	@echo ""
	@echo "Configurar cache (MB):"
	@echo "  CACHE=64 make run-worker"
	@echo ""
	@echo "Para simular fallos (30% probabilidad):"
	@echo "  FAIL=30 make run-worker"

# Test de tolerancia a fallos
test-fault-tolerance: build
	chmod +x scripts/test_fault_tolerance.sh
	./scripts/test_fault_tolerance.sh

# Test de cache y spill
test-cache: build
	chmod +x scripts/test_cache.sh
	./scripts/test_cache.sh

# Ver métricas de fallos
metrics:
	curl -s http://127.0.0.1:8080/api/v1/metrics/failures | python3 -m json.tool

# Ver estado persistido
state:
	curl -s http://127.0.0.1:8080/api/v1/state | python3 -m json.tool

# Ver archivo de estado en disco
state-file:
	cat /tmp/minispark/state/jobs.json | python3 -m json.tool

# Formato de código
fmt:
	cargo fmt

# Verificar código
check:
	cargo check
	cargo clippy

# Crear datos de prueba
create-test-data:
	mkdir -p data
	echo "text" > data/input.csv
	echo "hello world" >> data/input.csv
	echo "foo bar baz" >> data/input.csv
	echo "hello foo" >> data/input.csv
	echo "world bar" >> data/input.csv
	@echo "Datos creados en data/input.csv"

# Crear datos grandes para probar cache/spill
create-large-data:
	mkdir -p data
	echo "text" > data/large.csv
	@for i in $$(seq 1 10000); do \
		echo "line $$i with some text data for testing cache and spill functionality" >> data/large.csv; \
	done
	@echo "Datos grandes creados en data/large.csv (10000 líneas)"