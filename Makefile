.PHONY: build test clean run-master run-worker run-client demo

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

# Ejecutar master
run-master:
	cargo run --bin master

# Ejecutar worker (se puede especificar WORKER_PORT)
run-worker:
	WORKER_PORT=$(or $(PORT),9000) cargo run --bin worker

# Ejecutar cliente - submit job
run-client-submit:
	cargo run --bin client -- submit

# Ejecutar cliente - status
run-client-status:
	cargo run --bin client -- status $(ID)

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

# Formato de código
fmt:
	cargo fmt

# Verificar código
check:
	cargo check
	cargo clippy
