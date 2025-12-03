#!/bin/bash
set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

cleanup() {
    echo -e "${YELLOW}Limpiando procesos...${NC}"
    pkill -f "target/release/master" 2>/dev/null || true
    pkill -f "target/release/worker" 2>/dev/null || true
}
trap cleanup EXIT

echo -e "${GREEN}=== TEST: E2E Multinodo con Fallos Simulados ===${NC}"

# =============================
# Build
# =============================
cargo build --release

# =============================
# Reset de /tmp/minispark
# =============================
rm -rf C:/tmp/minispark/*
mkdir -p C:/tmp/minispark

# (Opcional) Asegurar que exista el directorio data
mkdir -p data

# =============================
# Lanzar master
# =============================
echo -e "${YELLOW}Iniciando master...${NC}"
./target/release/master &
sleep 2

# =============================
# Lanzar workers
# =============================
echo -e "${YELLOW}Iniciando Worker 1${NC}"
MASTER_URL="http://127.0.0.1:8080" \
WORKER_PORT=9000 \
WORKER_THREADS=3 \
FAIL_PROBABILITY=0 \
./target/release/worker &
sleep 2

echo -e "${YELLOW}Iniciando Worker 2${NC}"
MASTER_URL="http://127.0.0.1:8080" \
WORKER_PORT=9010 \
WORKER_THREADS=3 \
FAIL_PROBABILITY=0 \
./target/release/worker &
sleep 2

echo -e "${YELLOW}Iniciando Worker 3${NC}"
MASTER_URL="http://127.0.0.1:8080" \
WORKER_PORT=9020 \
WORKER_THREADS=3 \
FAIL_PROBABILITY=0 \
./target/release/worker &
sleep 3

# =============================
# Job wordcount
# =============================
echo -e "${GREEN}Enviando job WORDCOUNT (wc-multi)...${NC}"
JOB_INFO=$(./target/release/client submit \
    --name wc-multi \
    --parallelism 6 \
    --input data/input.csv)

echo "$JOB_INFO"
JOB_ID=$(echo "$JOB_INFO" | awk '/ID:/ {print $NF}')
echo "JOB_ID: $JOB_ID"

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}ERROR: No se pudo obtener el ID del job wc-multi.${NC}"
    exit 1
fi

echo -e "${YELLOW}Esperando a que termine wc-multi...${NC}"
for i in {1..60}; do
    STATUS=$(./target/release/client status "$JOB_ID" 2>&1 || true)
    echo "$STATUS" | grep "Estado:" || true

    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}Job wc-multi completado correctamente.${NC}"
        break
    fi

    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}Job wc-multi falló.${NC}"
        echo "$STATUS"
        exit 1
    fi

    sleep 1
done

echo -e "${GREEN}Resultados WORDCOUNT:${NC}"
./target/release/client results "$JOB_ID" || true

# =============================
# Métricas del master
# =============================
echo -e "${GREEN}=== Métricas del sistema (master) ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/system   | python -m json.tool || true

echo -e "${GREEN}=== Métricas de jobs (master) ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/jobs     | python -m json.tool || true

echo -e "${GREEN}=== Métricas de fallos (master) ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/failures | python -m json.tool || true

echo -e "${GREEN}=== DONE (E2E multinodo) ===${NC}"
