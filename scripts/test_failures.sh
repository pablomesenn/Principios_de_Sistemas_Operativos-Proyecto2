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

echo -e "${GREEN}=== TEST: Fallos Simulados + Reintentos ===${NC}"

cargo build --release

rm -rf C:/tmp/minispark/*
mkdir -p data

echo "text" > data/failure_test.csv
for i in $(seq 1 200); do
    echo "line $i foo bar baz" >> data/failure_test.csv
done

echo -e "${YELLOW}Iniciando master...${NC}"
./target/release/master &
sleep 2

echo -e "${YELLOW}Iniciando worker con FAIL_PROBABILITY=50% ...${NC}"
MASTER_URL="http://127.0.0.1:8080" WORKER_PORT=9000 WORKER_THREADS=2 FAIL_PROBABILITY=50 ./target/release/worker &
sleep 2

echo -e "${GREEN}Enviando job (espera reintentos)...${NC}"
JOB=$(./target/release/client submit --input data/failure_test.csv --parallelism 8)
JOB_ID=$(echo "$JOB" | awk '{print $NF}')

echo ""
echo -e "${GREEN}Esperando 10 segundos mientras ocurren fallos...${NC}"
sleep 10

echo -e "${GREEN}=== Métricas de fallos desde el master ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/failures | python -m json.tool

echo ""
echo -e "${GREEN}>>> Debes ver job_failures > 0 y logs de reintentos <<<${NC}"
