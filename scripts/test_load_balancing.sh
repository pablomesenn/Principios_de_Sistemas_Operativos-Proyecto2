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

echo -e "${GREEN}=== TEST: Awareness de Carga ===${NC}"

cargo build --release

rm -rf C:/tmp/minispark/*
mkdir -p data

echo "text" > data/lb_test.csv
for i in $(seq 1 200000); do
    echo "line $i hello world foo bar" >> data/lb_test.csv
done

echo -e "${YELLOW}Iniciando master...${NC}"
./target/release/master &
sleep 2

echo -e "${YELLOW}Iniciando Worker 1 (lento – 1 hilo)...${NC}"
MASTER_URL="http://127.0.0.1:8080" WORKER_PORT=9000 WORKER_THREADS=1 ./target/release/worker &
sleep 2

echo -e "${YELLOW}Iniciando Worker 2 (rápido – 4 hilos)...${NC}"
MASTER_URL="http://127.0.0.1:8080" WORKER_PORT=9001 WORKER_THREADS=4 ./target/release/worker &
sleep 3

echo -e "${GREEN}Enviando job (parallelism=12) para forzar carga...${NC}"
JOB_INFO=$(./target/release/client submit --input data/lb_test.csv --parallelism 12)
JOB_ID=$(echo "$JOB_INFO" | awk '{print $NF}')
echo "JOB_ID: $JOB_ID"

echo -e "${GREEN}=== Métricas del sistema después de activar Worker 2 ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/system | python -m json.tool
