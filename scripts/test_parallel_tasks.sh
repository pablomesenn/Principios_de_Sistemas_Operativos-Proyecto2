#!/bin/bash
set -e

YELLOW='\033[1;33m'
GREEN='\033[0;32m'
NC='\033[0m'

cleanup() {
    echo -e "${YELLOW}Limpiando procesos...${NC}"
    pkill -f "target/release/master" 2>/dev/null || true
    pkill -f "target/release/worker" 2>/dev/null || true
}
trap cleanup EXIT

echo -e "${GREEN}=== TEST: Tareas Paralelas en Worker ===${NC}"

cargo build --release

rm -rf C:/tmp/minispark/*
mkdir -p data

echo "text" > data/parallel_test.csv
for i in $(seq 1 100000); do
    echo "line $i foo bar baz" >> data/parallel_test.csv
done

echo -e "${YELLOW}Iniciando master...${NC}"
./target/release/master &
sleep 2

echo -e "${YELLOW}Iniciando worker con 4 hilos...${NC}"
MASTER_URL="http://127.0.0.1:8080" WORKER_PORT=9000 WORKER_THREADS=10 ./target/release/worker &
sleep 2

echo -e "${GREEN}Enviando job con parallelism=12...${NC}"
JOB=$(./target/release/client submit --input data/parallel_test.csv --parallelism 12)
JOB_ID=$(echo "$JOB" | awk '{print $NF}')

# HAY QUE CONSULTAR EL PROGRESO REPETIDAMENTE
# curl -s http://127.0.0.1:10000/metrics | python -m json.tool | grep -A5 '"tasks"'
