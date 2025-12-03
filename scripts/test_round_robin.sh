#!/bin/bash
# scripts/test_round_robin.sh
# Test para verificar que Round-Robin está distribuyendo tareas equitativamente

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

cleanup() {
    echo -e "${YELLOW}Limpiando procesos...${NC}"
    pkill -f "target/release/master" 2>/dev/null || true
    pkill -f "target/release/worker" 2>/dev/null || true
    sleep 1
}

trap cleanup EXIT

echo -e "${GREEN}"
echo "============================================"
echo "   TEST: Round-Robin + Load Awareness"
echo "============================================"
echo -e "${NC}"

# Compilar
echo -e "${YELLOW}Compilando...${NC}"
cargo build --release

# Limpiar estado
rm -rf /tmp/minispark/*
mkdir -p data

# Crear datos de prueba grandes para generar muchas tareas
echo -e "${YELLOW}Creando datos de prueba (500 líneas)...${NC}"
echo "text" > data/round_robin_test.csv
for i in $(seq 1 500); do
    echo "line $i hello world foo bar baz" >> data/round_robin_test.csv
done

# Iniciar master
echo -e "${YELLOW}Iniciando master...${NC}"
./target/release/master &
MASTER_PID=$!
sleep 2

# Iniciar 3 workers
echo -e "${YELLOW}Iniciando Worker 1 (4 hilos)...${NC}"
MASTER_URL="http://127.0.0.1:8080" WORKER_PORT=9000 WORKER_THREADS=4 ./target/release/worker &
WORKER1_PID=$!
sleep 1

echo -e "${YELLOW}Iniciando Worker 2 (4 hilos)...${NC}"
MASTER_URL="http://127.0.0.1:8080" WORKER_PORT=9001 WORKER_THREADS=4 ./target/release/worker &
WORKER2_PID=$!
sleep 1

echo -e "${YELLOW}Iniciando Worker 3 (4 hilos)...${NC}"
MASTER_URL="http://127.0.0.1:8080" WORKER_PORT=9002 WORKER_THREADS=4 ./target/release/worker &
WORKER3_PID=$!
sleep 2

# Verificar workers registrados
echo ""
echo -e "${GREEN}=== Estado inicial del sistema ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/system | python3 -m json.tool 2>/dev/null || \
    curl -s http://127.0.0.1:8080/api/v1/metrics/system

# Ver estadísticas del scheduler
echo ""
echo -e "${GREEN}=== Estadísticas del Scheduler ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/scheduler | python3 -m json.tool 2>/dev/null || \
    curl -s http://127.0.0.1:8080/api/v1/metrics/scheduler

# Enviar job con muchas particiones para ver distribución
echo ""
echo -e "${YELLOW}Enviando job con 12 particiones (4 por worker idealmente)...${NC}"
JOB_RESPONSE=$(./target/release/client submit --input data/round_robin_test.csv --parallelism 12 2>&1)
echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}Error: No se pudo obtener ID del job${NC}"
    exit 1
fi

# Monitorear distribución de carga durante ejecución
echo ""
echo -e "${YELLOW}Monitoreando distribución de carga...${NC}"
for i in {1..10}; do
    echo ""
    echo "--- Iteración $i ---"
    
    # Ver carga de cada worker
    echo "Carga de workers:"
    curl -s http://127.0.0.1:8080/api/v1/metrics/system | grep -o '"worker_loads":{[^}]*}' 2>/dev/null || \
        curl -s http://127.0.0.1:8080/api/v1/metrics/system
    
    # Ver estado del job
    STATUS=$(./target/release/client status "$JOB_ID" 2>&1)
    PROGRESS=$(echo "$STATUS" | grep "Progreso" | awk '{print $2}')
    echo "Progreso del job: $PROGRESS"
    
    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}Job completado!${NC}"
        break
    fi
    
    sleep 2
done

# Resultado final
echo ""
echo -e "${GREEN}=== Resultado Final ===${NC}"
./target/release/client status "$JOB_ID"

echo ""
echo -e "${GREEN}=== Métricas del Job ===${NC}"
curl -s "http://127.0.0.1:8080/api/v1/jobs/$JOB_ID/metrics" | python3 -m json.tool 2>/dev/null || \
    curl -s "http://127.0.0.1:8080/api/v1/jobs/$JOB_ID/metrics"

echo ""
echo -e "${GREEN}=== Estado final del scheduler ===${NC}"
curl -s http://127.0.0.1:8080/api/v1/metrics/scheduler | python3 -m json.tool 2>/dev/null || \
    curl -s http://127.0.0.1:8080/api/v1/metrics/scheduler

echo ""
echo -e "${GREEN}============================================"
echo "   Test Round-Robin completado"
echo "============================================${NC}"
echo ""
echo "Verifica que:"
echo "  1. worker_loads muestra distribución equilibrada"
echo "  2. next_index incrementa (Round-Robin funcionando)"
echo "  3. Las tareas se distribuyeron entre los 3 workers"