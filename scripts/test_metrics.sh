#!/bin/bash

# scripts/test_metrics.sh
# Script para probar métricas y observabilidad

set -e

echo "=== Test de Métricas y Observabilidad ==="
echo ""

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

cleanup() {
    echo ""
    echo -e "${YELLOW}Limpiando procesos...${NC}"
    pkill -f "target/release/master" 2>/dev/null || true
    pkill -f "target/release/worker" 2>/dev/null || true
    sleep 1
}

trap cleanup EXIT

# Compilar
echo -e "${YELLOW}Compilando...${NC}"
cargo build --release

# Limpiar estado anterior
rm -rf /tmp/minispark/*
mkdir -p data

# Crear datos de prueba
echo -e "${YELLOW}Creando datos de prueba...${NC}"
echo "text" > data/metrics_test.csv
for i in $(seq 1 500); do
    echo "line $i hello world foo bar baz test data for metrics" >> data/metrics_test.csv
done
echo "Datos creados: 500 líneas"

# Iniciar master
echo ""
echo -e "${GREEN}=== Iniciando sistema ===${NC}"
echo ""

echo "Iniciando master..."
./target/release/master &
MASTER_PID=$!
sleep 2

# Iniciar worker
echo "Iniciando worker..."
./target/release/worker &
WORKER_PID=$!
sleep 2

# === TEST 1: Métricas del sistema ===
echo ""
echo -e "${GREEN}=== TEST 1: Métricas del sistema ===${NC}"
echo ""

echo "Métricas del sistema (antes del job):"
curl -s http://127.0.0.1:8080/api/v1/metrics/system | python -m json.tool
echo ""

# === TEST 2: Enviar job y ver métricas ===
echo -e "${GREEN}=== TEST 2: Métricas durante ejecución ===${NC}"
echo ""

echo "Enviando job..."
JOB_RESPONSE=$(./target/release/client submit --input data/metrics_test.csv --parallelism 4 2>&1)
JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}Error: No se pudo obtener ID del job${NC}"
    exit 1
fi

echo "Job ID: $JOB_ID"
echo ""

# Esperar completación
for i in {1..30}; do
    sleep 1
    STATUS=$(./target/release/client status "$JOB_ID" 2>&1)

    if echo "$STATUS" | grep -q "Succeeded"; then
        echo "$STATUS"
        break
    fi

    if echo "$STATUS" | grep -q "Failed"; then
        echo "$STATUS"
        echo -e "${RED}Job falló${NC}"
        break
    fi
done

# === TEST 3: Métricas del job ===
echo ""
echo -e "${GREEN}=== TEST 3: Métricas del job ===${NC}"
echo ""

echo "Métricas del job:"
curl -s "http://127.0.0.1:8080/api/v1/jobs/$JOB_ID/metrics" | python -m json.tool
echo ""

# === TEST 4: Estadísticas de etapas ===
echo -e "${GREEN}=== TEST 4: Estadísticas de etapas ===${NC}"
echo ""

echo "Estadísticas por etapa:"
curl -s "http://127.0.0.1:8080/api/v1/jobs/$JOB_ID/stages" | python -m json.tool
echo ""

# === TEST 5: Métricas de todos los jobs ===
echo -e "${GREEN}=== TEST 5: Métricas de todos los jobs ===${NC}"
echo ""

echo "Métricas de todos los jobs:"
curl -s http://127.0.0.1:8080/api/v1/metrics/jobs | python -m json.tool
echo ""

# === TEST 6: Métricas del worker ===
echo -e "${GREEN}=== TEST 6: Métricas del worker ===${NC}"
echo ""

echo "Métricas del worker (puerto 10000):"
curl -s http://127.0.0.1:10000/metrics 2>/dev/null | python -m json.tool || echo "(Worker metrics endpoint no disponible)"
echo ""

# === TEST 7: Métricas del sistema actualizadas ===
echo -e "${GREEN}=== TEST 7: Métricas del sistema (después del job) ===${NC}"
echo ""

echo "Métricas del sistema:"
curl -s http://127.0.0.1:8080/api/v1/metrics/system | python -m json.tool
echo ""

# Resumen
echo -e "${GREEN}=== Resumen de endpoints de métricas ===${NC}"
echo ""
echo "Master (puerto 8080):"
echo "  GET /api/v1/metrics/system    - Métricas del sistema"
echo "  GET /api/v1/metrics/jobs      - Métricas de todos los jobs"
echo "  GET /api/v1/metrics/failures  - Contadores de fallos"
echo "  GET /api/v1/jobs/{id}/metrics - Métricas de un job"
echo "  GET /api/v1/jobs/{id}/stages  - Estadísticas por etapa"
echo ""
echo "Worker (puerto 10000 por defecto):"
echo "  GET /metrics                  - Métricas del worker"
echo ""
echo -e "${GREEN}=== Tests de Métricas completados ===${NC}"
