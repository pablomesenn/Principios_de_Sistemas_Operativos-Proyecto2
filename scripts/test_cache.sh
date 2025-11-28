#!/bin/bash

# scripts/test_cache.sh
# Script para probar cache en memoria y spill a disco

set -e

echo "=== Test de Cache y Spill a Disco ==="
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
mkdir -p /tmp/minispark/state
mkdir -p /tmp/minispark/spill
mkdir -p data

# Crear datos de prueba (más grande para forzar uso de cache)
echo -e "${YELLOW}Creando datos de prueba...${NC}"
echo "text" > data/cache_test.csv
for i in $(seq 1 1000); do
    echo "line $i hello world foo bar baz test data" >> data/cache_test.csv
done
echo "Datos creados: 1000 líneas"

# === TEST 1: Cache básico ===
echo ""
echo -e "${GREEN}=== TEST 1: Cache en memoria ===${NC}"
echo ""

# Iniciar master
echo "Iniciando master..."
./target/release/master &
MASTER_PID=$!
sleep 2

# Iniciar worker con cache de 1MB (pequeño para forzar spill)
echo "Iniciando worker con cache de 1MB..."
CACHE_MAX_MB=1 ./target/release/worker &
WORKER_PID=$!
sleep 2

# Enviar job
echo "Enviando job..."
JOB_RESPONSE=$(./target/release/client submit --input data/cache_test.csv --parallelism 4 2>&1)
echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}Error: No se pudo obtener ID del job${NC}"
    exit 1
fi

echo ""
echo "Job ID: $JOB_ID"
echo "Esperando completación..."

# Esperar hasta 60 segundos
for i in {1..30}; do
    sleep 2
    STATUS=$(./target/release/client status "$JOB_ID" 2>&1)
    
    if echo "$STATUS" | grep -q "Succeeded"; then
        echo "$STATUS"
        echo -e "${GREEN}✓ TEST 1 PASADO: Job completado${NC}"
        break
    fi
    
    if echo "$STATUS" | grep -q "Failed"; then
        echo "$STATUS"
        echo -e "${RED}✗ TEST 1 FALLIDO: Job falló${NC}"
        break
    fi
    
    echo "Progreso: $(echo "$STATUS" | grep "Progreso" | awk '{print $2}')"
done

# Verificar archivos de spill
echo ""
echo "Archivos de spill:"
ls -la /tmp/minispark/spill/ 2>/dev/null || echo "  (ninguno)"

SPILL_COUNT=$(ls -1 /tmp/minispark/spill/*.json 2>/dev/null | wc -l || echo "0")
if [ "$SPILL_COUNT" -gt "0" ]; then
    echo -e "${GREEN}✓ Spill a disco funcionando: $SPILL_COUNT archivos${NC}"
else
    echo -e "${YELLOW}No hubo spill (datos pequeños o cache suficiente)${NC}"
fi

# === TEST 2: Persistencia de estado ===
echo ""
echo -e "${GREEN}=== TEST 2: Persistencia de estado ===${NC}"
echo ""

# Verificar archivo de estado
if [ -f "/tmp/minispark/state/jobs.json" ]; then
    echo "Estado persistido:"
    cat /tmp/minispark/state/jobs.json | python3 -m json.tool 2>/dev/null || cat /tmp/minispark/state/jobs.json
    echo -e "${GREEN}✓ Persistencia funcionando${NC}"
else
    echo -e "${RED}✗ No se encontró archivo de estado${NC}"
fi

# Consultar estado via API
echo ""
echo "Estado via API:"
curl -s http://127.0.0.1:8080/api/v1/state | python3 -m json.tool 2>/dev/null || echo "(no json formatter)"

# Limpiar
kill $WORKER_PID 2>/dev/null || true
kill $MASTER_PID 2>/dev/null || true
sleep 2

# === TEST 3: Recuperación de estado ===
echo ""
echo -e "${GREEN}=== TEST 3: Verificación de estado post-reinicio ===${NC}"
echo ""

# Reiniciar master y verificar que carga estado
echo "Reiniciando master..."
./target/release/master &
MASTER_PID=$!
sleep 3

# Esperar a que cargue
sleep 2

# Verificar estado
echo "Estado cargado:"
curl -s http://127.0.0.1:8080/api/v1/state | python3 -m json.tool 2>/dev/null || echo "(no json formatter)"

echo ""
echo -e "${GREEN}=== Tests de Cache y Persistencia completados ===${NC}"