#!/bin/bash

# scripts/test_fault_tolerance.sh
# Script para probar tolerancia a fallos del sistema mini-Spark

set -e

echo "=== Test de Tolerancia a Fallos ==="
echo ""

# Colores
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Función para limpiar procesos al salir
cleanup() {
    echo ""
    echo -e "${YELLOW}Limpiando procesos...${NC}"
    pkill -f "target/release/master" 2>/dev/null || true
    pkill -f "target/release/worker" 2>/dev/null || true
    pkill -f "target/debug/master" 2>/dev/null || true
    pkill -f "target/debug/worker" 2>/dev/null || true
    rm -rf /tmp/minispark/*
}

trap cleanup EXIT

# Compilar
echo -e "${YELLOW}Compilando...${NC}"
cargo build --release

# Crear directorio de datos
mkdir -p /tmp/minispark
mkdir -p data

# Crear archivo de prueba
echo -e "${YELLOW}Creando datos de prueba...${NC}"
cat > data/input.csv << 'EOF'
text
hello world
foo bar baz
hello foo
world bar
test data
hello world foo
EOF

echo "Datos creados en data/input.csv"

# === TEST 1: Fallo simulado de tarea ===
echo ""
echo -e "${GREEN}=== TEST 1: Reintentos por fallo de tarea ===${NC}"
echo ""

# Iniciar master
echo "Iniciando master..."
./target/release/master &
MASTER_PID=$!
sleep 2

# Iniciar worker con 30% probabilidad de fallo
echo "Iniciando worker con 30% probabilidad de fallo..."
FAIL_PROBABILITY=30 ./target/release/worker &
WORKER_PID=$!
sleep 2

# Enviar job
echo "Enviando job..."
JOB_RESPONSE=$(./target/release/client submit --input data/input.csv --parallelism 2 2>&1)
echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}Error: No se pudo obtener ID del job${NC}"
    exit 1
fi

echo ""
echo "Job ID: $JOB_ID"
echo "Esperando completación (con reintentos)..."

# Esperar hasta 60 segundos
for i in {1..30}; do
    sleep 2
    STATUS=$(./target/release/client status "$JOB_ID" 2>&1)
    echo "$STATUS"
    
    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}✓ TEST 1 PASADO: Job completado exitosamente con reintentos${NC}"
        break
    fi
    
    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}✗ TEST 1 FALLIDO: Job falló${NC}"
        break
    fi
done

# Verificar métricas de fallos
echo ""
echo "Métricas de fallos:"
curl -s http://127.0.0.1:8080/api/v1/metrics/failures | python3 -m json.tool 2>/dev/null || echo "(no json formatter)"

# Limpiar para siguiente test
kill $WORKER_PID 2>/dev/null || true
kill $MASTER_PID 2>/dev/null || true
sleep 2
rm -rf /tmp/minispark/*

# === TEST 2: Worker caído durante ejecución ===
echo ""
echo -e "${GREEN}=== TEST 2: Replanificación por worker caído ===${NC}"
echo ""

# Iniciar master
echo "Iniciando master..."
./target/release/master &
MASTER_PID=$!
sleep 2

# Iniciar 2 workers
echo "Iniciando worker 1 (puerto 9000)..."
WORKER_PORT=9000 ./target/release/worker &
WORKER1_PID=$!
sleep 1

echo "Iniciando worker 2 (puerto 9001)..."
WORKER_PORT=9001 ./target/release/worker &
WORKER2_PID=$!
sleep 2

# Enviar job con más particiones
echo "Enviando job con 4 particiones..."
JOB_RESPONSE=$(./target/release/client submit --input data/input.csv --parallelism 4 2>&1)
echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

# Esperar un poco y matar worker 1
sleep 3
echo ""
echo -e "${YELLOW}Matando worker 1 (simulando fallo)...${NC}"
kill -9 $WORKER1_PID 2>/dev/null || true

# Esperar completación
echo "Esperando replanificación y completación..."
for i in {1..30}; do
    sleep 2
    STATUS=$(./target/release/client status "$JOB_ID" 2>&1)
    echo "$STATUS"
    
    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}✓ TEST 2 PASADO: Job completado después de replanificación${NC}"
        break
    fi
    
    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}✗ TEST 2 FALLIDO: Job falló${NC}"
        break
    fi
done

# Métricas finales
echo ""
echo "Métricas de fallos:"
curl -s http://127.0.0.1:8080/api/v1/metrics/failures | python3 -m json.tool 2>/dev/null || echo "(no json formatter)"

echo ""
echo -e "${GREEN}=== Tests de tolerancia a fallos completados ===${NC}"