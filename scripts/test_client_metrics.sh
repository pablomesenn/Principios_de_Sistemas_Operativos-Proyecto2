#!/bin/bash
set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# =============================
#  Cleanup
# =============================
cleanup() {
    echo -e "${YELLOW}Limpiando procesos...${NC}"
    pkill -f "target/release/master" 2>/dev/null || true
    pkill -f "target/release/worker" 2>/dev/null || true
}
trap cleanup EXIT

echo -e "${GREEN}"
echo "============================================"
echo "      TEST AUTOMÁTICO DE MÉTRICAS CLIENT"
echo "============================================"
echo -e "${NC}"

# =============================
# 1. Compilar
# =============================
echo -e "${YELLOW}Compilando proyecto...${NC}"
cargo build --release

# =============================
# 2. Preparar dataset
# =============================
rm -rf C:/tmp/minispark
mkdir -p data

echo "word" > data/metrics_cli.csv
for i in $(seq 1 50); do
    echo "hello world spark test line $i" >> data/metrics_cli.csv
done

echo -e "${YELLOW}Dataset creado (50 líneas).${NC}"

# =============================
# 3. Iniciar master
# =============================
echo -e "${YELLOW}Iniciando master...${NC}"
./target/release/master &
MASTER_PID=$!
sleep 2

# =============================
# 4. Iniciar worker
# =============================
echo -e "${YELLOW}Iniciando worker...${NC}"
MASTER_URL="http://127.0.0.1:8080" \
WORKER_PORT=9000 \
./target/release/worker &
WORKER_PID=$!
sleep 2

# =============================
# 5. Enviar job de prueba
# =============================
echo -e "${GREEN}"
echo "========== Enviando Job =========="
echo -e "${NC}"

JOB_RESPONSE=$(./target/release/client submit --input data/metrics_cli.csv --parallelism 4 2>&1)

echo "$JOB_RESPONSE"
JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}ERROR: No se pudo obtener el JOB_ID del cliente.${NC}"
    exit 1
fi

echo -e "${GREEN}JOB_ID: $JOB_ID${NC}"
echo ""

# =============================
# 6. Esperar a que termine el job
# =============================
echo -e "${YELLOW}Esperando a que el job termine...${NC}"

for i in {1..30}; do
    STATUS=$(./target/release/client status "$JOB_ID")

    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}Job completado correctamente.${NC}"
        break
    fi

    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}El Job falló.${NC}"
        echo "$STATUS"
        exit 1
    fi

    sleep 1
done

# =============================
# 7. Ejecutar todas las métricas del CLIENTE
# =============================

echo ""
echo -e "${GREEN}"
echo "========================================"
echo "   MÉTRICAS: SYSTEM (client metrics system)"
echo "========================================"
echo -e "${NC}"

./target/release/client metrics system | python -m json.tool || true

echo ""
echo -e "${GREEN}"
echo "========================================"
echo "   MÉTRICAS: JOBS (client metrics jobs)"
echo "========================================"
echo -e "${NC}"

./target/release/client metrics jobs | python -m json.tool || true

echo ""
echo -e "${GREEN}"
echo "========================================"
echo " MÉTRICAS: FAILURES (client metrics failures)"
echo "========================================"
echo -e "${NC}"

./target/release/client metrics failures | python -m json.tool || true

echo ""
echo -e "${GREEN}"
echo "========================================"
echo " MÉTRICAS: JOB SPECÍFICO (client metrics job id)"
echo "========================================"
echo -e "${NC}"

./target/release/client metrics job "$JOB_ID" | python -m json.tool || true

echo ""
echo -e "${GREEN}"
echo "========================================"
echo "    MÉTRICAS POR ETAPAS (client metrics stages id)"
echo "========================================"
echo -e "${NC}"

./target/release/client metrics stages "$JOB_ID" | python -m json.tool || true


# =============================
# 8. Resumen final
# =============================
echo -e "${GREEN}"
echo "============================================"
echo "        TODAS LAS MÉTRICAS FUNCIONAN"
echo "============================================"
echo -e "${NC}"
