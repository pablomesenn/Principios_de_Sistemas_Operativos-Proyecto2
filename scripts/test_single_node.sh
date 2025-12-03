#!/bin/bash
set -e

# =============================
#  Colores
# =============================
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

# =============================
#  Cleanup automático al salir
# =============================
cleanup() {
    echo -e "${YELLOW}Limpiando procesos...${NC}"
    pkill -f "target/release/master" 2>/dev/null || true
    pkill -f "target/release/worker" 2>/dev/null || true
}
trap cleanup EXIT

echo -e "${GREEN}"
echo "============================================"
echo "      TEST AUTOMÁTICO SINGLE NODE"
echo "============================================"
echo -e "${NC}"

# =============================
# 1. Limpieza de estado previo
# =============================
echo -e "${YELLOW}Limpiando /tmp/minispark...${NC}"
rm -rf C:/tmp/minispark/*
mkdir -p C:/tmp/minispark

# =============================
# 2. Compilar proyecto
# =============================
echo -e "${YELLOW}Compilando proyecto (release)...${NC}"
cargo build --release

# =============================
# 3. Iniciar master
# =============================
echo -e "${YELLOW}Iniciando master...${NC}"
./target/release/master &
sleep 2

# =============================
# 4. Iniciar worker único
# =============================
echo -e "${YELLOW}Iniciando worker único...${NC}"
MASTER_URL="http://127.0.0.1:8080" \
WORKER_PORT=9000 \
WORKER_THREADS=4 \
CACHE_MAX_MB=64 \
./target/release/worker &
sleep 3

# =============================
# 5. Enviar job de wordcount
# =============================
echo -e "${GREEN}"
echo "========== Enviando Job Wordcount =========="
echo -e "${NC}"

JOB_RESPONSE=$(./target/release/client \
  submit \
  --name wordcount-test \
  --parallelism 4 \
  --input data/input.csv 2>&1)

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
    STATUS_OUTPUT=$(./target/release/client status "$JOB_ID" 2>&1)
    echo "$STATUS_OUTPUT" | grep "Estado:" || true

    if echo "$STATUS_OUTPUT" | grep -q "Succeeded"; then
        echo -e "${GREEN}Job completado correctamente.${NC}"
        break
    fi

    if echo "$STATUS_OUTPUT" | grep -q "Failed"; then
        echo -e "${RED}El Job falló.${NC}"
        echo "$STATUS_OUTPUT"
        exit 1
    fi

    sleep 1
done

# =============================
# 7. Mostrar resultados
# =============================
echo -e "${GREEN}"
echo "=============== Resultados ================"
echo -e "${NC}"

./target/release/client results "$JOB_ID" || true

# =============================
# 8. Resumen final
# =============================
echo -e "${GREEN}"
echo "============================================"
echo "       TEST SINGLE NODE COMPLETADO"
echo "============================================"
echo -e "${NC}"
