#!/bin/bash
# scripts/benchmark_1m.sh
# Benchmark oficial Mini-Spark – 1M registros

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Detectar raíz del proyecto (carpeta que contiene docker-compose.yml)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

echo -e "${GREEN}"
echo "============================================"
echo "        Mini-Spark Benchmark – 1M"
echo "============================================"
echo -e "${NC}"

# ------------------------------------------------------------
# 1. Crear dataset de 1M si no existe (EN ./data, no en ./scripts)
# ------------------------------------------------------------
echo -e "${YELLOW}1. Preparando dataset 1,000,000 registros...${NC}"

DATA_DIR="$ROOT_DIR/data"
mkdir -p "$DATA_DIR"
DATA_FILE="$DATA_DIR/benchmark_1m.csv"

if [ ! -f "$DATA_FILE" ]; then
    echo "text" > "$DATA_FILE"
    echo "   Generando dataset grande... (esto puede tardar)"
    for i in $(seq 1 1000000); do
        echo "line $i hello world foo bar baz test benchmark data lorem ipsum" >> "$DATA_FILE"
    done
    echo "   Dataset generado en $DATA_FILE"
else
    echo "   Dataset ya existe en $DATA_FILE, usando existente."
fi

# ------------------------------------------------------------
# 2. Build del proyecto
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}2. Compilando proyecto (modo release)...${NC}"
cargo build --release

# ------------------------------------------------------------
# 3. Levantar cluster
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}3. Iniciando cluster (master + workers)...${NC}"
docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
docker-compose up -d master worker1 worker2

echo "   Esperando a que master esté listo..."

until curl -s http://localhost:8080/health >/dev/null 2>&1; do
    sleep 1
done

echo -e "${GREEN}   Master OK${NC}"

# ------------------------------------------------------------
# 4. Ejecutar BENCHMARK (WORDCOUNT)
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}4. Ejecutando WordCount sobre 1M registros...${NC}"

START_TS=$(date +%s%3N)

JOB_RESPONSE=$(docker-compose run --rm client \
    submit \
    --input /app/data/benchmark_1m.csv \
    --parallelism 6 2>&1)

echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')
if [ -z "$JOB_ID" ]; then
    echo -e "${RED}   ERROR: No se obtuvo ID del job${NC}"
    docker-compose down >/dev/null 2>&1 || true
    exit 1
fi

echo ""
echo "   Job ID = $JOB_ID"
echo "   Esperando a que termine..."

# ------------------------------------------------------------
# 5. Polling de estado hasta completarlo
# ------------------------------------------------------------
while true; do
    STATUS=$(docker-compose run --rm client status "$JOB_ID" 2>&1 || true)

    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}   Job completado exitosamente${NC}"
        break
    fi

    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}   Job falló${NC}"
        echo "$STATUS"
        docker-compose down >/dev/null 2>&1 || true
        exit 1
    fi

    PROG=$(echo "$STATUS" | grep "Progreso" | awk '{print $2}')
    if [ -n "$PROG" ]; then
        echo "   Progreso: $PROG"
    else
        echo "   Progreso: (sin info)"
    fi
    sleep 2
done

END_TS=$(date +%s%3N)
TOTAL_MS=$((END_TS - START_TS))

echo ""
echo -e "${GREEN}Tiempo total: ${TOTAL_MS} ms${NC}"

# ------------------------------------------------------------
# 6. Obtener métricas del master
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}6. Obteniendo métricas del master...${NC}"

mkdir -p "$ROOT_DIR/results"
cd "$ROOT_DIR"

curl -s "http://localhost:8080/api/v1/jobs/${JOB_ID}/metrics" > results/benchmark_1m_metrics.json || echo '{}' > results/benchmark_1m_metrics.json
curl -s "http://localhost:8080/api/v1/metrics/system" > results/system_metrics.json || echo '{}' > results/system_metrics.json
curl -s "http://localhost:8080/api/v1/jobs/${JOB_ID}/stages" > results/benchmark_1m_stages.json || echo '[]' > results/benchmark_1m_stages.json

cat <<EOF > results/benchmark_1m.json
{
  "job_id": "$JOB_ID",
  "duration_ms": $TOTAL_MS,
  "timestamp": "$(date)",
  "system_metrics": $(cat results/system_metrics.json),
  "job_metrics": $(cat results/benchmark_1m_metrics.json),
  "stage_timeline": $(cat results/benchmark_1m_stages.json)
}
EOF

echo -e "${GREEN}   Resultados guardados en results/benchmark_1m.json${NC}"

# ------------------------------------------------------------
# 7. Apagar cluster
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}7. Apagando cluster...${NC}"
docker-compose down >/dev/null 2>&1

echo ""
echo -e "${GREEN}============================================"
echo "       Benchmark completado con éxito"
echo "============================================${NC}"
#!/bin/bash
# scripts/benchmark_1m.sh
# Benchmark oficial Mini-Spark – 1M registros

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Detectar raíz del proyecto (carpeta que contiene docker-compose.yml)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

echo -e "${GREEN}"
echo "============================================"
echo "        Mini-Spark Benchmark – 1M"
echo "============================================"
echo -e "${NC}"

# ------------------------------------------------------------
# 1. Crear dataset de 1M si no existe (EN ./data, no en ./scripts)
# ------------------------------------------------------------
echo -e "${YELLOW}1. Preparando dataset 1,000,000 registros...${NC}"

DATA_DIR="$ROOT_DIR/data"
mkdir -p "$DATA_DIR"
DATA_FILE="$DATA_DIR/benchmark_1m.csv"

if [ ! -f "$DATA_FILE" ]; then
    echo "text" > "$DATA_FILE"
    echo "   Generando dataset grande... (esto puede tardar)"
    for i in $(seq 1 1000000); do
        echo "line $i hello world foo bar baz test benchmark data lorem ipsum" >> "$DATA_FILE"
    done
    echo "   Dataset generado en $DATA_FILE"
else
    echo "   Dataset ya existe en $DATA_FILE, usando existente."
fi

# ------------------------------------------------------------
# 2. Build del proyecto
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}2. Compilando proyecto (modo release)...${NC}"
cargo build --release

# ------------------------------------------------------------
# 3. Levantar cluster
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}3. Iniciando cluster (master + workers)...${NC}"
docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
docker-compose up -d master worker1 worker2

echo "   Esperando a que master esté listo..."

until curl -s http://localhost:8080/health >/dev/null 2>&1; do
    sleep 1
done

echo -e "${GREEN}   Master OK${NC}"

# ------------------------------------------------------------
# 4. Ejecutar BENCHMARK (WORDCOUNT)
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}4. Ejecutando WordCount sobre 1M registros...${NC}"

START_TS=$(date +%s%3N)

JOB_RESPONSE=$(docker-compose run --rm client \
    submit \
    --input /app/data/benchmark_1m.csv \
    --parallelism 6 2>&1)

echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')
if [ -z "$JOB_ID" ]; then
    echo -e "${RED}   ERROR: No se obtuvo ID del job${NC}"
    docker-compose down >/dev/null 2>&1 || true
    exit 1
fi

echo ""
echo "   Job ID = $JOB_ID"
echo "   Esperando a que termine..."

# ------------------------------------------------------------
# 5. Polling de estado hasta completarlo
# ------------------------------------------------------------
while true; do
    STATUS=$(docker-compose run --rm client status "$JOB_ID" 2>&1 || true)

    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}   Job completado exitosamente${NC}"
        break
    fi

    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}   Job falló${NC}"
        echo "$STATUS"
        docker-compose down >/dev/null 2>&1 || true
        exit 1
    fi

    PROG=$(echo "$STATUS" | grep "Progreso" | awk '{print $2}')
    if [ -n "$PROG" ]; then
        echo "   Progreso: $PROG"
    else
        echo "   Progreso: (sin info)"
    fi
    sleep 2
done

END_TS=$(date +%s%3N)
TOTAL_MS=$((END_TS - START_TS))

echo ""
echo -e "${GREEN}Tiempo total: ${TOTAL_MS} ms${NC}"

# ------------------------------------------------------------
# 6. Obtener métricas del master
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}6. Obteniendo métricas del master...${NC}"

mkdir -p "$ROOT_DIR/results"
cd "$ROOT_DIR"

curl -s "http://localhost:8080/api/v1/jobs/${JOB_ID}/metrics" > results/benchmark_1m_metrics.json || echo '{}' > results/benchmark_1m_metrics.json
curl -s "http://localhost:8080/api/v1/metrics/system" > results/system_metrics.json || echo '{}' > results/system_metrics.json
curl -s "http://localhost:8080/api/v1/jobs/${JOB_ID}/stages" > results/benchmark_1m_stages.json || echo '[]' > results/benchmark_1m_stages.json

cat <<EOF > results/benchmark_1m.json
{
  "job_id": "$JOB_ID",
  "duration_ms": $TOTAL_MS,
  "timestamp": "$(date)",
  "system_metrics": $(cat results/system_metrics.json),
  "job_metrics": $(cat results/benchmark_1m_metrics.json),
  "stage_timeline": $(cat results/benchmark_1m_stages.json)
}
EOF

echo -e "${GREEN}   Resultados guardados en results/benchmark_1m.json${NC}"

# ------------------------------------------------------------
# 7. Apagar cluster
# ------------------------------------------------------------
echo ""
echo -e "${YELLOW}7. Apagando cluster...${NC}"
docker-compose down >/dev/null 2>&1

echo ""
echo -e "${GREEN}============================================"
echo "       Benchmark completado con éxito"
echo "============================================${NC}"
