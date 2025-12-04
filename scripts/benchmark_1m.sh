#!/bin/bash
# scripts/benchmark_1m.sh
# Benchmark oficial Mini-Spark - 1M registros

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$ROOT_DIR"

echo -e "${GREEN}"
echo "============================================"
echo "        Mini-Spark Benchmark - 1M Records"
echo "============================================"
echo -e "${NC}"

# 1. Crear dataset de 1M
echo -e "${YELLOW}1. Preparing 1,000,000 records dataset...${NC}"

DATA_DIR="$ROOT_DIR/data"
mkdir -p "$DATA_DIR"
DATA_FILE="$DATA_DIR/benchmark_1m.csv"

if [ ! -f "$DATA_FILE" ]; then
    echo "text" > "$DATA_FILE"
    echo "   Generating large dataset... (this may take a moment)"
    for i in $(seq 1 1000000); do
        echo "line $i hello world foo bar baz test benchmark data lorem ipsum" >> "$DATA_FILE"
    done
    echo "   Dataset generated at $DATA_FILE"
else
    echo "   Dataset already exists at $DATA_FILE, using it."
fi

# 2. Build del proyecto
echo ""
echo -e "${YELLOW}2. Compiling project (release mode)...${NC}"
cargo build --release 2>&1 | grep -E "(Compiling|Finished)" || true

# 3. Levantar cluster
echo ""
echo -e "${YELLOW}3. Starting cluster (master + workers)...${NC}"
docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
docker-compose up -d master worker1 worker2

echo "   Waiting for master to be ready..."

until curl -s http://localhost:8080/health >/dev/null 2>&1; do
    sleep 1
done

echo -e "${GREEN}   Master OK${NC}"

# 4. Ejecutar BENCHMARK
echo ""
echo -e "${YELLOW}4. Running WordCount on 1M records...${NC}"

START_TS=$(date +%s%3N)

JOB_RESPONSE=$(docker-compose run --rm client \
    submit \
    --input /app/data/benchmark_1m.csv \
    --parallelism 6 2>&1)

echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')
if [ -z "$JOB_ID" ]; then
    echo -e "${RED}   ERROR: Could not get job ID${NC}"
    docker-compose down >/dev/null 2>&1 || true
    exit 1
fi

echo ""
echo "   Job ID = $JOB_ID"
echo "   Waiting for completion..."

# 5. Polling de estado
COUNTER=0
while true; do
    STATUS=$(docker-compose run --rm client status "$JOB_ID" 2>&1 || true)

    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}   Job completed successfully${NC}"
        break
    fi

    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}   Job failed${NC}"
        echo "$STATUS"
        docker-compose down >/dev/null 2>&1 || true
        exit 1
    fi

    PROG=$(echo "$STATUS" | grep "Progreso" | awk '{print $2}')
    if [ -n "$PROG" ]; then
        echo "   Progress: $PROG (elapsed: $((COUNTER * 2))s)"
    else
        echo "   Progress: (waiting...)"
    fi
    COUNTER=$((COUNTER + 1))
    sleep 2
done

END_TS=$(date +%s%3N)
TOTAL_MS=$((END_TS - START_TS))
TOTAL_SEC=$((TOTAL_MS / 1000))

echo ""
echo -e "${GREEN}Total time: ${TOTAL_MS} ms (${TOTAL_SEC}s)${NC}"

# 6. Obtener metricas
echo ""
echo -e "${YELLOW}6. Collecting metrics from master...${NC}"

mkdir -p "$ROOT_DIR/results"
cd "$ROOT_DIR"

curl -s "http://localhost:8080/api/v1/jobs/${JOB_ID}/metrics" > results/benchmark_1m_metrics.json || echo '{}' > results/benchmark_1m_metrics.json
curl -s "http://localhost:8080/api/v1/metrics/system" > results/system_metrics.json || echo '{}' > results/system_metrics.json
curl -s "http://localhost:8080/api/v1/jobs/${JOB_ID}/stages" > results/benchmark_1m_stages.json || echo '[]' > results/benchmark_1m_stages.json

# Calculate throughput
RECORDS=1000000
THROUGHPUT=$(echo "scale=2; $RECORDS / ($TOTAL_MS / 1000)" | bc)

cat > results/benchmark_1m_report.txt <<EOF
=============================================
Mini-Spark Benchmark Report - 1M Records
=============================================

ENVIRONMENT:
  Dataset: 1,000,000 lines of text
  Parallelism: 6 partitions
  Workers: 2 (docker-compose)
  Operation: WordCount (flat_map -> map -> reduce)

RESULTS:
  Total Time: ${TOTAL_MS} ms (${TOTAL_SEC} seconds)
  Throughput: ${THROUGHPUT} records/second
  Job ID: ${JOB_ID}
  Status: Succeeded

METRICS FILES:
  - Job metrics: results/benchmark_1m_metrics.json
  - System metrics: results/system_metrics.json
  - Stage timeline: results/benchmark_1m_stages.json
  - Full report: results/benchmark_1m.json

Command to reproduce:
  bash scripts/benchmark_1m.sh

EOF

cat > results/benchmark_1m.json <<EOF
{
  "job_id": "$JOB_ID",
  "duration_ms": $TOTAL_MS,
  "duration_sec": $TOTAL_SEC,
  "throughput_records_per_sec": $THROUGHPUT,
  "total_records": $RECORDS,
  "parallelism": 6,
  "workers": 2,
  "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "system_metrics": $(cat results/system_metrics.json),
  "job_metrics": $(cat results/benchmark_1m_metrics.json),
  "stage_timeline": $(cat results/benchmark_1m_stages.json)
}
EOF

echo -e "${GREEN}   Results saved in results/benchmark_1m.json${NC}"
echo -e "${GREEN}   Report saved in results/benchmark_1m_report.txt${NC}"

# 7. Apagar cluster
echo ""
echo -e "${YELLOW}7. Shutting down cluster...${NC}"
docker-compose down >/dev/null 2>&1

echo ""
echo -e "${GREEN}============================================"
echo "       Benchmark completed successfully"
echo "============================================${NC}"
echo ""
echo "Check results:"
echo "  cat results/benchmark_1m_report.txt"
echo "  cat results/benchmark_1m.json | python3 -m json.tool"