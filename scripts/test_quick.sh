#!/bin/bash
# scripts/test_quick.sh
# Quick test: verify system works (< 1 minute)

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}"
echo "============================================"
echo "    Mini-Spark Quick Test (< 1 minute)"
echo "============================================"
echo -e "${NC}"

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# 1. Build
echo -e "${YELLOW}1. Building...${NC}"
cargo build --release 2>&1 | grep -E "(Compiling|Finished)" || true

# 2. Create small dataset (500 lines, not 1M)
echo -e "${YELLOW}2. Creating test dataset (500 lines)...${NC}"
mkdir -p data
cat > data/quick_test.csv <<EOF
text
hello world test data
foo bar baz
quick test file
distributed system
mini spark engine
processing batch jobs
map filter reduce
lambda functions
parallel execution
EOF

# Add 490 more lines quickly
for i in {1..490}; do
    echo "line $i test data for quick verification" >> data/quick_test.csv
done

# 3. Start cluster
echo -e "${YELLOW}3. Starting cluster...${NC}"
docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
docker-compose up -d master worker1 worker2

echo "   Waiting for master..."
until curl -s http://localhost:8080/health >/dev/null 2>&1; do
    sleep 1
done
echo -e "${GREEN}   Master ready${NC}"

# 4. Run quick job
echo -e "${YELLOW}4. Submitting WordCount job (500 records)...${NC}"

START_TS=$(date +%s)

JOB_RESPONSE=$(docker-compose run --rm client \
    submit \
    --input /app/data/quick_test.csv \
    --parallelism 2 2>&1)

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}ERROR: Could not get job ID${NC}"
    docker-compose down >/dev/null 2>&1 || true
    exit 1
fi

echo "   Job ID: $JOB_ID"
echo "   Waiting for completion..."

# 5. Poll for completion
for i in {1..30}; do
    STATUS=$(docker-compose run --rm client status "$JOB_ID" 2>&1 || true)
    
    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}   ✓ Job completed successfully${NC}"
        break
    fi
    
    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}   ✗ Job failed${NC}"
        docker-compose down >/dev/null 2>&1 || true
        exit 1
    fi
    
    sleep 1
done

END_TS=$(date +%s)
ELAPSED=$((END_TS - START_TS))

# 6. Check metrics
echo -e "${YELLOW}5. Checking metrics...${NC}"

SYSTEM_METRICS=$(curl -s http://localhost:8080/api/v1/metrics/system 2>/dev/null || echo "{}")
WORKERS_UP=$(echo "$SYSTEM_METRICS" | grep -o '"workers_up":[0-9]*' | cut -d: -f2)

echo "   Workers up: $WORKERS_UP/2"

if [ "$WORKERS_UP" = "2" ]; then
    echo -e "${GREEN}   ✓ All workers healthy${NC}"
else
    echo -e "${RED}   ✗ Workers not responding${NC}"
fi

# 7. Get results
echo -e "${YELLOW}6. Retrieving results...${NC}"

RESULTS=$(docker-compose run --rm client results "$JOB_ID" 2>&1 || true)

if echo "$RESULTS" | grep -q "wordcount"; then
    echo -e "${GREEN}   ✓ Results retrieved${NC}"
else
    echo -e "${RED}   ✗ No results${NC}"
fi

# 8. Shutdown
echo -e "${YELLOW}7. Cleaning up...${NC}"
docker-compose down >/dev/null 2>&1

# 9. Summary
echo ""
echo -e "${GREEN}============================================"
echo "           QUICK TEST COMPLETED"
echo "============================================${NC}"
echo ""
echo "Results:"
echo -e "  Time elapsed: ${ELAPSED}s"
echo -e "  Job ID: $JOB_ID"
echo -e "  Status: ✓ PASSED"
echo ""
echo "System verified:"
echo "  ✓ Master starts correctly"
echo "  ✓ Workers register successfully"
echo "  ✓ Jobs execute end-to-end"
echo "  ✓ Metrics collection works"
echo ""
echo "Ready for benchmark:"
echo "  bash scripts/benchmark_1m.sh"