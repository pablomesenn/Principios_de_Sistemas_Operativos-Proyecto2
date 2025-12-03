#!/bin/bash
# scripts/docker-fault-test.sh
# Test de tolerancia a fallos matando un worker durante ejecución

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}"
echo "============================================"
echo "  Test de Tolerancia a Fallos (Docker)"
echo "============================================"
echo -e "${NC}"

# Crear datos grandes para que el job tarde más
echo -e "${YELLOW}1. Creando datos de prueba (1000 líneas)...${NC}"
mkdir -p data
echo "text" > data/fault_test.csv
for i in $(seq 1 1000); do
    echo "line $i hello world foo bar baz test data" >> data/fault_test.csv
done

# Iniciar cluster con 3 workers
echo -e "${YELLOW}2. Iniciando cluster con 3 workers...${NC}"
docker-compose down -v 2>/dev/null || true
docker-compose up -d master worker1 worker2
docker-compose up -d --scale worker3=1 2>/dev/null || docker-compose --profile full up -d worker3

sleep 5

# Verificar workers
echo -e "${YELLOW}3. Verificando workers registrados...${NC}"
curl -s http://localhost:8080/api/v1/metrics/system | python3 -m json.tool 2>/dev/null || \
    curl -s http://localhost:8080/api/v1/metrics/system

# Enviar job con muchas particiones
echo ""
echo -e "${YELLOW}4. Enviando job con 8 particiones...${NC}"
JOB_RESPONSE=$(docker-compose run --rm client submit --input /app/data/fault_test.csv --parallelism 8 2>&1)
echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

# Esperar un poco y matar worker2
sleep 3
echo ""
echo -e "${RED}5. ¡MATANDO worker2 para simular fallo!${NC}"
docker-compose stop worker2

# Esperar completación
echo ""
echo -e "${YELLOW}6. Esperando que el job se complete (con replanificación)...${NC}"
for i in {1..60}; do
    STATUS=$(docker-compose run --rm client status "$JOB_ID" 2>&1)
    
    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}$STATUS${NC}"
        echo ""
        echo -e "${GREEN}============================================"
        echo "  ✓ TEST PASADO: Job completado a pesar del fallo"
        echo "============================================${NC}"
        break
    fi
    
    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}$STATUS${NC}"
        echo -e "${RED}  ✗ TEST FALLIDO: Job no se recuperó${NC}"
        break
    fi
    
    PROGRESS=$(echo "$STATUS" | grep "Progreso" | awk '{print $2}')
    echo "   Progreso: $PROGRESS (esperando replanificación...)"
    sleep 2
done

# Mostrar métricas de fallos
echo ""
echo -e "${YELLOW}7. Métricas de fallos:${NC}"
curl -s http://localhost:8080/api/v1/metrics/failures | python3 -m json.tool 2>/dev/null || \
    curl -s http://localhost:8080/api/v1/metrics/failures

# Limpiar
echo ""
echo -e "${YELLOW}8. Limpiando...${NC}"
docker-compose down