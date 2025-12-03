#!/bin/bash
# scripts/docker-demo.sh
# Script de demostración del cluster Mini-Spark en Docker

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}"
echo "============================================"
echo "     Mini-Spark Docker Demo"
echo "============================================"
echo -e "${NC}"

# Verificar que Docker esté corriendo
if ! docker info > /dev/null 2>&1; then
    echo -e "${RED}Error: Docker no está corriendo${NC}"
    exit 1
fi

# Crear datos de prueba
echo -e "${YELLOW}1. Creando datos de prueba...${NC}"
mkdir -p data
echo "text" > data/input.csv
for i in $(seq 1 100); do
    echo "hello world foo bar baz line $i" >> data/input.csv
done
echo "   Creados 100 líneas en data/input.csv"

# Construir imágenes
echo -e "${YELLOW}2. Construyendo imágenes Docker...${NC}"
docker-compose build

# Iniciar cluster
echo -e "${YELLOW}3. Iniciando cluster (master + 2 workers)...${NC}"
docker-compose up -d master worker1 worker2
sleep 5

# Verificar que el master esté listo
echo -e "${YELLOW}4. Verificando salud del cluster...${NC}"
until curl -s http://localhost:8080/health > /dev/null 2>&1; do
    echo "   Esperando al master..."
    sleep 2
done
echo -e "${GREEN}   Master listo!${NC}"

# Mostrar estado del sistema
echo -e "${YELLOW}5. Estado del sistema:${NC}"
curl -s http://localhost:8080/api/v1/metrics/system | python3 -m json.tool 2>/dev/null || \
    curl -s http://localhost:8080/api/v1/metrics/system

# Enviar job
echo ""
echo -e "${YELLOW}6. Enviando job WordCount...${NC}"
JOB_RESPONSE=$(docker-compose run --rm client submit --input /app/data/input.csv --parallelism 4 2>&1)
echo "$JOB_RESPONSE"

JOB_ID=$(echo "$JOB_RESPONSE" | grep "ID:" | awk '{print $2}')

if [ -z "$JOB_ID" ]; then
    echo -e "${RED}Error: No se pudo obtener el ID del job${NC}"
    docker-compose logs
    exit 1
fi

# Esperar resultado
echo ""
echo -e "${YELLOW}7. Esperando resultado (Job ID: $JOB_ID)...${NC}"
for i in {1..30}; do
    STATUS=$(docker-compose run --rm client status "$JOB_ID" 2>&1)
    
    if echo "$STATUS" | grep -q "Succeeded"; then
        echo -e "${GREEN}$STATUS${NC}"
        echo -e "${GREEN}   ¡Job completado exitosamente!${NC}"
        break
    fi
    
    if echo "$STATUS" | grep -q "Failed"; then
        echo -e "${RED}$STATUS${NC}"
        echo -e "${RED}   Job falló${NC}"
        break
    fi
    
    PROGRESS=$(echo "$STATUS" | grep "Progreso" | awk '{print $2}')
    echo "   Progreso: $PROGRESS"
    sleep 2
done

# Mostrar métricas finales
echo ""
echo -e "${YELLOW}8. Métricas del job:${NC}"
docker-compose run --rm client metrics job "$JOB_ID" 2>/dev/null | python3 -m json.tool 2>/dev/null || \
    docker-compose run --rm client metrics job "$JOB_ID"

# Mostrar logs
echo ""
echo -e "${YELLOW}9. Logs recientes:${NC}"
docker-compose logs --tail=20

echo ""
echo -e "${GREEN}============================================"
echo "     Demo completada"
echo "============================================${NC}"
echo ""
echo "Comandos útiles:"
echo "  docker-compose logs -f              # Ver logs en tiempo real"
echo "  docker-compose ps                   # Ver estado de contenedores"
echo "  docker-compose down                 # Apagar cluster"
echo "  docker-compose down -v              # Apagar y limpiar volúmenes"