#!/bin/bash
# scripts/run_tests.sh
# Ejecutar todas las pruebas unitarias

set -e

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${GREEN}"
echo "============================================"
echo "   Mini-Spark - Pruebas Unitarias"
echo "============================================"
echo -e "${NC}"

# Crear directorio temporal si no existe
mkdir -p /tmp/minispark

echo -e "${YELLOW}Ejecutando pruebas unitarias...${NC}"
echo ""

# Ejecutar tests con output detallado
cargo test --workspace -- --nocapture 2>&1 | tee test_output.log

# Contar resultados
PASSED=$(grep -c "test .* ok" test_output.log || echo "0")
FAILED=$(grep -c "test .* FAILED" test_output.log || echo "0")

echo ""
echo -e "${GREEN}============================================"
echo "   Resumen de Pruebas"
echo "============================================${NC}"
echo ""
echo -e "Pasadas: ${GREEN}$PASSED${NC}"
echo -e "Fallidas: ${RED}$FAILED${NC}"

if [ "$FAILED" = "0" ]; then
    echo ""
    echo -e "${GREEN}✓ Todas las pruebas pasaron${NC}"
else
    echo ""
    echo -e "${RED}✗ Hay pruebas fallidas${NC}"
    exit 1
fi

# Limpiar
rm -f test_output.log