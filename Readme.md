Para probar: 

cargo build

# Terminal 1
make run-master

# Terminal 2
make run-worker

# Terminal 3
make run-client-submit
cargo run --bin client -- submit
cargo run --bin client -- submit-join

make create-test-data
make run-client-submit

--------------------------------


Guía de Desarrollo por Fases
FASE 1 - Correcciones y Base 

Corregir edition en todos los Cargo.toml
Crear carpetas /docs y /scripts
Crear estructura de datos para Task (tarea individual vs Job completo)
Implementar endpoint para que workers reciban tareas del master

FASE 2 - Planificación y Ejecución Básica 

Implementar cola de tareas en el master
Implementar planificador round-robin con awareness de carga
Worker: pool de hilos para ejecutar tareas
Implementar operadores básicos: map, filter
Ejecutar un job simple end-to-end

FASE 3 - Operadores Avanzados 

Implementar flat_map
Implementar reduce_by_key con shuffle
Implementar join por clave
Manejo de particiones de datos
Lectura/escritura CSV y JSONL

FASE 4 - Tolerancia a Fallos 

Detección de worker caído (timeout de heartbeat)
Marcar worker como DOWN
Replanificar tareas pendientes
Implementar reintentos (al menos 1)
Task attempt ID para idempotencia

FASE 5 - Memoria y Almacenamiento 

Cache en memoria por partición
Spill a disco cuando supere umbral
Persistencia mínima del estado del job (archivo o sqlite)

FASE 6 - Observabilidad y Métricas

Métricas por nodo (CPU aprox, memoria, tareas activas)
Métricas por job (tiempo, etapas, throughput)
Logging estructurado con niveles
Endpoint de resultados funcional

FASE 7 - Infraestructura y Demo 

docker-compose.yml
Makefile (build, test, demo)
Pruebas unitarias, integración, E2E
Benchmark con 1M registros
Video demostrativo