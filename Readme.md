# Mini-Spark

## Descripción
#### Autores: Pablo Mesén Alvarado y Luis Urbina Salazar

Mini-Spark es un motor de procesamiento distribuido minimalista, inspirado en Apache Spark, diseñado para experimentar y entender los principios fundamentales del procesamiento distribuido de datos. El proyecto permite ejecutar trabajos distribuidos de ejemplo (word count, join, etc), gestionar tolerancia a fallos, realizar balanceo de carga, cache intermedia y exponer métricas en tiempo real.

**Este proyecto implementa la Ruta A: Batch DAG** del curso de Principios de Sistemas Operativos del TEC Cartago.

## Tabla de Contenidos
- [Características principales](#características-principales)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Requisitos](#requisitos)
- [Resultados del Benchmark](#resultados-del-benchmark)
- [Guía rápida: Ejecución local](#guía-rápida-ejecución-local)
- [Guía rápida: Ejecución con Docker](#guía-rápida-ejecución-con-docker)
- [Documentación completa del Makefile](#documentación-completa-del-makefile)
- [Descripción de los scripts](#descripción-de-los-scripts)
- [Generación de datos de prueba](#generación-de-datos-de-prueba)
- [Endpoints de Métricas](#endpoints-de-métricas)
- [Arquitectura General](#arquitectura-general)
- [Tests incluidos](#tests-incluidos)

---

## Características principales

- **Arquitectura distribuida**: Separación clara de procesos Master, Worker y Cliente.
- **Tolerancia a fallos**: Detección y recuperación automática ante la caída de workers.
- **Métricas en tiempo real**: Exposición vía endpoints HTTP/JSON del estado de jobs, etapas y workers.
- **Balanceo de carga**: Distribución automática del trabajo entre los workers disponibles (round-robin + load-aware).
- **Soporte de cache**: Reutilización de resultados intermedios en los workers con spill a disco.
- **Planificador inteligente**: Round-robin con awareness de carga para distribuir tareas equitativamente.
- **Ejemplo de jobs**: WordCount, Join, etc, sobre archivos CSV.
- **Extensible y didáctico**: Fácil de entender y modificar, ideal para aprendizaje.

---

## Estructura del proyecto

```
.
├── Dockerfile             # Imágenes (Master, Worker, Client)
├── docker-compose.yml     # Orquestación de servicios
├── Makefile               # Comandos build, test, datos, métricas, Docker, etc.
├── scripts/               # Bash scripts para testear, crear datos, automatizar pruebas
├── data/                  # Carpeta de datos de entrada/salida
├── common/                # Crate Rust con tipos y utilidades compartidas
├── master/                # Binario de proceso Master
├── worker/                # Binario de proceso Worker
├── client/                # Binario para enviar jobs
└── README.md              # Documentación principal
```

---

## Requisitos

- **Rust** >= 1.70  
- **Docker** y **Docker Compose** (para despliegue del clúster)  
- Herramientas GNU/Linux (`bash`, `curl`, `python3`)  
- Recomendado: Linux o WSL2 sobre Windows

---

## Resultados del Benchmark

### Benchmark Oficial: 1,000,000 registros (WordCount)

**Entorno:**
- Dataset: 1,000,000 líneas de texto
- Operación: WordCount (flat_map → map → reduce)
- Paralelismo: 6 particiones
- Workers: 2 (en Docker)
- Master: Coordinación y planificación

**Resultados:**
- **Tiempo total:** 17,021 ms (17 segundos)
- **Throughput:** 58,754.40 registros/segundo (entrada)
- **Throughput interno:** 1,320,790.23 registros/segundo (picos internos)
- **Registros procesados:** 17,170,273 (incluye amplificaciones por operadores)
- **Tareas ejecutadas:** 30 tareas exitosas (sin reintentos)
- **Etapas completadas:** 5 (read → flatmap → map → shuffle_write → reduce)

**Desglose por etapa:**
| Etapa | Duración | Tareas | Registros procesados |
|-------|----------|--------|----------------------|
| read | ~2s | 6 | 451,849 |
| flatmap | ~2s | 6 | 5,422,188 |
| map | ~3s | 6 | 5,422,188 |
| shuffle_write | ~2s | 6 | 5,422,188 |
| reduce | ~1s | 6 | 451,860 |

**Conclusión:** El sistema procesa **~59K registros/segundo** en modo entrada, demostrando escalabilidad y eficiencia en la distribución de trabajo entre múltiples workers.

---

## Guía rápida: Ejecución local

1. **Compilar el proyecto:**
   ```sh
   make build
   ```
2. **Crear datos de prueba:**
   ```sh
   make create-test-data
   ```
3. **Arrancar el proceso master:**
   ```sh
   make run-master
   ```
4. **En otras terminales, levantar uno o varios workers:**
   ```sh
   make run-worker
   # O en otros puertos:
   WORKER_PORT=9001 make run-worker
   ```
5. **Enviar un job desde el cliente:**
   ```sh
   make run-client-submit
   ```

---

## Guía rápida: Ejecución con Docker

1. **Build imágenes Docker:**
   ```sh
   make docker-build
   ```
2. **Levantar el clúster completo (Master + 2 Workers + Demo):**
   ```sh
   make docker-demo
   ```
   Esto ejecuta un procesamiento distribuido de ejemplo con 1M registros y muestra métricas en tiempo real.

3. **Detener el clúster:**
   ```sh
   make docker-down
   ```

---

## Documentación completa del Makefile

El Makefile es el corazón de la automatización en Mini-Spark. A continuación se detallan todos los comandos disponibles:

### Construcción y limpieza

- **`make build`** - Compila todos los binarios en modo release
- **`make build-debug`** - Compila en modo debug con símbolos
- **`make clean`** - Elimina binarios compilados

### Ejecución local de servicios

- **`make run-master`** - Arranca el master en puerto 8080
- **`make run-worker`** - Inicia un worker (configurable con WORKER_PORT, WORKER_THREADS, CACHE_MAX_MB)
- **`make run-client-submit`** - Envía un job de wordcount
- **`make run-client-join`** - Envía un job de join
- **`make run-client-status`** - Consulta estado de un job (requiere ID)

### Generación de datos de prueba

- **`make create-test-data`** - Genera datos pequeños (~10 líneas)
- **`make create-large-data`** - Genera datos medianos (~10,000 líneas)
- **`make create-1m-data`** - Genera dataset de benchmark (1M líneas)
- **`make create-join-data`** - Genera datos para pruebas de JOIN

### Testing y métricas

- **`make test`** - Ejecuta tests unitarios
- **`make test-metrics`** - Prueba endpoints de métricas
- **`make test-fault-tolerance`** - Simula caída de workers y recuperación
- **`make test-cache`** - Verifica funcionamiento del cache
- **`make test-load-balancing`** - Verifica distribución de carga

### Consulta de métricas (local)

- **`make metrics-system`** - Métricas del sistema
- **`make metrics-jobs`** - Métricas de todos los jobs
- **`make metrics-failures`** - Contadores de fallos
- **`make metrics-job`** - Métricas de un job específico (ID variable)

### Docker

- **`make docker-build`** - Compila imágenes Docker
- **`make docker-up`** - Despliega cluster (master + 2 workers)
- **`make docker-demo`** - Demo automática con 1M registros
- **`make docker-test`** - Test de tolerancia a fallos en Docker
- **`make docker-down`** - Detiene y elimina contenedores
- **`make docker-logs`** - Ver logs en tiempo real

---

## Descripción de los scripts

El directorio `scripts/` contiene utilidades para testing y automatización:

- **`docker-demo.sh`** - Demo completa del clúster con múltiples etapas
- **`docker-fault-test.sh`** - Mata un worker durante ejecución para probar recuperación
- **`test_metrics.sh`** - Verifica todos los endpoints de métricas
- **`test_fault_tolerance.sh`** - Prueba reintentos y replanificación
- **`test_cache.sh`** - Verifica cache en memoria y spill a disco
- **`test_load_balancing.sh`** - Verifica distribución de trabajo
- **`test_parallel_tasks.sh`** - Prueba ejecución paralela en workers
- **`test_round_robin.sh`** - Verifica algoritmo de scheduling
- **`benchmark_1m.sh`** - Benchmark oficial de 1M registros

---

## Generación de datos de prueba

```sh
# Pequeño (rápido para desarrollo)
make create-test-data

# Mediano (~10K líneas)
make create-large-data

# Grande - Benchmark oficial (1M líneas)
make create-1m-data

# Para pruebas de JOIN
make create-join-data
```

---

## Endpoints de Métricas

**Master (puerto 8080):**

```sh
# Métricas generales del sistema
curl http://127.0.0.1:8080/api/v1/metrics/system

# Métricas de todos los jobs
curl http://127.0.0.1:8080/api/v1/metrics/jobs

# Conteo de fallos
curl http://127.0.0.1:8080/api/v1/metrics/failures

# Métricas detalladas de un job específico
curl http://127.0.0.1:8080/api/v1/jobs/{JOB_ID}/metrics

# Estadísticas por etapa (DAG nodes)
curl http://127.0.0.1:8080/api/v1/jobs/{JOB_ID}/stages

# Estado persistente
curl http://127.0.0.1:8080/api/v1/state
```

**Worker (puerto 10000 + WORKER_PORT):**

```sh
# Métricas del worker específico
curl http://127.0.0.1:10000/metrics
```

---

## Arquitectura General

### Componentes principales

**1. Master (Coordinador)**
- Escucha en puerto 8080
- Registra workers mediante heartbeat
- Recibe jobs con DAG (Directed Acyclic Graph)
- Convierte DAG en tareas físicas
- Inserta etapas de shuffle automáticamente para reduce_by_key
- Distribuye tareas a workers con round-robin + load awareness
- Detecta fallos de workers (timeout de heartbeat)
- Replanifica tareas fallidas automáticamente
- Expone métricas vía endpoints REST
- Persiste estado de jobs a disco (/tmp/minispark/state)

**2. Workers**
- Escuchan en puertos configurables (9000, 9001, etc)
- Pool de hilos configurables (WORKER_THREADS)
- Poll continuo al master pidiendo tareas
- Ejecutan operadores (map, filter, flat_map, reduce_by_key, etc)
- Cache en memoria con spill a disco automático (configurable)
- Reportan resultados al master
- Exponen métricas locales en puerto WORKER_PORT + 1000

**3. Cliente**
- CLI para enviar jobs
- Consulta estado y progreso
- Descarga resultados
- Obtiene métricas del sistema

### Flujo de datos (WordCount)

```
CSV Input (1M registros)
        ↓
    [read_csv] - lectura particionada
        ↓
   [flat_map] - split_words (1 línea → N palabras)
        ↓
    [map] - pair_with_one (palabra → (palabra, "1"))
        ↓
[shuffle_write] - hash partitioning por clave
        ↓
 [shuffle_read] - agrupa claves
        ↓
   [reduce_by_key] - sum por palabra
        ↓
    Resultados (palabras únicas con conteos)
```

### Tolerancia a fallos

- **Heartbeat**: Cada 2 segundos cada worker reporta al master
- **Timeout**: Si no hay heartbeat en 10 segundos, worker se marca DOWN
- **Replanificación**: Tareas del worker DOWN se reencolan
- **Reintentos**: Máximo 3 intentos por tarea
- **Idempotencia**: Se registra el attempt para evitar duplicados

### Scheduling (Round-Robin + Load Awareness)

```
Workers: [W1(load=2), W2(load=1), W3(load=3)]
MaxDiff: 2

get_task():
  1. Calcula min_load entre workers UP
  2. Round-robin sobre workers
  3. Asigna a worker si: active_tasks <= min_load + max_load_diff
  4. Si está sobrecargado, intenta siguiente
```

---

## Tests incluidos

Todos pueden ejecutarse con `make`:

```sh
make test                    # Tests unitarios
make test-metrics            # Endpoints de métricas
make test-fault-tolerance    # Caída y recuperación de workers
make test-cache              # Cache en memoria y spill
make test-load-balancing     # Distribución de trabajo
make test-parallel-tasks     # Paralelismo en workers
make test-round-robin        # Algoritmo de scheduling
```

---

## Pasos para reproducir el benchmark de 1M registros

```bash
# 1. Build
make build

# 2. Crear dataset (si no existe)
make create-1m-data

# 3. Opción A: Docker (recomendado)
make docker-demo

# Opción B: Local
# Terminal 1:
make run-master

# Terminal 2:
CACHE_MAX_MB=128 make run-worker

# Terminal 3:
./target/release/client submit --input data/benchmark_1m.csv --parallelism 6

# Ver resultados
curl http://127.0.0.1:8080/api/v1/metrics/jobs | python3 -m json.tool
```

---

## Declaración de IA

Este proyecto ha utilizado herramientas de inteligencia artificial como Claude, ChatGPT y Cursor como apoyo para la generación, corrección y mejora del código, documentación y scripts vinculados. Estas soluciones se emplearon como asistentes de programación y redacción técnica bajo supervisión humana.


## Licencia

Proyecto académico para el curso de Principios de Sistemas Operativos del TEC Cartago.