# Mini-Spark

## Descripción
#### Autores: Pablo Mesén Alvarado y Luis Urbina Salazar

Mini-Spark es un motor de procesamiento distribuido minimalista, inspirado en Apache Spark, diseñado para experimentar y entender los principios fundamentales del procesamiento distribuido de datos. El proyecto permite ejecutar trabajos distribuidos de ejemplo (word count, join, etc), gestionar tolerancia a fallos, realizar balanceo de carga, cache intermedia y exponer métricas en tiempo real.

## Tabla de Contenidos
- [Características principales](#características-principales)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Requisitos](#requisitos)
- [Guía rápida: Ejecución local](#guía-rápida-ejecución-local)
- [Guía rápida: Ejecución con Docker](#guía-rápida-ejecución-con-docker)
- [Documentación completa del Makefile](#documentación-completa-del-makefile)
- [Descripción de los scripts](#descripción-de-los-scripts)
- [Generación de datos de prueba](#generación-de-datos-de-prueba)
- [Endpoints de Métricas](#endpoints-de-métricas)
- [Tests incluidos](#tests-incluidos)
- [Licencia](#licencia)

---

## Características principales

- **Arquitectura distribuida**: Separación clara de procesos Master, Worker y Cliente.
- **Tolerancia a fallos**: Detección y recuperación automática ante la caída de workers.
- **Métricas en tiempo real**: Exposición vía endpoints HTTP/JSON del estado de jobs, etapas y workers.
- **Balanceo de carga**: Distribución automática del trabajo entre los workers disponibles.
- **Soporte de cache**: Reutilización de resultados intermedios en los workers.
- **Ejemplo de jobs**: WordCount, Join, etc, sobre archivos CSV.
- **Extensible y didáctico**: Fácil de entender y modificar, ideal para aprendizaje.

---

## Estructura del proyecto

```
.
├── Dockerfile             # Imágenes (Master, Worker, Client)
├── Makefile               # Comandos build, test, datos, métricas, Docker, etc.
├── scripts/               # Bash scripts para testear, crear datos, automatizar pruebas
├── data/                  # Carpeta de datos de entrada/salida
├── common/                # Crate Rust con tipos y utilidades compartidas
├── master/                # Binario de proceso Master
├── worker/                # Binario de proceso Worker
├── client/                # Binario para enviar jobs
├── tests/                 # Pruebas automáticas y de integración
└── Readme.md              # Documentación principal
```

---

## Requisitos

- **Rust** >= 1.70  
- **Docker** (para despliegue del clúster)  
- Herramientas GNU/Linux (`bash`, `curl`)  
- Recomendado: Linux o WSL2 sobre Windows

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
   WORKER_PORT=10001 make run-worker
   ```
5. **Enviar un job desde el cliente:**
   ```sh
   make run-client-submit
   # También puedes correr jobs concretos:
   ./target/release/client run wordcount data/input.csv
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
   Esto ejecuta un procesamiento distribuido de ejemplo y muestra el uso de métricas.

3. **Detener el clúster:**
   ```sh
   make docker-down
   ```

---

## Documentación completa del Makefile

El Makefile es el corazón de la automatización en Mini-Spark. A continuación se detallan todos los comandos disponibles y su propósito:

### Construcción y limpieza

- **`make build`**  
  Compila todos los binarios (master, worker, client) en modo release.
- **`make clean`**  
  Elimina binarios compilados y datos temporales.

### Ejecución local de servicios

- **`make run-master`**  
  Arranca el proceso master localmente en el puerto 8080.
- **`make run-worker`**  
  Inicia un worker local en el puerto definido por la variable `WORKER_PORT` (por defecto 10000):  
  ```sh
  WORKER_PORT=10001 make run-worker
  ```
  Puedes lanzar múltiples workers en diferentes terminales usando distintos puertos.
- **`make run-client`**  
  Lanza el binario del cliente para uso interactivo.
- **`make run-client-submit`**  
  Envía automáticamente un trabajo de ejemplo desde el cliente al master.
- **`make run-client-wordcount`**  
  Ejecuta un job WordCount concreto.
- **`make run-client-join`**  
  Ejecuta un job Join concreto.

### Generación de datos de prueba

- **`make create-test-data`**  
  Genera datasets de prueba pequeños en `data/`.
- **`make create-large-data`**  
  Crea un archivo de datos grande (~10,000 líneas).
- **`make create-1m-data`**  
  Genera un dataset de benchmarking de 1 millón de líneas.
- **`make create-join-data`**  
  Genera archivos especiales para pruebas de operaciones JOIN.

### Testing y métricas

- **`make test`**  
  Ejecuta los tests unitarios y de integración de todos los crates.
- **`make test-metrics`**  
  Realiza pruebas automáticas de todos los endpoints de métricas del sistema mediante scripts.
- **`make test-fault-tolerance`**  
  Test de tolerancia a fallos: prueba que el sistema se recupere ante caídas de workers.
- **`make test-cache`**  
  Prueba el funcionamiento del cache de workers.
- **`make test-load-balance`**  
  Verifica el balanceo de carga entre workers.

### Orquestación y Docker

- **`make docker-build`**  
  Compila el proyecto y construye las imágenes de Docker para master, worker y client.
- **`make docker-up`**  
  Despliega un clúster de servicios (master, workers y client) usando Docker Compose.
- **`make docker-demo`**  
  Lanza el clúster y ejecuta automáticamente una demo end-to-end con métricas.
- **`make docker-test`**  
  Lanza pruebas de tolerancia a fallos dentro de Docker.
- **`make docker-down`**  
  Detiene y elimina los contenedores y recursos Docker usados por Mini-Spark.

### Ayuda

- **`make help`**  
  Muestra todos los comandos disponibles y su descripción.

---

## Descripción de los scripts

El directorio `scripts/` contiene utilidades Bash para testing, generación de datos y otras tareas automatizadas clave.

### Scripts principales:

- **`create_test_data.sh`**  
  Genera conjuntos de datos pequeños (`data/input.csv`, etc) para pruebas rápidas.
- **`create_large_data.sh`**  
  Crea archivos de datos más grandes o personalizados para tests de estrés.
- **`create_1m_data.sh`**  
  Genera el dataset de 1 millón de líneas para benchmarking (`data/benchmark_1m.csv`).
- **`create_join_data.sh`**  
  Crea archivos especiales necesarios para pruebas de JOIN entre datasets.
- **`test_metrics.sh`**  
  Script automatizado que prueba todos los endpoints de métricas (`/api/v1/metrics/system`, `/api/v1/metrics/jobs`, etc). Muestra resultados y los formatea en JSON.  
  Incluye pruebas de:
  - Métricas del sistema y trabajos
  - Estadísticas por etapa
  - Métricas individuales por worker
  - Resumen de endpoints y ejemplo de consulta vía `curl`
- **`test_fault_tolerance.sh`**  
  Simula la caída y recuperación de workers, verificando la tolerancia a fallos del sistema.
- **`test_cache.sh`**  
  Verifica el funcionamiento y efectividad del cache de los workers.
- **`test_load_balance.sh`**  
  Automatiza la verificación del reparto de trabajos entre múltiples workers.
- **`run_demo.sh`**  
  Arranca y coordina todos los componentes para correr una demo end-to-end.
- **Otros scripts**: utilidades menores de monitoreo, limpieza, inicialización, logs de pruebas, etc.

**Nota:** La mayoría de los scripts pueden ejecutarse directamente o vía los comandos correspondientes del Makefile.

---

## Generación de datos de prueba

Puedes crear datasets en la carpeta `data/` para probar funcionalidad, performance o casos edge con los siguientes comandos:

- Crear datos mínimos para pruebas básicas:
  ```sh
  make create-test-data
  ```
- Crear un dataset grande (~10,000 líneas):
  ```sh
  make create-large-data
  ```
- Crear un gran dataset de benchmark (1 millón de líneas):
  ```sh
  make create-1m-data
  ```
- Crear datasets para pruebas de join:
  ```sh
  make create-join-data
  ```

Todos los archivos generados aparecen automáticamente en la carpeta `data/`.

---

## Endpoints de Métricas

Mini-Spark expone endpoints HTTP REST para monitoreo interno y benchmarking.

**En el Master (puerto 8080):**

| Endpoint                                | Descripción                       |
|------------------------------------------|-----------------------------------|
| `/api/v1/metrics/system`                 | Métricas generales del sistema    |
| `/api/v1/metrics/jobs`                   | Métricas de todos los jobs        |
| `/api/v1/metrics/failures`               | Conteo de fallos                  |
| `/api/v1/jobs/{job_id}/metrics`          | Métricas detalladas de un job     |
| `/api/v1/jobs/{job_id}/stages`           | Estadísticas por etapa del job    |
| `/api/v1/state`                          | Estado persistente de los jobs    |

**En cada Worker (por defecto puerto 10000):**

| Endpoint         | Descripción                    |
|------------------|-------------------------------|
| `/metrics`       | Métricas del worker           |

**Consulta rápida:**
```sh
curl -s http://127.0.0.1:8080/api/v1/metrics/system | python3 -m json.tool
```

---

## Tests incluidos

El sistema cuenta con un conjunto amplio de scripts automáticos y comandos de Makefile para:

- Tolerancia a fallos (`make test-fault-tolerance`)
- Balanceo de carga entre workers
- Cache intermedia en los workers
- Verificación de endpoints de métricas (`make test-metrics`)
- Ejecución multinodo y pruebas de integración end-to-end

Todos los tests muestran logs por consola y también se pueden encontrar registros detallados en archivos bajo `scripts/` y otras carpetas de pruebas.
