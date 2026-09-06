# ⚡ PySpark Modern Masterclass (Spark 3.5+ / 4.0)

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.12-blue?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-3.5%20%7C%204.0-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.2+-00ADD8?logo=delta&logoColor=white)](https://delta.io/)
[![Apache Arrow](https://img.shields.io/badge/Apache_Arrow-Enabled-D22128?logo=apachearrow&logoColor=white)](https://arrow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Dev_Ready-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Testing](https://img.shields.io/badge/Tested_with-pytest_%7C_chispa-0A9EDC?logo=pytest&logoColor=white)](https://github.com/MrPowers/chispa)

Repositorio práctico de **Ingeniería de Datos con Apache Spark y Python**, modernizado integralmente a partir del curso clásico de Platzi (2020).

Este proyecto sustituye las prácticas obsoletas (máquinas virtuales en Vagrant, RDDs lentos, UDFs no vectorizadas y almacenamiento en CSV plano) por los estándares actuales de la industria: **Spark Connect**, **Adaptive Query Execution (AQE)**, **Apache Arrow**, arquitectura Lakehouse con **Delta Lake 3.x** y testing determinista con **Chispa**.

---

## 📊 Matriz de Modernización: 2020 vs 2026

| Dimensión | Enfoque Clásico (2020) | Estándar Moderno en este Repo | Beneficio Técnico |
| :--- | :--- | :--- | :--- |
| **Punto de Entrada** | `SparkContext` y `SQLContext` | `SparkSession` + **Spark Connect** | Arquitectura cliente-servidor por gRPC; notebooks ligeros sin JVM pesada en local. |
| **Abstracción** | RDDs (`map`, `filter`, `reduceByKey`) | **DataFrame API** + esquemas explícitos (`StructType`) | Optimización de memoria *off-heap* (Project Tungsten) y poda de columnas con Catalyst. |
| **Optimizador** | Catalyst estático | **Adaptive Query Execution (AQE)** | Fusión dinámica de particiones de shuffle, mitigación de data skew y Broadcast Joins al vuelo. |
| **UDFs** | Python nativo fila por fila (`@udf`) | **Vectorized Pandas UDFs** (`@pandas_udf` + Arrow) | Transferencia columnar en C/C++ sin costo de serialización Py4J (hasta 100x más rápido). |
| **Almacenamiento** | Archivos CSV planos en HDFS | **Delta Lake 3.x** (Parquet transaccional) | Soporte ACID, mutaciones directas (`UPDATE`/`DELETE`), auditoría en `_delta_log` y Time Travel. |
| **Entorno Local** | Máquina virtual pesada con Vagrant | **Conda (`spark_env`)** o **Docker Compose** | Configuración lista en 2 minutos con OpenJDK 17 y Python 3.11 integrado. |
| **Testing** | Validación visual en celdas | **Pytest** + **Chispa** en **GitHub Actions** | Comparación determinista de DataFrames con `assert_df_equality`. |

---

## 🗂️ Estructura del Repositorio

```text
curso_spark/
├── .devcontainer/
│   └── devcontainer.json               # Configuración para VS Code Remote Containers
├── .github/
│   └── workflows/
│       └── ci.yml                      # Pipeline de integración continua con GitHub Actions
├── data/
│   ├── raw/                            # Datasets fuente normalizados (Juegos Olímpicos)
│   │   ├── deportista.csv
│   │   ├── resultados.csv
│   │   ├── juegos.csv
│   │   └── equipos.csv
│   └── delta/                          # Directorio de persistencia para tablas Delta Lake
├── docker/
│   ├── Dockerfile                      # Imagen Java 17 LTS + Python 3.11
│   └── docker-compose.yml              # Servicios: JupyterLab (:8888) y Spark UI (:4040)
├── notebooks/                          # 6 Cuadernos formativos con teoría, código y retos
│   ├── 01_sesion_y_spark_connect.ipynb
│   ├── 02_de_rdds_a_dataframes.ipynb
│   ├── 03_transformaciones_y_sql_moderno.ipynb
│   ├── 04_optimizacion_aqe_y_particionado.ipynb
│   ├── 05_arrow_y_vectorized_pandas_udf.ipynb
│   └── 06_lakehouse_con_delta_lake.ipynb
├── src/                                # Código modular para producción
│   ├── __init__.py
│   ├── config.py                       # Factoría optimizada de SparkSession
│   └── etl/
│       ├── __init__.py
│       └── olympics_pipeline.py        # Funciones analíticas puras y reutilizables
├── tests/                              # Suite de pruebas unitarias
│   ├── __init__.py
│   ├── conftest.py                     # Fixture de SparkSession para tests locales
│   └── test_olympics_pipeline.py       # Pruebas con assert_df_equality de Chispa
├── .gitignore
├── Makefile
├── environment.yml                     # Especificación del entorno Conda (spark_env)
├── pyproject.toml                      # Gestión de dependencias moderna
└── README.md                           # Documentación técnica principal
```

---

## 🗺️ Ruta de Aprendizaje (Notebooks)

Los notebooks están organizados secuencialmente para cubrir desde los fundamentos hasta el despliegue en producción:

1. **`01_sesion_y_spark_connect.ipynb`**: Creación de `SparkSession`, diferencias con `SparkContext`, parámetros clave de ejecución y arquitectura desacoplada cliente-servidor de Spark Connect.
2. **`02_de_rdds_a_dataframes.ipynb`**: Demostración práctica de por qué los RDDs son un cuello de botella en Python. Uso de esquemas explícitos (`StructType`) e inspección de planes de ejecución con `.explain()`.
3. **`03_transformaciones_y_sql_moderno.ipynb`**: Encadenamiento con `.transform()`, eliminación de transferencias de red mediante `F.broadcast()` y analítica avanzada con Window Functions (`dense_rank`, `partitionBy`).
4. **`04_optimizacion_aqe_y_particionado.ipynb`**: Diferencias técnicas y de rendimiento entre `coalesce()` y `repartition()`, shuffle partitions y comportamiento de Adaptive Query Execution (AQE).
5. **`05_arrow_y_vectorized_pandas_udf.ipynb`**: UDFs vectorizadas con Apache Arrow y `@pandas_udf` (Series to Series) para ejecutar transformaciones en C++ sobre lotes columnares.
6. **`06_lakehouse_con_delta_lake.ipynb`**: Implementación de arquitectura Lakehouse con Delta Lake 3.x: particionado, mutaciones ACID directas (`UPDATE`), auditoría en `_delta_log` y consultas históricas con Time Travel.

---

## 🚀 Puesta en Marcha

### Opción A: Con Conda (Recomendado para Windows)
Usa el entorno preconfigurado con Python 3.11 y OpenJDK 17:

```bash
# 1. Crear el entorno virtual desde el archivo
conda env create -f environment.yml

# 2. Activar el entorno
conda activate spark_env

# 3. Iniciar Jupyter Notebook o JupyterLab
jupyter notebook
# o bien:
jupyter lab
```

### Opción B: Con Docker Compose
Ideal para ejecutar todo en un contenedor aislado sin configurar dependencias en el sistema anfitrión:

```bash
# Levantar los servicios en segundo plano
docker compose -f docker/docker-compose.yml up --build -d

# Interfaces disponibles:
# - JupyterLab : http://localhost:8888
# - Spark UI   : http://localhost:4040

# Detener los servicios:
docker compose -f docker/docker-compose.yml down
```

---

## 🧪 Pruebas Unitarias

Para verificar las transformaciones del pipeline con `pytest` y `chispa`:

```bash
pytest tests/ -v
```

---

## 📤 Publicación en GitHub

Para conectar esta carpeta local con tu propio repositorio remoto:

```bash
git init -b main
git add .
git commit -m "feat: initial commit - modern pyspark masterclass"
git remote add origin [https://github.com/TU_USUARIO/TU_REPOSITORIO.git](https://github.com/TU_USUARIO/TU_REPOSITORIO.git)
git push -u origin main
```
