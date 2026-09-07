# PySpark & Delta Lake: Modern Data Engineering Pipeline

Pipeline de ingeniería de datos distribuida y suite pedagógica avanzada diseñada para dominar las arquitecturas modernas sobre **Apache Spark 3.5**, **Delta Lake 3.1** y el ecosistema **Lakehouse**.

El repositorio implementa patrones de diseño de software desacoplados: la lógica de transformación reside en librerías modulares comprobadas mediante pruebas unitarias distribuidas con **Chispa**, mientras que los cuadernos interactivos desglosan la mecánica física del motor (Catalyst Optimizer, Tungsten, serialización Apache Arrow y logs transaccionales ACID).

---

## Arquitectura del Proyecto

```text
               +-------------------------------------------+
               |          Raw Storage (data/raw/)          |
               |  - deportista.csv  - resultados.csv       |
               +---------------------+---------------------+
                                     |
                                     v
               +-------------------------------------------+
               |      Modular Engine (src/olympics_pipeline)
               |  - Strict DDL Schemas                     |
               |  - Pure Functions (.transform())          |
               |  - Broadcast Hash Joins                   |
               +---------------------+---------------------+
                                     |
                    +----------------+----------------+
                    |                                 |
                    v                                 v
     +------------------------------+  +-------------------------------+
     |  Test Suite (tests/chispa)   |  |   Lakehouse Storage (Delta)   |
     |  - Schema match              |  |   - ACID Transactions         |
     |  - Data equality by row      |  |   - _delta_log/ JSON commits  |
     |  - CI automation via GitHub  |  |   - Time Travel / Upserts     |
     +------------------------------+  +-------------------------------+

```

---

## Estructura del Repositorio

```text
curso_spark/
├── .github/
│   └── workflows/
│       └── ci.yml               # Pipeline de integración continua (Ubuntu, Python 3.11, OpenJDK 17)
├── data/
│   ├── raw/                     # Datos sintéticos normalizados (Deportistas, Equipos, Resultados)
│   └── processed/               # Almacenamiento transaccional Delta Lake (ignorado por Git)
├── notebooks/
│   ├── 01_fundamentos_sparksession.ipynb      # Inicialización canónica, Spark UI, Tungsten y memoria
│   ├── 02_de_rdds_a_dataframes.ipynb          # Serialización Py4J, Catalyst Optimizer y esquemas DDL
│   ├── 03_transformaciones_y_sql_moderno.ipynb# Broadcast Hash Joins, Window Functions y .transform()
│   ├── 04_optimizacion_aqe_y_particionado.ipynb# AQE, Skew Joins, Coalesce vs Repartition
│   ├── 05_arrow_y_vectorized_pandas_udf.ipynb # Serialización Zero-Copy y UDFs vectorizadas
│   └── 06_lakehouse_con_delta_lake.ipynb      # Transacciones ACID, mutaciones a nivel de fila y Time Travel
├── scripts/
│   └── setup_estudiante.bat     # Configuración y bootstrapping de binarios Hadoop para Windows
├── src/
│   ├── __init__.py
│   └── olympics_pipeline.py     # Lógica pura de negocio encapsulada en funciones reutilizables
├── tests/
│   ├── __init__.py
│   └── test_olympics_pipeline.py# Suite de pruebas unitarias distribuidas con Chispa y Pytest
├── .gitignore                   # Exclusiones de Git (metadatos, logs, caches y entornos)
├── environment.yml              # Especificación declarativa de dependencias en Conda
├── Makefile                     # Interfaz de automatización de comandos de desarrollo
└── README.md

```

---

## Contenido Técnico del Curso

| Módulo | Enfoque Principal | Conceptos Clave Implementados |
| --- | --- | --- |
| **01. Fundamentos e Inicialización Canónica** | Arquitectura del Driver/Executors y sesión única | Inicialización robusta con `SparkSession.builder`, configuración de interfaces de red locales (`127.0.0.1`), inspección del Spark UI y asignación de memoria. |
| **02. De RDDs a DataFrames** | Optimización de planes lógicos y físicos | Comparativa de rendimiento entre RDDs (overhead de serialización Python/Java) y DataFrames con esquemas explícitos `StructType`. Generación de planes lógicos/físicos mediante Catalyst. |
| **03. Transformaciones y SQL Moderno** | Operaciones analíticas y joins optimizados | Estrategias de unión distribuida (`BroadcastHashJoin` vs `SortMergeJoin`), particionado dinámico de ventanas analíticas (`Window.partitionBy().orderBy()`) y encadenamiento funcional mediante `.transform()`. |
| **04. Optimización Física y AQE** | Afinamiento en tiempo de ejecución | Adaptive Query Execution (AQE), mitigación de particiones desbalanceadas (*Skew Join Handling*), coalescencia dinámica de particiones de shuffle y diferencias de coste entre `coalesce()` y `repartition()`. |
| **05. Vectorización con Apache Arrow** | Eliminación de cuellos de botella en UDFs | Protocolo IPC de Apache Arrow en memoria columnar, eliminación de la serialización fila a fila (*Zero-Copy*) e implementación de UDFs vectorizadas (`@pandas_udf`) tipo *Series-to-Series*. |
| **06. Lakehouse con Delta Lake** | Capa de almacenamiento transaccional ACID | Ingestión columnar Parquet con registro de confirmación `_delta_log/`, mutaciones atómicas (`UPDATE`, `DELETE`), resolución de esquemas (*Schema Enforcement*) y consultas históricas con Time Travel (`versionAsOf`). |

---

## Stack Tecnológico

* **Motor de Cómputo Distribuido:** Apache Spark 3.5.0 (PySpark).
* **Capa Lakehouse:** Delta Lake 3.1.0.
* **Aceleración Vectorizada:** Apache Arrow (PyArrow 14+) & Pandas.
* **Testing & Calidad de Código:** Pytest 7.4+, Chispa 0.9.4, Flake8.
* **Runtime & Entorno:** Python 3.11, OpenJDK 17, Conda.

---

## Prácticas de Ingeniería Implementadas

### 1. Funciones de Transformación Puras y Desacopladas

Para garantizar la testabilidad, las transformaciones de negocio no se programan en celdas de cuadernos. Se desacoplan en `src/olympics_pipeline.py` aceptando un DataFrame y retornando otro DataFrame transformado:

```python
def calcular_medallero_por_pais(df_deportistas: DataFrame, df_resultados: DataFrame) -> DataFrame:
    return (
        df_resultados
        .join(F.broadcast(df_deportistas), on="deportista_id", how="inner")
        .groupBy("pais", "medalla")
        .count()
    )

```

### 2. Pruebas Unitarias de DataFrames Distribuidos

El proyecto utiliza **Chispa** para validar no solo el esquema sino el contenido exacto de los datos generados, manejando tolerancias de coma flotante y descartando diferencias de particionado físico:

```python
from chispa.dataframe_comparer import assert_df_equality

def test_calcular_medallero_por_pais(spark):
    df_actual = calcular_medallero_por_pais(input_dep, input_res)
    assert_df_equality(df_actual, df_esperado, ignore_row_order=True)

```

### 3. Compatibilidad Multiplataforma (Windows / POSIX)

El proyecto mitiga de forma transparente las limitaciones nativas de I/O de Hadoop en entornos Windows:

* Configura `SPARK_LOCAL_IP` a `127.0.0.1` para evitar bloqueos en la resolución DNS local.
* Provee scripts de descarga y enlace para binarios de Hadoop 3.3.6 (`winutils.exe` y `hadoop.dll`).
* Normaliza rutas de acceso (`/` POSIX) y expone la configuración transaccional de Delta Lake en memoria mediante `configure_spark_with_delta_pip`.

---

## Guía de Instalación y Uso Rápido

### Requisitos Previos

* Git instalado en el sistema.
* Miniconda o Anaconda instalado.

### 1. Clonar el repositorio

```bash
git clone https://github.com/TU_USUARIO/TU_REPOSITORIO.git
cd TU_REPOSITORIO

```

### 2. Crear y activar el entorno virtual

```bash
conda env create -f environment.yml
conda activate spark_env

```

*(En Windows, si es la primera vez que configuras Hadoop, ejecuta `scripts/setup_estudiante.bat` como Administrador).*

### 3. Ejecutar las pruebas unitarias

```bash
pytest tests/ -v

```

### 4. Iniciar el servidor interactivo de Jupyter Lab

```bash
jupyter lab

```

---

## Automatización con Makefile

Para entornos UNIX o consolas Git Bash / WSL en Windows, se incluye un `Makefile` con objetivos comunes de ciclo de vida de desarrollo:

```bash
make test       # Ejecuta la suite de pruebas unitarias con Pytest y Chispa
make lint       # Analiza el código fuente con Flake8 asegurando PEP8
make clean      # Elimina directorios temporales, caches y tablas Delta locales
make run-ci     # Ejecuta linting y pruebas en secuencia simulando el flujo de CI

```

---

## Pipeline de Integración Continua (CI)

Cada `git push` o `pull request` contra la rama `main` ejecuta un flujo automatizado en GitHub Actions bajo un contenedor Ubuntu:

1. Instala un entorno limpio con Python 3.11 y OpenJDK 17 Temurin.
2. Descarga dependencias canónicas de Apache Spark y Delta Lake.
3. Ejecuta la suite completa de pruebas unitarias distribuidas en `tests/`, garantizando que ninguna modificación rompa la lógica del pipeline.