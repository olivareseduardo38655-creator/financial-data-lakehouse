# Financial Transactional Data Lakehouse

![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.5.0-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0.0-00ADD8?style=for-the-badge&logo=deltalake&logoColor=white)
![Status](https://img.shields.io/badge/Status-Production_Ready-success?style=for-the-badge)

## Descripción Ejecutiva

Este proyecto implementa una arquitectura **Data Lakehouse** de nivel empresarial diseñada para el sector FinTech. El sistema orquesta el ciclo de vida completo de los datos, desde la ingesta de registros transaccionales crudos hasta la generación de modelos analíticos de alto valor (Customer 360 y Daily Ledgers).

A diferencia de los Data Warehouses tradicionales, esta solución utiliza **Delta Lake** para garantizar transacciones ACID (Atomicidad, Consistencia, Aislamiento, Durabilidad) sobre un almacenamiento de objetos escalable. Permite análisis histórico, corrección de datos mediante Time Travel y cargas incrementales eficientes.

## Objetivos Técnicos

1.  **Arquitectura Medallion:** Implementación estricta de capas de refinamiento (Bronze, Silver, Gold) para asegurar trazabilidad y calidad.
2.  **Gestión de Transacciones ACID:** Uso de Delta Logs para garantizar la integridad de los datos financieros ante fallos de escritura.
3.  **Lógica Incremental (CDC):** Implementación de estrategias MERGE (Upsert) para procesar cambios en dimensiones lentamente cambiantes (SCD Type 1) sin duplicidad.
4.  **Time Travel & Auditoría:** Capacidad de consultar versiones anteriores de los datos para recuperación de desastres y auditoría forense.
5.  **Clean Code:** Desarrollo modular siguiendo principios SOLID y segregación de interfaces para la lógica de negocio y la infraestructura.

## Arquitectura del Sistema

El flujo de datos sigue un proceso lineal de enriquecimiento y normalización.

\\\mermaid
graph LR
    %% Definición de Nodos
    SRC[Fuentes de Datos<br/>(CSV/APIs)] -->|Ingesta Raw| B[Bronze Layer<br/>(Delta: Append-Only)]
    
    subgraph Core Processing
        B -->|Limpieza & Validación| S[Silver Layer<br/>(Delta: Upsert/Merge)]
        S -->|Agregación de Negocio| G[Gold Layer<br/>(Delta: Star Schema)]
    end

    G -->|Consumo| BI[Analytics & Reporting]
    G -->|Consumo| ML[Modelos de Riesgo]
    
    %% Estilos
    style B fill:#fff,stroke:#333,stroke-width:2px
    style S fill:#e5e5e5,stroke:#333,stroke-width:2px
    style G fill:#cccccc,stroke:#333,stroke-width:2px
\\\

## Estructura del Repositorio

\\\	ext
.
├── bronze/             # Almacenamiento crudo (Inmutable)
├── silver/             # Datos limpios y deduplicados
├── gold/               # Tablas de hechos y dimensiones de negocio
├── config/             # Configuraciones globales de Spark
├── docs/               # Documentación de arquitectura y diagramas
├── pipelines/          # Código fuente ETL
│   ├── data_generator.py    # Generador de datos sintéticos financieros
│   ├── ingest_bronze.py     # Ingesta Raw -> Bronze
│   ├── process_silver.py    # Calidad de datos Bronze -> Silver
│   ├── process_gold.py      # Agregaciones Silver -> Gold
│   ├── incremental_load.py  # Lógica de Merge/Upsert (Día 2)
│   └── demo_time_travel.py  # Demostración de recuperación histórica
├── environment.yml     # Definición de entorno (IaC)
└── README.md           # Documentación técnica
\\\

## Guía de Instalación y Ejecución

El proyecto incluye configuración automática para entornos Windows (gestión de winutils y Hadoop binaries).

### Prerrequisitos
* Anaconda o Miniconda instalado.
* Git.

### Despliegue

1.  **Clonar el repositorio:**
    \\\ash
    git clone <url-del-repo>
    cd data-lakehouse-project
    \\\

2.  **Configurar el entorno virtual:**
    \\\ash
    conda env create -f environment.yml
    conda activate lakehouse-env
    \\\

3.  **Ejecutar el Pipeline End-to-End:**
    \\\ash
    # 1. Generación de datos
    python pipelines/data_generator.py
    
    # 2. Ingesta Inicial
    python pipelines/ingest_bronze.py
    
    # 3. Procesamiento y Calidad
    python pipelines/process_silver.py
    
    # 4. Generación de Valor
    python pipelines/process_gold.py
    \\\

## Lógica de Negocio y Gobierno de Datos

Reglas aplicadas por capa para asegurar la calidad de la información financiera.

| Capa | Estrategia de Escritura | Reglas de Gobierno y Calidad |
|------|-------------------------|------------------------------|
| **Bronze** | Append Only | Preservación del dato crudo original. Adición de metadatos de auditoría (ingestion_timestamp, source_file). |
| **Silver** | Merge (Upsert) | Eliminación de duplicados. Estandarización de emails (lower, 	rim). Casteo de tipos estrictos (Moneda, Fechas). Integridad referencial. |
| **Gold** | Overwrite | Generación de vistas agregadas. Cálculo de métricas derivadas (Balance Total, Movimiento Diario). Modelo dimensional para consumo BI. |

---
**Autor:** [Tu Nombre] - Data Engineer & Financial Specialist
