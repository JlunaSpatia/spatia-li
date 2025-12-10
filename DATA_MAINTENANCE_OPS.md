# 🗄️ GOBERNANZA DE DATOS: MANTENIMIENTO Y OPERACIONES

**Versión:** 2.0 (Architecture Refactor)  
**Estrategia:** "Decoupled Ingestion & Compute"  
**Owner:** Jesús Luna  

Este documento define los procedimientos para mantener el **Data Lake** de Spatia actualizado.  
La arquitectura se ha dividido en dos fases para permitir escalabilidad multi-ciudad:

1.  **Ingest (Data Lake):** Procesos I/O Bound (Descargas, Scraping, Verificación de ficheros).
2.  **Compute (Enrichment):** Procesos CPU Bound (Cruce geométrico, H3 Indexing, Interpolación).

---

## 1. CATÁLOGO DE PROCESOS (ETL DEFINITIONS)

Los procesos están registrados en la base de datos (`etl_definitions`) y organizados físicamente en la carpeta `processes/`.

| ID | Nombre Tarea | Tipo | Frecuencia | Script Path | Alcance (Scope) |
|:---|:---|:---|:---|:---|:---|
| **10** | **Ingesta INE (Censo)** | INGEST | 365 días | `processes/ingest/10_ingest_ine.py` | **GLOBAL** (Release anual) |
| **20** | **Ingesta WorldPop** | INGEST | 365 días | `processes/ingest/20_ingest_worldpop.py` | **GLOBAL** (Release anual) |
| **30** | **Scraping Google POIs** | INGEST | 90 días | `processes/ingest/30_scrape_poi.py` | **MULTI-CITY** (Por ciudad) |
| **03** | **Enrich Income (Renta)** | COMPUTE | N/A* | `processes/compute/03_enrich_income.py` | On-Demand |
| **04** | **Enrich Target (Jóvenes)** | COMPUTE | N/A* | `processes/compute/04_enrich_target_pop.py` | On-Demand |

*\* Las tareas de cómputo se ejecutan tras una ingesta o al añadir una nueva ciudad.*

---

## 2. PROCEDIMIENTOS DE INGESTA (DATA INGEST)

Estos procesos traen el dato "crudo" a `data/raw`. Su misión es **disponibilidad**, no procesamiento.

### A. Tarea 10: Ingesta INE (Manual Verificada)
*Fuente anual irremplazable. Actualiza el semáforo global.*

* **Objetivo:** Obtener el CSV maestro de Renta y el Shapefile censal.
* **Procedimiento:**
    1.  Ir a la web del INE (URL en el script).
    2.  Descargar "Indicadores de renta media y mediana" (CSV separado por `;`).
    3.  Guardar en `data/raw/` siguiendo el patrón: `INE_YYYY_Renta.csv` (ej: `INE_2024_Renta.csv`).
    4.  **Ejecutar Verificación:**
        ```bash
        # Verifica que el archivo existe y actualiza la fecha en BBDD
        python processes/ingest/10_ingest_ine.py GLOBAL_RELEASE
        ```

### B. Tarea 20: WorldPop (Automático)
*Datos raster de población mundial.*

* **Objetivo:** Descargar los `.tif` de población (100m grid).
* **Ejecución:**
    ```bash
    python processes/ingest/20_ingest_worldpop.py
    ```

### C. Tarea 30: Google POIs (Scraping por Ciudad)
*El pulso del mercado. Se ejecuta independientemente por ciudad.*

* **Objetivo:** Actualizar competidores y POIs en una ciudad específica.
* **Gestión de Ciudades:** Las ciudades activas se definen en `config.py` dentro de la lista `ACTIVE_CITIES`.
* **Ejecución Manual (Consola):**
    ```bash
    # Requiere argumento de ciudad
    python processes/ingest/30_scrape_poi.py MADRID
    python processes/ingest/30_scrape_poi.py VALENCIA
    ```
* **Nota:** Este proceso actualiza el semáforo específico de esa ciudad en el Ops Center, sin afectar a las demás.

---

## 3. PROCEDIMIENTOS DE CÓMPUTO (DATA ENRICHMENT)

Estos procesos leen los datos de `data/raw` y los cruzan con los hexágonos H3 en la base de datos (`retail_hexagons`).

**¿Cuándo se ejecutan?**
1.  Cuando hay una **Nueva Release Global** (ej: sale el dato INE 2025).
2.  Cuando **añadimos una Nueva Ciudad** (ej: activamos Bilbao y queremos calcular sus datos con el fichero INE existente).

### A. Tarea 03: Enrich Income (Renta)
* **Lógica:** Busca automáticamente el archivo `INE_*_Renta.csv` más reciente en `data/raw` y lo cruza espacialmente con **todos** los hexágonos de la BBDD.
* **Comando:**
    ```bash
    python processes/compute/03_enrich_income.py
    ```

### B. Tarea 04: Enrich Target (Target Pop)
* **Lógica:** Cruza los hexágonos con el Raster `.tif` de WorldPop para contar población joven (15-35 años).
* **Comando:**
    ```bash
    python processes/compute/04_enrich_target_pop.py
    ```

---

## 4. OPS CENTER (PANEL DE CONTROL)

La gestión del día a día se realiza desde la aplicación visual, diseñada para entender la diferencia entre tareas globales y locales.

* **Acceso:**
    ```bash
    streamlit run app/pages/admin_ops.py
    ```

### Semáforos Inteligentes 🚦
El panel calcula el estado basándose en la columna `scope` del historial:

1.  **Tareas Globales (INE/WorldPop):**
    * Miran solo ejecuciones con `scope='GLOBAL_RELEASE'` o `scope='GLOBAL'`.
    * Ignoran parches locales (ej: si procesas solo un barrio nuevo).
    * **Alerta:** Se pone rojo si hace > 365 días de la última descarga oficial.

2.  **Tareas Multi-Ciudad (Google POIs):**
    * Se genera dinámicamente una fila por cada ciudad en `config.py`.
    * Miran ejecuciones con `scope='NOMBRE_CIUDAD'`.
    * **Alerta:** Se pone rojo si hace > 90 días que no se escanea esa ciudad específica.

### Flujo de Trabajo Típico

#### Escenario 1: Mantenimiento Anual (Diciembre)
1.  El Ops Center muestra **ROJO** en "Ingesta INE".
2.  El operador descarga el CSV manual del INE.
3.  El operador pulsa **"RUN"** en la tarea 10 (Ingest).
4.  El semáforo pasa a **VERDE** (365 días restantes).
5.  El operador pulsa **"RUN"** en la tarea 03 (Compute) para propagar el dato nuevo a todos los mapas.

#### Escenario 2: Nueva Ciudad (ej: Sevilla)
1.  Añadir `"SEVILLA"` en la lista `ACTIVE_CITIES` de `config.py`.
2.  Refrescar Ops Center. Aparece "Google POIs (SEVILLA)" en **BLANCO/ROJO**.
3.  Pulsar **"RUN"** en Tarea 30 (Google) para Sevilla.
4.  Pulsar **"RUN"** en Tarea 03 y 04 (Compute) para calcular Renta/Target en los hexágonos de Sevilla (el script usará los datos INE ya descargados previamente).

---

## 5. ESTRUCTURA DE CARPETAS DEL PROYECTO

```text
spatia-li/
├── config.py             # Configuración Maestra (Ciudades Activas, DB)
├── utils.py              # Decorador de Logging y Scope
├── app/
│   └── pages/
│       └── admin_ops.py  # El Panel de Control (Streamlit)
├── data/
│   └── raw/              # "Data Lake": Aquí viven los CSVs del INE y TIFs
└── processes/            # Lógica de Negocio (ETL)
    ├── ingest/           # Scripts de Descarga/Scraping/Verificación (IDs 10, 20, 30)
    └── compute/          # Scripts de Cálculo Matemático (IDs 03, 04)