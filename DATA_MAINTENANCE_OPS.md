# 🗄️ GOBERNANZA DE DATOS: MANTENIMIENTO (STOCKPILING)

**Versión:** 1.2 (Final MVP)  
**Estrategia:** "Data Lake Freshness"  
**Owner:** Jesús Luna  

El objetivo de este plan es asegurar que la base de datos contiene la información más reciente disponible (Stockpiling). **NO ejecutamos el modelo de IA en estas tareas**, solo mantenemos la materia prima fresca y lista para cuando se requiera un estudio.

---

## 1. CALENDARIO DE INGESTA (DATA INGESTION SCHEDULE)

Tabla maestra de procesos definidos en PostgreSQL (`public.etl_definitions`).

| ID Tarea | Proceso | Frecuencia | Script / Path | Descripción |
| :--- | :--- | :--- | :--- | :--- |
| **1** | **Censo Madrid (Ayto)** | **30 días** (Mensual) | `etl/automations/tools/01_clean_coords_locales.py` | **Madrid Only.** Descarga automática del Portal de Datos Abiertos. Detecta cierres/aperturas oficiales. |
| **2** | **Google Maps (Scanner)** | **90 días** (Trimestral) | `market_scanner/` | **CRÍTICO.** Barrido masivo de POIs vía ScrapingDog. Carga la materia prima de Lifestyle en `retail_poi_master`. |
| **3** | **Renta INE** | **365 días** (Anual) | `etl/03_enrich_demog.py` | Ingesta de Renta Media por Sección Censal. Requiere descarga manual del CSV del INE. |
| **4** | **WorldPop (Target)** | **365 días** (Anual) | `etl/04_enrich_target_pop.py` | Ingesta de bandas de edad (15-35). Requiere descarga manual de TIFs y pre-procesado. |

---

## 2. PROCEDIMIENTOS DE ACTUALIZACIÓN PASO A PASO

### A. Tarea 1: Censo de Locales Madrid (Mensual)
*Fuente oficial del Ayuntamiento. Alta fiabilidad para licencias, baja frescura comercial.*

* **Objetivo:** Detectar "Locales Cerrados" y "Cambios de Actividad" oficiales en Madrid.
* **Fuente:** [Datos Abiertos Madrid - Censo de Locales](https://datos.madrid.es/sites/v/index.jsp?vgnextoid=23160329ff639410VgnVCM2000000c205a0aRCRD&vgnextchannel=374512b9ace9f310VgnVCM100000171f5a0aRCRD).
* **Ejecución:**
    ```bash
    # El script descarga el CSV automáticamente si la URL no ha cambiado
    python etl/automations/tools/01_clean_coords_locales.py
    ```

### B. Tarea 2: Google Points of Interest (Trimestral)
*El pulso real del mercado. Cobertura Global. Es el input del "Vibe Score".*

* **Objetivo:** Tener la foto más reciente de aperturas, ratings y popularidad en `retail_poi_master`.
* **Ejecución:**
    1.  Ir al módulo de escaneo:
        ```bash
        cd market_scanner
        ```
    2.  Lanzar barrido (Coste de créditos API):
        ```bash
        python 01_fetch_city.py
        ```
    3.  Limpiar y fusionar duplicados:
        ```bash
        python 02_clean_and_merge.py
        ```
    4.  Cargar a PostGIS (Ingesta final):
        ```bash
        python 03_load_master_to_postgis.py
        ```

### C. Tarea 3: INE Renta (Anual)
*Poder adquisitivo estructural.*

* **Objetivo:** Actualizar la capa `avg_income` en los hexágonos enriquecidos.
* **Fuente:** [INE - Atlas de Distribución de Renta de los Hogares](https://www.ine.es/dynt3/inebase/index.htm?padre=12385&capsel=12384).
* **Ejecución:**
    1.  **Manual:** Descargar el último CSV disponible (Separador `;`).
    2.  Guardar en `data/raw/` (ej: `ADHR_2024.csv`).
    3.  Lanzar script de cruce:
        ```bash
        python etl/03_enrich_demog.py
        ```

### D. Tarea 4: WorldPop Age Bands (Anual)
*Masa crítica de target específico (Jóvenes 15-35).*

* **Objetivo:** Actualizar la capa `target_pop` (población filtrada) en los hexágonos.
* **Fuente:** [WorldPop Hub - Spain 100m](https://hub.worldpop.org/).
* **Ejecución:**
    1.  **Manual:** Descargar los nuevos `.tif` correspondientes a las franjas de edad y sexo deseadas.
    2.  Guardar en `data/raw/worldpop_parts/`.
    3.  Combinar las bandas en un solo raster:
        ```bash
        python etl/00_prep_worldpop.py
        ```
    4.  Inyectar en base de datos:
        ```bash
        python etl/04_enrich_target_pop.py
        ```

---

## 3. ¿Y EL MODELO? (EJECUCIÓN AD-HOC)

La inteligencia artificial (`scikit-learn`, `similarity`, `context-smoothing`) **NO** forma parte del mantenimiento rutinario. Es el **Producto**. Se ejecuta a demanda (On-Demand) cuando:
1.  Entra un nuevo cliente.
2.  Se requiere un estudio de expansión.
3.  Se acaba de completar una carga trimestral de Google (Tarea 2) y queremos refrescar los mapas.

**Cadena de Mando (Pipeline de Inteligencia):**

```bash
# 1. Calcular Vibe & Lifestyle (Usa datos frescos de Tarea 2)
# Genera: score_hipster, score_retail, gravity_score
python etl/05_enrich_floating_demand.py

# 2. Propagar Contexto (Usa datos de Tareas 1, 3 y 4)
# Genera: income_smooth, target_pop_smooth
python etl/06_context_smoothing.py

# 3. Entrenar y Puntuar (El Cerebro)
# Genera: similarity_final (0-100)
python etl/07_train_model.py

# 4. Seleccionar Ganadores (Reporte Final)
# Genera: Top 10 Locations independientes
python etl/08_select_top_locations.py

# 5. (Opcional) Análisis Financiero
# Genera: opportunity_index (Score / Precio Alquiler)
python etl/09_enrich_financial.py



## 7. SISTEMA DE OBSERVABILIDAD Y CONTROL (OPS CENTER)

El sistema cuenta con una capa de gestión para evitar la "fatiga de scripts" y garantizar que los datos no caduquen silenciosamente.

### A. The Watchdog (Alerta Temprana)
Un script autónomo que verifica la "frescura" de los datos contra la tabla `etl_definitions`.

* **Script:** `etl/automations/watchdog.py`
* **Frecuencia:** Ejecutar diariamente vía CRON (ej: 09:00 AM).
* **Lógica:**
    1.  Consulta la última fecha `SUCCESS` en `etl_history`.
    2.  Compara con `frequency_days` de la definición.
    3.  Si `(Hoy - Última Ejecución) > Frecuencia` -> **Alerta a Telegram**.

### B. Ops Control Center (Panel de Admin)
Interfaz gráfica para gestionar el pipeline sin tocar la consola.

* **Script:** `app/admin_ops.py`
* **Acceso:** `streamlit run app/admin_ops.py`
* **Funcionalidades:**
    * 🚦 Semáforo de estado (Verde/Rojo) por tarea.
    * ▶️ **Botón de Ejecución Manual:** Lanza los scripts de Python en segundo plano.
    * 📜 **Logs:** Muestra la salida de la consola y guarda el historial en SQL.

### C. Dashboard de Cliente (Top Picks)
La cara visible del producto. Visualiza los resultados de la tabla `retail_results`.

* **Script:** `app/dashboard_top_picks.py`
* **Acceso:** `streamlit run app/dashboard_top_picks.py`
* **Key Features:**
    * Filtrado por Presupuesto y Distancia al Metro.
    * Ranking "Oro/Plata/Bronce" visual en mapa 3D (PyDeck).
    * **IA Insight:** Integración con GPT-4 para explicar *por qué* una ubicación es buena.