# 🧬 RETAIL GENOME™: MASTER BLUEPRINT

**Versión:** 1.0 (MVP Completed)  
**Estado:** Producción Local (Sovereign Stack)  
**Owner:** Jesús Luna  
**Fecha de Actualización:** Diciembre 2025

---

## 1. LA TESIS DE PRODUCTO (THE WHY)

Retail Genome no es un mapa de chinchetas; es un **Sistema de Soporte a la Decisión (DSS)** basado en Vectores Matemáticos.

* **El Problema:** La expansión retail se basa en "intuición", "tráfico visual puntual" y datos censales estáticos.
* **La Solución:** Gemelos Digitales. *"Si la tienda A funciona, buscamos matemáticamente el vector idéntico en una nueva ciudad."*
* **La Diferencia (Why Us?):**
    * **Granularidad:** Usamos Hexágonos H3 (Uber) en lugar de Códigos Postales.
    * **Dinamsimo:** Usamos OSRM (Rutas reales) en lugar de Radios.
    * **Contexto:** Usamos *Context Smoothing* (El valor de un punto depende de sus vecinos).

---

## 2. ARQUITECTURA TÉCNICA (THE STACK)

Operamos bajo una arquitectura "Sovereign Stack" (100% Local y Owned). No dependemos de APIs de pago en tiempo de ejecución.

### A. Infraestructura (Dockerized)
* **Database:** PostGIS (`spatia_postgis`) en puerto `5432`.
* **Routing:** * OSRM Walking (`osrm-walk`) en puerto `5001`.
    * OSRM Driving (`osrm-drive`) en puerto `5000`.
* **UI:** Streamlit + PyDeck (Dashboard) en puerto `8501`.
* **ETL:** Python 3.11 + GeoPandas + SQLAlchemy + Scikit-learn.

### B. Fuentes de Datos
1.  **Base Física:** OpenStreetMap (Vías, Edificios, Metros).
2.  **Demografía:** * **WorldPop:** Raster de Población (Target: Jóvenes).
    * **INE:** Renta por Sección Censal (Interpolación Areal).
3.  **Lifestyle (Vital):** Google Maps.
    * **Origen:** Módulo `market_scanner` (Scrapingdog API).
    * **Persistencia:** Tabla `public.retail_poi_master`.

---

## 3. EL PIPELINE DE DATOS (THE HOW)

El sistema ejecuta 9 scripts secuenciales en `~/spatia-li/etl/` para transformar datos crudos en decisiones de inversión.

| Script | Nombre | Función Principal |
| :--- | :--- | :--- |
| **`00`** | `prep_worldpop.py` | **Cocina Demográfica:** Suma rasters `.tif` por edad para crear el target "Jóvenes (15-35)". |
| **`01`** | `build_base.py` | **Malla Geoespacial:** Genera hexágonos H3 (Res 9) y calcula distancias OSRM a paradas de bus/metro. |
| **`02`** | `load_to_postgis.py` | **Persistencia:** Guarda la geometría H3 en PostGIS con índices espaciales (`retail_hexagons`). |
| **`03`** | `enrich_demog.py` | **Renta (Dinero):** Cruza Hexágonos con Secciones Censales (INE) usando **Interpolación Areal**. |
| **`04`** | `enrich_target_pop.py` | **Masa Crítica:** Cruza Hexágonos con Raster WorldPop usando **Zonal Statistics**. |
| **`05`** | `enrich_floating_demand.py` | **Vibe & Lifestyle:** **CRÍTICO.** Calcula el "Hipster Score" y detecta marcas *Affinity*. |
| **`06`** | `context_smoothing.py` | **El "Blender":** Aplica algoritmo **K-Ring** (100% Centro + 60% Vecinos + 30% Periferia). |
| **`07`** | `train_model.py` | **El Cerebro:** Entrena con tiendas existentes, aplica Similitud del Coseno y Vetos Duros. |
| **`08`** | `select_top_locations.py` | **El Selector:** Elige el Top 10 por ciudad usando **Non-Maximum Suppression** (evita candidatos pegados). |
| **`09`** | `enrich_financial.py` | **Arbitraje:** Cruza con precios de alquiler simulados para hallar "Gemas" (Alto Score / Bajo Precio). |

---

## 4. ALGORITMOS CORE (LA SALSA SECRETA)

### A. The Trident Score (Cálculo de Lifestyle - Script 05)
No contamos locales ("hay 3 bares"), medimos su impacto comercial ponderado:

$$Impacto = Peso \times \left(\frac{Rating}{5.0}\right) \times \log_{10}(Reviews + 1) \times PriceFactor$$

* **Affinity Brands:** Si el nombre contiene "Scalpers", "Nude Project" o "Goiko", el Peso se dispara (300 pts) y el PriceFactor se fuerza a 1.5.
* **Semántica:** Clasificamos locales en vectores según keywords en `main_type`: 
    * `Hipster` (Yoga, Specialty Coffee, Vintage).
    * `Health` (Crossfit, Gym).
    * `Retail` (Boutiques).
    * `Night` (Pubs).

### B. Context Smoothing (K-Ring - Script 06)
Rompemos la "Falacia de la Isla". Un punto vale lo que vale su entorno.
* **Volumen (Población, Gravedad):** Se suma con decaimiento. (Mis vecinos suman clientes potenciales).
* **Cualidad (Renta):** Se promedia con decaimiento. (El nivel socioeconómico se suaviza).

### C. Cosine Similarity & Contrast (El Modelo - Script 07)
1.  Creamos un **Vector Ideal** promediando las tiendas de éxito (Fuencarral/Goya/Valencia).
2.  Calculamos la **Similitud del Coseno** de cada hexágono candidato contra el Ideal.
3.  Aplicamos **Contrast Stretching** (`Score^20`) para polarizar el mapa y destacar solo la excelencia (99% match).

---

## 5. REGLAS DE NEGOCIO (CONFIGURACIÓN BLUE BANANA)

**Vetos Duros (Kill Switches):**
1.  **Renta Mínima:** Si `income_smooth` < 85% de la peor tienda exitosa -> Score 0.
2.  **Canibalización:** Si distancia a tienda existente < 1.500m -> Score 0.

**Pesos del Modelo (Feature Weights):**
* `income_smooth` (Renta): **x6.0** (Lo más crítico).
* `score_hipster` (Vibe): **x4.0** (El factor diferencial "Cool").
* `target_pop` (Masa): **x3.0**.
* `score_retail` (Tiendas): **x3.0**.
* `dist_transit` (Metro): **x1.5**.

---

## 6. MANTENIMIENTO DE DATOS

Para que el sistema siga vivo, se deben actualizar estos inputs:

1.  **Google Maps (Lifestyle):**
    * Ejecutar pipeline `market_scanner` (Python + Scrapingdog).
    * Volcar CSV a PostGIS: `public.retail_poi_master`.
    * *Frecuencia:* Trimestral.

2.  **INE (Renta):**
    * Descarga manual del "Atlas de Distribución de Renta" (CSV separado por `;`).
    * *Frecuencia:* Anual.

3.  **WorldPop (Población):**
    * Descarga manual de Rasters 100m (.tif).
    * *Frecuencia:* Anual.

---

> **🤖 INSTRUCCIÓN PARA LA IA:**
> Si te pego este documento, asume inmediatamente que este es el estado actual del proyecto `spatia-li`. Los scripts descritos ya existen en la carpeta `etl/` y la base de datos PostgreSQL tiene esta estructura (tablas `retail_hexagons`, `retail_hexagons_enriched`, `retail_results`). No reinicies la lógica, continúa el desarrollo o análisis desde este punto.