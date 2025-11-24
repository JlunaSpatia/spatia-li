# Spatia-LI  
**Motor de Location Intelligence – MVP Open Source**

Spatia-LI es el motor interno de análisis espacial y analítica avanzada diseñado por **Spatia Consulting**.  
Este repositorio contiene el **MVP técnico completo** para:

- Análisis de nuevas aperturas retail  
- Evaluación de ubicaciones candidatas  
- Estudios de reubicación y canibalización  
- Simulación de escenarios de expansión  
- Geomarketing y análisis territorial  

Todo ello utilizando únicamente **herramientas open-source y gratuitas**.

---

# 🚀 Visión del proyecto

El objetivo principal de **Spatia-LI** es construir una plataforma modular de Location Intelligence capaz de:

- Integrar múltiples fuentes de datos abiertos (INE, OSM, tráfico, rutas…)  
- Unificarlas en una **malla espacial H3**  
- Calcular métricas clave (demanda, competencia, sinergias, accesibilidad…)  
- Generar un **score MCDA** personalizado por tipo de retailer  
- Simular escenarios de expansión (1, 3, 5 tiendas…)  
- Exponer resultados mediante dashboards y (futuro) APIs

Actualmente estamos en el **MVP técnico**, con toda la infraestructura montada en local vía Docker + WSL.

---

# 🧩 Arquitectura General (Windows + WSL + Docker)

                      WINDOWS HOST
             (Docker Desktop + WSL Integration)
            |                          |
    Ubuntu (WSL)                   Docker Engine

/home/jesus/spatia-li (accesible desde WSL)
Python 3.11 (venv)
GeoPandas, H3, Pandas
ETLs (H3, POIs, OD, etc.)
--------------------------------

/home/jesus/spatia-li (accesible desde WSL)
Python 3.11 (venv)
GeoPandas, H3, Pandas
ETLs (H3, POIs, OD, etc.)
--------------------------------



✔ Infraestructura reproducible  
✔ GIS + Routing + DB totalmente integrados  
✔ Preparado para escalar  
✔ Compatible con cualquier futuro cloud

---

# 🐳 Infraestructura (Docker)

La infraestructura se define en:

docker/docker-compose.yml



## Servicios desplegados

### **spatia_postgis**
- PostgreSQL + extensión PostGIS
- Aquí viven todas las tablas espaciales:
  - dim_h3  
  - poi_raw  
  - fact_pois_h3  
  - fact_od_h3  
  - marts  

### **spatia_pgadmin**
Interfaz web para Postgres:  
👉 http://localhost:8080  
Usuario: **jluna@spatiaconsulting.com**  
Password: **admin123**

### **spatia_osrm**
Motor de rutas OSRM, usando `madrid.osm.pbf`.  
Escucha en: http://localhost:5000  

Test:

```bash
curl -4 "http://127.0.0.1:5000/route/v1/driving/-3.7,40.4;-3.6,40.45?overview=false"


📁 Estructura del repositorio


spatia-li/
│
├── docker/
│   ├── docker-compose.yml
│   └── osrm_data/
│       └── madrid.osm.pbf
│
├── data/
│   ├── raw/
│   └── processed/
│
├── etl/
│   ├── generate_h3_madrid.py
│   ├── pois_osm.py               (próximo)
│   ├── calc_od_matrix.py         (próximo)
│   └── ...
│
├── sql/
│   └── init/
│
├── scoring/
│   ├── mcda/
│   ├── huff/
│   └── scenarios/
│
├── notebooks/
│   └── exploracion/
│
└── docs/
    ├── infra.md
    ├── etl_h3.md
    ├── troubleshooting.md
    ├── architecture.md
    └── roadmap.md

🔧 Guía rápida de uso (imprescindible)

1️⃣ Arrancar infraestructura

cd ~/spatia-li/docker
docker compose up -d

Verifica:

docker ps


2️⃣ Activar entorno Python

cd ~/spatia-li
source venv/bin/activate

3️⃣ Ejecutar script de H3


python etl/generate_h3_madrid.py


Esto genera:

dim_h3 en PostGIS

malla hexagonal del municipio

4️⃣ Testear OSRM
curl -4 "http://127.0.0.1:5000/route/v1/driving/-3.70379,40.41678;-3.67689,40.42028?overview=false"
Si devuelve JSON → OK.

5️⃣ Git (WSL)

Tu repo real está en:

/home/jesus/spatia-li/.git

Comandos:
git add .
git commit -m "mensaje"
git push origin main



🧠 ETL de H3: cómo funciona

Archivo:
etl/generate_h3_madrid.py

Flujo de trabajo

Cargar shapefile BARRIOS.shp desde data/raw/

Reproyectar a EPSG:4326

Unir barrios → polígono del municipio

Invertir coordenadas (lon/lat) para H3
Si no → hexágonos aparecen en Kenya

polyfill de H3 (res=8)

Convertir hex→geometría real con shapely

Insertar en PostGIS tabla dim_h3


🐞 Troubleshooting (problemas típicos)
❗ OSRM no responde

Docker Desktop no está abierto

Puerto 5000 ocupado

Reiniciar contenedor:

docker compose restart osrm


❗ Hexágonos en Kenya

Coordenadas invertidas
Solución aplicada en el ETL:

corrected = [(lon, lat) for lat, lon in boundary]


❗ PostGIS error find_srid

Recrear tabla usando SRID 4326 explícito:

gdf.to_postgis(..., dtype={"geom": Geometry("POLYGON", srid=4326)})



❗ fallos con Python / h3 / geopandas

Siempre activar entorno:

source venv/bin/activate


Usar Python 3.11 (no 3.12).


🛣 Roadmap del MVP
Fase 1 (listo)

✔ Infra Docker
✔ OSRM Madrid
✔ PostGIS
✔ ETL H3 funcional
✔ Documentación base

Fase 2 (en curso)

🔶 POIs desde OSM (competencia, sinergias, oferta)
🔶 Clasificación automática (taxonomía de negocio)
🔶 Agregación H3 → fact_pois_h3

Fase 3

🔶 Matriz OD con OSRM
🔶 fact_od_h3
🔶 Accesibilidad por ubicación y por hexágono

Fase 4

🔶 Feature Engine
🔶 mart_h3_features
🔶 mart_site_features

Fase 5

🔶 Scoring MCDA
🔶 Huff Model
🔶 Simulación de escenarios

Fase 6

🔶 Dashboard MVP
🔶 Exportadores PPT/PDF

Fase 7

🔶 API REST interna
🔶 Multi–ciudad
🔶 Multi–retailer

✔ Estado actual del MVP
Módulo	Estado
Infraestructura	✅ COMPLETA
OSRM	✅ COMPLETO
PostGIS	✅ COMPLETO
Malla H3	✅ COMPLETO
POIs	⏳ EMPEZANDO
OD Matrix	⏳ PENDIENTE
Feature Engine	⏳ PENDIENTE
Scoring	⏳ PENDIENTE
Dashboard	⏳ PENDIENTE
💡 Conclusión

Este README resume toda la arquitectura y operación del proyecto en un único documento:

Cómo arrancarlo

Cómo desarrollarlo

Cómo extenderlo

Cómo depurarlo

En qué estado está cada módulo

Spatia-LI ya está funcionando con una arquitectura sólida y profesional.
El siguiente paso es integrar los POIs, base fundamental del análisis retail.