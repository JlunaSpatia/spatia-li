# Spatia-LI  
**Motor de Location Intelligence – MVP Open Source**

Spatia-LI es el motor interno de análisis espacial y analítica avanzada diseñado por **Spatia Consulting**.  
Este repositorio contiene el **MVP técnico** necesario para realizar:

- Análisis de nuevas aperturas retail  
- Evaluación de ubicaciones candidatas  
- Estudios de reubicación y canibalización  
- Simulación de escenarios de expansión  
- Geomarketing y análisis territorial  

Todo ello utilizando únicamente **herramientas open-source y gratuitas**.

---

## 🚀 Visión del proyecto

El objetivo de Spatia-LI es construir una plataforma modular de Location Intelligence capaz de:

- Integrar múltiples fuentes de datos abiertos (INE, OSM, movilidad, POIs…)  
- Estandarizarlas sobre una **malla espacial H3**  
- Calcular métricas relevantes (demanda, oferta, competencia, accesibilidad…)  
- Generar un **score multicriterio (MCDA)** por ubicación o zona  
- Permitir simulaciones de escenarios de expansión  
- Exponer resultados mediante dashboards y (futuro) API propia

Actualmente estamos en el **MVP técnico**, ejecutado en local vía Docker.



---

## 🏗 Arquitectura del MV


┌──────────────────────────────────────────────────────────┐
│ LOCAL DEVELOPMENT (Windows) │
└──────────────────────────────────────────────────────────┘
│
▼
┌──────────────────────────────────────────────────────────┐
│ DOCKER DESKTOP │
│ Infra reproducible – PostGIS + pgAdmin + OSRM │
│ │
│ ┌──────────────────────────────┬──────────────────────┐ │
│ │ spatia_postgis (DB espacial) │ spatia_pgadmin (UI) │ │
│ │ - H3, POIs, INE, features │ - SQL GUI │ │
│ └──────────────────────────────┴──────────────────────┘ │
│ │ │ │
│ ▼ ▼ │
│ spatia_osrm (routing) Browser 8080 │
└──────────────────────────────────────────────────────────┘
│
▼
Python + H3 + GeoPandas
(ETL, feature engine, scoring)



---

## 📦 Stack tecnológico (todo *free*)

### **Infraestructura**
- Docker Desktop  
- PostgreSQL + PostGIS  
- pgAdmin  
- OSRM (routing engine)

### **Procesamiento y modelado**
- Python  
- H3-Py (malla geoespacial)  
- GeoPandas  
- Shapely  
- Pandas  
- SQLAlchemy  

### **Datos**
- INE (demografía)
- OpenStreetMap (POIs, red viaria, transporte)
- Comunidad de Madrid Open Data
- GeoFabrik (PBF para OSRM)

---

## 📁 Estructura del repositorio

spatia-li/
│
├── docker/
│ ├── docker-compose.yml # Infra local: PostGIS, pgAdmin, OSRM
│ └── osrm_data/ # PBF y archivos procesados de OSRM
│
├── sql/
│ └── init/ # Scripts DDL (tablas core)
│
├── etl/
│ ├── ingest/ # Ingesta de datos (INE/OSM/etc.)
│ ├── transform/ # Limpieza y normalización
│ └── generate_h3_madrid.py # Script para generar malla H3
│
├── dbt/
│ ├── models/
│ │ ├── staging/
│ │ ├── dimensions/
│ │ ├── facts/
│ │ └── marts/
│ └── dbt_project.yml
│
├── scoring/
│ ├── mcda/
│ ├── huff/
│ └── scenarios/
│
├── data/
│ ├── raw/ # Datos de origen (no versionados)
│ └── processed/ # Datos limpios (no versionados)
│
├── dashboards/
│ └── superset/ # (fase posterior)
│
├── notebooks/
│ └── exploracion/ # Análisis ad hoc
│
└── docs/
├── arquitectura.md
├── modelo_h3.md
└── roadmap.md

