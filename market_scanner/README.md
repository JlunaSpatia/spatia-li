# 🛒 Market Scanner (Retail Genome ETL)

Este módulo es el motor de extracción de datos comerciales para **Retail Genome**. Su función es generar una "radiografía" completa de la actividad comercial (Retail, Horeca y Servicios) de ciudades enteras utilizando una estrategia de barrido por malla (Grid Strategy).

## 📡 Fuente del Dato (Data Provenance)

Todos los datos generados por este módulo provienen de **Google Maps** a través del proveedor de scraping **Scrapingdog**.

* **Proveedor:** Scrapingdog
* **API Utilizada:** Google Maps Search API
* **Documentación Oficial:** [https://api.scrapingdog.com/google_maps](https://api.scrapingdog.com/google_maps)
* **Pricing:** [https://www.scrapingdog.com/pricing](https://www.scrapingdog.com/pricing)
* **Coste Unitario:** 5 Créditos por Petición (aprox. 20 locales por petición).

> **⚠️ Nota Legal:** Los datos obtenidos son información pública accesible en la web. Este módulo solo automatiza su lectura. No se almacenan datos personales sensibles, solo datos de negocio agregados (Reviews, Ratings, Estado de Apertura).

---

## 📂 Estructura del Proyecto

```text
market_scanner/
├── config.py              # ⚙️ CONFIGURACIÓN: API Keys, Coordenadas de ciudades y Categorías.
├── 01_fetch_city.py       # 🚜 OBRERO: Descarga los datos crudos (RAW) usando Grid Strategy.
├── 02_clean_and_merge.py  # 🧹 REFINADOR: Elimina duplicados y genera el dataset maestro.
├── README.md              # 📄 Este archivo.
└── data/
    ├── raw/               # 🛑 SOLO LECTURA: Archivos JSON gigantes con duplicados.
    │   └── 2025_Q1/       #    Se guardan por trimestre. NO BORRAR.
    └── processed/         # ✅ LISTOS PARA USAR: Archivos CSV/JSON limpios y únicos.
        └── 2025_Q1/       #    Estos son los que se cargan en QGIS o PostgreSQL.