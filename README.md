# ETL COVID-19 México en Rust

Este proyecto implementa un proceso ETL (Extract, Transform, Load) para la base de datos oficial de COVID-19 en México.  
El código está escrito en Rust y automatiza la extracción, limpieza y carga de datos en una base de datos SQLite local.

---

## 📋 Descripción

- **Extract:** Obtiene datos actualizados directamente del sitio oficial del gobierno mexicano sobre COVID-19.
- **Transform:** Limpia y procesa los datos para dejarlos en un formato estructurado y homogéneo.
- **Load:** Inserta la información limpia en una base de datos SQLite local para facilitar análisis posteriores.

---

## ⚙️ Requisitos

- Rust (versión estable recomendada)
- Conexión a internet para la extracción de datos
- SQLite (no requiere instalación externa, usa archivo local)
- Librerías de Rust especificadas en `Cargo.toml`
