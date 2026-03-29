# 🦀 ETL COVID-19 México en Rust

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Rust](https://img.shields.io/badge/Language-Rust-orange.svg)](https://www.rust-lang.org/)

---

## 📖 Descripción general

Este proyecto implementa un **pipeline ETL** (Extracción, Transformación y Carga) para los datos oficiales de COVID‑19 en México. El código está escrito en **Rust** y procesa los datos de forma totalmente automática, almacenándolos en una base de datos **SQLite** local lista para análisis.

---

## 📑 Tabla de contenidos

1. [Arquitectura](#arquitectura)
2. [Extracción](#extracción)
3. [Transformación](#transformación)
4. [Carga](#carga)
5. [Requisitos](#requisitos)
6. [Uso rápido](#uso-rápido)
7. [Contribuir](#contribuir)
8. [Licencia](#licencia)

---

## 🏗️ Arquitectura

```mermaid
flowchart LR
    subgraph Extract
        A[Descargar ZIP] --> B[Descomprimir]
        B --> C[Leer XLSX]
    end
    subgraph Transform
        C --> D[Limpiar columnas]
        D --> E[Normalizar tipos]
        E --> F[Filtrar registros inválidos]
    end
    subgraph Load
        F --> G[Crear tabla SQLite]
        G --> H[Insertar rows]
    end
    style Extract fill:#f9f,stroke:#333,stroke-width:2px
    style Transform fill:#bbf,stroke:#333,stroke-width:2px
    style Load fill:#bfb,stroke:#333,stroke-width:2px
```

---

## 📥 Extracción

- **Fuente:** <https://www.gob.mx/salud/documentos/datos-abiertos-covid-19>
- Se descarga el archivo ZIP con `reqwest` (ver `src/download.rs`).
- El ZIP se descomprime usando `unzip` y se extrae el archivo XLSX (`src/unzip.rs`).
- El contenido XLSX se lee con la crate `calamine` (`src/xlxs_to_pl.rs`).

---

## 🔧 Transformación

- **Limpieza básica:**
  - Eliminación de filas con datos faltantes.
  - Normalización de fechas al formato `YYYY‑MM‑DD`.
  - Conversión de números a tipos apropiados (`i32`, `f64`).
- **Normalización de campos:**
  - Se unifican nombres de columnas a minúsculas y snake_case.
  - Se generan columnas auxiliares como `fecha_iso`.
- Todo el proceso está encapsulado en `src/pl_sql.rs` y `src/utils.rs`.

---

## 📤 Carga

- Se crea una base de datos SQLite (`data_covid19.mx.db`).
- La tabla principal `covid_cases` se define con los tipos apropiados.
- Los registros limpios se insertan mediante `rusqlite` en bloques de 1000 filas para rendimiento.

## 📊 Esquema de la base de datos

```mermaid
erDiagram
    SI_NO {
        integer CLAVE PK
        text DESCRIPCION
    }
    TIPO_PACIENTE {
        integer CLAVE PK
        text DESCRIPCION
    }
    RESULTADO_LAB {
        integer CLAVE PK
        text DESCRIPCION
    }
    NACIONALIDAD {
        integer CLAVE PK
        text DESCRIPCION
    }
    CLASIFICACION_FINAL_COVID {
        integer CLAVE PK
        text CLASIFICACION
        text DESCRIPCION
    }
    CLASIFICACION_FINAL_FLU {
        integer CLAVE PK
        text CLASIFICACION
        text DESCRIPCION
    }
    RESULTADO_ANTIGENO {
        integer CLAVE PK
        text DESCRIPCION
    }
    ENTIDADES {
        integer CLAVE PK
        text DESCRIPCION
        text ABREVIATURA
    }
    ORIGEN {
        integer CLAVE PK
        text DESCRIPCION
    }
    PAISES {
        integer CLAVE PK
        text PAIS
    }
    SEXO {
        integer CLAVE PK
        text DESCRIPCION
    }
    SECTOR {
        integer CLAVE PK
        text DESCRIPCION
    }
    RESULTADO_PCR {
        integer CLAVE PK
        text DESCRIPCION
    }
    MUNICIPIOS {
        integer CLAVE PK
        integer CLAVE_ENTIDAD
        integer CLAVE_MUNICIPIO
        text DESCRIPCION
    }
    COVID19MEXICO {
        text FECHA_ACTUALIZACION
        text ID_REGISTRO PK
        integer ORIGEN FK
        integer SECTOR FK
        integer ENTIDAD_UM FK
        integer SEXO FK
        integer ENTIDAD_NAC FK
        integer ENTIDAD_RES FK
        integer MUNICIPIO_RES FK
        integer TIPO_PACIENTE FK
        text FECHA_INGRESO
        text FECHA_SINTOMAS
        text FECHA_DEF
        integer INTUBADO FK
        integer NEUMONIA FK
        integer EDAD
        integer NACIONALIDAD FK
        integer EMBARAZO FK
        integer HABLA_LENGUA_INDIG FK
        integer INDIGENA FK
        integer DIABETES FK
        integer EPOC FK
        integer ASMA FK
        integer INMUSUPR FK
        integer HIPERTENSION FK
        integer OTRA_COM FK
        integer CARDIOVASCULAR FK
        integer OBESIDAD FK
        integer RENAL_CRONICA FK
        integer TABAQUISMO FK
        integer OTRO_CASO FK
        integer TOMA_MUESTRA_LAB FK
        integer RESULTADO_LAB FK
        integer TOMA_MUESTRA_ANTIGENO FK
        integer RESULTADO_ANTIGENO FK
        integer CLASIFICACION_FINAL FK
        integer MIGRANTE FK
        integer PAIS_NACIONALIDAD FK
        integer PAIS_ORIGEN FK
        integer UCI FK
    }
    COVID19MEXICO_DESCRIPCIONES {
        text FECHA_ACTUALIZACION
        text ID_REGISTRO PK
        integer ORIGEN
        text ORIGEN_DESC
        integer SECTOR
        text SECTOR_DES
        integer ENTIDAD_UM
        text ENTIDAD_DESC
        integer SEXO
        text SEXO_DESC
        integer ENTIDAD_NAC
        text ENTIDAD_NAC_DESC
        integer ENTIDAD_RES
        text ENTIDAD_RES_DESC
        integer MUNICIPIO_RES
        text MUNICIPIO_RES_DESC
        integer TIPO_PACIENTE
        text TIPO_PACIENTE_DESC
        text FECHA_INGRESO
        text FECHA_SINTOMAS
        text FECHA_DEF
        integer INTUBADO
        text INTUBADO_DESC
        integer NEUMONIA
        text NEUMONIA_DESC
        integer EDAD
        integer NACIONALIDAD
        text NACIONALIDAD_DESC
        integer EMBARAZO
        text EMBARAZO_DESC
        integer HABLA_LENGUA_INDIG
        text HABLA_LENGUA_INDIG_DESC
        integer INDIGENA
        text INDIGENA_DESC
        integer DIABETES
        text DIABETES_DESC
        integer EPOC
        text EPOC_DESC
        integer ASMA
        text ASMA_DESC
        integer INMUSUPR
        text INMUSUPR_DESC
        integer HIPERTENSION
        text HIPERTENSION_DESC
        integer OTRA_COM
        text OTRA_COM_DESC
        integer CARDIOVASCULAR
        text CARDIOVASCULAR_DESC
        integer OBESIDAD
        text OBESIDAD_DESC
        integer RENAL_CRONICA
        text RENAL_CRONICA_DESC
        integer TABAQUISMO
        text TABAQUISMO_DESC
        integer OTRO_CASO
        text OTRO_CASO_DESC
        integer TOMA_MUESTRA_LAB
        text TOMA_MUESTRA_LAB_DESC
        integer RESULTADO_LAB
        text RESULTADO_LAB_DESC
        integer TOMA_MUESTRA_ANTIGENO
        text TOMA_MUESTRA_ANTIGENO_DESC
        integer RESULTADO_ANTIGENO
        text RESULTADO_ANTIGENO_DESC
        integer CLASIFICACION_FINAL
        text CLASIFICACION_FINAL_DESC
        integer MIGRANTE
        text MIGRANTE_DESC
        integer PAIS_NACIONALIDAD
        text PAIS_NACIONALIDAD_DESC
        integer PAIS_ORIGEN
        text PAIS_ORIGEN_DESC
        integer UCI
        text UCI_DESC
    }

    COVID19MEXICO }|--|| ORIGEN : "ORIGEN"
    COVID19MEXICO }|--|| SECTOR : "SECTOR"
    COVID19MEXICO }|--|| ENTIDADES : "ENTIDAD_UM"
    COVID19MEXICO }|--|| SEXO : "SEXO"
    COVID19MEXICO }|--|| ENTIDADES : "ENTIDAD_NAC"
    COVID19MEXICO }|--|| ENTIDADES : "ENTIDAD_RES"
    COVID19MEXICO }|--|| MUNICIPIOS : "MUNICIPIO_RES"
    COVID19MEXICO }|--|| TIPO_PACIENTE : "TIPO_PACIENTE"
    COVID19MEXICO }|--|| SI_NO : "INTUBADO"
    COVID19MEXICO }|--|| SI_NO : "NEUMONIA"
    COVID19MEXICO }|--|| NACIONALIDAD : "NACIONALIDAD"
    COVID19MEXICO }|--|| SI_NO : "EMBARAZO"
    COVID19MEXICO }|--|| SI_NO : "HABLA_LENGUA_INDIG"
    COVID19MEXICO }|--|| SI_NO : "INDIGENA"
    COVID19MEXICO }|--|| SI_NO : "DIABETES"
    COVID19MEXICO }|--|| SI_NO : "EPOC"
    COVID19MEXICO }|--|| SI_NO : "ASMA"
    COVID19MEXICO }|--|| SI_NO : "INMUSUPR"
    COVID19MEXICO }|--|| SI_NO : "HIPERTENSION"
    COVID19MEXICO }|--|| SI_NO : "OTRA_COM"
    COVID19MEXICO }|--|| SI_NO : "CARDIOVASCULAR"
    COVID19MEXICO }|--|| SI_NO : "OBESIDAD"
    COVID19MEXICO }|--|| SI_NO : "RENAL_CRONICA"
    COVID19MEXICO }|--|| SI_NO : "TABAQUISMO"
    COVID19MEXICO }|--|| SI_NO : "OTRO_CASO"
    COVID19MEXICO }|--|| SI_NO : "TOMA_MUESTRA_LAB"
    COVID19MEXICO }|--|| RESULTADO_LAB : "RESULTADO_LAB"
    COVID19MEXICO }|--|| SI_NO : "TOMA_MUESTRA_ANTIGENO"
    COVID19MEXICO }|--|| RESULTADO_ANTIGENO : "RESULTADO_ANTIGENO"
    COVID19MEXICO }|--|| CLASIFICACION_FINAL_COVID : "CLASIFICACION_FINAL"
    COVID19MEXICO }|--|| SI_NO : "MIGRANTE"
    COVID19MEXICO }|--|| PAISES : "PAIS_NACIONALIDAD"
    COVID19MEXICO }|--|| PAISES : "PAIS_ORIGEN"
    COVID19MEXICO }|--|| SI_NO : "UCI"
    
    COVID19MEXICO_DESCRIPCIONES }|..|| COVID19MEXICO : "basada en"
    COVID19MEXICO_DESCRIPCIONES }|..|| ORIGEN : "ORIGEN_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| SECTOR : "SECTOR_DES"
    COVID19MEXICO_DESCRIPCIONES }|..|| ENTIDADES : "ENTIDAD_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| SEXO : "SEXO_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| MUNICIPIOS : "MUNICIPIO_RES_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| TIPO_PACIENTE : "TIPO_PACIENTE_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| SI_NO : "INTUBADO_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| NACIONALIDAD : "NACIONALIDAD_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| RESULTADO_LAB : "RESULTADO_LAB_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| RESULTADO_ANTIGENO : "RESULTADO_ANTIGENO_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| CLASIFICACION_FINAL_COVID : "CLASIFICACION_FINAL_DESC"
    COVID19MEXICO_DESCRIPCIONES }|..|| PAISES : "PAIS_NACIONALIDAD_DESC"
```

---

## ⚙️ Requisitos

- **Rust** (stable) – <https://www.rust-lang.org/tools/install>
- Conexión a internet (para la extracción).
- No se necesita instalación externa de SQLite; el driver se encarga de crear el archivo local.
- Dependencias declaradas en `Cargo.toml` (incluye `reqwest`, `tokio`, `calamine`, `rusqlite`, `color-eyre`).

---

## 🚀 Uso rápido

```bash
# Clonar el repositorio
git clone https://github.com/LuisFHernadezV/dataCovid19MX.git
cd dataCovid19MX

# Instalar dependencias y compilar
cargo build --release

# Ejecutar el pipeline ETL
cargo run --release
```

El programa descargará, procesará y cargará los datos en `data_covid19.mx.db` dentro del directorio del proyecto.

---

## 🤝 Contribuir

1. Haz fork del proyecto.
2. Crea una rama (`git checkout -b feature/mi-mejora`).
3. Commit y push.
4. Abre un Pull Request describiendo los cambios.

---

## 📄 Licencia

Este proyecto está bajo la licencia MIT. Ver el archivo `LICENSE` para más detalles.

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
