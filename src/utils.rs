use crate::download::download_file;
use crate::pl_sql::{SqliteColOption, SqliteDataType, SqliteSchema};
use crate::unzip::extract_zip;
use crate::xlxs_to_pl::ExcelReader;
use calamine::{open_workbook, Reader, Xlsx};
use futures::future::join;
use polars::prelude::*;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::fs::create_dir_all;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::thread::{self, JoinHandle};
use tokio::runtime::Runtime;
pub async fn download_urls(urls: Vec<&str>, dir: &Path) -> Result<(), color_eyre::eyre::Error> {
    create_dir_all(dir).expect("No se pudo crear la carpeta");
    for url in urls {
        let name_file = Path::new(&url).file_name().unwrap();
        let path = env::current_dir().unwrap().join(dir).join(name_file);
        download_file(url, &path).await?;
    }
    Ok(())
}

pub fn unzip_data(
    files: Vec<&'static str>,
    dir: &'static Path,
) -> Result<(), color_eyre::eyre::Error> {
    create_dir_all(dir).expect("No se pudo crear la carpeta");
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    for file in files {
        let handle = thread::spawn(move || {
            let _ = extract_zip(file, dir);
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }

    Ok(())
}
fn string_to_static_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}
pub fn to_str(s: String) -> &'static str {
    string_to_static_str(s)
}
fn simple_hash(s: &str) -> u64 {
    let mut hash: u64 = 0;
    let modulo: u64 = 1_000_000_000 + 9;

    for char in s.chars() {
        hash = (hash + (char as u64)) % modulo;
    }

    hash
}
fn hash_column(exp: Expr) -> Expr {
    exp.map(
        |c: Column| -> PolarsResult<Option<Column>> {
            let out: UInt64Chunked = c
                .str()?
                .apply_nonnull_values_generic(DataType::UInt64, simple_hash);
            Ok(Some(out.into_column()))
        },
        GetOutput::from_type(DataType::UInt64),
    )
}
pub fn get_df_cat<P: AsRef<Path>>(
    path: P,
) -> Result<HashMap<String, DataFrame>, color_eyre::eyre::Error> {
    let mut result: HashMap<String, DataFrame> = HashMap::new();
    let workbook: Xlsx<BufReader<File>> = open_workbook(path.as_ref())?;
    for sheet in workbook.sheet_names() {
        let df = ExcelReader::new(path.as_ref())?
            .with_sheet(Some(sheet.clone()))
            .finish()?;
        let mut df_lazy: LazyFrame;
        let firs_col = df.clone().get_column_names()[0].clone();
        let secon_col = df.clone().get_column_names()[1].clone();
        if sheet == "Catálogo de ENTIDADES" {
            df_lazy = df
                .lazy()
                .with_columns([col(firs_col.clone()).cast(DataType::UInt64)])
                .rename(["CLAVE_ENTIDAD"], ["CLAVE"], false);
        } else if sheet == "Catálogo MUNICIPIOS" {
            df_lazy = df
                .lazy()
                .with_columns([col(firs_col.clone()).cast(DataType::UInt64)])
                .select([
                    concat_str([col(firs_col.clone()), col(secon_col.clone())], "", false)
                        .cast(DataType::UInt64)
                        .alias("CLAVE"),
                    all(),
                ])
                .with_columns([col(secon_col.clone()).cast(DataType::UInt64)]);
        } else {
            df_lazy = df
                .lazy()
                .with_columns([col(firs_col.clone()).cast(DataType::UInt64)]);
        }
        if sheet == "Catálogo RESULTADO_LAB" {
            // drop the columns that has all values nuull
            df_lazy = df_lazy.select([col(firs_col), col(secon_col)]);
        }

        let name_table = sheet.split_whitespace().last().unwrap_or_default();
        result.insert(name_table.to_string(), df_lazy.collect()?);
    }
    Ok(result)
}
pub fn get_schema_pl<P: AsRef<Path>>(path: P) -> Result<SchemaRef, color_eyre::eyre::Error> {
    let df = ExcelReader::new(path)?.finish()?;
    let col_name = df.clone().column("NOMBRE DE VARIABLE")?.clone();
    let col_type = df.clone().column("FORMATO O FUENTE")?.clone();
    let mut schema = Schema::with_capacity(df.height());
    for (col, typ) in col_name.phys_iter().zip(col_type.phys_iter()) {
        let column = col.str_value().to_string();
        let type_col = typ.str_value().to_string();
        if type_col.contains("CATÁLOGO") || type_col.contains("CATALÓGO") {
            schema.with_column(column.into(), DataType::UInt64);
        } else {
            schema.with_column(column.into(), DataType::String);
        }
    }

    Ok(SchemaRef::new(schema))
}

pub fn get_schema_sql<P: AsRef<Path>>(path: P) -> Result<SqliteSchema, color_eyre::eyre::Error> {
    let df = ExcelReader::new(path)?.finish()?;
    let col_name = df.clone().column("NOMBRE DE VARIABLE")?.clone();
    let col_type = df.clone().column("FORMATO O FUENTE")?.clone();
    let mut schema: SqliteSchema =
        SqliteSchema::new("FECHA_ACTUALIZACION", SqliteColOption::default());
    for (col, typ) in col_name
        .phys_iter()
        .skip(2)
        .zip(col_type.phys_iter().skip(2))
    {
        let column = col.str_value().to_string();
        let type_col = typ.str_value().to_string();
        let id_ref = type_col
            .clone()
            .split(":")
            .skip(1)
            .collect::<String>()
            .replace(' ', "");
        if type_col.contains("CATÁLOGO") || type_col.contains("CATALÓGO") {
            schema.with_column(
                column.clone(),
                SqliteColOption::default()
                    .with_type_sql(SqliteDataType::INTEGER)
                    .foreign_key(id_ref, "CLAVE".into()),
            );
        } else {
            schema.with_column(
                column,
                SqliteColOption::default().with_type_sql(SqliteDataType::TEXT),
            );
        }
    }
    schema.with_column(
        "EDAD",
        SqliteColOption::default().with_type_sql(SqliteDataType::INTEGER),
    );

    schema.with_column(
        "ID_REGISTRO",
        SqliteColOption::default().with_primary_key(true),
    );
    Ok(schema)
}
pub fn get_unique_contry(df: &LazyFrame, col_name: &str, id_name: &str) -> PolarsResult<LazyFrame> {
    let pais_nacionalidad = df
        .clone()
        .select([when(col("PAIS_NACIONALIDAD").eq(lit("99")))
            .then(lit("SE INGONARA"))
            .otherwise(col("PAIS_NACIONALIDAD"))
            .unique_stable()
            .alias(col_name)]);
    let pais_origen = df.clone().select([when(col("PAIS_ORIGEN").eq(lit("97")))
        .then(lit("NO APLICA"))
        .otherwise(col("PAIS_ORIGEN"))
        .unique_stable()
        .alias(col_name)]);
    let pais = concat([pais_nacionalidad, pais_origen], UnionArgs::default())?;
    Ok(pais
        .select([hash_column(col(col_name)).alias(id_name), col(col_name)])
        .unique_stable(None, UniqueKeepStrategy::First))
}
pub fn trim_cols(column: &Column) -> Expr {
    col(column.name().as_str()).str().strip_chars(lit(" "))
}
pub fn clean_data_covid(df: LazyFrame) -> LazyFrame {
    df.with_columns(vec![
        concat_str(
            [
                col("ENTIDAD_RES"),
                col("MUNICIPIO_RES")
                    .cast(DataType::String)
                    .str()
                    .zfill(lit(3)),
            ],
            "",
            false,
        )
        .cast(DataType::UInt64)
        .alias("MUNICIPIO_RES"),
        hash_column(
            when(col("PAIS_NACIONALIDAD").eq(lit("99")))
                .then(lit("SE INGONARA"))
                .otherwise(col("PAIS_NACIONALIDAD")),
        )
        .alias("PAIS_NACIONALIDAD"),
        hash_column(
            when(col("PAIS_ORIGEN").eq(lit("97")))
                .then(lit("NO APLICA"))
                .otherwise(col("PAIS_ORIGEN")),
        )
        .alias("PAIS_ORIGEN"),
        when(col("FECHA_DEF").eq(lit("9999-99-99")))
            .then(lit(NULL))
            .otherwise(col("FECHA_DEF"))
            .alias("FECHA_DEF"),
    ])
}
pub fn is_dir_empty<P: AsRef<Path>>(path: P) -> std::io::Result<bool> {
    let mut entries = fs::read_dir(path)?;
    Ok(entries.next().is_none())
}
pub fn get_all_data(
    dir_csv: &'static Path,
    dir_dicc: &Path,
) -> Result<(), color_eyre::eyre::Error> {
    // Declaramos los url con que se van a descargar
    let urls = vec![
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2020/COVID19MEXICO2020.zip",
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2021/COVID19MEXICO2021.zip",
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2022/COVID19MEXICO2022.zip",
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2023/COVID19MEXICO2023.zip",
     ]; // add more urls as you need
        // Declaramos la carpeta donde se van a descargar
    let dir_zip_files = Path::new("data_zip");
    let rt = Runtime::new().unwrap();
    let url_dicc = "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/diccionario_datos_abiertos.zip";
    let dir_dicc_zip = Path::new("dicc_zip");
    create_dir_all(dir_dicc_zip).expect("No se pudo crear la carpeta");
    let path_file_zip_dicc = env::current_dir()
        .unwrap()
        .join(dir_dicc_zip)
        .join(Path::new(url_dicc).file_name().unwrap());
    // descargamos los archivos
    let _ = rt.block_on(join(
        download_urls(urls, dir_zip_files),
        download_file(url_dicc, &path_file_zip_dicc),
    ));
    let mut zip_files = Vec::new();
    for entry in fs::read_dir(dir_zip_files)? {
        let entry = entry?;
        let name = entry.file_name();
        let file = dir_zip_files.join(name).to_string_lossy().to_string();
        zip_files.push(to_str(file));
    }
    // los descomprimimos en una carpeta a parte
    unzip_data(zip_files, dir_csv)?;
    for entry in fs::read_dir(dir_dicc_zip)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "zip" {
                    extract_zip(path.to_str().unwrap(), dir_dicc)?;
                }
            }
        }
    }
    Ok(())
}
