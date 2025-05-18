use db_cov19mx::download::download_file;
use db_cov19mx::pl_sql::*;
use db_cov19mx::unzip::extract_zip;
use db_cov19mx::utils::*;
use futures::future::join;
use polars::prelude::*;
use std::env;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;
use tokio::runtime::Runtime;

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
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
    let path_file_zip_dicc = env::current_dir()
        .unwrap()
        .join(dir_dicc_zip)
        .join(url_dicc);
    // descargamos los archivos
    let _ = rt.block_on(join(
        download_urls(urls, dir_zip_files),
        download_file(url_dicc, &path_file_zip_dicc),
    ));
    // optenemos los archivos descargados
    let mut zip_files = Vec::new();
    for entry in fs::read_dir(dir_zip_files)? {
        let entry = entry?;
        let name = entry.file_name();
        let file = dir_zip_files.join(name).to_string_lossy().to_string();
        zip_files.push(to_str(file));
    }
    // los descomprimimos en una carpeta a parte
    let dir_csv = Path::new("data_csv");
    unzip_data(zip_files, dir_csv)?;
    let dir_dicc = Path::new("data_dicc");
    for entry in fs::read_dir(dir_dicc_zip)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension() {
                if ext == "zip" {
                    extract_zip(dir_dicc_zip.join(path).to_str().unwrap(), dir_dicc)?;
                }
            }
        }
    }
    let mut files_data = Vec::new();
    for entry in fs::read_dir(dir_csv)? {
        let entry = entry?;
        let file = dir_csv.join(entry.file_name());
        files_data.push(file);
    }
    let file_des = dir_dicc.join("240708 Descriptores_.xlsx");
    let schema = get_schema_pl(&file_des)?;
    let mut lf = LazyCsvReader::new_paths(files_data.into())
        .with_has_header(true)
        .with_dtype_overwrite(Some(schema))
        .finish()?;
    let file_cat = dir_dicc.join("240708 Catalogos.xlsx");
    let mut tables_cat = get_df_cat(file_cat)?;
    let schema_des = SqliteSchema::new(
        "CLAVE",
        SqliteColOption::default()
            .with_type_sql(SqliteDataType::INTEGER)
            .with_primary_key(true),
    );
    let df_contrys = get_unique_contry(&lf, "PAIS", "CLAVE")?;
    tables_cat.insert("PAISES".into(), df_contrys.collect()?);
    let sql_write = SqlWriter::new("db_cov19mx.db")?;
    for (table_name, mut df) in tables_cat {
        sql_write
            .clone()
            .with_schema(Some(schema_des.clone()))
            .with_table(Some(table_name))
            .with_index(false)
            .finish(&mut df)?;
    }
    let mut schema_sql = get_schema_sql(file_des)?;
    schema_sql.with_column(
        "PAIS_NACIONALIDAD",
        SqliteColOption::default()
            .with_type_sql(SqliteDataType::INTEGER)
            .foreign_key("PAISES", "CLAVE"),
    );
    schema_sql.with_column(
        "PAIS_ORIGEN",
        SqliteColOption::default()
            .with_type_sql(SqliteDataType::INTEGER)
            .foreign_key("PAISES", "CLAVE"),
    );
    lf = clean_data_covid(lf);
    let split_lf = |n: Option<u64>| -> color_eyre::Result<()> {
        if let Some(n) = n {
            let mut i = n;
            let mut df = lf.clone().slice(0, i as u32).collect()?;
            while !df.is_empty() {
                sql_write
                    .clone()
                    .with_schema(Some(schema_sql.clone()))
                    .with_table(Some("COVID19MEXICO".to_string()))
                    .with_batch_size(NonZeroUsize::new(200_000).unwrap())
                    .with_index(false)
                    .finish(&mut df)?;
                i += n;
                df = lf.clone().slice(i as i64, (i + n) as u32).collect()?;
            }
        } else {
            sql_write
                .with_schema(Some(schema_sql.clone()))
                .with_table(Some("COVID19MEXICO".to_string()))
                .with_batch_size(NonZeroUsize::new(200_000).unwrap())
                .with_index(false)
                .finish(&mut lf.collect()?)?;
        }
        Ok(())
    };
    split_lf(Some(1_000_000))?;
    Ok(())
}
