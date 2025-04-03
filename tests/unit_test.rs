use chrono::prelude::*;
use db_cov19mx::download::download_file;
use db_cov19mx::pl_sql::*;
use db_cov19mx::unzip::extract_zip;
use db_cov19mx::utils::download_urls;
use db_cov19mx::utils::get_unique_contry;
use db_cov19mx::utils::unzip_data;
use db_cov19mx::xlxs_to_pl::ExcelReader;
use polars::prelude::*;
use std::env;
use std::fs::create_dir_all;
use std::path::Path;
use tokio::runtime::Runtime;
#[cfg(test)]
#[tokio::test]
#[ignore = "ok"]
async fn test_download() {
    let url = "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/diccionario_datos_abiertos.zip";
    let name_file = Path::new(&url).file_name().unwrap();
    let dir = Path::new("diccionario_datos_abiertos");
    create_dir_all(dir).expect("No se pudo crear la carpeta");
    let path = env::current_dir().unwrap().join(dir).join(name_file);
    assert!(download_file(url, &path).await.is_ok());
}
#[test]
#[ignore = "ok"]
fn test_exctract_zip() {
    let path = "diccionario_datos_abiertos/diccionario_datos_abiertos.zip";
    let dir = env::current_dir()
        .unwrap()
        .join("diccionario_datos_abiertos");
    assert!(extract_zip(path, &dir).is_ok());
}
#[test]
#[ignore = "ok"]
fn test_get_data() {
    let urls = vec![
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2020/COVID19MEXICO2020.zip",
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2021/COVID19MEXICO2021.zip",
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2022/COVID19MEXICO2022.zip",
        "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2023/COVID19MEXICO2023.zip",
     ];
    let dir = Path::new("zip_files");
    let funtion = download_urls(urls, dir);
    let rt = Runtime::new().unwrap();
    let _ = rt.block_on(funtion);
}
#[test]
#[ignore = "ok"]
fn test_unzip_files() {
    let files = vec![
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/zip_files/COVID19MEXICO2020.zip",
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/zip_files/COVID19MEXICO2021.zip",
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/zip_files/COVID19MEXICO2022.zip",
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/zip_files/COVID19MEXICO2023.zip",
    ];
    assert!(unzip_data(files, Path::new("csv_files")).is_ok());
}
#[test]
#[ignore = "ok"]
fn test_concat() -> Result<(), color_eyre::eyre::Error> {
    let df1 = LazyCsvReader::new(
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/csv_files/COVID19MEXICO2020.csv",
    )
    .with_has_header(true)
    .with_infer_schema_length(Some(10000))
    .finish()?;
    let df2 = LazyCsvReader::new(
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/csv_files/COVID19MEXICO2021.csv",
    )
    .with_has_header(true)
    .with_infer_schema_length(Some(10000))
    .finish()?;
    let df = concat([df1, df2], UnionArgs::default())?;
    assert_eq!(12698741, df.collect()?.height());
    Ok(())
}
#[test]
#[ignore = "ok"]
fn test_df_paths() -> Result<(), color_eyre::eyre::Error> {
    let df = LazyCsvReader::new_paths(
        [
            "csv_files/COVID19MEXICO2020.csv".into(),
            "csv_files/COVID19MEXICO2021.csv".into(),
        ]
        .into(),
    )
    .with_has_header(true)
    .with_infer_schema_length(Some(10000))
    .finish()?;
    assert_eq!(12698741, df.collect()?.height());
    Ok(())
}
#[test]
#[ignore = "ok"]
fn test_excel() -> Result<(), Box<dyn std::error::Error>> {
    let reader = ExcelReader::new("/home/luish/Documentos/Proyects/Rust/db_cov19mx/diccionario_datos_abiertos/240708 Descriptores_.xlsx")?;
    let df = reader.with_sheet(Some("Hoja1")).finsh().unwrap();
    println!("{df}");
    panic!()
}
#[test]
#[ignore = "ok"]
fn test_build_schema() {
    let df: DataFrame = df!(
        "name" => ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
        "birthdate" => [
            NaiveDate::from_ymd_opt(1997, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(1985, 2, 15).unwrap(),
            NaiveDate::from_ymd_opt(1983, 3, 22).unwrap(),
            NaiveDate::from_ymd_opt(1981, 4, 30).unwrap(),
        ],
        "weight" => [57.9, 72.5, 53.6, 83.1],  // (kg)
        "height" => [1.56, 1.77, 1.65, 1.75],  // (m)
    )
    .unwrap();
    let qry = SqliteSchema::from_polars_schema(df.schema()).finish("test");
    println!("{qry}");
}

#[test]
#[ignore = "ok"]
fn test_pl_to_sql() -> Result<(), Box<dyn std::error::Error>> {
    let df: DataFrame = df!(
        "name" => ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
        "birthdate" => [
            NaiveDate::from_ymd_opt(1997, 1, 10).unwrap(),
            NaiveDate::from_ymd_opt(1985, 2, 15).unwrap(),
            NaiveDate::from_ymd_opt(1983, 3, 22).unwrap(),
            NaiveDate::from_ymd_opt(1981, 4, 30).unwrap(),
        ],
        "weight" => [57.9, 72.5, 53.6, 83.1],  // (kg)
        "height" => [1.56, 1.77, 1.65, 1.75],  // (m)
    )
    .unwrap();

    let db_url = "Test.db";

    SqlWriter::new(db_url)?
        .with_table(Some("test"))
        .if_exists(Some("replace"))?
        .finish(&df)?;
    println!("{df:?}");
    panic!();
}
#[test]
#[ignore = "ok"]
fn test_unique_contry() -> PolarsResult<()> {
    let mut df = LazyCsvReader::new("csv_files/COVID19MEXICO2020.csv")
        .with_has_header(true)
        .with_infer_schema_length(Some(10000))
        .finish()?;
    df = get_unique_contry(&df, "PAIS", "Id_PAISES")?;
    eprintln!("{}", df.collect()?);

    Ok(())
}
