use chrono::prelude::*;
use db_cov19mx::download::download_file;
use db_cov19mx::pl_sql::*;
use db_cov19mx::unzip::extract_zip;
use db_cov19mx::utils::{
    clean_data_covid, download_urls, get_df_cat, get_schema_pl, get_schema_sql, get_unique_contry,
    trim_cols, unzip_data,
};
use db_cov19mx::xlxs_to_pl::ExcelReader;
use polars::prelude::*;
use std::env;
use std::fs;
use std::fs::create_dir_all;
use std::num::NonZeroUsize;
use std::path::Path;
use std::time::Instant;
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
    let df = reader.with_sheet(Some("Hoja1")).finish().unwrap();
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
    let mut df: DataFrame = df!(
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
        .if_exists(IfExistsOption::Replace)
        .finish(&mut df)?;
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
#[test]
#[ignore = "ok"]
fn test_get_df_cat() -> Result<(), color_eyre::eyre::Error> {
    let path = "/home/luish/Documentos/Proyects/Rust/db_cov19mx/data_dicc/240708 Catalogos.xlsx";
    let dfs = get_df_cat(path)?;
    for (sheet, df) in dfs {
        println!("{sheet}");
        println!("{df}");
    }
    panic!()
}
#[test]
#[ignore = "ok"]
fn test_get_schema_pl() {
    let path_schema = "/home/luish/Documentos/Proyects/Rust/db_cov19mx/diccionario_datos_abiertos/240708 Descriptores_.xlsx";
    let path_data =
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/csv_files/COVID19MEXICO2021.csv";
    let schema = get_schema_pl(path_schema).unwrap();
    let lf = LazyCsvReader::new(path_data)
        .with_has_header(true)
        .with_dtype_overwrite(Some(schema))
        .finish()
        .unwrap();
    println!("{:?}", lf.collect().unwrap());

    panic!()
}
#[test]
#[ignore = "ok"]
fn test_get_schema_sql() {
    let path_schema =
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/data_dicc/240708 Descriptores_.xlsx";
    let mut schema = get_schema_sql(path_schema).unwrap();
    let qry = schema.finish("test");
    println!("{qry}");

    panic!()
}
#[test]
#[ignore = "ok"]
fn test_tables_to_sql() -> Result<(), color_eyre::eyre::Error> {
    let path = "/home/luish/Documentos/Proyects/Rust/db_cov19mx/data_dicc/240708 Catalogos.xlsx";
    let dfs = get_df_cat(path)?;

    let schema_des = SqliteSchema::new(
        "CLAVE",
        SqliteColOption::default()
            .with_type_sql(SqliteDataType::INTEGER)
            .with_primary_key(true),
    );

    let sql_write = SqlWriter::new("test.db")?;

    for (table_name, mut df) in dfs {
        df = df
            .clone()
            .lazy()
            .with_columns(
                df.get_columns()
                    .iter()
                    .filter(|s| s.dtype() == &DataType::String)
                    .map(trim_cols)
                    .collect::<Vec<_>>(),
            )
            .collect()?;
        sql_write
            .clone()
            .with_schema(Some(schema_des.clone()))
            .with_index(false)
            .with_table(Some(table_name))
            .finish(&mut df)?;
    }
    panic!()
}
#[test]
#[ignore = "ok"]
fn test_big_table_insert() -> Result<(), color_eyre::eyre::Error> {
    let lf = LazyCsvReader::new(
        "/home/luish/Documentos/Proyects/Rust/db_cov19mx/csv_files/COVID19MEXICO2020.csv",
    )
    .with_has_header(true)
    .with_infer_schema_length(Some(10000))
    .finish()?;

    let db_url = "Test.db";
    let start = Instant::now();

    SqlWriter::new(db_url)?
        .with_table(Some("test"))
        .with_index(false)
        .if_exists(IfExistsOption::Replace)
        .with_batch_size(NonZeroUsize::new(200_000).unwrap())
        .finish(&mut lf.collect()?)?;
    let duration = start.elapsed();
    println!("Duración de finish: {:.2?}", duration);
    assert!(duration.as_secs_f64() < 2.0);
    Ok(())
}
#[test]
#[ignore = "ok"]
fn test_cols_trim() -> Result<(), color_eyre::eyre::Error> {
    let path = "/home/luish/Documentos/Proyects/Rust/db_cov19mx/data_dicc/240708 Catalogos.xlsx";
    let dfs = get_df_cat(path)?;
    for (sheet, mut df) in dfs {
        println!("{df}");
        df = df
            .clone()
            .lazy()
            .with_columns(
                df.get_columns()
                    .iter()
                    .filter(|s| s.dtype() == &DataType::String)
                    .map(trim_cols)
                    .collect::<Vec<_>>(),
            )
            .collect()?;
        println!("{sheet}");
        println!("{df}");
    }
    panic!()
}
#[test]
#[ignore = "reason"]
fn data_unique() -> Result<(), color_eyre::eyre::Error> {
    let dir_csv = Path::new("data_csv");
    let mut files_data = Vec::new();
    for entry in fs::read_dir(dir_csv)? {
        let entry = entry?;
        let file = dir_csv.join(entry.file_name());
        files_data.push(file);
    }
    let mut lf = LazyCsvReader::new_paths(files_data.into())
        .with_has_header(true)
        .with_infer_schema_length(Some(10000))
        .with_n_rows(Some(1_000_000))
        .finish()?;
    lf = clean_data_covid(lf);
    let not_colum = [
        "ID_REGISTRO",
        "FECHA_ACTUALIZACION",
        "FECHA_INGRESO",
        "FECHA_SINTOMAS",
        "FECHA_DEF",
        "EDAD",
    ];

    for name in lf.clone().collect()?.get_column_names() {
        if !not_colum.contains(&name.as_str()) {
            let mut file = fs::File::create(format!("{}.csv", name))?;
            let mut uniq_value = lf.clone().select([col(name.as_str()).unique()]).collect()?;
            println!("{}", uniq_value);
            CsvWriter::new(&mut file)
                .include_header(true)
                .with_separator(b',')
                .finish(&mut uniq_value)?;
        }
    }
    panic!()
}
#[test]
// #[ignore = "reason"]
fn test_load_data_covid() -> Result<(), color_eyre::eyre::Error> {
    let dir_dicc = Path::new("/home/luish/Documentos/Proyects/Rust/db_cov19mx/data_dicc");
    let dir_csv = Path::new("/home/luish/Documentos/Proyects/Rust/db_cov19mx/data_csv");
    let file_des = dir_dicc.join("240708 Descriptores_.xlsx");
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
    let mut files_data = Vec::new();
    for entry in fs::read_dir(dir_csv)? {
        let entry = entry?;
        let file = dir_csv.join(entry.file_name());
        files_data.push(file);
    }
    let file_des = dir_dicc.join("240708 Descriptores_.xlsx");
    let schema = get_schema_pl(&file_des)?;

    let sql_write = SqlWriter::new("db_cov19mx.db")?;
    let mut lf = LazyCsvReader::new_paths(files_data.clone().into())
        .with_has_header(true)
        .with_dtype_overwrite(Some(schema.clone()))
        .with_n_rows(Some(2_230))
        .finish()?;
    lf = clean_data_covid(lf);
    let by = 1;
    let file_cat = dir_dicc.join("240708 Catalogos.xlsx");
    let tables_cat = get_df_cat(file_cat)?;
    let mun_uniques: Vec<_> = tables_cat
        .get("MUNICIPIOS")
        .unwrap()
        .column("CLAVE")?
        .as_series()
        .unwrap()
        .iter()
        .map(|s| s.str_value().parse().unwrap())
        .collect();
    let is_in_mun = |exp: Expr| -> Expr {
        exp.map(
            move |c: Column| -> PolarsResult<Option<Column>> {
                let out: BooleanChunked = c
                    .u64()?
                    .apply_nonnull_values_generic(DataType::Boolean, |x| mun_uniques.contains(&x));
                Ok(Some(out.into_column()))
            },
            GetOutput::from_type(DataType::Boolean),
        )
    };
    lf = lf.filter(is_in_mun(col("MUNICIPIO_RES")));
    for n in (2_220..lf.clone().collect()?.height()).step_by(by) {
        println!("{}", n);
        let mut df = lf.clone().slice(n as i64, by as u32).collect()?;
        let res = sql_write
            .clone()
            .with_schema(Some(schema_sql.clone()))
            .with_table(Some("COVID19MEXICO".to_string()))
            .if_exists(IfExistsOption::Replace)
            .with_strict_insert(false)
            .with_index(false)
            .finish(&mut df);
        if res.is_err() {
            let mut file = fs::File::create("error.csv")?;
            eprintln!("Error en el indice {} df: {}", n, df);
            CsvWriter::new(&mut file)
                .include_header(true)
                .with_separator(b',')
                .finish(&mut df)?;

            return res;
        }
    }

    Ok(())
}
