use crate::download::download_file;
use crate::unzip::extract_zip;
use polars::prelude::*;
use sha2::{Digest, Sha256};
use std::env;
use std::fs::create_dir_all;
use std::path::Path;
use std::thread::{self, JoinHandle};
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
fn generic_hash(s: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(s.as_bytes());
    // Get the result and convert to a byte array
    let result = hasher.finalize();

    // Parse enough bytes from the hash to create a u64
    // We'll use the first 8 bytes
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&result[0..8]);

    // Convert the bytes to u64 and apply the modulo
    let hash_int = u64::from_be_bytes(bytes);
    hash_int % 1_000_000_000
}
fn hash_column(exp: Expr) -> Expr {
    exp.map(
        |c: Column| -> PolarsResult<Option<Column>> {
            let out: UInt64Chunked = c
                .str()?
                .apply_nonnull_values_generic(DataType::UInt64, generic_hash);
            Ok(Some(out.into_column()))
        },
        GetOutput::from_type(DataType::UInt64),
    )
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

pub fn clean_data_covid(df: LazyFrame) -> LazyFrame {
    df.with_columns(vec![
        concat_str([col("ENTIDAD_RES"), col("MUNICIPIO_RES")], "", false)
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
            .otherwise(col("FECHA_DEF")),
    ])
}
