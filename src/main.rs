use db_cov19mx::pl_sql::*;
use db_cov19mx::utils::*;
use polars::prelude::*;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    let dir_csv = Path::new("data_csv");
    let dir_dicc = Path::new("data_dicc");
    if is_dir_empty(dir_csv)? || is_dir_empty(dir_dicc)? {
        get_all_data(dir_csv, dir_dicc)?;
    }
    let file_des = dir_dicc.join("240708 Descriptores_.xlsx");
    let schema = get_schema_pl(&file_des)?;
    // Creamos un vector con los archivos csv que seran leidos
    let mut files_data = Vec::new();
    for entry in fs::read_dir(dir_csv)? {
        let entry = entry?;
        let file = dir_csv.join(entry.file_name());
        files_data.push(file);
    }
    let mut lf = LazyCsvReader::new_paths(files_data.into())
        .with_has_header(true)
        .with_dtype_overwrite(Some(schema))
        .finish()?;
    // Leemos el archivo que contiene todas las tablas con las que
    // se reelacionará la tabla final
    let file_cat = dir_dicc.join("240708 Catalogos.xlsx");
    let mut tables_cat = get_df_cat(file_cat)?;
    let schema_des = SqliteSchema::new(
        "CLAVE",
        SqliteColOption::default()
            .with_type_sql(SqliteDataType::INTEGER)
            .with_primary_key(true),
    );
    // Como la columna de los paises vienen por nombre se hace una tabla con la
    // que se puede relacionar con un hashmap y lo arreglamos en la tabla principal
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
    //limpiamos la data cambiando las columnas de los paises por sus hashmap y ademas hacemos unos
    // cambios en la columna de las entidades que nos perimtan mapear bien las dos tablas
    lf = clean_data_covid(lf);
    // Creamos una función que divide la data en lotes para hacerlo menos pesado con la opcion de
    // poder hacerlo todo en una vez lo cual requiere mas recursos computacionales.
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
