// use futures::executor::block_on;
// use calamine::{open_workbook, Data, Reader, Xlsx};
// use polars::prelude::*;

fn main() -> color_eyre::Result<()> {
    color_eyre::install()?;
    //     let urls = vec![ "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2020/covid19mexico2020.zip",
    //         "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2021/covid19mexico2021.zip",
    //         "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2022/covid19mexico2022.zip",
    //         "https://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/historicos/2023/covid19mexico2023.zip",
    // ];
    // let mut workbook: Xlsx<_> = open_workbook("/home/luish/Documentos/Proyects/Rust/db_cov19mx/diccionario_datos_abiertos/240708 Descriptores_.xlsx").expect("Cannot open file");
    //
    // // Read whole worksheet data and provide some statistics
    // if let Ok(r) = workbook.worksheet_range("Hoja1") {
    //     for row in r.rows() {
    //         println!("row={:?}, row[0]={:?}", row, row[0]);
    //     }
    // }
    Ok(())
}
