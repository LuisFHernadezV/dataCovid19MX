use anyhow::Result;
use calamine::{open_workbook, Reader, Xlsx};
use color_eyre::eyre::Ok;
use polars::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
fn excel_to_dataframe(
    mut workbook: Xlsx<BufReader<File>>,
    sheet: Option<String>,
) -> Result<DataFrame, color_eyre::eyre::Error> {
    // Obtener la primera hoja de trabajo
    let sheet_name = sheet.unwrap_or_else(|| workbook.sheet_names()[0].clone());
    let range = workbook.worksheet_range(&sheet_name)?;

    // Extraer los datos
    let mut header_row = Vec::new();
    let mut data_rows: Vec<Vec<String>> = Vec::new();

    for (row_idx, row) in range.rows().enumerate() {
        if row_idx == 0 {
            // Procesar la fila de encabezados
            for cell in row {
                header_row.push(cell.to_string());
            }
        } else {
            // Procesar las filas de datos
            let mut data_row = Vec::new();
            for cell in row {
                data_row.push(cell.to_string());
            }
            data_rows.push(data_row);
        }
    }

    // Crear series para cada columna
    let mut series_vec: Vec<Column> = Vec::new();

    for (col_idx, col_name) in header_row.iter().enumerate() {
        let mut column_data: Vec<String> = Vec::new();

        for row in &data_rows {
            if col_idx < row.len() {
                column_data.push(row[col_idx].clone());
            } else {
                column_data.push(String::new());
            }
        }

        // Crear la serie para esta columna
        let series = Series::new(col_name.into(), column_data);
        series_vec.push(series.into_column());
    }

    // Crear el DataFrame
    let df = DataFrame::new(series_vec)?;

    Ok(df)
}
pub struct ExcelReader<P>
where
    P: AsRef<Path>,
{
    file_path: P,
    workbook: Option<Xlsx<BufReader<File>>>,
    sheet: Option<String>,
}

impl<P: AsRef<Path>> ExcelReader<P> {
    pub fn new(file_path: P) -> Self {
        let workbook: Option<Xlsx<BufReader<File>>> = open_workbook(&file_path).ok();
        ExcelReader {
            file_path,
            workbook,
            sheet: None,
        }
    }
    pub fn with_sheet(mut self, sheet: Option<String>) -> Self {
        self.sheet = sheet;
        self
    }
    pub fn get_file_path(&self) -> &P {
        &self.file_path
    }

    pub fn finsh(mut self) -> Result<DataFrame, color_eyre::eyre::Error> {
        let workbook = self.workbook.take().unwrap();
        excel_to_dataframe(workbook, self.sheet)
    }
}
