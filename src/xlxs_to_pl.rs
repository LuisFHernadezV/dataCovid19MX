use anyhow::Result;
use calamine::{open_workbook, Reader, Xlsx};
use color_eyre::eyre::Ok;
use polars::prelude::*;
use std::path::Path;
pub fn excel_to_dataframe<P: AsRef<Path>>(path: P) -> Result<DataFrame, color_eyre::eyre::Error> {
    // Abrir el archivo Excel
    let mut workbook: Xlsx<_> = open_workbook(path)?;

    // Obtener la primera hoja de trabajo
    let sheet_name = workbook.sheet_names()[0].clone();
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
pub struct ExcelReader {
    file_path: String,
}

impl ExcelReader {
    pub fn new(file_path: &str) -> Self {
        ExcelReader {
            file_path: file_path.to_string(),
        }
    }

    pub fn read_to_dataframe(&self) -> Result<DataFrame, color_eyre::eyre::Error> {
        excel_to_dataframe(&self.file_path)
    }

    pub fn read_sheet_to_dataframe(
        &self,
        sheet_name: &str,
    ) -> Result<DataFrame, color_eyre::eyre::Error> {
        // Abrir el archivo Excel
        let mut workbook: Xlsx<_> = open_workbook(&self.file_path)?;

        // Obtener la hoja de trabajo especificada
        let range = workbook.worksheet_range(sheet_name)?;

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
            let series = Column::new(col_name.into(), column_data);
            series_vec.push(series);
        }

        // Crear el DataFrame
        let df = DataFrame::new(series_vec)?;

        Ok(df)
    }
}
