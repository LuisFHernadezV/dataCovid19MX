use crate::utils::to_str;
use calamine::{open_workbook, Data, Reader, Xlsx};
use color_eyre::eyre::Ok;
use polars::prelude::*;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

fn data_excel_to_polars(data_excel: &Data) -> AnyValue {
    match data_excel {
        Data::Empty => AnyValue::Null,
        Data::Int(i) => AnyValue::Int64(*i),
        Data::Float(f) => AnyValue::Float64(*f),
        Data::Bool(b) => AnyValue::Boolean(*b),
        _ => AnyValue::String(to_str(data_excel.to_string())),
    }
}

fn excel_to_dataframe(
    workbook: &mut Xlsx<BufReader<File>>,
    sheet: Option<String>,
) -> Result<DataFrame, color_eyre::eyre::Error> {
    // Obtener la primera hoja de trabajo
    let sheet_name = sheet.unwrap_or_else(|| workbook.sheet_names()[0].clone());
    let range = workbook.worksheet_range(&sheet_name)?;

    // Extraer los datos
    let mut header_row = Vec::new();
    let mut data_rows: Vec<Vec<AnyValue>> = Vec::new();

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
                data_row.push(data_excel_to_polars(cell));
            }
            data_rows.push(data_row);
        }
    }

    // Crear series para cada columna
    let mut series_vec: Vec<Column> = Vec::new();

    for (col_idx, col_name) in header_row.iter().enumerate() {
        let mut column_data: Vec<AnyValue> = Vec::new();

        for row in &data_rows {
            if col_idx < row.len() {
                column_data.push(row[col_idx].clone());
            } else {
                column_data.push(AnyValue::Null);
            }
        }

        // Crear la serie para esta columna
        let series = Series::from_any_values(col_name.into(), column_data.as_ref(), false)?;
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
    file_path: Option<P>,
    workbook: Xlsx<BufReader<File>>,
    sheet: Option<String>,
}

impl<P: AsRef<Path>> ExcelReader<P> {
    pub fn new(file_path: P) -> Result<Self, color_eyre::eyre::Error> {
        let workbook: Xlsx<BufReader<File>> = open_workbook(&file_path)?;
        let file_path = Some(file_path);
        Ok(ExcelReader {
            file_path,
            workbook,
            sheet: None,
        })
    }
    pub fn from_workbook(workbook: Xlsx<BufReader<File>>) -> Self {
        ExcelReader {
            file_path: None,
            workbook,
            sheet: None,
        }
    }
    pub fn with_sheet<T: Into<String>>(mut self, sheet: Option<T>) -> Self {
        self.sheet = sheet.map(|t| t.into());
        self
    }
    pub fn get_file_path(&self) -> Option<&P> {
        self.file_path.as_ref()
    }
    pub fn sheet_names(&self) -> Vec<String> {
        self.workbook.sheet_names()
    }

    pub fn finsh(&mut self) -> Result<DataFrame, color_eyre::eyre::Error> {
        excel_to_dataframe(&mut self.workbook, self.sheet.clone())
    }
}
