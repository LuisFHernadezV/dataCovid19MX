// use crate::my_errors::MyError;
use color_eyre::eyre::Ok;
use reqwest;
use std::fs::write;
use std::path::PathBuf;

// Estructura para descargar archivos de un link en un directorio
pub async fn download_file(url: &str, path: &PathBuf) -> Result<(), color_eyre::eyre::Error> {
    let bts = reqwest::get(url).await?.bytes().await?;
    write(path, &bts)?;
    Ok(())
}
