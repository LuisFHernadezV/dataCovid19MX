use crate::download::download_file;
use crate::unzip::extract_zip;
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
