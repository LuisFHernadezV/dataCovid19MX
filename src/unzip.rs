use color_eyre::eyre::Ok;
use std::fs::{self, File};
use std::io::{self};
use std::path::Path;
use zip::ZipArchive;

pub fn extract_zip(zip_path: &str, output_dir: &Path) -> Result<(), color_eyre::eyre::Error> {
    // Create the output directory if it doesn't exist
    fs::create_dir_all(output_dir)?;

    // Open the zip file
    let zip_file = File::open(zip_path)?;

    // Create a ZipArchive from the file
    let mut archive = ZipArchive::new(zip_file)?;

    // Iterate through all files in the archive
    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let outpath = Path::new(output_dir).join(file.name());

        // Create directory structure if needed
        if file.name().ends_with('/') {
            fs::create_dir_all(&outpath)?;
        } else {
            // Create parent directory if needed
            if let Some(parent) = outpath.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent)?;
                }
            }

            // Extract file
            let mut outfile = File::create(&outpath)?;
            io::copy(&mut file, &mut outfile)?;
        }

        // Set permissions (Unix-like systems only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            if let Some(mode) = file.unix_mode() {
                fs::set_permissions(&outpath, fs::Permissions::from_mode(mode))?;
            }
        }

        println!("Extracted: {}", file.name());
    }

    Ok(())
}
