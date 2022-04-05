
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::{fs::File, path::Path};
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version="0.1.0", about="Parquet Metadata Reader - Command Line Interface (CLI) Application")]
struct Cli {
    /// Parquet dataset filename.
    #[clap(short, long, default_value = "datasets/Social_Vulnerability_Index_2018_-_United_States__tract.parquet")]
    filename: String,
}

pub fn rust_parquet(){

    let args = Cli::parse();
    let path = Path::new(&args.filename);

    if let Ok(file) = File::open(&path) {

        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_metadata = reader.metadata();
        let rows = parquet_metadata.file_metadata().num_rows();
        let fields = parquet_metadata.file_metadata().schema().get_fields();

        println!("Parquet dataset filename: {}", path.to_str().unwrap());
        println!("");
        println!("Parquet dataset column names:");

        for (order, column) in fields.iter().enumerate() {            
            let name = column.name();
            println!("#: {} | Name: {}", order, name);
        }        
        println!("");
        println!("Parquet dataset - number of rows: {}", rows);
    }
}