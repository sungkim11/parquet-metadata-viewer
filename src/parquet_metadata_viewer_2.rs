use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::basic::Type as PhysicalType;

use std::{fs::File, path::Path};
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version="0.1.0", about="Parquet Metadata Viewer - Command Line Interface (CLI) Application")]
struct Cli {
    /// File Name.
    #[clap(short, long, default_value = "datasets/Social_Vulnerability_Index_2018_-_United_States__tract.parquet")]
    filename: String,
}

pub fn parquet_metadata_viewer(){

    let args = Cli::parse();
    let path = Path::new(&args.filename);

    if let Ok(file) = File::open(&path) {

        let reader = SerializedFileReader::new(file).unwrap();
        let parquet_metadata = reader.metadata();
        let rows = parquet_metadata.file_metadata().num_rows();
        let fields = parquet_metadata.file_metadata().schema().get_fields();

        println!("Parquet dataset filename: {}", path.to_str().unwrap());
        println!("");
        println!("Parquet dataset column names, data types (both Parquet and Rust), and statistics:");
        
        for (order, column) in fields.iter().enumerate() {
          
            let name = column.name();            
            let data_type = column.get_physical_type();
            let column_stat = parquet_metadata.row_groups()[0].columns()[order].statistics();

            let rust_type = match data_type {					
                PhysicalType::FIXED_LEN_BYTE_ARRAY  => "String",
                PhysicalType::BYTE_ARRAY            => "String",
                PhysicalType::INT64                 => "i64",
                PhysicalType::INT32                 => "i32",
                PhysicalType::FLOAT                 => "f32",
                PhysicalType::DOUBLE                => "f64",
                _ =>panic!("Cannot convert this parquet file, unhandled data type for column {}", name),									
            };
            //println!("#: {} | Name: {} | Data Type: {} | Rust Type: {} | Statistics: {:#?}", order, name, data_type, rust_type, column_stat.unwrap());
            println!("#: {:<5} | Name: {:10} | Data Type: {:10} | Rust Type: {:10} | Statistics: {:?}", order, name, data_type, rust_type, column_stat.unwrap());
        }      
        println!("");
        println!("Parquet dataset - number of rows: {}", rows);  
    }
}