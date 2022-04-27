use dbftopq::*;
use clap::*;
use std::fs;
use parquet::column::writer::ColumnWriter;
use std::path::Path;
use parquet::{
    schema::{printer},
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter}
    },
};
use std::sync::Arc;

fn main() {
    let args = clap_app!(dbftopq =>
        (version: "0.1")
        (author: "Raphael Campestrini <rca@seca.at>")
        (about: "Convert DBF file into Apache Parquet file")
        (@arg verbose: -v --verbose "be verbose")
        (@arg infile: +required "input file")
        (@arg outfile: +required "output file")
        (@arg plain: -p --plain "use PLAIN encoding")
        (@arg uncompressed: -u --uncompressed "disable compression")
    ).get_matches();
    
    let verbose = args.is_present("verbose");
    let in_file = args.value_of("infile").unwrap();
    let out_file = args.value_of("outfile").unwrap();
    let plain = args.is_present("plain");
    let uncompressed = args.is_present("uncompressed");

    let t_main = std::time::Instant::now();

    let buf = match fs::read(in_file) {
        Ok(buf) => buf,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };
    
    let in_file = match DBFFile::new(&buf) {
        Ok(in_file) => in_file,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let header = in_file.header;

    if verbose {
        println!("{}", header.to_string());

        println!("Field count: {}", in_file.fields.len());

        println!("Fields:");
        for field in in_file.fields {
            
            println!(
                "  Name: {}\n  Type: {}\n  Address: {}\n  Length: {}\n  Decimals: {}\n  Flags: {:#x}\n",
                field.name(),
                field.dtype(),
                field.address,
                field.length,
                field.decimal,
                field.flags
            );
        }
    }

    let enc = header.encoding();

    let mappings_all = in_file.mappings_all();

    if verbose {
        for mapping in &mappings_all {
            println!("{}", mapping.to_string());
        }
    }

    let (in_fields, schema) = DBFFile::to_parquet_schema(&mappings_all[..]);

    if verbose {
        printer::print_schema(&mut std::io::stdout(), &schema);
    }

    let path = Path::new(out_file);

    let prop_builder = WriterProperties::builder()
    .set_created_by("dbftopq".to_string())
    .set_statistics_enabled(false)
    .set_compression(parquet::basic::Compression::SNAPPY);
    
    let prop_builder = if plain {
        prop_builder.set_dictionary_enabled(false)
    } else {
        prop_builder
    };

    let prop_builder = if uncompressed {
        prop_builder.set_compression(parquet::basic::Compression::UNCOMPRESSED)
    } else {
        prop_builder
    };
    

    let props = Arc::new(prop_builder.build());
    let file = match fs::File::create(&path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let mut writer = match SerializedFileWriter::new(file, schema, props) {
        Ok(writer) => writer,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let mut row_group_writer = match writer.next_row_group() {
        Ok(writer) => writer,
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    };

    let mut column_index = 0;
    let t_loop = std::time::Instant::now();
    let data_size = header.calc_datasize();

    loop {
        let t_col = std::time::Instant::now();

        let mut col_writer = match row_group_writer.next_column() {
            Ok(col_writer) => {
                match col_writer {
                    Some(col_writer) => col_writer,
                    None => break
                }
            }
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        };

        let in_field = &in_fields[column_index];
        
        match col_writer {
            ColumnWriter::BoolColumnWriter(ref mut c) => {
                let (values, def_levels) = in_field.load_bool(&buf, &header);
                //let rep_levels = vec![0; values.len()];

                match c.write_batch(&values, Some(&def_levels), None) {
                    Ok(_) => (),
                    Err(err) => {
                        eprintln!("{}", err);
                        std::process::exit(1);
                    }
                }
            },
            ColumnWriter::ByteArrayColumnWriter(ref mut c) => {
                let (values, def_levels) = in_field.load_string(&buf, &enc, &header);
                //let rep_levels = vec![0; values.len()];

                match c.write_batch(&values[..], Some(&def_levels), None) {
                    Ok(_) => (),
                    Err(err) => {
                        eprintln!("{}", err);
                        std::process::exit(1);
                    }
                }
            },
            ColumnWriter::FloatColumnWriter(ref mut c) => {
                let (values, def_levels) = in_field.load_float(&buf, &header);
                //let rep_levels = vec![0; values.len()];

                match c.write_batch(&values, Some(&def_levels), None) {
                    Ok(_) => (),
                    Err(err) => {
                        eprintln!("{}", err);
                        std::process::exit(1);
                    }
                }
            },
            ColumnWriter::Int64ColumnWriter(ref mut c) => {
                match in_field.dtype() {
                    'T' => {
                        let (values, def_levels) = in_field.load_datetime(&buf, &header);
                        //let rep_levels = vec![0; values.len()];

                        match c.write_batch(&values, Some(&def_levels), None) {
                            Ok(_) => (),
                            Err(err) => {
                                eprintln!("{}", err);
                                std::process::exit(1);
                            }
                        }
                    },
                    _ => ()
                }
            },
            ColumnWriter::Int32ColumnWriter(ref mut c) => {
                match in_field.dtype() {
                    'I' => {
                        let (values, def_levels) = in_field.load_int32(&buf, &header);
                        //let rep_levels = vec![0; values.len()];

                        match c.write_batch(&values, Some(&def_levels), None) {
                            Ok(_) => (),
                            Err(err) => {
                                eprintln!("{}", err);
                                std::process::exit(1);
                            }
                        }
                    },
                    'D' => {
                        let (values, def_levels) = in_field.load_date(&buf, &header);
                        //let rep_levels = vec![0; values.len()];

                        match c.write_batch(&values, Some(&def_levels), None) {
                            Ok(_) => (),
                            Err(err) => {
                                eprintln!("{}", err);
                                std::process::exit(1);
                            }
                        }
                    },
                    _ => ()
                }
                
            },
            _ => ()
        }

        match row_group_writer.close_column(col_writer) {
            Ok(_) => (),
            Err(err) => {
                eprintln!("{}", err);
                std::process::exit(1);
            }
        }

        if verbose {
            println!("Converted field {} [{}] in {} ms", in_field.name(), in_field.dtype(), t_col.elapsed().as_millis());
        }

        column_index += 1;
    }

    let speed = (data_size as f64) / 1048576.0 / t_loop.elapsed().as_secs_f64(); // MiB/s

    match writer.close_row_group(row_group_writer) {
        Ok(_) => (),
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    }

    match writer.close() {
        Ok(_) => (),
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    }

    if verbose {
        println!("Runtime: {} ms\nSpeed: {:.1} MiB/s", t_main.elapsed().as_millis(), speed);
    }
}