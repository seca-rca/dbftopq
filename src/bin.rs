use dbftoparquet;
use clap::*;

fn main() {
    let matches = clap_app!(dbftopq =>
        (version: "0.1")
        (author: "Raphael Campestrini <rca@seca.at>")
        (about: "Convert DBF file into Apache Parquet file")
        (@arg verbose: -v --verbose "be verbose")
        (@arg infile: +required "input file")
        (@arg outfile: +required "output file")
    ).get_matches();

    let stdout = std::io::stdout();
    
    let mut out = match matches.is_present("verbose") {
        true => Some(stdout),
        false => None
    };

    match dbftoparquet::DBFFile::dbf_to_parquet(
        matches.value_of("infile").unwrap(),
        matches.value_of("outfile").unwrap(),
        &mut out.as_mut()
    ) {
        Ok(_) => (),
        Err(err) => {
            eprintln!("{}", err);
            std::process::exit(1);
        }
    }
}