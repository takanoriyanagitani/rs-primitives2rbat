use std::io;
use std::process::ExitCode;

use std::path::Path;

use rs_primitives2rbat::arrow;

use arrow::record_batch::RecordBatch;

use rs_primitives2rbat::datatype::BasicSchema;

use rs_primitives2rbat::conv::sync::fs::FsKvStore;
use rs_primitives2rbat::conv::sync::fs::fs2batch;

fn env2root_dir() -> Result<String, io::Error> {
    std::env::var("ENV_DATA_DIR").map_err(io::Error::other)
}

fn env2schema_filename() -> Result<String, io::Error> {
    std::env::var("ENV_BASIC_SCHEMA_JSON").map_err(io::Error::other)
}

fn rdr2schema<R>(rdr: R) -> Result<BasicSchema, io::Error>
where
    R: io::Read,
{
    let br = io::BufReader::new(rdr);
    serde_json::from_reader(br).map_err(io::Error::other)
}

fn env2schema() -> Result<BasicSchema, io::Error> {
    let ofile: Option<std::fs::File> = env2schema_filename()
        .ok()
        .and_then(|f| std::fs::File::open(f).ok());
    match ofile {
        Some(f) => rdr2schema(f),
        None => rdr2schema(io::stdin().lock()),
    }
}

fn sub() -> Result<(), io::Error> {
    let dirname: String = env2root_dir()?;
    let k2name = |key: &str| Path::new(&dirname).join(key).with_extension("dat");
    let parsed: BasicSchema = env2schema()?;
    let rbat: RecordBatch = fs2batch(
        parsed,
        FsKvStore {
            key2filename: k2name,
        },
    )?;
    println!("{rbat:?}");
    Ok(())
}

fn main() -> ExitCode {
    match sub() {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
