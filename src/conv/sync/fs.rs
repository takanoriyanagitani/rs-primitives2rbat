use std::io;

use io::BufReader;

use std::fs::File;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use rs_ints2arrow::arrow;

use crate::datatype::BasicSchema;

use super::KvStore;

pub struct FsKvStore<K> {
    pub key2filename: K,
}

impl<K> KvStore for FsKvStore<K>
where
    K: Fn(&str) -> PathBuf,
{
    type Reader = BufReader<File>;
    type Key = String;

    fn open_by_key(&self, key: Self::Key) -> Result<Self::Reader, io::Error> {
        let filename: PathBuf = (self.key2filename)(&key);
        let f = File::open(&filename)
            .map_err(|e| format!("unable to open the file {filename:?}: {e}"))
            .map_err(io::Error::other)?;
        Ok(BufReader::new(f))
    }
}

pub fn fs2batch<K>(sch: BasicSchema, store: FsKvStore<K>) -> Result<RecordBatch, io::Error>
where
    K: Fn(&str) -> PathBuf,
{
    super::kvstore2batch(sch, store)
}
