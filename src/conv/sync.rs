use std::io;
use std::sync::Arc;

use io::Read;

use rs_ints2arrow::arrow;

use arrow::array::Array;
use arrow::record_batch::RecordBatch;

use arrow::datatypes::DataType;
use arrow::datatypes::Schema;

use rs_ints2arrow::sync::raw2ints2arrow8;
use rs_ints2arrow::sync::raw2uints2arrow8;

use rs_ints2arrow::sync::raw2ints2arrow16be;
use rs_ints2arrow::sync::raw2ints2arrow16le;
use rs_ints2arrow::sync::raw2ints2arrow32be;
use rs_ints2arrow::sync::raw2ints2arrow32le;
use rs_ints2arrow::sync::raw2ints2arrow64be;
use rs_ints2arrow::sync::raw2ints2arrow64le;

use rs_ints2arrow::sync::raw2uints2arrow16be;
use rs_ints2arrow::sync::raw2uints2arrow16le;
use rs_ints2arrow::sync::raw2uints2arrow32be;
use rs_ints2arrow::sync::raw2uints2arrow32le;
use rs_ints2arrow::sync::raw2uints2arrow64be;
use rs_ints2arrow::sync::raw2uints2arrow64le;

use crate::datatype::BasicField;
use crate::datatype::BasicSchema;
use crate::datatype::Endian;

pub mod fs;

pub trait KvStore {
    type Reader: Read;
    type Key: Eq;

    fn open_by_key(&self, key: Self::Key) -> Result<Self::Reader, io::Error>;
}

pub fn signed2array<R>(
    rdr: R,
    dtyp: &DataType,
    endian: &Endian,
) -> Result<Arc<dyn Array>, io::Error>
where
    R: Read,
{
    match endian {
        Endian::Unspecified => match dtyp {
            DataType::Int8 => Ok(Arc::new(raw2ints2arrow8(rdr)?)),
            _ => Err(io::Error::other("unsupported data type")),
        },
        Endian::Big => match dtyp {
            DataType::Int8 => Ok(Arc::new(raw2ints2arrow8(rdr)?)),
            DataType::Int16 => Ok(Arc::new(raw2ints2arrow16be(rdr)?)),
            DataType::Int32 => Ok(Arc::new(raw2ints2arrow32be(rdr)?)),
            DataType::Int64 => Ok(Arc::new(raw2ints2arrow64be(rdr)?)),
            _ => Err(io::Error::other("unsupported data type")),
        },
        Endian::Little => match dtyp {
            DataType::Int8 => Ok(Arc::new(raw2ints2arrow8(rdr)?)),
            DataType::Int16 => Ok(Arc::new(raw2ints2arrow16le(rdr)?)),
            DataType::Int32 => Ok(Arc::new(raw2ints2arrow32le(rdr)?)),
            DataType::Int64 => Ok(Arc::new(raw2ints2arrow64le(rdr)?)),
            _ => Err(io::Error::other("unsupported data type")),
        },
    }
}

pub fn unsigned2array<R>(
    rdr: R,
    dtyp: &DataType,
    endian: &Endian,
) -> Result<Arc<dyn Array>, io::Error>
where
    R: Read,
{
    match endian {
        Endian::Unspecified => match dtyp {
            DataType::UInt8 => Ok(Arc::new(raw2uints2arrow8(rdr)?)),
            _ => Err(io::Error::other("unsupported data type")),
        },
        Endian::Big => match dtyp {
            DataType::UInt8 => Ok(Arc::new(raw2uints2arrow8(rdr)?)),
            DataType::UInt16 => Ok(Arc::new(raw2uints2arrow16be(rdr)?)),
            DataType::UInt32 => Ok(Arc::new(raw2uints2arrow32be(rdr)?)),
            DataType::UInt64 => Ok(Arc::new(raw2uints2arrow64be(rdr)?)),
            _ => Err(io::Error::other("unsupported data type")),
        },
        Endian::Little => match dtyp {
            DataType::UInt8 => Ok(Arc::new(raw2uints2arrow8(rdr)?)),
            DataType::UInt16 => Ok(Arc::new(raw2uints2arrow16le(rdr)?)),
            DataType::UInt32 => Ok(Arc::new(raw2uints2arrow32le(rdr)?)),
            DataType::UInt64 => Ok(Arc::new(raw2uints2arrow64le(rdr)?)),
            _ => Err(io::Error::other("unsupported data type")),
        },
    }
}

pub fn reader2array<R>(
    rdr: R,
    dtyp: &DataType,
    endian: &Endian,
) -> Result<Arc<dyn Array>, io::Error>
where
    R: Read,
{
    match dtyp {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            signed2array(rdr, dtyp, endian)
        }

        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            unsigned2array(rdr, dtyp, endian)
        }

        _ => Err(io::Error::other("unsupported data type")),
    }
}

pub fn kvstore2batch<S, R>(sch: BasicSchema, store: S) -> Result<RecordBatch, io::Error>
where
    R: Read,
    S: KvStore<Reader = R, Key = String>,
{
    let fields: &[BasicField] = &sch.fields;
    let mut v: Vec<Arc<dyn Array>> = Vec::with_capacity(fields.len());
    for field in fields {
        let name: &str = &field.name;
        let rdr = store.open_by_key(name.into())?;
        let dtyp: &DataType = &field.dtyp.raw;
        let endian: &Endian = &field.endian;
        let arr: Arc<dyn Array> = reader2array(rdr, dtyp, endian)?;
        v.push(arr);
    }
    let s: Schema = (&sch).into();
    RecordBatch::try_new(s.into(), v).map_err(io::Error::other)
}
