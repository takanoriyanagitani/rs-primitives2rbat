use serde::Deserialize;
use serde::Deserializer;

use rs_ints2arrow::arrow;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;

#[derive(Debug, Clone, Copy, Deserialize)]
pub enum Endian {
    /// Not applicable(e.g., non-binary data)
    Unspecified,

    Little,
    Big,
}

#[derive(Debug, Clone)]
pub struct BasicDataType {
    pub raw: DataType,
}

impl From<BasicDataType> for DataType {
    fn from(b: BasicDataType) -> Self {
        b.raw
    }
}

impl<'de> Deserialize<'de> for BasicDataType {
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as Deserializer<'de>>::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = String::deserialize(deserializer)?;
        let dtyp: DataType = str::parse(&s).map_err(serde::de::Error::custom)?;
        Ok(Self { raw: dtyp })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BasicField {
    pub name: String,
    pub dtyp: BasicDataType,
    pub nullable: bool,
    pub endian: Endian,
}

impl From<BasicField> for Field {
    fn from(b: BasicField) -> Self {
        Self::new(b.name, b.dtyp.raw, b.nullable)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct BasicSchema {
    pub fields: Vec<BasicField>,
}

impl From<&BasicSchema> for Schema {
    fn from(b: &BasicSchema) -> Self {
        let fields: Vec<Field> = b.fields.iter().cloned().map(Field::from).collect();
        Self::new(fields)
    }
}
