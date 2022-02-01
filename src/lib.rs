use std::fs;
use packed_struct::prelude::*;
use std::fmt;
use std::str;
use std::str::FromStr;
use chrono::*;
use std::borrow::Cow;
use std::i32;
use std::convert::TryFrom;
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType},
    schema::{printer, types::Type},
    file::{
        properties::WriterProperties,
        writer::{FileWriter, SerializedFileWriter}
    },
};
use parquet::column::writer::ColumnWriter;
use std::sync::Arc;
use std::path::Path;

pub enum EncodingLabel {
    Enc(&'static [u8]),
    Cp(u16),
    None
}

impl EncodingLabel {
    pub fn is_some(&self) -> bool {
        return match self {
            EncodingLabel::None => false,
            _ => true
        }
    }
}

impl std::fmt::Display for EncodingLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodingLabel::Enc(l) => {
                match str::from_utf8(l) {
                    Ok(s) => write!(f, "{}", s),
                    Err(_) => return Err(std::fmt::Error)
                }
            },
            EncodingLabel::Cp(c) => write!(f, "cp{}", c),
            EncodingLabel::None => write!(f, "")
        }
    }
}

pub enum Encoding {
    Enc(&'static encoding_rs::Encoding),
    Cp(&'static oem_cp::code_table_type::TableType),
    None
}

#[derive(PackedStruct, Copy, Clone)]
#[packed_struct(endian="lsb")]
pub struct DBFHeader {
    dbversion: u8, // 0
    year: u8, // 1
    month: u8, // 2
    day: u8, // 3
    numrecords: u32, // 4-7
    headerlen: u16, // 8-9
    recordlen: u16, // 10-11
    reserved1: ReservedZero<packed_bits::Bits128>, // 12-27
    flags: u8, // 28
    language_driver: u8, // 29
    reserved4: ReservedZero<packed_bits::Bits16> // 30-31
}

impl DBFHeader {
    pub fn language_driver_to_label(language_driver: u8) -> EncodingLabel {
        return match language_driver {
            0x00 => EncodingLabel::Enc("ascii".as_bytes()), // ASCII
            0x01 => EncodingLabel::Cp(437),
            0x02 => EncodingLabel::Cp(850),
            0x03 => EncodingLabel::Enc("cp1252".as_bytes()),
            0x04 => EncodingLabel::Enc("mac".as_bytes()),
            0x08 => EncodingLabel::Cp(865),
            0x09 => EncodingLabel::Cp(437),
            0x0a => EncodingLabel::Cp(850),
            0x0b => EncodingLabel::Cp(437),
            0x0d => EncodingLabel::Cp(437),
            0x0e => EncodingLabel::Cp(850),
            0x0f => EncodingLabel::Cp(437),
            0x10 => EncodingLabel::Cp(850),
            0x11 => EncodingLabel::Cp(437),
            0x12 => EncodingLabel::Cp(850),
            0x13 => EncodingLabel::Cp(932),
            0x14 => EncodingLabel::Cp(850),
            0x15 => EncodingLabel::Cp(437),
            0x16 => EncodingLabel::Cp(850),
            0x17 => EncodingLabel::Cp(865),
            0x18 => EncodingLabel::Cp(437),
            0x19 => EncodingLabel::Cp(437),
            0x1a => EncodingLabel::Cp(850),
            0x1b => EncodingLabel::Cp(437),
            0x1c => EncodingLabel::Cp(863),
            0x1d => EncodingLabel::Cp(850),
            0x1f => EncodingLabel::Cp(852),
            0x22 => EncodingLabel::Cp(852),
            0x23 => EncodingLabel::Cp(852),
            0x24 => EncodingLabel::Cp(860),
            0x25 => EncodingLabel::Cp(850),
            0x26 => EncodingLabel::Cp(866),
            0x37 => EncodingLabel::Cp(850),
            0x40 => EncodingLabel::Cp(852),
            0x4d => EncodingLabel::Enc("gbk".as_bytes()), // cp936
            0x4e => EncodingLabel::Enc("windows-949".as_bytes()),
            0x4f => EncodingLabel::Enc("big5".as_bytes()), // cp950
            0x50 => EncodingLabel::Cp(874),
            0x57 => EncodingLabel::Enc("cp1252".as_bytes()),
            0x58 => EncodingLabel::Enc("cp1252".as_bytes()),
            0x59 => EncodingLabel::Enc("cp1252".as_bytes()),
            0x64 => EncodingLabel::Cp(852),
            0x65 => EncodingLabel::Cp(866),
            0x66 => EncodingLabel::Cp(865),
            0x67 => EncodingLabel::Cp(861),
            0x6a => EncodingLabel::Cp(737),
            0x6b => EncodingLabel::Cp(857),
            0x78 => EncodingLabel::Enc("big5".as_bytes()), // cp950
            0x79 => EncodingLabel::Enc("windows-949".as_bytes()),
            0x7a => EncodingLabel::Enc("gbk".as_bytes()), // cp936
            0x7b => EncodingLabel::Enc("ms-932".as_bytes()),
            0x7c => EncodingLabel::Cp(874),
            0x7d => EncodingLabel::Enc("cp1255".as_bytes()),
            0x7e => EncodingLabel::Enc("cp1256".as_bytes()),
            0xc8 => EncodingLabel::Enc("cp1250".as_bytes()),
            0xc9 => EncodingLabel::Enc("cp1251".as_bytes()),
            0xca => EncodingLabel::Enc("cp1254".as_bytes()),
            0xcb => EncodingLabel::Enc("cp1253".as_bytes()),
            0x96 => EncodingLabel::Enc("x-mac-cyrillic".as_bytes()),
            0x97 => EncodingLabel::Enc("mac".as_bytes()), // mac-latin2
            0x98 => EncodingLabel::Enc("mac".as_bytes()), // mac-greek
            _ => EncodingLabel::None
        }
    }
    
    pub fn encoding_label_to_encoding(label: EncodingLabel) -> Encoding {
        return match label {
            EncodingLabel::Enc(l) => {
                match encoding_rs::Encoding::for_label(l) {
                    Some(e) => Encoding::Enc(e),
                    None => Encoding::None
                }
            },
            EncodingLabel::Cp(c) => {
                match (*oem_cp::code_table::DECODING_TABLE_CP_MAP).get(&c) {
                    Some(c) => Encoding::Cp(c),
                    None => Encoding::None
                }
            },
            EncodingLabel::None => Encoding::None
        }
    }

    pub fn encoding(&self) -> Encoding {
        return DBFHeader::encoding_label_to_encoding(DBFHeader::language_driver_to_label(self.language_driver))
    }

    pub fn encoding_label(&self) -> EncodingLabel {
        return DBFHeader::language_driver_to_label(self.language_driver)
    }

    pub fn dbversion_string(&self) -> Option<&'static str> {
        return match self.dbversion {
            0x02 | 0xfb => Some("FoxBASE"),
            0x03 => Some("FoxBASE+/Dbase III plus, no memo"),
            0x30 => Some("Visual FoxPro"),
            0x31 => Some("Visual FoxPro, autoincrement enabled"),
            0x32 => Some("Visual FoxPro with field type Varchar or Varbinary"),
            0x43 => Some("dBASE IV SQL table files, no memo"),
            0x63 => Some("dBASE IV SQL system files, no memo"),
            0x83 => Some("FoxBASE+/dBASE III PLUS, with memo"),
            0x8b => Some("dBASE IV with memo"),
            0xcb => Some("dBASE IV SQL table files, with memo"),
            0xf5 => Some("FoxPro 2.x (or earlier) with memo"),
            0xe5 => Some("HiPer-Six format with SMT memo file"),
            _ => None
        }
    }

    pub fn is_valid(&self) -> bool {
        return self.dbversion_string().is_some() && self.numrecords > 0 && self.recordlen > 0 && self.encoding_label().is_some()
    }

    pub fn calc_filesize(&self) -> usize {
        self.headerlen as usize + self.numrecords as usize * self.recordlen as usize
    }

    pub fn to_string(&self) -> String {
        format!(
            "DB version: {}\nDate: {:02}-{:02}-{:02}\nNo. of records: {}\nHeader length: {}\nRecord length: {}\nLanguage driver: {}\nFlags: {:#x}",
            self.dbversion_string().unwrap_or("Unknown"),
            self.year,
            self.month,
            self.day,
            self.numrecords,
            self.headerlen,
            self.recordlen,
            self.encoding_label(),
            self.flags
        )
    }
}

#[derive(PackedStruct, Clone)]
#[packed_struct(endian="lsb")]
pub struct DBFField {
    _name: [u8; 11],
    _dtype: u8,
    address: u32,
    length: u8,
    decimal: u8,
    flags: u8,
    reserved1: ReservedZero<packed_bits::Bits32>,
    reserved2: ReservedZero<packed_bits::Bits8>,
    reserved3: ReservedZero<packed_bits::Bits64>,
}

pub trait DateTimestamp {
    fn timestamp(&self) -> i32;
}

impl DateTimestamp for chrono::NaiveDate {
    fn timestamp(&self) -> i32 {
        const UNIX_EPOCH_DAY: i32 = 719_163;
        let gregorian_day = self.num_days_from_ce();
        (gregorian_day - UNIX_EPOCH_DAY) * 86_400
    }
}

pub enum DecodeError {
    NoDecoderFound
}

impl DBFField {
    pub fn name(&self) -> &str {
        // SAFETY: we rely on the field being ascii
        return unsafe { str::from_utf8_unchecked(&self._name).trim_end_matches(char::from(0)) }
    }

    pub fn dtype(&self) -> char {
        return self._dtype as char
    }

    pub fn record_address(&self, record: usize, header: &DBFHeader) -> usize {
        return header.headerlen as usize + (record * header.recordlen as usize) + self.address as usize;
    }

    // SAFETY: DBFFile makes sure the buffer is at least as long as the highest record byte index
    pub fn record_bytes<'a>(&self, buf: &'a [u8], record: usize, header: &DBFHeader) -> &'a [u8] {
        let start: usize = self.record_address(record, header);
        let end: usize = start + self.length as usize;

        return &buf[start..end];
    }

    pub fn load_string<'a>(&self, buf: &'a [u8], enc: &Encoding, header: &DBFHeader) -> (Vec<parquet::data_type::ByteArray>, Vec<i16>) {
        let mut values: Vec<parquet::data_type::ByteArray> = Vec::with_capacity(header.numrecords as usize);
        let mut def_levels: Vec<i16> = Vec::with_capacity(header.numrecords as usize);

        for i in 0..header.numrecords {
            let res = Self::parse_c(self.record_bytes(buf, i as usize, header), enc);

            match res {
                Ok(o) => {
                    match o {
                        Some(s) => {
                            values.push(parquet::data_type::ByteArray::from(s.trim_end_matches(&[' ', '\0'][..]).to_owned().into_bytes()));
                            def_levels.push(1);
                        },
                        None => {
                            values.push(parquet::data_type::ByteArray::from(Vec::new()));
                            def_levels.push(0);
                        }
                    }
                },
                Err(_) => {
                    values.push(parquet::data_type::ByteArray::from(Vec::new()));
                    def_levels.push(0);
                }
            }
        }
        
        return (values, def_levels);
    }

    pub fn parse_c<'a>(buf: &'a [u8], enc: &Encoding) -> Result<Option<std::borrow::Cow<'a, str>>, DecodeError> {
        match enc {
            Encoding::Enc(e) => {
                return Ok(e.decode_without_bom_handling_and_without_replacement(buf)); 
            },
            Encoding::Cp(c) => {
                return match c.decode_string_checked(buf) {
                    Some(s) => Ok(Some(Cow::Owned(s))),
                    None => Ok(None)
                };
            },
            Encoding::None => {
                return Err(DecodeError::NoDecoderFound)
            }
        }
    }

    pub fn load_bool<'a>(&self, buf: &'a [u8], header: &DBFHeader) -> (Vec<bool>, Vec<i16>) {
        let mut values: Vec<bool> = Vec::with_capacity(header.numrecords as usize);
        let mut def_levels: Vec<i16> = Vec::with_capacity(header.numrecords as usize);

        for i in 0..header.numrecords {
            let res = Self::parse_l(self.record_bytes(buf, i as usize, header));

            match res {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1);
                },
                None => {
                    values.push(false);
                    def_levels.push(0);
                }
            }
        }
        
        return (values, def_levels);
    }

    pub fn parse_l(buf: &[u8]) -> Option<bool> {
        return match buf[0] as char {
            'T' | 't' | 'Y' | 'y' => Some(true),
            'F' | 'f' | 'N' | 'n' => Some(false),
            _ => None
        };
    }

    pub fn load_int32<'a>(&self, buf: &'a [u8], header: &DBFHeader) -> (Vec<i32>, Vec<i16>) {
        let mut values: Vec<i32> = Vec::with_capacity(header.numrecords as usize);
        let mut def_levels: Vec<i16> = Vec::with_capacity(header.numrecords as usize);

        for i in 0..header.numrecords {
            let res = Self::parse_i(self.record_bytes(buf, i as usize, header));

            match res {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1);
                },
                None => {
                    values.push(0);
                    def_levels.push(0);
                }
            }
        }
        
        return (values, def_levels);
    }

    pub fn parse_i(buf: &[u8]) -> Option<i32> {
        let ibuf = <[u8; 4]>::try_from(buf);

        return match ibuf {
            Ok(b) => Some(i32::from_le_bytes(b)),
            Err(_) => None
        };
    }

    pub fn load_float<'a>(&self, buf: &'a [u8], header: &DBFHeader) -> (Vec<f32>, Vec<i16>) {
        let mut values: Vec<f32> = Vec::with_capacity(header.numrecords as usize);
        let mut def_levels: Vec<i16> = Vec::with_capacity(header.numrecords as usize);

        for i in 0..header.numrecords {
            let res = Self::parse_f(self.record_bytes(buf, i as usize, header));

            match res {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1);
                },
                None => {
                    values.push(0.0);
                    def_levels.push(0);
                }
            }
        }
        
        return (values, def_levels);
    }

    pub fn parse_f(buf: &[u8]) -> Option<f32> {
        // SAFETY: we rely on the field being ascii
        let s = unsafe { str::from_utf8_unchecked(buf).trim_start_matches(' ') };

        return match s.parse::<f32>() {
            Ok(f) => Some(f),
            Err(_) => None
        };
    }

    pub fn load_date<'a>(&self, buf: &'a [u8], header: &DBFHeader) -> (Vec<i32>, Vec<i16>) {
        let mut values: Vec<i32> = Vec::with_capacity(header.numrecords as usize);
        let mut def_levels: Vec<i16> = Vec::with_capacity(header.numrecords as usize);

        for i in 0..header.numrecords {
            let res = Self::parse_i(self.record_bytes(buf, i as usize, header));

            match res {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1);
                },
                None => {
                    values.push(0);
                    def_levels.push(0);
                }
            }
        }
        
        return (values, def_levels);
    }
    
    pub fn parse_d(buf: &[u8]) -> Option<i32> {
        // SAFETY: we rely on the field being ascii
        let y = match i32::from_str(unsafe { str::from_utf8_unchecked(&buf[0..4]) }) {
            Ok(s) => s,
            Err(_) => return None
        };

        // SAFETY: we rely on the field being ascii
        let m = match u32::from_str(unsafe { str::from_utf8_unchecked(&buf[4..6]) }) {
            Ok(s) => s,
            Err(_) => return None
        };

        // SAFETY: we rely on the field being ascii
        let d = match u32::from_str(unsafe { str::from_utf8_unchecked(&buf[6..8]) }) {
            Ok(s) => s,
            Err(_) => return None
        };

        let date = match chrono::NaiveDate::from_ymd_opt(y, m, d) {
            Some(d) => d,
            None => return None
        };

        return Option::Some(date.timestamp());
    }

    pub fn to_string(&self) -> String {
        format!(
            "Name: {}\nType: {}\nAddress: {}\nLength: {}\nDecimals: {}\nFlags: {:#x}",
            self.name(),
            self.dtype(),
            self.address,
            self.length,
            self.decimal,
            self.flags
        )
    }
}

#[derive(Copy, Clone)]
pub struct DBFFields<'a> {
    buf: &'a [u8],
    offset: usize,
    len: usize
}

// TODO: implement DBFError
impl<'a> DBFFields<'a> {
    pub fn new(header: &DBFHeader, buf: &'a [u8]) -> Result<Self, DBFError> {
        let mut i: usize = 0;
        let marker: u8 = 0x0d;

        loop {
            let idx = 32 * i + 32;

            if idx > header.headerlen as usize {
                return Err(DBFError::InvalidFields)
            }

            if buf[idx] == marker {
                break
            }

            i += 1;
        }

        Ok(Self { buf: buf, offset: 0, len: i })
    }
}

impl<'a> ExactSizeIterator for DBFFields<'a> {
    fn len(&self) -> usize {
        self.len - self.offset
    }
}

impl<'a> Iterator for DBFFields<'a> {
        type Item = DBFField;

        fn size_hint(&self) -> (usize, Option<usize>) {
            return (self.len(), Some(self.len()))
        }

        fn next(&mut self) -> Option<Self::Item> {
            if self.offset >= self.len() {
                return None
            }

            let start: usize = 32*self.offset+32;
            let end: usize = start + 32;

            self.offset += 1;

            // UNWRAP: buffer length is OK.
            Some(DBFField::unpack_from_slice(&self.buf[start..end]).unwrap())
        }
}

pub struct DBFFile<'a> {
    header: DBFHeader,
    fields: DBFFields<'a>
}

pub struct DBFFieldToParquetColumn {
    in_field: DBFField,
    out_field: Option<Type>
}

impl DBFFieldToParquetColumn {
    pub fn new(in_field: DBFField, out_field: Option<Type>) -> Self {
        Self { in_field: in_field, out_field: out_field }
    }
}

use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBFToParquetError {
    #[error(transparent)]
    IOError(#[from] std::io::Error),
    #[error(transparent)]
    ParquetError(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    DBFError(#[from] DBFError)
}

#[derive(Error, Debug)]
pub enum DBFError {
    #[error("invalid DBF header")]
    InvalidHeader,
    #[error("invalid DBF fields")]
    InvalidFields
}

impl<'a> DBFFile<'a> {
    pub fn to_parquet_columns(&self) -> Vec<DBFFieldToParquetColumn> {
        let in_fields = self.fields;
        let mut out_fields: Vec<DBFFieldToParquetColumn> = Vec::with_capacity(in_fields.len());

        for in_field in in_fields {
            let field_name = in_field.name().to_lowercase();
            let mut out_field: Option<Type> = None;

            // UNWRAP: we know our types are OK.
            match in_field.dtype() {
                'C' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::BYTE_ARRAY)
                    .with_logical_type(LogicalType::UTF8)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap());
                },
                'D' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::INT32)
                    .with_logical_type(LogicalType::DATE)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap());
                },
                'L' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::BOOLEAN)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap());
                },
                'F' | 'N' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::FLOAT)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap());
                },
                'I' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::INT32)
                    .with_repetition(Repetition::REQUIRED)
                    .build()
                    .unwrap());
                },
                _ => ()
            }

            out_fields.push(DBFFieldToParquetColumn::new(in_field, out_field));
        }
    
        return out_fields;
    }

    pub fn linked_fields<'c>(fields: &'c [DBFFieldToParquetColumn]) -> Vec<&DBFFieldToParquetColumn> {
        return fields.iter().filter(|f| f.out_field.is_some()).collect();
    }

    pub fn to_parquet_schema<'c>(linked_fields: &'c [&DBFFieldToParquetColumn]) -> Type {
        let mut out_fields: Vec<parquet::schema::types::TypePtr> = linked_fields.iter().map(|linked_field| {
            // UNWRAP: we know this is Some(), since we filter in linked_fields
            let out_ref = linked_field.out_field.as_ref().unwrap();
            Arc::new(out_ref.clone())
        }).collect();

        // UNWRAP: we know our types are OK.
        return Type::group_type_builder("schema").with_fields(&mut out_fields).build().unwrap();
    }

    pub fn new(data: &'a [u8]) -> Result<Self, DBFError> {
        if !(data.len() >= 32) {
            return Err(DBFError::InvalidHeader);
        }

        // UNWRAP: buffer length is OK.
        let header = DBFHeader::unpack_from_slice(&data[0..32]).unwrap();

        if !(header.is_valid()) || !(data.len() >= header.calc_filesize()) {
            return Err(DBFError::InvalidHeader);
        }

        let fields = DBFFields::new(&header, data);

        match fields {
            Ok(fields) => return Ok(Self {header: header, fields: fields}),
            Err(err) => return Err(err)
        }
    }

    pub fn dbf_to_parquet<'c, 'd, W>(in_file: &'c str, out_file: &'d str, out: &mut Option<W>) -> Result<(), DBFToParquetError>
    where W: std::io::Write + std::marker::Send {
        let now = std::time::Instant::now();

        let buf = match fs::read(in_file) {
            Ok(buf) => buf,
            Err(err) => return Err(DBFToParquetError::IOError(err))
        };
        
        let in_file = match DBFFile::new(&buf) {
            Ok(in_file) => in_file,
            Err(err) => return Err(DBFToParquetError::DBFError(err))
        };

        let header = in_file.header;

        if let Some(out) = out {
            writeln!(out, "{}", header.to_string());
        }
    
        let enc = header.encoding();
    
        let columns = in_file.to_parquet_columns();
        let linked_fields = DBFFile::linked_fields(&columns);
        let schema = Arc::new(DBFFile::to_parquet_schema(&linked_fields[..]));

        if let Some(out) = out {
            printer::print_schema(out, &schema);
        }
    
        let path = Path::new(out_file);
    
        let prop_builder = WriterProperties::builder()
        .set_created_by("dbf-to-parquet <https://github.com/seca-rca/dbf-to-parquet>".to_string())
        .set_statistics_enabled(false)
        .set_compression(parquet::basic::Compression::UNCOMPRESSED);
    
        let props = Arc::new(prop_builder.build());
        let file = match fs::File::create(&path) {
            Ok(file) => file,
            Err(err) => return Err(DBFToParquetError::IOError(err))
        };
    
        let mut writer = match SerializedFileWriter::new(file, schema, props) {
            Ok(writer) => writer,
            Err(err) => return Err(DBFToParquetError::ParquetError(err))
        };

        let mut row_group_writer = match writer.next_row_group() {
            Ok(writer) => writer,
            Err(err) => return Err(DBFToParquetError::ParquetError(err))
        };
    
        let mut column_index = 0;
    
        loop {
            let now = std::time::Instant::now();

            let mut col_writer = match row_group_writer.next_column() {
                Ok(col_writer) => {
                    match col_writer {
                        Some(col_writer) => col_writer,
                        None => break
                    }
                }
                Err(err) => return Err(DBFToParquetError::ParquetError(err))
            };

            let in_field = &linked_fields[column_index].in_field;
            
            match col_writer {
                ColumnWriter::BoolColumnWriter(ref mut c) => {

                    let (values, def_levels) = in_field.load_bool(&buf, &header);
                    match c.write_batch(&values, Some(&def_levels), None) {
                        Ok(_) => (),
                        Err(err) => return Err(DBFToParquetError::ParquetError(err))
                    }
                },
                ColumnWriter::ByteArrayColumnWriter(ref mut c) => {
                    let (values, def_levels) = in_field.load_string(&buf, &enc, &header);
                    match c.write_batch(&values[..], Some(&def_levels), None) {
                        Ok(_) => (),
                        Err(err) => return Err(DBFToParquetError::ParquetError(err))
                    }
                },
                ColumnWriter::FloatColumnWriter(ref mut c) => {
                    let (values, def_levels) = in_field.load_float(&buf, &header);
                    match c.write_batch(&values, Some(&def_levels), None) {
                        Ok(_) => (),
                        Err(err) => return Err(DBFToParquetError::ParquetError(err))
                    }
                },
                ColumnWriter::Int32ColumnWriter(ref mut c) => {
                    match in_field.dtype() {
                        'I' => {
                            let (values, def_levels) = in_field.load_int32(&buf, &header);
                            match c.write_batch(&values, Some(&def_levels), None) {
                                Ok(_) => (),
                                Err(err) => return Err(DBFToParquetError::ParquetError(err))
                            }
                        },
                        'D' => {
                            let (values, def_levels) = in_field.load_date(&buf, &header);
                            match c.write_batch(&values, Some(&def_levels), None) {
                                Ok(_) => (),
                                Err(err) => return Err(DBFToParquetError::ParquetError(err))
                            }
                        },
                        _ => ()
                    }
                    
                },
                _ => ()
            }
    
            match row_group_writer.close_column(col_writer) {
                Ok(_) => (),
                Err(err) => return Err(DBFToParquetError::ParquetError(err))
            }

            if let Some(out) = out {
                writeln!(out, "Converted field {} [{}] in {} us", in_field.name(), in_field.dtype(), now.elapsed().as_micros());
            }

            column_index += 1;
        }

        match writer.close_row_group(row_group_writer) {
            Ok(_) => (),
            Err(err) => return Err(DBFToParquetError::ParquetError(err))
        }

        match writer.close() {
            Ok(_) => (),
            Err(err) => return Err(DBFToParquetError::ParquetError(err))
        }

        if let Some(out) = out {
            writeln!(out, "Runtime: {} ms", now.elapsed().as_millis());
        }

        Ok(())
    }
}