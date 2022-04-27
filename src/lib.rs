use packed_struct::prelude::*;
use std::convert::TryInto;
use std::fmt;
use std::str;
use std::str::FromStr;
use chrono::*;
use std::borrow::Cow;
use std::i32;
use std::convert::TryFrom;
use parquet::{
    basic::{LogicalType, Repetition, Type as PhysicalType, TimestampType, TimeUnit},
    schema::{types::Type},
};
use std::sync::Arc;

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
        self.headerlen as usize + self.calc_datasize()
    }

    pub fn calc_datasize(&self) -> usize {
        self.numrecords as usize * self.recordlen as usize
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
    pub address: u32,
    pub length: u8,
    pub decimal: u8,
    pub flags: u8,
    pub reserved1: ReservedZero<packed_bits::Bits32>,
    pub reserved2: ReservedZero<packed_bits::Bits8>,
    pub reserved3: ReservedZero<packed_bits::Bits64>,
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

// Writing NULL to Parquet: https://stackoverflow.com/questions/51333205/write-null-value-to-parquet-file
//
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
                            //values.push(parquet::data_type::ByteArray::from(Vec::new()));
                            def_levels.push(0);
                        }
                    }
                },
                Err(_) => {
                    //values.push(parquet::data_type::ByteArray::from(Vec::new()));
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
                    //values.push(false);
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
                    //values.push(0);
                    def_levels.push(0);
                }
            }
        }
        
        return (values, def_levels);
    }

    pub fn parse_i(buf: &[u8]) -> Option<i32> {
        /*let ibuf = <[u8; 4]>::try_from(buf);

        return match ibuf {
            Ok(b) => Some(i32::from_le_bytes(b)),
            Err(_) => None
        };*/

        return Some(i32::from_le_bytes(buf.try_into().unwrap()))
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
                    //values.push(0.0);
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
            let res = Self::parse_d(self.record_bytes(buf, i as usize, header));

            match res {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1);
                },
                None => {
                    //values.push(0);
                    def_levels.push(0);
                }
            }
        }

        //let sum: i16 = def_levels.iter().sum();
        //println!("Len values: {}, sum def levels: {}", values.len(), sum);
        
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

        //println!("Bytes: {:?}", &buf);
        //println!("Str: {}", unsafe { str::from_utf8_unchecked(&buf) });

        let date = match chrono::NaiveDate::from_ymd_opt(y, m, d) {
            Some(d) => d,
            None => return None
        };

        //println!("Date: {}", date);

        let duration = date.signed_duration_since(chrono::NaiveDate::from_ymd(1970, 1, 1));
        let days = i32::try_from(duration.num_days());

        // https://github.com/ClickHouse/ClickHouse/blob/d7b88d76830e641130715b35194f350347e7caae/src/Processors/Formats/Impl/ArrowColumnToCHColumn.cpp#L167
        // https://github.com/ClickHouse/ClickHouse/blob/d9e5ca21195a2689725488ff2519ff2233f1bbd3/src/Common/DateLUTImpl.h#L22
        // https://github.com/ClickHouse/ClickHouse/issues/36459
        let date_lut_max_extend_day_num: i32 = 0x20000 - 16436;

        //println!("Duration: {}", duration);
        //println!("Days: {}", days.ok()?);

        match days {
            Ok(d) => {
                if d <= date_lut_max_extend_day_num {
                    return Some(d);
                } else {
                    return Some(date_lut_max_extend_day_num)
                }
            },
            Err(_) => return None
        }
    }

    pub fn load_datetime<'a>(&self, buf: &'a [u8], header: &DBFHeader) -> (Vec<i64>, Vec<i16>) {
        let mut values: Vec<i64> = Vec::with_capacity(header.numrecords as usize);
        let mut def_levels: Vec<i16> = Vec::with_capacity(header.numrecords as usize);

        for i in 0..header.numrecords {
            let res = Self::parse_t(self.record_bytes(buf, i as usize, header));

            match res {
                Some(s) => {
                    values.push(s);
                    def_levels.push(1);
                },
                None => {
                    //values.push(0);
                    def_levels.push(0);
                }
            }
        }

        //let sum: i16 = def_levels.iter().sum();
        //println!("Len values: {}, sum def levels: {}", values.len(), sum);
        
        return (values, def_levels);
    }

    // thanks: http://www.independent-software.com/dbase-dbf-dbt-file-format.html#reading-records
    //
    // DateTime values are encoded as 32 bits numbers.
    // The high word is the date, encoded as the number of days since Jan 1, 4713BC,
    // and the low word is the time, encoded as (Hours * 3,600,000) + (Minutes * 60,000) + (Seconds * 1,000)
    // (the number of milliseconds since midnight).
    pub fn parse_t(buf: &[u8]) -> Option<i64> {
        let base_foxpro_millis: i64 = 210866803200000;
        let day_to_millis_factor: i64 = 24 * 60 * 60 * 1000;

        let (dbuf, tbuf) = buf.split_at(std::mem::size_of::<i32>());

        let date_days = i32::from_le_bytes(dbuf.try_into().unwrap()) as i64;
        let time_millis = i32::from_le_bytes(tbuf.try_into().unwrap()) as i64;

        let timestamp = date_days * day_to_millis_factor - base_foxpro_millis + time_millis;

        // make sure we are in bounds of datetime64[ns]
        // https://github.com/apache/arrow/blob/a9f2091f8518590c72d25452dc60c8173ee6223c/cpp/src/arrow/compute/kernels/scalar_cast_temporal.cc#L59
        // https://github.com/apache/arrow/blob/b0c75dee34de65834e5a83438e6581f90970fd3d/python/pyarrow/table.pxi#L2591
        let min_val = i64::MIN / 1000000;
        let max_val = i64::MAX / 1000000;

        if timestamp < min_val {
            return Some(min_val);
        }
        else if timestamp > max_val {
            return Some(max_val);
        }

        //let datetime = Utc.timestamp_millis(date_days * day_to_millis_factor - base_foxpro_millis + time_millis);
        //print!("{}\n", date_days * day_to_millis_factor - base_foxpro_millis + time_millis);
        return Some(timestamp);

        //return None;
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
            if self.offset >= self.len {
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
    pub header: DBFHeader,
    pub fields: DBFFields<'a>
}

pub struct DBFMapping {
    pub in_field: DBFField,
    pub out_field: Option<Type>
}

impl DBFMapping {
    pub fn new(in_field: DBFField, out_field: Option<Type>) -> Self {
        Self { in_field: in_field, out_field: out_field }
    }

    pub fn to_string(&self) -> String {
        match &self.out_field {
            Some(out_field) => return format!("{} [{}] => {} [{}]", self.in_field.name(), self.in_field.dtype(), out_field.name(), out_field.get_physical_type()),
            None => return format!("{} [{}] => ()", self.in_field.name(), self.in_field.dtype())
        }
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
    pub fn mappings_all(&self) -> Vec<DBFMapping> {
        let in_fields = self.fields;
        let mut out_fields: Vec<DBFMapping> = Vec::with_capacity(in_fields.len());

        for in_field in in_fields {
            let field_name = in_field.name().to_lowercase();
            let mut out_field: Option<Type> = None;

            // UNWRAP: we know our types are OK.
            match in_field.dtype() {
                'C' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::STRING(Default::default())))
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap());
                },
                'D' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::INT32)
                    .with_logical_type(Some(LogicalType::DATE(Default::default())))
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap());
                },
                'L' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::BOOLEAN)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap());
                },
                'F' | 'N' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::FLOAT)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap());
                },
                'I' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::INT32)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap());
                },
                'T' => {
                    out_field = Some(Type::primitive_type_builder(&field_name, PhysicalType::INT64)
                    .with_logical_type(Some(LogicalType::TIMESTAMP(TimestampType::new(false, TimeUnit::MILLIS(Default::default())))))
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap());
                }
                _ => ()
            }

            out_fields.push(DBFMapping::new(in_field, out_field));
        }
    
        return out_fields;
    }

    pub fn to_parquet_schema<'c>(mappings: &'c [DBFMapping]) -> (Vec<DBFField>, Arc<Type>) {
        let mut in_fields: Vec<DBFField> = vec![];
        let mut out_fields: Vec<parquet::schema::types::TypePtr> = mappings.iter().filter(|m| m.out_field.is_some()).map(|mapping| {
            in_fields.push(mapping.in_field.clone());
            let out_ref = mapping.out_field.as_ref().unwrap();
            Arc::new(out_ref.clone())
        }).collect();

        // UNWRAP: we know our types are OK.
        return (in_fields, Arc::new(Type::group_type_builder("schema").with_fields(&mut out_fields).build().unwrap()));
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
}