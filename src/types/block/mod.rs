use std::{
    borrow::Cow,
    cmp,
    default::Default,
    fmt,
    io::{Cursor, Read},
    marker::PhantomData,
    os::raw::c_char,
};

use byteorder::{LittleEndian, WriteBytesExt};
use chrono_tz::Tz;
use clickhouse_rs_cityhash_sys::city_hash_128;
use lz4::liblz4::{LZ4_compressBound, LZ4_compress_default};

use crate::{
    binary::{protocol, Encoder, ReadEx},
    errors::{Error, FromSqlError, Result},
    types::{
        column::{self, ArcColumnWrapper, Column, ColumnData, ColumnFrom},
        ColumnType, Complex, FromSql, Simple, SqlType, Value,
    },
};

use self::chunk_iterator::ChunkIterator;
pub(crate) use self::row::BlockRef;
pub use self::{
    block_info::BlockInfo,
    builder::{extract_timezone, RCons, RNil, RowBuilder},
    row::{Row, Rows},
};

mod block_info;
mod builder;
mod chunk_iterator;
mod compressed;
mod row;

const INSERT_BLOCK_SIZE: usize = 1_048_576;

const DEFAULT_CAPACITY: usize = 100;

pub trait ColumnIdx {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize>;
}

pub trait Sliceable {
    fn slice_type() -> SqlType;
}

macro_rules! sliceable {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl Sliceable for $t {
                fn slice_type() -> SqlType {
                    SqlType::$k
                }
            }
        )*
    };
}

sliceable! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64
}

/// Represents Clickhouse Block
#[derive(Default)]
pub struct Block<K: ColumnType = Simple> {
    info: BlockInfo,
    columns: Vec<Column<K>>,
    capacity: usize,
}

impl<L: ColumnType, R: ColumnType> PartialEq<Block<R>> for Block<L> {
    fn eq(&self, other: &Block<R>) -> bool {
        if self.columns.len() != other.columns.len() {
            return false;
        }

        for i in 0..self.columns.len() {
            if self.columns[i] != other.columns[i] {
                return false;
            }
        }

        true
    }
}

impl<K: ColumnType> Clone for Block<K> {
    fn clone(&self) -> Self {
        Self {
            info: self.info,
            columns: self.columns.iter().map(|c| (*c).clone()).collect(),
            capacity: self.capacity,
        }
    }
}

impl<K: ColumnType> AsRef<Block<K>> for Block<K> {
    fn as_ref(&self) -> &Self {
        self
    }
}

impl ColumnIdx for usize {
    #[inline(always)]
    fn get_index<K: ColumnType>(&self, _: &[Column<K>]) -> Result<usize> {
        Ok(*self)
    }
}

impl<'a> ColumnIdx for &'a str {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize> {
        match columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name() == *self)
        {
            None => Err(Error::FromSql(FromSqlError::OutOfRange)),
            Some((index, _)) => Ok(index),
        }
    }
}

impl ColumnIdx for String {
    fn get_index<K: ColumnType>(&self, columns: &[Column<K>]) -> Result<usize> {
        self.as_str().get_index(columns)
    }
}

impl Block {
    /// Constructs a new, empty `Block`.
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_CAPACITY)
    }

    /// Constructs a new, empty `Block` with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            info: Default::default(),
            columns: vec![],
            capacity,
        }
    }

    pub(crate) fn load<R>(reader: &mut R, tz: Tz, compress: bool) -> Result<Self>
    where
        R: Read + ReadEx,
    {
        if compress {
            let mut cr = compressed::make(reader);
            Self::raw_load(&mut cr, tz)
        } else {
            Self::raw_load(reader, tz)
        }
    }

    fn raw_load<R>(reader: &mut R, tz: Tz) -> Result<Block<Simple>>
    where
        R: ReadEx,
    {
        let mut block = Block::new();
        block.info = BlockInfo::read(reader)?;

        let num_columns = reader.read_uvarint()?;
        let num_rows = reader.read_uvarint()?;

        for _ in 0..num_columns {
            let column = Column::read(reader, num_rows as usize, tz)?;
            block.append_column(column);
        }

        Ok(block)
    }
}

impl<K: ColumnType> Block<K> {
    /// Return the number of rows in the current block.
    pub fn row_count(&self) -> usize {
        match self.columns.first() {
            None => 0,
            Some(column) => column.len(),
        }
    }

    /// Return the number of columns in the current block.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// This method returns a slice of columns.
    #[inline(always)]
    pub fn columns(&self) -> &[Column<K>] {
        &self.columns
    }

    fn append_column(&mut self, column: Column<K>) {
        let column_len = column.len();

        if !self.columns.is_empty() && self.row_count() != column_len {
            panic!("all columns in block must have same size.")
        }

        self.columns.push(column);
    }

    /// Get the value of a particular cell of the block.
    pub fn get<'a, T, I>(&'a self, row: usize, col: I) -> Result<T>
    where
        T: FromSql<'a>,
        I: ColumnIdx + Copy,
    {
        let column_index = col.get_index(self.columns())?;
        T::from_sql(self.columns[column_index].at(row))
    }

    /// Add new column into this block
    pub fn add_column<S>(self, name: &str, values: S) -> Self
    where
        S: ColumnFrom,
    {
        self.column(name, values)
    }

    /// Add new column into this block
    pub fn column<S>(mut self, name: &str, values: S) -> Self
    where
        S: ColumnFrom,
    {
        let data = S::column_from::<ArcColumnWrapper>(values);
        let column = column::new_column(name, data);

        self.append_column(column);
        self
    }

    /// Returns true if the block contains no elements.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// This method returns a iterator of rows.
    pub fn rows(&self) -> Rows<K> {
        Rows {
            row: 0,
            block_ref: BlockRef::Borrowed(self),
            kind: PhantomData,
        }
    }

    /// This method is a convenient way to pass row into a block.
    pub fn push<B: RowBuilder>(&mut self, row: B) -> Result<()> {
        row.apply(self)
    }

    /// This method finds a column by identifier.
    pub fn get_column<I>(&self, col: I) -> Result<&Column<K>>
    where
        I: ColumnIdx + Copy,
    {
        let column_index = col.get_index(self.columns())?;
        let column = &self.columns[column_index];
        Ok(column)
    }

    /// Adds a value to a block column
    ///
    /// # Arguments
    /// * `key` - A string representing the column name
    /// * `value` - The value to add to the end of the column
    ///
    /// This method should be cautiously used outside of RowBuilder implementations.
    /// Clickhouse requires that every column has the same number of items
    ///
    ///
    /// This can be used to add the RowBuilder trait to custom-defined structs
    /// ```
    /// use clickhouse_rs::errors;
    /// use clickhouse_rs::types;
    ///
    /// struct Person {
    ///  name: String,
    ///  age: i32
    /// }
    ///
    /// impl types::RowBuilder for Person {
    ///     fn apply<K>(
    ///        self,
    ///        block: &mut types::Block<K>,
    ///     ) -> Result<(), errors::Error>
    ///     where
    ///         K: types::ColumnType,
    ///     {
    ///          block.push_value("name", self.name.into())?;
    ///          block.push_value("age", self.age.into())?;
    ///          Ok(())
    ///     }
    ///}
    ///
    /// # fn main(){
    /// let mut block = types::Block::new();
    /// let person = Person {
    ///   name: "Bob".to_string(),
    ///   age: 42,
    /// };
    ///
    /// block.push(person).unwrap();
    /// assert_eq!(block.row_count(), 1);
    /// # }
    /// ```
    /// Note this requires the types to have either Into or From for Value implemented
    pub fn push_value(&mut self, key: &str, value: Value) -> Result<()> {
        let col_index = match key.get_index(&self.columns) {
            Ok(col_index) => col_index,
            Err(Error::FromSql(FromSqlError::OutOfRange)) => {
                if self.row_count() <= 1 {
                    let sql_type = From::from(value.clone());

                    let timezone = extract_timezone(&value);

                    let column = Column {
                        name: key.clone().into(),
                        data: <dyn ColumnData>::from_type::<ArcColumnWrapper>(
                            sql_type,
                            timezone,
                            self.capacity,
                        )?,
                        _marker: PhantomData,
                    };

                    self.columns.push(column);
                    return self.push_value(key, value);
                } else {
                    return Err(Error::FromSql(FromSqlError::OutOfRange));
                }
            }
            Err(err) => return Err(err),
        };

        self.columns[col_index].push(value);
        Ok(())
    }
}

impl Block<Simple> {
    pub fn concat(blocks: &[Self]) -> Block<Complex> {
        let first = blocks.first().expect("blocks should not be empty.");

        for block in blocks {
            assert_eq!(
                first.column_count(),
                block.column_count(),
                "all columns should have the same size."
            );
        }

        let num_columns = first.column_count();
        let mut columns = Vec::with_capacity(num_columns);
        for i in 0_usize..num_columns {
            let chunks = blocks.iter().map(|block| &block.columns[i]);
            columns.push(Column::concat(chunks));
        }

        Block {
            info: first.info,
            columns,
            capacity: blocks.iter().map(|b| b.capacity).sum(),
        }
    }
}

impl<K: ColumnType> Block<K> {
    pub(crate) fn cast_to(&self, header: &Block<K>) -> Result<Self> {
        let info = self.info;
        let mut columns = self.columns.clone();
        columns.reverse();

        if header.column_count() != columns.len() {
            return Err(Error::FromSql(FromSqlError::OutOfRange));
        }

        let mut new_columns = Vec::with_capacity(columns.len());
        for column in header.columns() {
            let dst_type = column.sql_type();
            let old_column = columns.pop().unwrap();
            let new_column = old_column.cast_to(dst_type)?;
            new_columns.push(new_column);
        }

        Ok(Block {
            info,
            columns: new_columns,
            capacity: self.capacity,
        })
    }

    pub(crate) fn write(&self, encoder: &mut Encoder, compress: bool) {
        if compress {
            let mut tmp_encoder = Encoder::new();
            self.write(&mut tmp_encoder, false);
            let tmp = tmp_encoder.get_buffer();

            let mut buf = Vec::new();
            let size;
            unsafe {
                buf.resize(9 + LZ4_compressBound(tmp.len() as i32) as usize, 0_u8);
                size = LZ4_compress_default(
                    tmp.as_ptr() as *const c_char,
                    (buf.as_mut_ptr() as *mut c_char).add(9),
                    tmp.len() as i32,
                    buf.len() as i32,
                );
            }
            buf.resize(9 + size as usize, 0_u8);

            let buf_len = buf.len() as u32;
            {
                let mut cursor = Cursor::new(&mut buf);
                cursor.write_u8(0x82).unwrap();
                cursor.write_u32::<LittleEndian>(buf_len).unwrap();
                cursor.write_u32::<LittleEndian>(tmp.len() as u32).unwrap();
            }

            let hash = city_hash_128(&buf);
            encoder.write(hash.lo);
            encoder.write(hash.hi);
            encoder.write_bytes(buf.as_ref());
        } else {
            self.info.write(encoder);
            encoder.uvarint(self.column_count() as u64);
            encoder.uvarint(self.row_count() as u64);

            for column in &self.columns {
                column.write(encoder);
            }
        }
    }

    pub(crate) fn send_data(&self, encoder: &mut Encoder, compress: bool) {
        encoder.uvarint(protocol::CLIENT_DATA);
        encoder.string(""); // temporary table
        for chunk in self.chunks(INSERT_BLOCK_SIZE) {
            chunk.write(encoder, compress);
        }
    }

    pub(crate) fn chunks(&self, n: usize) -> ChunkIterator<K> {
        ChunkIterator::new(n, self)
    }
}

impl<K: ColumnType> fmt::Debug for Block<K> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let titles: Vec<&str> = self.columns.iter().map(|column| column.name()).collect();

        let cells: Vec<_> = self.columns.iter().map(|col| text_cells(&col)).collect();

        let titles_len: Vec<_> = titles
            .iter()
            .map(|t| t.chars().count())
            .zip(cells.iter().map(|w| column_width(w)))
            .map(|(a, b)| cmp::max(a, b))
            .collect();

        print_line(f, &titles_len, "\n\u{250c}", '┬', "\u{2510}\n")?;

        for (i, title) in titles.iter().enumerate() {
            write!(f, "\u{2502}{:>width$} ", title, width = titles_len[i] + 1)?;
        }
        write!(f, "\u{2502}")?;

        if self.row_count() > 0 {
            print_line(f, &titles_len, "\n\u{251c}", '┼', "\u{2524}\n")?;
        }

        for j in 0..self.row_count() {
            for (i, col) in cells.iter().enumerate() {
                write!(f, "\u{2502}{:>width$} ", col[j], width = titles_len[i] + 1)?;
            }

            let new_line = (j + 1) != self.row_count();
            write!(f, "\u{2502}{}", if new_line { "\n" } else { "" })?;
        }

        print_line(f, &titles_len, "\n\u{2514}", '┴', "\u{2518}")
    }
}

fn column_width(column: &[String]) -> usize {
    column.iter().map(|cell| cell.len()).max().unwrap_or(0)
}

fn print_line(
    f: &mut fmt::Formatter,
    lens: &[usize],
    left: &str,
    center: char,
    right: &str,
) -> fmt::Result {
    write!(f, "{}", left)?;
    for (i, len) in lens.iter().enumerate() {
        if i != 0 {
            write!(f, "{}", center)?;
        }

        write!(f, "{:\u{2500}>width$}", "", width = len + 2)?;
    }
    write!(f, "{}", right)
}

fn text_cells<K: ColumnType>(data: &Column<K>) -> Vec<String> {
    (0..data.len()).map(|i| format!("{}", data.at(i))).collect()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_write_default() {
        let expected = [1_u8, 0, 2, 255, 255, 255, 255, 0, 0, 0];
        let mut encoder = Encoder::new();
        Block::<Simple>::default().write(&mut encoder, false);
        assert_eq!(encoder.get_buffer_ref(), &expected)
    }

    #[test]
    fn test_compress_block() {
        let expected = vec![
            245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0,
            0, 0, 23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116,
            114, 105, 110, 103, 3, 97, 98, 99,
        ];

        let block = Block::<Simple>::new().column("s", vec!["abc"]);

        let mut encoder = Encoder::new();
        block.write(&mut encoder, true);

        let actual = encoder.get_buffer();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decompress_block() {
        let expected = Block::<Simple>::new().column("s", vec!["abc"]);

        let source = vec![
            245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0,
            0, 0, 23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116,
            114, 105, 110, 103, 3, 97, 98, 99,
        ];

        let mut cursor = Cursor::new(&source[..]);
        let actual = Block::load(&mut cursor, Tz::UTC, true).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_read_empty_block() {
        let source = [1, 0, 2, 255, 255, 255, 255, 0, 0, 0];
        let mut cursor = Cursor::new(&source[..]);
        match Block::<Simple>::load(&mut cursor, Tz::Zulu, false) {
            Ok(block) => assert!(block.is_empty()),
            Err(_) => unreachable!(),
        }
    }

    #[test]
    fn test_empty() {
        assert!(Block::<Simple>::default().is_empty())
    }

    #[test]
    fn test_column_and_rows() {
        let block = Block::<Simple>::new()
            .column("hello_id", vec![5_u32, 6_u32])
            .column("value", vec!["lol", "zuz"]);

        assert_eq!(block.column_count(), 2);
        assert_eq!(block.row_count(), 2);
    }

    #[test]
    fn test_concat() {
        let block_a = make_block();
        let block_b = make_block();

        let actual = Block::concat(&[block_a, block_b]);
        assert_eq!(actual.row_count(), 4);
        assert_eq!(actual.column_count(), 1);

        assert_eq!(
            "5446d186-4e90-4dd8-8ec1-f9a436834613".to_string(),
            actual.get::<String, _>(0, 0).unwrap()
        );
        assert_eq!(
            "f7cf31f4-7f37-4e27-91c0-5ac0ad0b145b".to_string(),
            actual.get::<String, _>(1, 0).unwrap()
        );
        assert_eq!(
            "5446d186-4e90-4dd8-8ec1-f9a436834613".to_string(),
            actual.get::<String, _>(2, 0).unwrap()
        );
        assert_eq!(
            "f7cf31f4-7f37-4e27-91c0-5ac0ad0b145b".to_string(),
            actual.get::<String, _>(3, 0).unwrap()
        );
    }

    fn make_block() -> Block {
        Block::new().column(
            "9b96ad8b-488a-4fef-8087-8a9ae4800f00",
            vec![
                "5446d186-4e90-4dd8-8ec1-f9a436834613".to_string(),
                "f7cf31f4-7f37-4e27-91c0-5ac0ad0b145b".to_string(),
            ],
        )
    }

    #[test]
    fn test_chunks() {
        let first = Block::new().column("A", vec![1, 2]);
        let second = Block::new().column("A", vec![3, 4]);
        let third = Block::new().column("A", vec![5]);

        let block = Block::<Simple>::new().column("A", vec![1, 2, 3, 4, 5]);
        let mut iter = block.chunks(2);

        assert_eq!(Some(first), iter.next());
        assert_eq!(Some(second), iter.next());
        assert_eq!(Some(third), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_chunks_of_empty_block() {
        let block = Block::default();
        assert_eq!(1, block.chunks(100_500).count());
        assert_eq!(Some(block.clone()), block.chunks(100_500).next());
    }

    #[test]
    fn test_rows() {
        let expected = vec![1_u8, 2, 3];
        let block = Block::<Simple>::new().column("A", vec![1_u8, 2, 3]);
        let actual: Vec<u8> = block.rows().map(|row| row.get("A").unwrap()).collect();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_write_and_read() {
        let block = Block::<Simple>::new().column("y", vec![Some(1_u8), None]);

        let mut encoder = Encoder::new();
        block.write(&mut encoder, false);

        let mut reader = Cursor::new(encoder.get_buffer_ref());
        let rblock = Block::load(&mut reader, Tz::Zulu, false).unwrap();

        assert_eq!(block, rblock);
    }
}
