use std::{cmp, fmt, io::Cursor};

use byteorder::{LittleEndian, WriteBytesExt};
use chrono_tz::Tz;
use clickhouse_rs_cityhash_sys::{city_hash_128, UInt128};
use lz4::liblz4::{LZ4_compress_default, LZ4_compressBound, LZ4_decompress_safe};

use crate::{
    binary::{Encoder, protocol, ReadEx},
    errors::{Error, FromSqlError},
    types::{ClickhouseResult, FromSql},
};

use super::column::{self, Column, ColumnFrom};

pub use self::{
    block_info::BlockInfo,
    row::{Row, Rows},
};
use self::chunk_iterator::ChunkIterator;
pub(crate) use self::row::BlockRef;

mod block_info;
mod chunk_iterator;
mod row;

const INSERT_BLOCK_SIZE: usize = 1_048_576;

const DBMS_MAX_COMPRESSED_SIZE: u32 = 0x4000_0000; // 1GB

pub trait ColumnIdx {
    fn get_index(&self, columns: &[Column]) -> ClickhouseResult<usize>;
}

/// Represents Clickhouse Block
#[derive(Default)]
pub struct Block {
    info: BlockInfo,
    columns: Vec<Column>,
}

impl PartialEq<Block> for Block {
    fn eq(&self, other: &Block) -> bool {
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

impl Clone for Block {
    fn clone(&self) -> Self {
        Block {
            info: self.info,
            columns: self.columns.iter().map(|c| (*c).clone()).collect(),
        }
    }
}

impl AsRef<Block> for Block {
    fn as_ref(&self) -> &Block {
        self
    }
}

impl ColumnIdx for usize {
    fn get_index(&self, _: &[Column]) -> ClickhouseResult<usize> {
        Ok(*self)
    }
}

impl<'a> ColumnIdx for &'a str {
    fn get_index(&self, columns: &[Column]) -> ClickhouseResult<usize> {
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

impl Block {
    /// Constructs a new, empty Block.
    pub fn new() -> Block {
        Block::default()
    }

    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        tz: Tz,
        compress: bool,
    ) -> ClickhouseResult<Block> {
        if compress {
            let tmp = decompress(reader)?;
            let mut cursor = Cursor::new(&tmp);
            Block::load(&mut cursor, tz, false)
        } else {
            let mut block = Block::default();

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
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    fn append_column(&mut self, column: Column) {
        let column_len = column.len();

        if !self.columns.is_empty() && self.row_count() != column_len {
            panic!("all columns in block must have same count of rows.")
        }

        self.columns.push(column);
    }

    /// Get the value of a particular cell of the block.
    pub fn get<'a, T, I>(&'a self, row: usize, col: I) -> ClickhouseResult<T>
    where
        T: FromSql<'a>,
        I: ColumnIdx,
    {
        let column_index = col.get_index(self.columns())?;
        T::from_sql(self.columns[column_index].at(row))
    }

    /// Add new column into this block
    pub fn add_column<S>(mut self, name: &str, values: S) -> Block
    where
        S: ColumnFrom,
    {
        let data = S::column_from(values);
        let column = column::new_column(name, data);

        self.append_column(column);
        self
    }

    /// Returns true if the block contains no elements.
    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// This method returns a iterator of rows.
    pub fn rows(&self) -> Rows {
        Rows {
            row: 0,
            block_ref: BlockRef::Borrowed(&self),
        }
    }
}

impl Block {
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
                    tmp.as_ptr() as *const i8,
                    (buf.as_mut_ptr() as *mut i8).add(9),
                    tmp.len() as i32,
                    buf.len() as i32,
                );
            }
            buf.resize(9 + size as usize, 0u8);

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

    pub(crate) fn concat(blocks: &[Block]) -> Block {
        let first = blocks.first().expect("blocks should not be empty.");

        for block in blocks {
            assert_eq!(
                first.column_count(),
                block.column_count(),
                "all block should have the same columns."
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
        }
    }

    pub(crate) fn chunks(&self, n: usize) -> ChunkIterator {
        ChunkIterator::new(n, self)
    }
}

impl fmt::Debug for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        let titles: Vec<&str> = self.columns.iter().map(|column| column.name()).collect();

        let cells: Vec<_> = self.columns.iter().map(|col| text_cells(&col)).collect();

        let titles_len: Vec<_> = titles
            .iter()
            .map(|t| t.chars().count())
            .zip(cells.iter().map(|w| column_width(w)))
            .map(|(a, b)| cmp::max(a, b))
            .collect();

        print_line(f, &titles_len, "\n┌", '┬', "┐\n")?;

        for (i, title) in titles.iter().enumerate() {
            write!(f, "│{:>width$} ", title, width = titles_len[i] + 1)?;
        }
        write!(f, "│")?;

        if self.row_count() > 0 {
            print_line(f, &titles_len, "\n├", '┼', "┤\n")?;
        }

        for j in 0..self.row_count() {
            for (i, col) in cells.iter().enumerate() {
                write!(f, "│{:>width$} ", col[j], width = titles_len[i] + 1)?;
            }

            let new_line = (j + 1) != self.row_count();
            write!(f, "│{}", if new_line { "\n" } else { "" })?;
        }

        print_line(f, &titles_len, "\n└", '┴', "┘")
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
) -> Result<(), fmt::Error> {
    write!(f, "{}", left)?;
    for (i, len) in lens.iter().enumerate() {
        if i != 0 {
            write!(f, "{}", center)?;
        }

        write!(f, "{:─>width$}", "", width = len + 2)?;
    }
    write!(f, "{}", right)
}

fn text_cells(data: &Column) -> Vec<String> {
    (0..data.len()).map(|i| format!("{}", data.at(i))).collect()
}

fn decompress<R: ReadEx>(reader: &mut R) -> ClickhouseResult<Vec<u8>> {
    let h = UInt128 {
        lo: reader.read_scalar()?,
        hi: reader.read_scalar()?,
    };

    let method: u8 = reader.read_scalar()?;
    if method != 0x82 {
        let message: String = format!("unsupported compression method {}", method);
        return Err(raise_error(message));
    }

    let compressed: u32 = reader.read_scalar()?;
    let original: u32 = reader.read_scalar()?;

    if compressed > DBMS_MAX_COMPRESSED_SIZE {
        return Err(raise_error("compressed data too big".to_string()));
    }

    let mut tmp = vec![0u8; compressed as usize];
    {
        let mut cursor = Cursor::new(&mut tmp);
        cursor.write_u8(0x82)?;
        cursor.write_u32::<LittleEndian>(compressed)?;
        cursor.write_u32::<LittleEndian>(original)?;
    }
    reader.read_bytes(&mut tmp[9..])?;

    if h != city_hash_128(&tmp) {
        return Err(raise_error("data was corrupted".to_string()));
    }

    let data = vec![0u8; original as usize];
    let status;
    unsafe {
        status = LZ4_decompress_safe(
            (tmp.as_mut_ptr() as *const i8).add(9),
            data.as_ptr() as *mut i8,
            (compressed - 9) as i32,
            original as i32,
        )
    }

    if status < 0 {
        return Err(raise_error("can't decompress data".to_string()));
    }

    Ok(data)
}

fn raise_error(message: String) -> Error {
    message.into()
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use chrono_tz::Tz;

    use crate::binary::Encoder;
    use crate::types::Block;

    use super::decompress;

    #[test]
    fn test_write_default() {
        let expected = [1_u8, 0, 2, 255, 255, 255, 255, 0, 0, 0];
        let mut encoder = Encoder::new();
        Block::default().write(&mut encoder, false);
        assert_eq!(encoder.get_buffer_ref(), &expected)
    }

    #[test]
    fn test_compress_block() {
        let expected = vec![
            245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0,
            0, 0, 23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116,
            114, 105, 110, 103, 3, 97, 98, 99,
        ];

        let block = Block::new().add_column("s", vec!["abc"]);

        let mut encoder = Encoder::new();
        block.write(&mut encoder, true);

        let actual = encoder.get_buffer();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decompress_block() {
        let expected = Block::new().add_column("s", vec!["abc"]);

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
    fn test_decompress() {
        let expected = vec![
            1u8, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116, 114, 105, 110, 103, 3, 97,
            98, 99,
        ];

        let source = vec![
            245_u8, 5, 222, 235, 225, 158, 59, 108, 225, 31, 65, 215, 66, 66, 36, 92, 130, 34, 0,
            0, 0, 23, 0, 0, 0, 240, 8, 1, 0, 2, 255, 255, 255, 255, 0, 1, 1, 1, 115, 6, 83, 116,
            114, 105, 110, 103, 3, 97, 98, 99,
        ];

        let mut cursor = Cursor::new(&source[..]);
        let actual = decompress(&mut cursor).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_read_empty_block() {
        let source = [1, 0, 2, 255, 255, 255, 255, 0, 0, 0];
        let mut cursor = Cursor::new(&source[..]);
        match Block::load(&mut cursor, Tz::Zulu, false) {
            Ok(block) => assert!(block.is_empty()),
            Err(_) => panic!("test_read_empty_block"),
        }
    }

    #[test]
    fn test_empty() {
        assert!(Block::default().is_empty())
    }

    #[test]
    fn test_column_and_rows() {
        let block = Block::new()
            .add_column("hello_id", vec![5_u32, 6_u32])
            .add_column("value", vec!["lol", "zuz"]);

        assert_eq!(block.column_count(), 2);
        assert_eq!(block.row_count(), 2);
    }

    #[test]
    fn test_concat() {
        let block_a = make_block();
        let block_b = make_block();

        let actual = Block::concat(&vec![block_a, block_b]);
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
        Block::new().add_column(
            "9b96ad8b-488a-4fef-8087-8a9ae4800f00",
            vec![
                "5446d186-4e90-4dd8-8ec1-f9a436834613".to_string(),
                "f7cf31f4-7f37-4e27-91c0-5ac0ad0b145b".to_string(),
            ],
        )
    }

    #[test]
    fn test_chunks() {
        let first = Block::new().add_column("A", vec![1, 2]);
        let second = Block::new().add_column("A", vec![3, 4]);
        let third = Block::new().add_column("A", vec![5]);

        let block = Block::new().add_column("A", vec![1, 2, 3, 4, 5]);
        let mut iter = block.chunks(2);

        assert_eq!(Some(first), iter.next());
        assert_eq!(Some(second), iter.next());
        assert_eq!(Some(third), iter.next());
        assert_eq!(None, iter.next());
    }

    #[test]
    fn test_chunks_of_empty_block() {
        let block = Block::default();
        assert_eq!(1, block.chunks(100500).count());
        assert_eq!(Some(block.clone()), block.chunks(100500).next());
    }

    #[test]
    fn test_rows() {
        let expected = vec![1_u8, 2, 3];
        let block = Block::new().add_column("A", vec![1_u8, 2, 3]);
        let actual: Vec<u8> = block.rows().map(|row| row.get("A").unwrap()).collect();
        assert_eq!(expected, actual);
    }
}
