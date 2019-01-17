use std::{
    io,
    io::Read,
    os::raw::{c_char, c_int},
};

use byteorder::{LittleEndian, WriteBytesExt};
use clickhouse_rs_cityhash_sys::{city_hash_128, UInt128};
use lz4::liblz4::LZ4_decompress_safe;

use crate::{binary::ReadEx, errors::Error, types::ClickhouseResult};

const DBMS_MAX_COMPRESSED_SIZE: u32 = 0x4000_0000; // 1GB

pub(crate) struct CompressedReader<'a, R> {
    reader: &'a mut R,
    cursor: io::Cursor<Vec<u8>>,
}

pub(crate) fn make<R>(reader: &mut R) -> CompressedReader<R> {
    CompressedReader {
        reader,
        cursor: io::Cursor::new(Vec::new()),
    }
}

impl<'a, R> CompressedReader<'a, R>
where
    R: Read + ReadEx,
{
    fn is_empty(&self) -> bool {
        let len = self.cursor.get_ref().len();
        let pos = self.cursor.position() as usize;
        len == pos
    }

    fn fill(&mut self) -> ClickhouseResult<()> {
        let tmp = decompress_buffer(&mut self.reader)?;
        self.cursor = io::Cursor::new(tmp);
        Ok(())
    }
}

impl<'a, R> Read for CompressedReader<'a, R>
where
    R: Read + ReadEx,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.is_empty() {
            self.fill()?;
        }

        self.cursor.read(buf)
    }
}

fn decompress_buffer<R>(reader: &mut R) -> ClickhouseResult<Vec<u8>>
where
    R: ReadEx,
{
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

    let mut tmp = vec![0_u8; compressed as usize];
    {
        let mut cursor = io::Cursor::new(&mut tmp);
        cursor.write_u8(0x82)?;
        cursor.write_u32::<LittleEndian>(compressed)?;
        cursor.write_u32::<LittleEndian>(original)?;
    }
    reader.read_bytes(&mut tmp[9..])?;

    if h != city_hash_128(&tmp) {
        return Err(raise_error("data was corrupted".to_string()));
    }

    let data = vec![0_u8; original as usize];
    let status = unsafe {
        LZ4_decompress_safe(
            (tmp.as_mut_ptr() as *const c_char).add(9),
            data.as_ptr() as *mut i8,
            (compressed - 9) as c_int,
            original as c_int,
        )
    };

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
    use super::*;

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

        let mut cursor = io::Cursor::new(&source[..]);
        let actual = decompress_buffer(&mut cursor).unwrap();

        assert_eq!(actual, expected);
    }
}
