use std::io;

use crate::{
    errors::{DriverError, Error},
    types::{ClickhouseResult, StatBuffer, Unmarshal, column::StringPool},
};

use std::str;

pub(crate) trait ReadEx {
    fn read_bytes(&mut self, rv: &mut [u8]) -> ClickhouseResult<()>;
    fn read_scalar<V>(&mut self) -> ClickhouseResult<V>
    where
        V: Copy + Unmarshal<V> + StatBuffer;
    fn read_string(&mut self) -> ClickhouseResult<String>;
    fn read_uvarint(&mut self) -> ClickhouseResult<u64>;
    fn read_str_into_buffer(&mut self, pool: &mut StringPool) -> ClickhouseResult<()>;
}

impl<T> ReadEx for T
where
    T: io::Read,
{
    fn read_bytes(&mut self, rv: &mut [u8]) -> ClickhouseResult<()> {
        let mut i = 0;
        while i < rv.len() {
            let res_nread = {
                let buf = &mut rv[i..];
                self.read(buf)
            };
            match res_nread {
                Ok(0) => {
                    let ret = io::Error::new(io::ErrorKind::WouldBlock, "would block");
                    return Err(ret.into());
                }
                Ok(nread) => i += nread,
                Err(e) => return Err(From::from(e)),
            }
        }
        Ok(())
    }

    fn read_scalar<V>(&mut self) -> ClickhouseResult<V>
    where
        V: Copy + Unmarshal<V> + StatBuffer,
    {
        let mut buffer = V::buffer();
        self.read_bytes(buffer.as_mut())?;
        Ok(V::unmarshal(buffer.as_ref()))
    }

    fn read_string(&mut self) -> ClickhouseResult<String> {
        let str_len = self.read_uvarint()? as usize;
        let mut buffer = vec![0_u8; str_len];
        self.read_bytes(buffer.as_mut())?;
        Ok(String::from_utf8(buffer)?)
    }

    fn read_uvarint(&mut self) -> ClickhouseResult<u64> {
        let mut x = 0_u64;
        let mut s = 0_u32;
        let mut i = 0_usize;
        loop {
            let b: u8 = self.read_scalar()?;

            if b < 0x80 {
                if i > 9 || i == 9 && b > 1 {
                    return Err(Error::Driver(DriverError::Overflow));
                }
                return Ok(x | (u64::from(b) << s));
            }

            x |= u64::from(b & 0x7f) << s;
            s += 7;

            i += 1;
        }
    }

    fn read_str_into_buffer(&mut self, pool: &mut StringPool) -> ClickhouseResult<()> {
        let str_len = self.read_uvarint()? as usize;
        let buffer = pool.allocate(str_len);
        self.read_bytes(buffer)?;
        str::from_utf8(buffer)?;
        Ok(())
    }
}

#[test]
fn test_read_uvarint() {
    use super::ReadEx;
    use std::io::Cursor;

    let bytes = [194_u8, 10];
    let mut cursor = Cursor::new(bytes);

    let actual = cursor.read_uvarint().unwrap();

    assert_eq!(actual, 1346)
}
