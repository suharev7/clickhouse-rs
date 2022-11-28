use crate::{
    binary::{Encoder, ReadEx},
    errors::{DriverError, Error, Result},
};

const BLOCK_INFO_OVERFLOWS: u64 = 1;
const BLOCK_INFO_BUCKET_NUM: u64 = 2;
const END_FIELD: u64 = 0;

#[allow(dead_code)]
#[derive(Copy, Clone)]
pub struct BlockInfo {
    is_overflows: bool,
    bucket_num: i32,
}

impl Default for BlockInfo {
    fn default() -> Self {
        Self {
            is_overflows: false,
            bucket_num: -1,
        }
    }
}

impl BlockInfo {
    pub(crate) fn read<R: ReadEx>(reader: &mut R) -> Result<Self> {
        let mut block_info = BlockInfo::default();

        loop {
            match reader.read_uvarint()? {
                BLOCK_INFO_OVERFLOWS => block_info.is_overflows = reader.read_scalar()?,
                BLOCK_INFO_BUCKET_NUM => block_info.bucket_num = reader.read_scalar()?,
                END_FIELD => break,
                i => {
                    return Err(Error::Driver(DriverError::UnknownPacket { packet: i }));
                }
            }
        }

        Ok(block_info)
    }

    pub fn write(&self, encoder: &mut Encoder) {
        encoder.uvarint(BLOCK_INFO_OVERFLOWS);
        encoder.write(self.is_overflows);
        encoder.uvarint(BLOCK_INFO_BUCKET_NUM);
        encoder.write(self.bucket_num);
        encoder.uvarint(END_FIELD);
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn test_read_write_blockinfo() {
        let b1 = BlockInfo {
            is_overflows: true,
            bucket_num: 3,
        };

        let mut enc = Encoder::new();
        b1.write(&mut enc);

        let mut buf = Cursor::new(enc.get_buffer());
        let b2 = BlockInfo::read(&mut buf).expect("Failed to deserialize BlockInfo");

        assert_eq!(b1.is_overflows, b2.is_overflows);
        assert_eq!(b1.bucket_num, b2.bucket_num);
    }
}
