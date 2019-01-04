use crate::binary::{Encoder, ReadEx};
use crate::types::ClickhouseResult;

#[allow(dead_code)]
#[derive(Copy, Clone)]
pub struct BlockInfo {
    num1: u64,
    is_overflows: bool,
    num2: u64,
    bucket_num: i32,
    num3: u64,
}

impl Default for BlockInfo {
    fn default() -> Self {
        BlockInfo {
            num1: 0,
            is_overflows: false,
            num2: 0,
            bucket_num: -1,
            num3: 0,
        }
    }
}

impl BlockInfo {
    pub fn read<R: ReadEx>(reader: &mut R) -> ClickhouseResult<BlockInfo> {
        let block_info = BlockInfo {
            num1: reader.read_uvarint()?,
            is_overflows: reader.read_scalar()?,
            num2: reader.read_uvarint()?,
            bucket_num: reader.read_scalar()?,
            num3: reader.read_uvarint()?,
        };
        Ok(block_info)
    }

    pub fn write(&self, encoder: &mut Encoder) {
        encoder.uvarint(1);
        encoder.write(self.is_overflows);
        encoder.uvarint(2);
        encoder.write(self.bucket_num);
        encoder.uvarint(0);
    }
}
