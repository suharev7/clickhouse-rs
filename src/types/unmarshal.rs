pub trait Unmarshal<T: Copy> {
    fn unmarshal(scratch: &[u8]) -> T;
}

impl Unmarshal<u8> for u8 {
    fn unmarshal(scratch: &[u8]) -> u8 {
        scratch[0]
    }
}

impl Unmarshal<u16> for u16 {
    fn unmarshal(scratch: &[u8]) -> u16 {
        (scratch[0] as u16) | (scratch[1] as u16) << 8
    }
}

impl Unmarshal<u32> for u32 {
    fn unmarshal(scratch: &[u8]) -> u32 {
        (scratch[0] as u32)
            | (scratch[1] as u32) << 8
            | (scratch[2] as u32) << 16
            | (scratch[3] as u32) << 24
    }
}

impl Unmarshal<u64> for u64 {
    fn unmarshal(scratch: &[u8]) -> u64 {
        (scratch[0] as u64)
            | (scratch[1] as u64) << 8
            | (scratch[2] as u64) << 16
            | (scratch[3] as u64) << 24
            | (scratch[4] as u64) << 32
            | (scratch[5] as u64) << 40
            | (scratch[6] as u64) << 48
            | (scratch[7] as u64) << 56
    }
}

impl Unmarshal<i8> for i8 {
    fn unmarshal(scratch: &[u8]) -> i8 {
        scratch[0] as i8
    }
}

impl Unmarshal<i16> for i16 {
    fn unmarshal(scratch: &[u8]) -> i16 {
        (scratch[0] as i16) | (scratch[1] as i16) << 8
    }
}

impl Unmarshal<i32> for i32 {
    fn unmarshal(scratch: &[u8]) -> i32 {
        (scratch[0] as i32)
            | (scratch[1] as i32) << 8
            | (scratch[2] as i32) << 16
            | (scratch[3] as i32) << 24
    }
}

impl Unmarshal<i64> for i64 {
    fn unmarshal(scratch: &[u8]) -> i64 {
        (scratch[0] as i64)
            | (scratch[1] as i64) << 8
            | (scratch[2] as i64) << 16
            | (scratch[3] as i64) << 24
            | (scratch[4] as i64) << 32
            | (scratch[5] as i64) << 40
            | (scratch[6] as i64) << 48
            | (scratch[7] as i64) << 56
    }
}

impl Unmarshal<f32> for f32 {
    fn unmarshal(scratch: &[u8]) -> f32 {
        let bits = (scratch[0] as u32)
            | (scratch[1] as u32) << 8
            | (scratch[2] as u32) << 16
            | (scratch[3] as u32) << 24;
        f32::from_bits(bits)
    }
}

impl Unmarshal<f64> for f64 {
    fn unmarshal(scratch: &[u8]) -> f64 {
        let bits = (scratch[0] as u64)
            | (scratch[1] as u64) << 8
            | (scratch[2] as u64) << 16
            | (scratch[3] as u64) << 24
            | (scratch[4] as u64) << 32
            | (scratch[5] as u64) << 40
            | (scratch[6] as u64) << 48
            | (scratch[7] as u64) << 56;
        f64::from_bits(bits)
    }
}

impl Unmarshal<bool> for bool {
    fn unmarshal(scratch: &[u8]) -> bool {
        scratch[0] != 0
    }
}
