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
        u16::from(scratch[0]) | u16::from(scratch[1]) << 8
    }
}

impl Unmarshal<u32> for u32 {
    fn unmarshal(scratch: &[u8]) -> u32 {
        u32::from(scratch[0])
            | u32::from(scratch[1]) << 8
            | u32::from(scratch[2]) << 16
            | u32::from(scratch[3]) << 24
    }
}

impl Unmarshal<u64> for u64 {
    fn unmarshal(scratch: &[u8]) -> u64 {
        u64::from(scratch[0])
            | u64::from(scratch[1]) << 8
            | u64::from(scratch[2]) << 16
            | u64::from(scratch[3]) << 24
            | u64::from(scratch[4]) << 32
            | u64::from(scratch[5]) << 40
            | u64::from(scratch[6]) << 48
            | u64::from(scratch[7]) << 56
    }
}

impl Unmarshal<i8> for i8 {
    fn unmarshal(scratch: &[u8]) -> i8 {
        scratch[0] as i8
    }
}

impl Unmarshal<i16> for i16 {
    fn unmarshal(scratch: &[u8]) -> i16 {
        i16::from(scratch[0]) | i16::from(scratch[1]) << 8
    }
}

impl Unmarshal<i32> for i32 {
    fn unmarshal(scratch: &[u8]) -> i32 {
        i32::from(scratch[0])
            | i32::from(scratch[1]) << 8
            | i32::from(scratch[2]) << 16
            | i32::from(scratch[3]) << 24
    }
}

impl Unmarshal<i64> for i64 {
    fn unmarshal(scratch: &[u8]) -> i64 {
        i64::from(scratch[0])
            | i64::from(scratch[1]) << 8
            | i64::from(scratch[2]) << 16
            | i64::from(scratch[3]) << 24
            | i64::from(scratch[4]) << 32
            | i64::from(scratch[5]) << 40
            | i64::from(scratch[6]) << 48
            | i64::from(scratch[7]) << 56
    }
}

impl Unmarshal<f32> for f32 {
    fn unmarshal(scratch: &[u8]) -> f32 {
        let bits = u32::from(scratch[0])
            | u32::from(scratch[1]) << 8
            | u32::from(scratch[2]) << 16
            | u32::from(scratch[3]) << 24;
        f32::from_bits(bits)
    }
}

impl Unmarshal<f64> for f64 {
    fn unmarshal(scratch: &[u8]) -> f64 {
        let bits = u64::from(scratch[0])
            | u64::from(scratch[1]) << 8
            | u64::from(scratch[2]) << 16
            | u64::from(scratch[3]) << 24
            | u64::from(scratch[4]) << 32
            | u64::from(scratch[5]) << 40
            | u64::from(scratch[6]) << 48
            | u64::from(scratch[7]) << 56;
        f64::from_bits(bits)
    }
}

impl Unmarshal<bool> for bool {
    fn unmarshal(scratch: &[u8]) -> bool {
        scratch[0] != 0
    }
}
