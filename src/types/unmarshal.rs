pub trait Unmarshal<T: Copy> {
    fn unmarshal(scratch: &[u8]) -> T;
}

macro_rules! int_unmarshals {
    ( $( $t:ident ),* ) => {
        $(
            impl Unmarshal<$t> for $t {
                fn unmarshal(scratch: &[u8]) -> Self {
                    let mut buffer = [0_u8; std::mem::size_of::<Self>()];
                    buffer.clone_from_slice(scratch);
                    Self::from_le_bytes(buffer)
                }
            }
        )*
    };
}

macro_rules! float_unmarshals {
    ( $( $t:ident: $b:ident ),* ) => {
        $(
            impl Unmarshal<$t> for $t {
                fn unmarshal(scratch: &[u8]) -> Self {
                    let mut buffer = [0_u8; std::mem::size_of::<Self>()];
                    buffer.clone_from_slice(scratch);
                    let bits = $b::from_le_bytes(buffer);
                    Self::from_bits(bits)
                }
            }
        )*
    };
}

int_unmarshals! { u8, u16, u32, u64, u128, i8, i16, i32, i64, i128 }
float_unmarshals! { f32: u32, f64: u64 }

impl Unmarshal<bool> for bool {
    fn unmarshal(scratch: &[u8]) -> Self {
        scratch[0] != 0
    }
}
