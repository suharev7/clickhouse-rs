pub trait Marshal {
    fn marshal(&self, scratch: &mut [u8]);
}

macro_rules! int_marshals {
    ( $( $t:ident ),* ) => {
        $(
            impl Marshal for $t {
                fn marshal(&self, scratch: &mut [u8]) {
                    scratch.clone_from_slice(&self.to_le_bytes());
                }
            }
        )*
    };
}

macro_rules! float_marshals {
    ( $( $t:ident ),* ) => {
        $(
            impl Marshal for $t {
                fn marshal(&self, scratch: &mut [u8]) {
                    let bits = self.to_bits();
                    scratch.clone_from_slice(&bits.to_le_bytes());
                }
            }
        )*
    };
}

int_marshals! { u8, u16, u32, u64, u128, i8, i16, i32, i64, i128 }
float_marshals! { f32, f64 }

impl Marshal for bool {
    fn marshal(&self, scratch: &mut [u8]) {
        scratch[0] = *self as u8;
    }
}

#[cfg(test)]
mod test {
    use std::fmt;

    use crate::types::{Marshal, StatBuffer, Unmarshal};
    use rand::distributions::{Distribution, Standard};
    use rand::random;

    fn test_some<T>()
    where
        T: Copy + fmt::Debug + StatBuffer + Marshal + Unmarshal<T> + PartialEq,
        Standard: Distribution<T>,
    {
        for _ in 0..100 {
            let mut buffer = T::buffer();
            let v = random::<T>();

            v.marshal(buffer.as_mut());
            let u = T::unmarshal(buffer.as_ref());

            assert_eq!(v, u);
        }
    }

    #[test]
    fn test_u8() {
        test_some::<u8>()
    }

    #[test]
    fn test_u16() {
        test_some::<u16>()
    }

    #[test]
    fn test_u32() {
        test_some::<u32>()
    }

    #[test]
    fn test_u64() {
        test_some::<u64>()
    }

    #[test]
    fn test_u128() {
        test_some::<u128>()
    }

    #[test]
    fn test_i8() {
        test_some::<i8>()
    }

    #[test]
    fn test_i16() {
        test_some::<i16>()
    }

    #[test]
    fn test_i32() {
        test_some::<i32>()
    }

    #[test]
    fn test_i64() {
        test_some::<i64>()
    }

    #[test]
    fn test_i128() {
        test_some::<i128>()
    }

    #[test]
    fn test_f32() {
        test_some::<f32>()
    }

    #[test]
    fn test_f64() {
        test_some::<f64>()
    }

    #[test]
    fn test_bool() {
        test_some::<bool>()
    }
}
