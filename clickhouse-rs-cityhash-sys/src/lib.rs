#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::os::raw::c_char;

#[repr(C)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct UInt128 {
    pub lo: u64,
    pub hi: u64,
}

extern "C" {
    fn CityHash128(s: *const c_char, len: usize) -> UInt128;
}

pub fn city_hash_128<B: AsRef<[u8]>>(source: B) -> UInt128 {
    let buffer = source.as_ref();
    unsafe { CityHash128(buffer.as_ptr() as *const c_char, buffer.len()) }
}

#[test]
fn test_city_hash_128() {
    let expected = UInt128 {
        lo: 0x900ff195577748fe,
        hi: 0x13a9176355b20d7e,
    };
    let actual = city_hash_128("abc");
    assert_eq!(expected, actual)
}
