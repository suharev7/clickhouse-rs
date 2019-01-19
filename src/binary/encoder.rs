use crate::{
    binary,
    types::{DateConverter, Marshal, StatBuffer, Value, ValueRef},
};

const MAX_VARINT_LEN64: usize = 10;

#[derive(Default)]
pub struct Encoder {
    buffer: Vec<u8>,
}

impl Encoder {
    pub fn new() -> Self {
        Encoder { buffer: Vec::new() }
    }

    pub fn uvarint(&mut self, v: u64) {
        let mut scratch = [0u8; MAX_VARINT_LEN64];
        let ln = binary::put_uvarint(&mut scratch[..], v);
        self.write_bytes(&scratch[..ln]);
    }

    pub fn string(&mut self, text: impl AsRef<str>) {
        let str = text.as_ref().as_bytes();
        self.uvarint(str.len() as u64);
        self.write_bytes(str);
    }

    pub fn value(&mut self, value: ValueRef) {
        match value {
            ValueRef::UInt8(v) => self.write(v),
            ValueRef::UInt16(v) => self.write(v),
            ValueRef::UInt32(v) => self.write(v),
            ValueRef::UInt64(v) => self.write(v),
            ValueRef::Int8(v) => self.write(v),
            ValueRef::Int16(v) => self.write(v),
            ValueRef::Int32(v) => self.write(v),
            ValueRef::Int64(v) => self.write(v),
            ValueRef::String(v) => self.string(v),
            ValueRef::Float32(v) => self.write(v),
            ValueRef::Float64(v) => self.write(v),
            ValueRef::Date(_) => self.write(u16::get_stamp(Value::from(value))),
            ValueRef::DateTime(_) => self.write(u32::get_stamp(Value::from(value))),
        }
    }

    pub fn write<T>(&mut self, value: T)
    where
        T: Copy + Marshal + StatBuffer,
    {
        let mut buffer = T::buffer();
        value.marshal(buffer.as_mut());
        self.write_bytes(buffer.as_ref());
    }

    pub fn write_bytes(&mut self, b: &[u8]) {
        self.buffer.extend_from_slice(b);
    }

    pub fn get_buffer(self) -> Vec<u8> {
        self.buffer
    }

    pub fn get_buffer_ref(&self) -> &[u8] {
        self.buffer.as_ref()
    }
}
