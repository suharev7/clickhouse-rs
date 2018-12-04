use std::convert;
use std::marker::PhantomData;
use std::mem;

use types::{Marshal, StatBuffer, Unmarshal};

pub struct List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    data: Vec<u8>,
    phantom: PhantomData<T>,
}

impl<T> List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    pub fn len(&self) -> usize {
        self.data.len() / mem::size_of::<T>()
    }

    pub fn at(&self, index: usize) -> T {
        let begin = index * mem::size_of::<T>();
        let end = begin + mem::size_of::<T>();
        let bits = &self.data[begin..end];
        let value = T::unmarshal(bits);
        value.into()
    }

    pub fn push(&mut self, value: T) {
        let mut buffer = T::buffer();
        value.marshal(buffer.as_mut());
        self.data.extend_from_slice(buffer.as_ref());
    }

    #[cfg(test)]
    pub fn new() -> List<T> {
        List {
            data: Vec::new(),
            phantom: PhantomData,
        }
    }

    pub fn with_capacity(capacity: usize) -> List<T> {
        List {
            data: Vec::with_capacity(capacity),
            phantom: PhantomData,
        }
    }
}

impl<T> convert::From<Vec<u8>> for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    fn from(data: Vec<u8>) -> List<T> {
        List {
            data,
            phantom: PhantomData,
        }
    }
}

impl<T> AsRef<[u8]> for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[cfg(test)]
mod test {
    use rand::random;

    use super::*;

    #[test]
    fn test_push_and_len() {
        let mut list = List::with_capacity(100500);

        for i in 0..100500 {
            assert_eq!(list.len(), i as usize);
            list.push(i);
        }
    }

    #[test]
    fn test_push_and_get() {
        let mut list = List::<f64>::new();
        let mut vs = Vec::<f64>::new();

        for _ in 0..100 {
            assert_eq!(list.len(), vs.len());

            for i in 0..vs.len() {
                assert_eq!(list.at(i), vs[i]);
            }

            let k = random();
            list.push(k);
            vs.push(k);
        }
    }
}
