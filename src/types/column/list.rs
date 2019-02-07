use std::{mem, slice};

use crate::types::{Marshal, StatBuffer, Unmarshal};

pub struct List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    data: Vec<T>,
}

impl<T> List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn at(&self, index: usize) -> T {
        self.data[index]
    }

    pub fn push(&mut self, value: T) {
        self.data.push(value);
    }

    #[cfg(test)]
    pub fn new() -> List<T> {
        List { data: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> List<T> {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn resize(&mut self, new_len: usize, value: T) {
        self.data.resize(new_len, value);
    }
}

impl<T> AsRef<[u8]> for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    fn as_ref(&self) -> &[u8] {
        let ptr = self.data.as_ptr() as *const u8;
        let size = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts(ptr, size) }
    }
}

impl<T> AsMut<[u8]> for List<T>
where
    T: StatBuffer + Unmarshal<T> + Marshal + Copy + Sync + 'static,
{
    fn as_mut(&mut self) -> &mut [u8] {
        let ptr = self.data.as_mut_ptr() as *mut u8;
        let size = self.len() * mem::size_of::<T>();
        unsafe { slice::from_raw_parts_mut(ptr, size) }
    }
}

#[cfg(test)]
mod test {
    use rand::random;

    use super::*;
    use std::f64::EPSILON;

    #[test]
    fn test_push_and_len() {
        let mut list = List::with_capacity(100_500);

        for i in 0..100_500 {
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

            for (i, v) in vs.iter().enumerate() {
                assert!((list.at(i) - *v).abs() < EPSILON);
            }

            let k = random();
            list.push(k);
            vs.push(k);
        }
    }
}
