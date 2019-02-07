use std::str::from_utf8_unchecked;
use std::io::Write;

const AVG_STR_SIZE: usize = 80;

#[derive(Copy, Clone)]
struct StringPtr {
    chunk: usize,
    shift: usize,
    len: usize,
}

pub(crate) struct StringPool {
    chunks: Vec<Vec<u8>>,
    pointers: Vec<StringPtr>,
    position: usize,
    capacity: usize,
}

pub(crate) struct StringIter<'a> {
    pool: &'a StringPool,
    index: usize,
}

impl<'a> Iterator for StringIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.pool.len() {
            let result = self.pool.get(self.index);
            self.index += 1;
            return Some(result);
        }

        None
    }
}

impl From<Vec<String>> for StringPool {
    fn from(source: Vec<String>) -> Self {
        let mut pool = StringPool::with_capacity(source.len());
        for s in source.iter() {
            let mut b = pool.allocate(s.len());
            b.write(s.as_bytes()).unwrap();
        }
        pool
    }
}

impl StringPool {
    pub(crate) fn with_capacity(capacity: usize) -> StringPool {
        StringPool {
            pointers: Vec::with_capacity(capacity),
            chunks: Vec::new(),
            position: 0,
            capacity,
        }
    }

    pub(crate) fn allocate(&mut self, size: usize) -> &mut [u8] {
        if self.free_space() < size || self.chunks.is_empty() {
            self.reserve(size);
            return self.allocate(size);
        }

        self.try_allocate(size).unwrap()
    }

    fn free_space(&self) -> usize {
        if let Some(buffer) = self.chunks.last() {
            return buffer.len() - self.position;
        }

        0
    }

    fn try_allocate(&mut self, size: usize) -> Option<&mut [u8]> {
        if self.chunks.len() > 0 {
            let chunk = self.chunks.len() - 1;

            let position = self.position;
            self.position += size;
            self.pointers.push(StringPtr {
                len: size,
                shift: position,
                chunk,
            });

            let buffer = &mut self.chunks[chunk];
            return Some(&mut buffer[position..position + size]);
        }

        None
    }

    fn reserve(&mut self, size: usize) {
        use std::cmp::max;
        self.position = 0;
        self.chunks
            .push(vec![0_u8; max(self.capacity * AVG_STR_SIZE, size)]);
    }

    pub(crate) fn get(&self, index: usize) -> &str {
        let pointer = self.pointers[index];
        let chunk = &self.chunks[pointer.chunk];
        let raw = &chunk[pointer.shift..pointer.shift + pointer.len];
        unsafe { from_utf8_unchecked(raw) }
    }

    pub(crate) fn len(&self) -> usize {
        self.pointers.len()
    }

    pub(crate) fn strings(&self) -> StringIter {
        StringIter {
            pool: self,
            index: 0,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_allocate() {
        let mut pool = StringPool::with_capacity(10);
        for i in 1..1000 {
            let buffer = pool.allocate(i);
            assert_eq!(buffer.len(), i);
            assert_eq!(buffer[0], 0);
            buffer[0] = 1
        }
    }

    #[test]
    fn test_get() {
        let mut pool = StringPool::with_capacity(10);

        for i in 0..1000 {
            let s = format!("text-{}", i);
            let mut buffer = pool.allocate(s.len());

            assert_eq!(buffer.len(), s.len());
            buffer.write(s.as_bytes()).unwrap();
        }

        for i in 0..1000 {
            let s = pool.get(i).to_string();
            assert_eq!(s, format!("text-{}", i));
        }
    }
}
