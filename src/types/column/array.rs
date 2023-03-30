use std::sync::Arc;

use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            column_data::{ArcColumnData, BoxColumnData},
            list::List,
            ArcColumnWrapper, ColumnData,
        },
        SqlType, Value, ValueRef,
    },
};

pub(crate) struct ArrayColumnData {
    pub(crate) inner: ArcColumnData,
    pub(crate) offsets: List<u64>,
}

impl ArrayColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        type_name: &str,
        rows: usize,
        tz: Tz,
    ) -> Result<Self> {
        let mut offsets = List::with_capacity(rows);
        offsets.resize(rows, 0_u64);
        reader.read_bytes(offsets.as_mut())?;

        let size = match rows {
            0 => 0,
            _ => offsets.at(rows - 1) as usize,
        };
        let inner =
            <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, type_name, size, tz)?;

        Ok(ArrayColumnData { inner, offsets })
    }
}

impl ColumnData for ArrayColumnData {
    fn sql_type(&self) -> SqlType {
        let inner_type = self.inner.sql_type();
        SqlType::Array(inner_type.into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let mut offset = 0_u64;

        for i in start..end {
            offset = self.offsets.at(i);
            encoder.write(offset);
        }

        self.inner.save(encoder, 0, offset as usize);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Array(_, vs) = value {
            let offsets_len = self.offsets.len();
            let prev = if offsets_len == 0 {
                0_usize
            } else {
                self.offsets.at(offsets_len - 1) as usize
            };

            let inner_column = Arc::get_mut(&mut self.inner).unwrap();
            self.offsets.push((prev + vs.len()) as u64);
            for v in vs.iter() {
                inner_column.push(v.clone());
            }
        } else {
            panic!("value should be an array")
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let sql_type = self.inner.sql_type();

        let start = if index > 0 {
            self.offsets.at(index - 1) as usize
        } else {
            0_usize
        };
        let end = self.offsets.at(index) as usize;
        let mut vs = Vec::with_capacity(end);
        for i in start..end {
            let v = self.inner.at(i);
            vs.push(v);
        }
        ValueRef::Array(sql_type.into(), Arc::new(vs))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone(),
            offsets: self.offsets.clone(),
        })
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        props: u32,
    ) -> Result<()> {
        if level == self.sql_type().level() {
            *pointers[0] = self.offsets.as_ptr() as *const u8;
            *(pointers[1] as *mut usize) = self.offsets.len();
            Ok(())
        } else {
            self.inner.get_internal(pointers, level, props)
        }
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if let SqlType::Array(inner_target) = target {
            if let Some(inner) = self.inner.cast_to(&self.inner, inner_target) {
                return Some(Arc::new(ArrayColumnData {
                    inner,
                    offsets: self.offsets.clone(),
                }));
            }
        }
        None
    }
}

#[cfg(test)]
mod test {
    use std::io::Cursor;

    use super::*;
    use crate::{types::Simple, Block};

    #[test]
    fn test_write_and_read() {
        let block = Block::<Simple>::new().column(
            "vals",
            vec![vec![7_u32, 8], vec![9, 1, 2], vec![3, 4, 5, 6]],
        );

        let mut encoder = Encoder::new();
        block.write(&mut encoder, false);

        let mut reader = Cursor::new(encoder.get_buffer_ref());
        let rblock = Block::load(&mut reader, Tz::Zulu, false).unwrap();

        assert_eq!(block, rblock);
    }
}
