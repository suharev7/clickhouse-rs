use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use chrono_tz::Tz;

use crate::types::column::{ColumnFrom, ColumnWrapper};
use crate::types::{HasSqlType, Marshal, StatBuffer, Unmarshal};
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

pub(crate) struct MapColumnData {
    pub(crate) keys: ArcColumnData,
    pub(crate) values: ArcColumnData,
    pub(crate) offsets: List<u64>,
    ///this is used in self.get_internal()
    ///first the keys are read until counter is equal to size
    ///then we switch to the values
    pub(crate) counter: Arc<RwLock<usize>>,
    pub(crate) size: usize,
}

impl MapColumnData {
    pub(crate) fn load<R: ReadEx>(
        reader: &mut R,
        types: (&str, &str),
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

        let keys = <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, types.0, size, tz)?;
        let values = <dyn ColumnData>::load_data::<ArcColumnWrapper, _>(reader, types.1, size, tz)?;

        Ok(Self {
            keys,
            values,
            offsets,
            counter: Arc::new(RwLock::new(0)),
            size,
        })
    }
}

impl ColumnData for MapColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Map(self.keys.sql_type().into(), self.values.sql_type().into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let mut offset = 0_u64;

        for i in start..end {
            offset = self.offsets.at(i);
            encoder.write(offset);
        }

        self.keys.save(encoder, 0, offset as usize);
        self.values.save(encoder, 0, offset as usize);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Map(_, _, vs) = value {
            let offsets_len = self.offsets.len();
            let prev = if offsets_len == 0 {
                0_usize
            } else {
                self.offsets.at(offsets_len - 1) as usize
            };

            let key_column = Arc::get_mut(&mut self.keys).unwrap();
            let value_column = Arc::get_mut(&mut self.values).unwrap();

            self.offsets.push((prev + vs.len()) as u64);
            for (k, v) in vs.iter() {
                key_column.push(k.clone());
                value_column.push(v.clone());
            }
        } else {
            panic!("value should be a map")
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let key_type = self.keys.sql_type();
        let value_type = self.values.sql_type();

        let start = if index > 0 {
            self.offsets.at(index - 1) as usize
        } else {
            0_usize
        };
        let end = self.offsets.at(index) as usize;
        let mut vs = HashMap::with_capacity(end);
        for i in start..end {
            let key = self.keys.at(i);
            let value = self.values.at(i);
            vs.insert(key, value);
        }
        ValueRef::Map(key_type.into(), value_type.into(), Arc::new(vs))
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            keys: self.keys.clone(),
            values: self.values.clone(),
            offsets: self.offsets.clone(),
            counter: self.counter.clone(),
            size: self.size,
        })
    }

    unsafe fn get_internal(&self, pointers: &[*mut *const u8], level: u8) -> Result<()> {
        let mut counter = self.counter.write().unwrap();

        if level == self.sql_type().level() {
            *pointers[0] = self.offsets.as_ptr() as *const u8;
            *(pointers[1] as *mut usize) = self.offsets.len();
            Ok(())
        } else if *counter >= self.size {
            self.values.get_internal(pointers, level)
        } else {
            *counter += 1;
            self.keys.get_internal(pointers, level)
        }
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if let SqlType::Map(key, value) = target {
            if let Some(inner) = self.keys.cast_to(&self.keys, key) {
                let keys = inner;

                if let Some(inner_values) = self.values.cast_to(&self.values, value) {
                    let values = inner_values;

                    return Some(Arc::new(Self {
                        keys,
                        values,
                        offsets: self.offsets.clone(),
                        counter: self.counter.clone(),
                        size: self.size,
                    }));
                }
            }
        }
        None
    }
}

impl<V> ColumnFrom for Vec<HashMap<String, V>>
where
    V: Copy
        + From<Value>
        + HasSqlType
        + Marshal
        + StatBuffer
        + Unmarshal<V>
        + Sync
        + Send
        + 'static,
    Value: From<V>,
{
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake_keys: Vec<String> = Vec::with_capacity(source.len());
        let fake_values: Vec<V> = Vec::with_capacity(source.len());

        let keys = Vec::column_from::<ArcColumnWrapper>(fake_keys);
        let key_type = keys.sql_type();

        let values = Vec::column_from::<ArcColumnWrapper>(fake_values);
        let value_type = values.sql_type();

        let mut data = MapColumnData {
            keys,
            values,
            offsets: List::with_capacity(source.len()),
            counter: Arc::new(RwLock::new(0)),
            size: source.len(),
        };

        for array in source {
            data.push(to_string_map(key_type.clone(), value_type.clone(), array));
        }

        W::wrap(data)
    }
}

impl<K, V> ColumnFrom for Vec<HashMap<K, V>>
where
    K: Copy
        + From<Value>
        + Marshal
        + HasSqlType
        + StatBuffer
        + Unmarshal<K>
        + Sync
        + Send
        + 'static,
    Value: From<K>,
    V: Copy
        + From<Value>
        + HasSqlType
        + Marshal
        + StatBuffer
        + Unmarshal<V>
        + Sync
        + Send
        + 'static,
    Value: From<V>,
{
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let fake_keys: Vec<K> = Vec::with_capacity(source.len());
        let fake_values: Vec<V> = Vec::with_capacity(source.len());

        let keys = Vec::column_from::<ArcColumnWrapper>(fake_keys);
        let key_type = keys.sql_type();

        let values = Vec::column_from::<ArcColumnWrapper>(fake_values);
        let value_type = values.sql_type();

        let mut data = MapColumnData {
            keys,
            values,
            offsets: List::with_capacity(source.len()),
            counter: Arc::new(RwLock::new(0)),
            size: source.len(),
        };

        for array in source {
            data.push(to_map(key_type.clone(), value_type.clone(), array));
        }

        W::wrap(data)
    }
}

fn to_string_map<V>(keys_type: SqlType, values_type: SqlType, vs: HashMap<String, V>) -> Value
where
    Value: From<V>,
{
    let mut inner = HashMap::with_capacity(vs.len());
    for (k, v) in vs {
        let key: Value = k.into();
        let value: Value = v.into();
        inner.insert(key, value);
    }
    Value::Map(keys_type.into(), values_type.into(), Arc::new(inner))
}

fn to_map<K, V>(keys_type: SqlType, values_type: SqlType, vs: HashMap<K, V>) -> Value
where
    Value: From<K>,
    Value: From<V>,
{
    let mut inner = HashMap::with_capacity(vs.len());
    for (k, v) in vs {
        let key: Value = k.into();
        let value: Value = v.into();
        inner.insert(key, value);
    }
    Value::Map(keys_type.into(), values_type.into(), Arc::new(inner))
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use std::io::Cursor;

    use super::*;
    use crate::{types::Simple, Block};

    #[test]
    fn test_write_and_read() {
        let mut a = HashMap::new();
        a.insert("test".to_string(), 4_u8);
        a.insert("foo".to_string(), 5);

        let block = Block::<Simple>::new().column("vals", vec![a]);

        let mut encoder = Encoder::new();
        block.write(&mut encoder, false);

        let mut reader = Cursor::new(encoder.get_buffer_ref());
        let rblock = Block::load(&mut reader, Tz::Zulu, false).unwrap();

        assert_eq!(block, rblock);
    }
}
