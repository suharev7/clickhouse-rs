use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            column_data::{ArcColumnData, BoxColumnData},
            list::List,
            ArcColumnWrapper, ColumnData, ColumnFrom, ColumnWrapper,
        },
        HasSqlType, Marshal, SqlType, StatBuffer, Unmarshal, Value, ValueRef,
    },
};
use chrono_tz::Tz;
use std::{collections::HashMap, sync::Arc};

pub(crate) struct MapColumnData {
    pub(crate) keys: ArcColumnData,
    pub(crate) values: ArcColumnData,
    pub(crate) offsets: List<u64>,
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
        let mut vs = HashMap::with_capacity(end - start);
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
            size: self.size,
        })
    }

    unsafe fn get_internal(
        &self,
        pointers: &[*mut *const u8],
        level: u8,
        props: u32,
    ) -> Result<()> {
        if props == 1 {
            self.keys.get_internal(pointers, level, 0)
        } else if level == self.sql_type().level() {
            *pointers[0] = self.offsets.as_ptr() as *const u8;
            *(pointers[1] as *mut usize) = self.offsets.len();
            Ok(())
        } else {
            let new_props = match props {
                0 => 0,
                _ => 1 | (((props >> 1) - 1) << 1),
            };
            self.values.get_internal(pointers, level, new_props)
        }
    }

    fn cast_to(&self, _this: &ArcColumnData, target: &SqlType) -> Option<ArcColumnData> {
        if let SqlType::Map(key, value) = target {
            let keys = self.keys.cast_to(&self.keys, key)?;
            let values = self.values.cast_to(&self.values, value)?;
            Some(Arc::new(Self {
                keys,
                values,
                offsets: self.offsets.clone(),
                size: self.size,
            }))
        } else {
            None
        }
    }

    fn get_timezone(&self) -> Option<Tz> {
        self.values.get_timezone()
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
    use super::*;
    use crate::{
        row,
        types::{column::datetime64::DEFAULT_TZ, Simple},
        Block,
    };
    use std::{collections::HashMap, io::Cursor};

    #[test]
    fn test_write_and_read() {
        let source = HashMap::from([("test".to_string(), 4_u8), ("foo".to_string(), 5)]);

        let block = Block::<Simple>::new().column("vals", vec![source]);

        let mut encoder = Encoder::new();
        block.write(&mut encoder, false);

        let mut reader = Cursor::new(encoder.get_buffer_ref());
        let rblock = Block::load(&mut reader, *DEFAULT_TZ, false).unwrap();

        assert_eq!(block, rblock);
    }

    #[test]
    fn test_double_iteration() {
        let value = Value::from(HashMap::from([(1_u8, 2_u8)]));

        let expected = vec![HashMap::from([(&1_u8, &2_u8)])];

        let mut block = Block::<Simple>::new();
        block.push(row! {vals: value}).unwrap();

        for _ in 0..2 {
            let actual = block
                .get_column("vals")
                .unwrap()
                .iter::<HashMap<u8, u8>>()
                .unwrap()
                .collect::<Vec<_>>();

            assert_eq!(&actual, &expected);
        }
    }

    #[test]
    fn test_iteration_simple_map() {
        let value = Value::from(HashMap::from([
            ("A".to_string(), 1_u8),
            ("B".to_string(), 2_u8),
            ("C".to_string(), 3_u8),
        ]));

        let mut block = Block::<Simple>::new();
        block.push(row! {vals: value}).unwrap();

        let actual = block
            .get_column("vals")
            .unwrap()
            .iter::<HashMap<&[u8], u8>>()
            .unwrap()
            .collect::<Vec<_>>();

        let expected = vec![HashMap::from([
            ("A".as_bytes(), &1_u8),
            ("B".as_bytes(), &2_u8),
            ("C".as_bytes(), &3_u8),
        ])];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_iteration_included_map() {
        let value = Value::from(HashMap::from([
            (10_u16, HashMap::from([(100_u16, 3000_u16)])),
            (20_u16, HashMap::from([(800_u16, 5000_u16), (900, 6000)])),
        ]));

        let mut block = Block::<Simple>::new();
        block.push(row! {vals: value}).unwrap();

        let actual = block
            .get_column("vals")
            .unwrap()
            .iter::<HashMap<u16, HashMap<u16, u16>>>()
            .unwrap()
            .collect::<Vec<_>>();

        let expected = vec![HashMap::from([
            (&10_u16, HashMap::from([(&100_u16, &3000_u16)])),
            (
                &20_u16,
                HashMap::from([(&800_u16, &5000_u16), (&900, &6000)]),
            ),
        ])];

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_iteration_optional_maps() {
        let first_value = Value::from(Some(HashMap::from([(1_u32, 2_u32)])));
        let second_value = Value::from(None as Option<HashMap<u32, u32>>);

        let mut block = Block::<Simple>::new();
        block.push(row! {vals: first_value}).unwrap();
        block.push(row! {vals: second_value}).unwrap();

        let actual = block
            .get_column("vals")
            .unwrap()
            .iter::<Option<HashMap<u32, u32>>>()
            .unwrap()
            .collect::<Vec<_>>();

        let expected = vec![Some(HashMap::from([(&1_u8, &2_u8)])), None];

        assert_eq!(format!("{expected:?}"), format!("{actual:?}"));
    }

    #[test]
    fn test_iterate_map_with_optioanl_value() {
        let key = Value::UInt32(1);
        let value = Value::from(Some(2_u32));

        let source = Value::Map(
            SqlType::from(key.clone()).into(),
            SqlType::from(value.clone()).into(),
            Arc::new(HashMap::from([(key, value)])),
        );

        let mut block = Block::<Simple>::new();
        block.push(row! {vals: source}).unwrap();

        let actual = block
            .get_column("vals")
            .unwrap()
            .iter::<HashMap<u32, Option<u32>>>()
            .unwrap()
            .collect::<Vec<_>>();

        let expected = vec![HashMap::from([(&1_u32, &Some(2_u32))])];

        assert_eq!(format!("{actual:?}"), format!("{expected:?}"));
    }

    #[test]
    fn test_get_optional_map_cells() {
        let first_value = Value::from(Some(HashMap::from([(1_u32, 2_u32)])));
        let second_value = Value::from(None::<HashMap<u32, u32>>);

        let mut block = Block::<Simple>::new();
        block.push(row! {vals: first_value}).unwrap();
        block.push(row! {vals: second_value}).unwrap();

        let mut actual = Vec::<Option<HashMap<u32, u32>>>::new();
        for i in 0..block.row_count() {
            actual.push(block.get(i, 0).unwrap());
        }

        let expected = vec![Some(HashMap::from([(&1_u8, &2_u8)])), None];

        assert_eq!(format!("{actual:?}"), format!("{expected:?}"),);
    }
}
