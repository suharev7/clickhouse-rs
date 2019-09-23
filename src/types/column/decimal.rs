use chrono_tz::Tz;

use crate::{
    binary::{Encoder, ReadEx},
    errors::Result,
    types::{
        column::{
            BoxColumnWrapper, column_data::BoxColumnData, ColumnFrom, ColumnWrapper, Either,
            list::List, nullable::NullableColumnData, VectorColumnData, column_data::ColumnData
        },
        Column,
        decimal::{Decimal, NoBits},
        from_sql::FromSql, SqlType, Value, ValueRef,
    },
};

pub(crate) struct DecimalColumnData {
    pub(crate) inner: Box<dyn ColumnData + Send + Sync>,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    pub(crate) nobits: NoBits,
}

pub(crate) struct DecimalAdapter {
    pub(crate) column: Column,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    pub(crate) nobits: NoBits,
}

pub(crate) struct NullableDecimalAdapter {
    pub(crate) column: Column,
    pub(crate) precision: u8,
    pub(crate) scale: u8,
    pub(crate) nobits: NoBits,
}

impl DecimalColumnData {
    pub(crate) fn load<T: ReadEx>(
        reader: &mut T,
        precision: u8,
        scale: u8,
        nobits: NoBits,
        size: usize,
        tz: Tz,
    ) -> Result<Self> {
        let type_name = match nobits {
            NoBits::N32 => "Int32",
            NoBits::N64 => "Int64",
        };
        let inner = ColumnData::load_data::<BoxColumnWrapper, _>(reader, type_name, size, tz)?;

        Ok(DecimalColumnData {
            inner,
            precision,
            scale,
            nobits,
        })
    }
}

impl ColumnFrom for Vec<Decimal> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut data = List::<i64>::with_capacity(source.len());
        let mut precision = 18;
        let mut opt_scale = None;
        for s in source {
            precision = std::cmp::max(precision, s.precision);
            if let Some(old_scale) = opt_scale {
                if old_scale != s.scale {
                    panic!("scale != scale")
                }
            } else {
                opt_scale = Some(s.scale);
            }

            data.push(s.internal());
        }
        let scale = opt_scale.unwrap_or(4);
        let inner = Box::new(VectorColumnData { data });

        let column = DecimalColumnData {
            inner,
            precision,
            scale,
            nobits: NoBits::N64,
        };

        W::wrap(column)
    }
}

impl ColumnFrom for Vec<Option<Decimal>> {
    fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
        let mut nulls: Vec<u8> = Vec::with_capacity(source.len());
        let mut data = List::<i64>::with_capacity(source.len());
        let mut precision = 18;
        let mut opt_scale = None;
        for os in source {
            if let Some(s) = os {
                precision = std::cmp::max(precision, s.precision);
                if let Some(old_scale) = opt_scale {
                    if old_scale != s.scale {
                        panic!("scale != scale")
                    }
                } else {
                    opt_scale = Some(s.scale);
                }

                data.push(s.internal());
                nulls.push(0);
            } else {
                data.push(0);
                nulls.push(1);
            }
        }
        let scale = opt_scale.unwrap_or(4);
        let inner = Box::new(VectorColumnData { data });

        let inner = DecimalColumnData {
            inner,
            precision,
            scale,
            nobits: NoBits::N64,
        };

        W::wrap(NullableColumnData {
            nulls,
            inner: Box::new(inner),
        })
    }
}

impl ColumnData for DecimalColumnData {
    fn sql_type(&self) -> SqlType {
        SqlType::Decimal(self.precision, self.scale)
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        self.inner.save(encoder, start, end)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn push(&mut self, value: Value) {
        if let Value::Decimal(decimal) = value {
            match self.nobits {
                NoBits::N32 => {
                    let internal: i32 = decimal.internal();
                    self.inner.push(internal.into())
                }
                NoBits::N64 => {
                    let internal: i64 = decimal.internal();
                    self.inner.push(internal.into())
                }
            }
        } else {
            panic!("value should be decimal ({:?})", value);
        }
    }

    fn at(&self, index: usize) -> ValueRef {
        let underlying: i64 = match self.nobits {
            NoBits::N32 => i64::from(i32::from(self.inner.at(index))),
            NoBits::N64 => i64::from(self.inner.at(index)),
        };

        ValueRef::Decimal(Decimal {
            underlying,
            precision: self.precision,
            scale: self.scale,
            nobits: self.nobits,
        })
    }

    fn clone_instance(&self) -> BoxColumnData {
        Box::new(Self {
            inner: self.inner.clone_instance(),
            precision: self.precision,
            scale: self.scale,
            nobits: self.nobits
        })
    }
}

impl ColumnData for DecimalAdapter {
    fn sql_type(&self) -> SqlType {
        SqlType::Decimal(self.precision, self.scale)
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        for i in start..end {
            if let ValueRef::Decimal(decimal) = self.at(i) {
                match self.nobits {
                    NoBits::N32 => {
                        let internal: i32 = decimal.internal();
                        encoder.write(internal);
                    }
                    NoBits::N64 => {
                        let internal: i64 = decimal.internal();
                        encoder.write(internal);
                    }
                }
            } else {
                panic!("should be decimal");
            }
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        if let ValueRef::Decimal(decimal) = self.column.at(index) {
            let mut d = decimal.set_scale(self.scale);
            d.precision = self.precision;
            d.nobits = self.nobits;
            ValueRef::Decimal(d)
        } else {
            panic!("should be decimal");
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}

impl ColumnData for NullableDecimalAdapter {
    fn sql_type(&self) -> SqlType {
        SqlType::Nullable(SqlType::Decimal(self.precision, self.scale).into())
    }

    fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
        let size = end - start;
        let mut nulls = vec![0; size];
        let mut values: Vec<Option<Decimal>> = vec![None; size];

        for (i, index) in (start..end).enumerate() {
            values[i] = Option::from_sql(self.at(index)).unwrap();
            if values[i].is_none() {
                nulls[i] = 1;
            }
        }

        encoder.write_bytes(nulls.as_ref());

        for value in values {
            let underlying = if let Some(v) = value { v.underlying } else { 0 };

            match self.nobits {
                NoBits::N32 => {
                    encoder.write(underlying as i32);
                }
                NoBits::N64 => {
                    encoder.write(underlying);
                }
            }
        }
    }

    fn len(&self) -> usize {
        self.column.len()
    }

    fn push(&mut self, _: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let value: Option<Decimal> = Option::from_sql(self.column.at(index)).unwrap();
        match value {
            None => ValueRef::Nullable(Either::Left(self.sql_type().into())),
            Some(mut v) => {
                v = v.set_scale(self.scale);
                v.precision = self.precision;
                v.nobits = self.nobits;
                let inner = ValueRef::Decimal(v);
                ValueRef::Nullable(Either::Right(Box::new(inner)))
            }
        }
    }

    fn clone_instance(&self) -> BoxColumnData {
        unimplemented!()
    }
}
