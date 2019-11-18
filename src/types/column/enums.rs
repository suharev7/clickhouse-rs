use chrono_tz::Tz;

use crate::{
	binary::{Encoder, ReadEx},
	errors::Result,
	types::{
		column::{
			BoxColumnWrapper, column_data::BoxColumnData, column_data::ColumnData,
			ColumnFrom, ColumnWrapper, Either, list::List, nullable::NullableColumnData,
			VectorColumnData,
		},
		Column,
		ColumnType,
		decimal::{Decimal, NoBits},
		from_sql::FromSql, SqlType, Value, ValueRef,
	},
};
use crate::types::column::decimal::DecimalColumnData;
use crate::types::column::numeric::save_data;
use crate::types::enums::Enum8;

pub(crate) struct Enum8ColumnData {
	pub(crate) data: List<i8>,
}

pub(crate) struct Enum8Adapter<K: ColumnType> {
	pub(crate) column: Column<K>,
	pub(crate) data: List<i8>,
}

pub(crate) struct NullableEnum8Adapter<K: ColumnType> {
	pub(crate) column: Column<K>,
	pub(crate) data: i8,
}

impl Enum8ColumnData {
	pub(crate) fn load<T: ReadEx>(
		reader: &mut T,
		data: List<i8>,
		size: usize,
		tz: Tz,
	) -> Result<Self> {
		let type_name = "Enum8";
		let _inner = ColumnData::load_data::<BoxColumnWrapper, _>(reader, type_name, size, tz)?;

		Ok(Enum8ColumnData {
			data,
		})
	}
}

impl ColumnData for Enum8ColumnData {
	fn sql_type(&self) -> SqlType {
		SqlType::Enum8
	}

	fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
		save_data::<i8>(self.data.as_ref(), encoder, start, end);
	}

	fn len(&self) -> usize {
		self.data.len()
	}

	fn push(&mut self, value: Value) {
		if let Value::Enum8(enum8) = value {
			self.data.push(enum8.0);
		} else {
			panic!("value should be enum8 ({:?})", value);
		}
	}

	fn at(&self, index: usize) -> ValueRef {
		let value = i8::from(self.data.at(index));
		ValueRef::Enum8(Enum8(value))
	}

	fn clone_instance(&self) -> BoxColumnData {
		Box::new(Self {
			data: self.data.clone(),
		})
	}

	unsafe fn get_internal(&self, _pointers: &[*mut *const u8], _level: u8) -> Result<()> {
// TODO
//        assert_eq!(level, 0);
//        self.inner.get_internal(pointers, 0)?;
//        *(pointers[2] as *mut NoBits) = self.nobits;
		Ok(())
	}
}

impl<K: ColumnType> ColumnData for Enum8Adapter<K> {
	fn sql_type(&self) -> SqlType {
		SqlType::Enum8
	}

	fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
		for i in start..end {
			if let ValueRef::Enum8(value) = self.at(i) {
				encoder.write(value.0);
			} else {
				panic!("should be enum8");
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
		if let ValueRef::Enum8(value) = self.column.at(index) {
			ValueRef::Enum8(value)
		} else {
			panic!("should be enum8");
		}
	}

	fn clone_instance(&self) -> BoxColumnData {
		unimplemented!()
	}
}

impl<K: ColumnType> ColumnData for NullableEnum8Adapter<K> {
	fn sql_type(&self) -> SqlType {
		SqlType::Nullable(SqlType::Enum8.into())
	}

	fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
		let size = end - start;
		let mut nulls = vec![0; size];
		let mut values: Vec<Option<i8>> = vec![None; size];
		// TODO Check this code
		for (i, index) in (start..end).enumerate() {
			values[i] = Option::from_sql(self.at(index)).unwrap();
			if values[i].is_none() {
				nulls[i] = 1;
			}
		}

		encoder.write_bytes(nulls.as_ref());

		for value in values {
			let underlying = if let Some(v) = value { v } else { 0 };
			encoder.write(underlying);
		}
	}

	fn len(&self) -> usize {
		self.column.len()
	}

	fn push(&mut self, _: Value) {
		unimplemented!()
	}

	fn at(&self, index: usize) -> ValueRef {
		let value: Option<i8> = Option::from_sql(self.column.at(index)).unwrap();
		match value {
			None => ValueRef::Nullable(Either::Left(self.sql_type().into())),
			Some(v) => {
				let inner = ValueRef::Enum8(Enum8(v));
				ValueRef::Nullable(Either::Right(Box::new(inner)))
			}
		}
	}

	fn clone_instance(&self) -> BoxColumnData {
		unimplemented!()
	}
}

impl ColumnFrom for Vec<Enum8> {
	fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
		let mut data = List::<i8>::with_capacity(source.len());

		for s in source {
			data.push(s.0);
		}
//		let inner = Box::new(VectorColumnData { data });

		let column = Enum8ColumnData {
			data
		};

		W::wrap(column)
	}
}

impl ColumnFrom for Vec<Option<Enum8>> {
	fn column_from<W: ColumnWrapper>(source: Self) -> W::Wrapper {
		let mut nulls: Vec<u8> = Vec::with_capacity(source.len());
		let mut data = List::<i8>::with_capacity(source.len());
		for os in source {
			if let Some(s) = os {
				data.push(s.0);
				nulls.push(0);
			} else {
				data.push(0);
				nulls.push(1);
			}
		}
//		let inner = Box::new(VectorColumnData { data });

		let inner = Enum8ColumnData {
			data,
		};

		W::wrap(NullableColumnData {
			nulls,
			inner: Box::new(inner),
		})
	}
}
