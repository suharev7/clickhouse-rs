use chrono_tz::Tz;

use crate::{
	binary::{Encoder, ReadEx},
	errors::Result,
	types::{
		column::{
			BoxColumnWrapper, column_data::BoxColumnData, column_data::ColumnData,
			ColumnFrom, ColumnWrapper, list::List, nullable::NullableColumnData,
			VectorColumnData,
		},
		Column,
		ColumnType,
		from_sql::FromSql, SqlType, Value, ValueRef,
	},
};
use crate::types::column::Either;
use crate::types::enums::Enum8;

pub(crate) struct Enum8ColumnData {
	pub(crate) enum_values: Vec<(String, i8)>,
	pub(crate) inner: Box<dyn ColumnData + Send + Sync>,
}

pub(crate) struct Enum8Adapter<K: ColumnType> {
	pub(crate) column: Column<K>,
	pub(crate) enum_values: Vec<(String, i8)>,
}

pub(crate) struct NullableEnum8Adapter<K: ColumnType> {
	pub(crate) column: Column<K>,
	pub(crate) enum_values: Vec<(String, i8)>,
}

impl Enum8ColumnData {
	pub(crate) fn load<T: ReadEx>(
		reader: &mut T,
		enum_values: Vec<(String, i8)>,
		size: usize,
		tz: Tz,
	) -> Result<Self> {
		let type_name = "Int8";
		let inner = ColumnData::load_data::<BoxColumnWrapper, _>(reader, type_name, size, tz)?;

		Ok(Enum8ColumnData {
			enum_values,
			inner,
		})
	}
}

impl ColumnData for Enum8ColumnData {
	fn sql_type(&self) -> SqlType {
		SqlType::Enum8(self.enum_values.clone())
	}

	fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
		self.inner.save(encoder, start, end)
	}

	fn len(&self) -> usize {
		self.inner.len()
	}

	fn push(&mut self, value: Value) {
		if let Value::Enum8(_values, enum8) = value {
			// TODO Validate enum or get string or smth else

			self.inner.push(Value::Int8(enum8.internal()));
		} else {
			panic!("value should be enum8 ({:?})", value);
		}
	}

	fn at(&self, index: usize) -> ValueRef {
		let _a = self.inner.at(index);

		let value = i8::from(self.inner.at(index));
		ValueRef::Enum8(self.enum_values.clone(), Enum8(value))
	}

	fn clone_instance(&self) -> BoxColumnData {
		Box::new(Self {
			inner: self.inner.clone_instance(),
			enum_values: self.enum_values.clone(),
		})
	}
}

impl<K: ColumnType> ColumnData for Enum8Adapter<K> {
	fn sql_type(&self) -> SqlType {
		SqlType::Enum8(self.enum_values.clone())
	}

	fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
		for i in start..end {
			if let ValueRef::Enum8(_enum_values, value) = self.at(i) {
				encoder.write(value.internal());
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
		if let ValueRef::Enum8(enum_values, value) = self.column.at(index) {
			ValueRef::Enum8(enum_values, value)
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
		SqlType::Nullable(SqlType::Enum8(self.enum_values.clone()).into())
	}

	fn save(&self, encoder: &mut Encoder, start: usize, end: usize) {
		let size = end - start;
		let mut nulls = vec![0; size];
		let mut values: Vec<Option<i8>> = vec![None; size];
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
				let inner = ValueRef::Enum8(self.enum_values.clone(), Enum8(v));
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
			data.push(s.internal());
		}
		let inner = Box::new(VectorColumnData { data });

		let column = Enum8ColumnData {
			inner,
			enum_values: vec![],
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
				data.push(s.internal());
				nulls.push(0);
			} else {
				data.push(0);
				nulls.push(1);
			}
		}
		let inner = Box::new(VectorColumnData { data });

		let inner = Enum8ColumnData {
			enum_values: vec![],
			inner,
		};

		W::wrap(NullableColumnData {
			nulls,
			inner: Box::new(inner),
		})
	}
}
