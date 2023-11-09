#![allow(clippy::cast_ptr_alignment)]

use chrono::prelude::*;
use chrono_tz::Tz;
use std::{
    collections::HashMap,
    hash::Hash,
    iter::FusedIterator,
    marker, mem,
    net::{Ipv4Addr, Ipv6Addr},
    ptr, slice,
};

use crate::{
    errors::{Error, FromSqlError, Result},
    types::{
        column::{
            column_data::{ArcColumnData, LowCardinalityAccessor},
            date::DateTimeInternals,
            datetime64::{to_datetime, to_native_datetime_opt},
            low_cardinality::{LowCardinalityIndex, LowCardinalityInternals},
            StringPool,
        },
        decimal::NoBits,
        Column, ColumnType, Complex, Decimal, Simple, SqlType,
    },
};

fn check_type(src: &SqlType, dst: &SqlType) -> bool {
    if let SqlType::SimpleAggregateFunction(_, nested) = src {
        check_type(nested, dst)
    } else {
        src == dst
    }
}

macro_rules! simple_num_iterable {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl<'a> Iterable<'a, Simple> for $t {
                type Iter = slice::Iter<'a, $t>;

                fn iter_with_props(column: &'a Column<Simple>, column_type: SqlType, props: u32) -> Result<Self::Iter> {
                    if !check_type(&column_type, &SqlType::$k) {
                        return Err(Error::FromSql(FromSqlError::InvalidType {
                            src: column.sql_type().to_string(),
                            dst: SqlType::$k.to_string(),
                        }));
                    }

                    unsafe {
                        let mut ptr: *const u8 = ptr::null();
                        let mut size: usize = 0;
                        column.get_internal(
                            &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                            0,
                            props,
                        )?;
                        assert_ne!(ptr, ptr::null());
                        Ok(slice::from_raw_parts(ptr as *const $t, size).iter())
                    }
                }
            }
        )*
    };
}

simple_num_iterable! {
    u8: UInt8,
    u16: UInt16,
    u32: UInt32,
    u64: UInt64,

    i8: Int8,
    i16: Int16,
    i32: Int32,
    i64: Int64,

    f32: Float32,
    f64: Float64,

    i128: Int128,
    u128: UInt128
}

macro_rules! iterator {
    (
        $name:ident: $type:ty
    ) => {
        impl<'a> Iterator for $name<'a> {
            type Item = $type;

            #[inline]
            fn next(&mut self) -> Option<Self::Item> {
                if self.ptr == self.end {
                    None
                } else {
                    Some(unsafe { self.next_unchecked() })
                }
            }

            #[inline]
            fn size_hint(&self) -> (usize, Option<usize>) {
                let exact = self.len();
                (exact, Some(exact))
            }

            #[inline]
            fn count(self) -> usize {
                self.len()
            }

            #[inline]
            fn nth(&mut self, n: usize) -> Option<Self::Item> {
                if n >= self.len() {
                    // This iterator is now empty.
                    self.ptr = self.end;
                    return None;
                }
                // We are in bounds. `post_inc_start` does the right thing even for ZSTs.
                self.post_inc_start(n);
                self.next()
            }
        }
    };
}

pub trait Iterable<'a, K: ColumnType> {
    type Iter: Iterator;

    fn iter(column: &'a Column<K>, column_type: SqlType) -> Result<Self::Iter> {
        Self::iter_with_props(column, column_type, 0)
    }

    fn iter_with_props(
        column: &'a Column<K>,
        column_type: SqlType,
        _props: u32,
    ) -> Result<Self::Iter>;
}

enum StringInnerIterator<'a> {
    String(&'a StringPool),
    FixedString(*const u8, usize),
    LowCardinality(
        *const LowCardinalityIndex,
        *const dyn LowCardinalityAccessor,
    ),
}

pub struct StringIterator<'a> {
    inner: StringInnerIterator<'a>,
    index: usize,
    size: usize,
}

pub struct DecimalIterator<'a> {
    ptr: *const u8,
    end: *const u8,
    precision: u8,
    scale: u8,
    nobits: NoBits,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct Ipv4Iterator<'a> {
    ptr: *const u8,
    end: *const u8,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct Ipv6Iterator<'a> {
    ptr: *const u8,
    end: *const u8,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct UuidIterator<'a> {
    ptr: *const u8,
    end: *const u8,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct DateIterator<'a> {
    lc_index: Option<*const LowCardinalityIndex>,
    ptr: *const u16,
    index: usize,
    len: usize,
    tz: Tz,
    _marker: marker::PhantomData<&'a ()>,
}

enum DateTimeInnerIterator {
    DateTime32(*const u32),
    DateTime64(*const i64, u32),
}

pub struct DateTimeIterator<'a> {
    lc_index: Option<*const LowCardinalityIndex>,
    inner: DateTimeInnerIterator,
    index: usize,
    len: usize,
    tz: Tz,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct NativeDateTimeIterator<'a> {
    lc_index: Option<*const LowCardinalityIndex>,
    inner: DateTimeInnerIterator,
    index: usize,
    len: usize,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct NullableIterator<'a, I> {
    inner: I,
    ptr: *const u8,
    end: *const u8,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct ArrayIterator<'a, I> {
    inner: I,
    offsets: &'a [u64],
    index: usize,
    size: usize,
}

pub struct MapIterator<'a, K, V> {
    inner: (K, V),
    offsets: &'a [u64],
    index: usize,
    size: usize,
}

impl ExactSizeIterator for StringIterator<'_> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size - self.index
    }
}

impl<'a> Iterator for StringIterator<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        match self.inner {
            StringInnerIterator::String(string_pool) => {
                if self.index == self.size {
                    return None;
                }
                let old_index = self.index;
                self.index += 1;
                Some(unsafe { string_pool.get_unchecked(old_index) })
            }
            StringInnerIterator::FixedString(buffer, str_len) => {
                if self.index >= self.size {
                    return None;
                }
                let shift = self.index * str_len;
                self.index += 1;
                unsafe {
                    let ptr = buffer.add(shift);
                    Some(slice::from_raw_parts(ptr, str_len))
                }
            }
            StringInnerIterator::LowCardinality(index, accessor) => {
                if self.index >= self.size {
                    return None;
                }
                unsafe {
                    let value_index = (*index).get_by_index(self.index);
                    self.index += 1;
                    Some((*accessor).get_string(value_index))
                }
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.len() {
            // This iterator is now empty.
            self.index = self.size;
            return None;
        }

        self.index += n;
        self.next()
    }
}

impl FusedIterator for StringIterator<'_> {}

impl<'a> DecimalIterator<'a> {
    #[inline(always)]
    unsafe fn next_unchecked_<T>(&mut self) -> Decimal
    where
        T: Copy + Sized,
        i64: From<T>,
    {
        let current_value = *(self.ptr as *const T);
        self.ptr = (self.ptr as *const T).offset(1) as *const u8;

        Decimal {
            underlying: current_value.into(),
            nobits: self.nobits,
            precision: self.precision,
            scale: self.scale,
        }
    }

    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> Decimal {
        match self.nobits {
            NoBits::N32 => self.next_unchecked_::<i32>(),
            NoBits::N64 => self.next_unchecked_::<i64>(),
        }
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe {
            match self.nobits {
                NoBits::N32 => self.ptr = (self.ptr as *const i32).add(n) as *const u8,
                NoBits::N64 => self.ptr = (self.ptr as *const i64).add(n) as *const u8,
            }
        }
    }
}

impl<'a> ExactSizeIterator for DecimalIterator<'a> {
    #[inline(always)]
    fn len(&self) -> usize {
        let size = match self.nobits {
            NoBits::N32 => mem::size_of::<i32>(),
            NoBits::N64 => mem::size_of::<i64>(),
        };
        (self.end as usize - self.ptr as usize) / size
    }
}

iterator! { DecimalIterator: Decimal }

iterator! { Ipv4Iterator: Ipv4Addr }

impl<'a> Ipv4Iterator<'a> {
    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> Ipv4Addr {
        let v = slice::from_raw_parts(self.ptr, 4);
        let mut m = [0_u8; 4];
        m.copy_from_slice(v);
        m.reverse();
        self.ptr = self.ptr.offset(4);

        Ipv4Addr::from(m)
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe {
            self.ptr = self.ptr.add(n * 4);
        }
    }
}

impl<'a> ExactSizeIterator for Ipv4Iterator<'a> {
    #[inline(always)]
    fn len(&self) -> usize {
        let size = 4;
        (self.end as usize - self.ptr as usize) / size
    }
}

iterator! { Ipv6Iterator: Ipv6Addr }

impl<'a> Ipv6Iterator<'a> {
    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> Ipv6Addr {
        let v = slice::from_raw_parts(self.ptr, 16);
        let mut m = [0_u8; 16];
        m.copy_from_slice(v);
        self.ptr = self.ptr.offset(16);

        Ipv6Addr::from(m)
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe {
            self.ptr = self.ptr.add(n * 16);
        }
    }
}

impl<'a> ExactSizeIterator for Ipv6Iterator<'a> {
    #[inline(always)]
    fn len(&self) -> usize {
        let size = 16;
        (self.end as usize - self.ptr as usize) / size
    }
}

iterator! { UuidIterator: uuid::Uuid }

impl<'a> UuidIterator<'a> {
    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> uuid::Uuid {
        let v = slice::from_raw_parts(self.ptr, 16);
        let mut m = [0_u8; 16];
        m.copy_from_slice(v);
        m[..8].reverse();
        m[8..].reverse();
        self.ptr = self.ptr.offset(16);

        uuid::Uuid::from_bytes(m)
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe {
            self.ptr = self.ptr.add(n * 16);
        }
    }
}

impl<'a> ExactSizeIterator for UuidIterator<'a> {
    #[inline(always)]
    fn len(&self) -> usize {
        let size = 16;
        (self.end as usize - self.ptr as usize) / size
    }
}

impl<'a> DateIterator<'a> {
    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> NaiveDate {
        let index = self
            .lc_index
            .map(|ix| unsafe { (*ix).get_by_index(self.index) })
            .unwrap_or(self.index);

        let current_value = *self.ptr.add(index);
        self.index += 1;

        let time = self
            .tz
            .timestamp_opt(i64::from(current_value) * 24 * 3600, 0)
            .unwrap();
        time.date_naive()
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        self.index += n;
    }
}

impl<'a> NativeDateTimeIterator<'a> {
    #[inline(always)]
    unsafe fn get_by_index(&self, index: usize) -> NaiveDateTime {
        let index_ = self
            .lc_index
            .map(|lx| unsafe { (*lx).get_by_index(index) })
            .unwrap_or(index);

        match &self.inner {
            DateTimeInnerIterator::DateTime32(ptr) => {
                let current_value = *ptr.add(index_);
                NaiveDateTime::from_timestamp_opt(i64::from(current_value), 0).unwrap()
            }
            DateTimeInnerIterator::DateTime64(ptr, precision) => {
                let current_value = *ptr.add(index_);
                to_native_datetime_opt(current_value, *precision).unwrap()
            }
        }
    }

    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> NaiveDateTime {
        let result = self.get_by_index(self.index);
        self.index += 1;
        result
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        self.index += n;
    }
}

impl<'a> DateTimeIterator<'a> {
    #[inline(always)]
    unsafe fn get_by_index(&self, index: usize) -> DateTime<Tz> {
        let index_ = self
            .lc_index
            .map(|lx| unsafe { (*lx).get_by_index(index) })
            .unwrap_or(index);

        match &self.inner {
            DateTimeInnerIterator::DateTime32(ptr) => {
                let current_value = *ptr.add(index_);
                self.tz.timestamp_opt(i64::from(current_value), 0).unwrap()
            }
            DateTimeInnerIterator::DateTime64(ptr, precision) => {
                let current_value = *ptr.add(index_);
                to_datetime(current_value, *precision, self.tz)
            }
        }
    }

    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> DateTime<Tz> {
        let result = self.get_by_index(self.index);
        self.index += 1;
        result
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        self.index += n;
    }
}

impl ExactSizeIterator for DateIterator<'_> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len - self.index
    }
}

impl ExactSizeIterator for DateTimeIterator<'_> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len - self.index
    }
}

impl<'a> Iterator for DateIterator<'a> {
    type Item = NaiveDate;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.len {
            None
        } else {
            Some(unsafe { self.next_unchecked() })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.len() {
            // This iterator is now empty.
            self.index = self.len;
            return None;
        }
        // We are in bounds. `post_inc_start` does the right thing even for ZSTs.
        self.post_inc_start(n);
        self.next()
    }
}

impl ExactSizeIterator for NativeDateTimeIterator<'_> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.len - self.index
    }
}

impl<'a> Iterator for NativeDateTimeIterator<'a> {
    type Item = NaiveDateTime;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.len() == 0 {
            None
        } else {
            Some(unsafe { self.next_unchecked() })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.len() {
            // This iterator is now empty.
            self.index = self.len;
            return None;
        }
        // We are in bounds. `post_inc_start` does the right thing even for ZSTs.
        self.post_inc_start(n);
        self.next()
    }
}

impl<'a> Iterator for DateTimeIterator<'a> {
    type Item = DateTime<Tz>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.len() == 0 {
            None
        } else {
            Some(unsafe { self.next_unchecked() })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        if n >= self.len() {
            // This iterator is now empty.
            self.index = self.len;
            return None;
        }
        // We are in bounds. `post_inc_start` does the right thing even for ZSTs.
        self.post_inc_start(n);
        self.next()
    }
}

impl<'a, I> ExactSizeIterator for NullableIterator<'a, I>
where
    I: Iterator,
{
    #[inline(always)]
    fn len(&self) -> usize {
        let start = self.ptr;
        self.end as usize - start as usize
    }
}

impl<'a, I> Iterator for NullableIterator<'a, I>
where
    I: Iterator,
{
    type Item = Option<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr == self.end {
            return None;
        }

        let value = self.inner.next()?;
        unsafe {
            let flag = *self.ptr;
            self.ptr = self.ptr.offset(1);

            Some(if flag != 0 { None } else { Some(value) })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }
}

impl<'a, I: Iterator> FusedIterator for NullableIterator<'a, I> {}

impl<'a, I: Iterator> ExactSizeIterator for ArrayIterator<'a, I> {
    #[inline(always)]
    fn len(&self) -> usize {
        self.size - self.index
    }
}

impl<'a, I: Iterator> Iterator for ArrayIterator<'a, I> {
    type Item = Vec<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.size {
            return None;
        }

        let start = if self.index > 0 {
            self.offsets[self.index - 1] as usize
        } else {
            0_usize
        };
        let end = self.offsets[self.index] as usize;

        let size = end - start;

        let mut v = Vec::with_capacity(size);
        for _ in 0..size {
            if let Some(item) = self.inner.next() {
                v.push(item);
            }
        }

        self.index += 1;
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }
}

impl<'a, I: Iterator> FusedIterator for ArrayIterator<'a, I> {}

impl<'a, K: Iterator, V: Iterator> ExactSizeIterator for MapIterator<'a, K, V>
where
    K::Item: Eq + Hash,
{
    #[inline(always)]
    fn len(&self) -> usize {
        self.size - self.index
    }
}

impl<'a, K: Iterator, V: Iterator> Iterator for MapIterator<'a, K, V>
where
    K::Item: Eq + Hash,
{
    type Item = HashMap<K::Item, V::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.size {
            return None;
        }

        let start = if self.index > 0 {
            self.offsets[self.index - 1] as usize
        } else {
            0_usize
        };
        let end = self.offsets[self.index] as usize;
        let size = end - start;

        let mut v = HashMap::with_capacity(size);
        for _ in 0..size {
            if let Some(key) = self.inner.0.next() {
                if let Some(value) = self.inner.1.next() {
                    v.insert(key, value);
                }
            }
        }

        self.index += 1;
        Some(v)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.len();
        (exact, Some(exact))
    }

    #[inline]
    fn count(self) -> usize {
        self.len()
    }
}

impl<'a> Iterable<'a, Simple> for Ipv4Addr {
    type Iter = Ipv4Iterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        _column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = unsafe {
            let mut inner: *const u8 = ptr::null();
            column.get_internal(&[&mut inner], 0, props)?;
            &*(inner as *const Vec<u8>)
        };

        let ptr = inner.as_ptr();
        let end = unsafe { ptr.add(inner.len()) };

        Ok(Ipv4Iterator {
            ptr,
            end,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a> Iterable<'a, Simple> for Ipv6Addr {
    type Iter = Ipv6Iterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        _column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = unsafe {
            let mut inner: *const u8 = ptr::null();
            column.get_internal(&[&mut inner], 0, props)?;
            &*(inner as *const Vec<u8>)
        };

        let ptr = inner.as_ptr();
        let end = unsafe { ptr.add(inner.len()) };

        Ok(Ipv6Iterator {
            ptr,
            end,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a> Iterable<'a, Simple> for uuid::Uuid {
    type Iter = UuidIterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        _column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = unsafe {
            let mut inner: *const u8 = ptr::null();
            column.get_internal(&[&mut inner], 0, props)?;
            &*(inner as *const Vec<u8>)
        };

        let ptr = inner.as_ptr();
        let end = unsafe { ptr.add(inner.len()) };

        Ok(UuidIterator {
            ptr,
            end,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a> Iterable<'a, Simple> for &[u8] {
    type Iter = StringIterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let mut size: usize = 0;
        let inner = match column_type {
            SqlType::LowCardinality(&SqlType::String)
            | SqlType::LowCardinality(&SqlType::FixedString(_)) => {
                let mut internals = LowCardinalityInternals::default();
                unsafe {
                    column.get_internals(&mut internals, 1, 0)?;
                    size = (*internals.index).len();
                    StringInnerIterator::LowCardinality(internals.index, internals.accessor)
                }
            }
            SqlType::String => {
                let string_pool = unsafe {
                    let mut string_pool: *const u8 = ptr::null();
                    column.get_internal(
                        &[&mut string_pool, &mut size as *mut usize as *mut *const u8],
                        0,
                        props,
                    )?;
                    &*(string_pool as *const StringPool)
                };
                StringInnerIterator::String(string_pool)
            }
            SqlType::FixedString(str_len) => {
                let buffer = unsafe {
                    let mut buffer: *const u8 = ptr::null();
                    column.get_internal(
                        &[&mut buffer, &mut size as *mut usize as *mut *const u8],
                        0,
                        0,
                    )?;
                    assert_ne!(buffer, ptr::null());
                    buffer
                };

                StringInnerIterator::FixedString(buffer, str_len)
            }
            _ => {
                return Err(Error::FromSql(FromSqlError::InvalidType {
                    src: column.sql_type().to_string(),
                    dst: SqlType::String.to_string(),
                }))
            }
        };

        Ok(StringIterator {
            inner,
            size,
            index: 0,
        })
    }
}

impl<'a> Iterable<'a, Simple> for Decimal {
    type Iter = DecimalIterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        _props: u32,
    ) -> Result<Self::Iter> {
        let SqlType::Decimal(precision, scale) = column_type else {
            return Err(Error::FromSql(FromSqlError::InvalidType {
                src: column.sql_type().to_string(),
                dst: SqlType::Decimal(255, 255).to_string(),
            }));
        };

        let (ptr, size, nobits) = unsafe {
            let mut ptr: *const u8 = ptr::null();
            let mut size: usize = 0;
            let mut nobits: NoBits = NoBits::N32;
            column.get_internal(
                &[
                    &mut ptr,
                    &mut size as *mut usize as *mut *const u8,
                    &mut nobits as *mut NoBits as *mut *const u8,
                ],
                0,
                0,
            )?;
            assert_ne!(ptr, ptr::null());
            (ptr, size, nobits)
        };

        let end = unsafe {
            match nobits {
                NoBits::N32 => (ptr as *const u32).add(size) as *const u8,
                NoBits::N64 => (ptr as *const u64).add(size) as *const u8,
            }
        };

        Ok(DecimalIterator {
            ptr,
            end,
            precision,
            scale,
            nobits,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a> Iterable<'a, Simple> for bool {
    type Iter = slice::Iter<'a, bool>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        if !check_type(&column_type, &SqlType::Bool) {
            return Err(Error::FromSql(FromSqlError::InvalidType {
                src: column.sql_type().to_string(),
                dst: SqlType::Bool.to_string(),
            }));
        }

        unsafe {
            let mut ptr: *const u8 = ptr::null();
            let mut size: usize = 0;
            column.get_internal(
                &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                0,
                props,
            )?;
            assert_ne!(ptr, ptr::null());
            Ok(slice::from_raw_parts(ptr as *const bool, size).iter())
        }
    }
}

impl<'a> Iterable<'a, Simple> for DateTime<Tz> {
    type Iter = DateTimeIterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let mut dt_inter;
        let len;
        let lc_index;

        match column_type {
            SqlType::LowCardinality(SqlType::DateTime(_)) => {
                let mut lc_inter = LowCardinalityInternals::default();
                dt_inter = DateTimeInternals::default();
                unsafe {
                    column.get_internals(&mut lc_inter, 1, 0)?;
                    column.get_internals(&mut dt_inter, 0, 0)?;
                    len = (*lc_inter.index).len();
                }
                lc_index = Some(lc_inter.index);
            }
            SqlType::DateTime(_) => {
                dt_inter = date_iter(column, props)?;
                len = dt_inter.len;
                lc_index = None;
            }
            _ => {
                return Err(cast_datetime_error(column.sql_type()));
            }
        };

        let inner = match dt_inter.precision {
            None => {
                let ptr = dt_inter.begin as *const u32;
                DateTimeInnerIterator::DateTime32(ptr)
            }
            Some(precision) => {
                let ptr = dt_inter.begin as *const i64;
                DateTimeInnerIterator::DateTime64(ptr, precision)
            }
        };

        Ok(DateTimeIterator {
            lc_index,
            inner,
            index: 0,
            len,
            tz: dt_inter.tz,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a> Iterable<'a, Simple> for NaiveDateTime {
    type Iter = NativeDateTimeIterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let mut dt_inter;
        let len;
        let lc_index;

        match column_type {
            SqlType::LowCardinality(SqlType::DateTime(_)) => {
                let mut lc_inter = LowCardinalityInternals::default();
                dt_inter = DateTimeInternals::default();
                unsafe {
                    column.get_internals(&mut lc_inter, 1, 0)?;
                    column.get_internals(&mut dt_inter, 0, 0)?;
                    len = (*lc_inter.index).len();
                }
                lc_index = Some(lc_inter.index);
            }
            SqlType::DateTime(_) => {
                dt_inter = date_iter(column, props)?;
                len = dt_inter.len;
                lc_index = None;
            }
            _ => {
                return Err(cast_datetime_error(column.sql_type()));
            }
        };

        let inner = match dt_inter.precision {
            None => {
                let ptr = dt_inter.begin as *const u32;
                DateTimeInnerIterator::DateTime32(ptr)
            }
            Some(precision) => {
                let ptr = dt_inter.begin as *const i64;
                DateTimeInnerIterator::DateTime64(ptr, precision)
            }
        };

        Ok(NativeDateTimeIterator {
            lc_index,
            inner,
            index: 0,
            len,
            _marker: marker::PhantomData,
        })
    }
}

fn cast_datetime_error(sql_type: SqlType) -> Error {
    Error::FromSql(FromSqlError::InvalidType {
        src: sql_type.to_string(),
        dst: "DateTime<?>".into(),
    })
}

impl<'a> Iterable<'a, Simple> for NaiveDate {
    type Iter = DateIterator<'a>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let mut dt_inter;
        let len;
        let lc_index;
        match column_type {
            SqlType::LowCardinality(SqlType::Date) => {
                let mut lc_inter = LowCardinalityInternals::default();
                dt_inter = DateTimeInternals::default();
                unsafe {
                    column.get_internals(&mut lc_inter, 1, 0)?;
                    column.get_internals(&mut dt_inter, 0, 0)?;
                    len = (*lc_inter.index).len();
                }
                lc_index = Some(lc_inter.index);
            }
            SqlType::Date => {
                dt_inter = date_iter(column, props)?;
                len = dt_inter.len;
                lc_index = None;
                assert!(dt_inter.precision.is_none());
            }
            _ => {
                return Err(Error::FromSql(FromSqlError::InvalidType {
                    src: column.sql_type().to_string(),
                    dst: SqlType::Date.to_string(),
                }));
            }
        };
        Ok(DateIterator {
            lc_index,
            index: 0,
            ptr: dt_inter.begin as *const u16,
            len,
            tz: dt_inter.tz,
            _marker: marker::PhantomData,
        })
    }
}

fn date_iter(column: &Column<Simple>, props: u32) -> Result<DateTimeInternals> {
    let mut internals = DateTimeInternals::default();
    unsafe { column.get_internals(&mut internals, 0, props) }?;
    assert_ne!(internals.begin, ptr::null());
    Ok(internals)
}

impl<'a, T> Iterable<'a, Simple> for Option<T>
where
    T: Iterable<'a, Simple>,
{
    type Iter = NullableIterator<'a, T::Iter>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = if let SqlType::Nullable(inner_type) = column_type {
            T::iter(column, inner_type.clone())?
        } else {
            return Err(Error::FromSql(FromSqlError::InvalidType {
                src: column_type.to_string(),
                dst: "Nullable".into(),
            }));
        };

        let (ptr, end) = unsafe {
            let mut ptr: *const u8 = ptr::null();
            let mut size: usize = 0;
            column.get_internal(
                &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                column_type.level(),
                props,
            )?;
            assert_ne!(ptr, ptr::null());
            let end = ptr.add(size);
            (ptr, end)
        };

        Ok(NullableIterator {
            inner,
            ptr,
            end,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a, T> Iterable<'a, Simple> for Vec<T>
where
    T: Iterable<'a, Simple>,
{
    type Iter = ArrayIterator<'a, T::Iter>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = if let SqlType::Array(inner_type) = column_type {
            T::iter_with_props(column, inner_type.clone(), props)?
        } else {
            return Err(Error::FromSql(FromSqlError::InvalidType {
                src: column_type.to_string(),
                dst: "Array".into(),
            }));
        };

        let mut size: usize = 0;
        let offsets = unsafe {
            let mut ptr: *const u8 = ptr::null();
            column.get_internal(
                &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                column_type.level(),
                0,
            )?;
            assert_ne!(ptr, ptr::null());
            slice::from_raw_parts(ptr as *const u64, size)
        };

        Ok(ArrayIterator {
            inner,
            offsets,
            index: 0,
            size,
        })
    }
}

impl<'a, K, V> Iterable<'a, Simple> for HashMap<K, V>
where
    K: Iterable<'a, Simple>,
    <<K as Iterable<'a, Simple>>::Iter as Iterator>::Item: Eq + Hash,
    V: Iterable<'a, Simple>,
{
    type Iter = MapIterator<'a, K::Iter, V::Iter>;

    fn iter_with_props(
        column: &'a Column<Simple>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let inner = if let SqlType::Map(k_type, v_type) = column_type {
            let k_level = (column.sql_type().map_level() - column_type.map_level()) as u32;
            let k_props = 1 | (k_level << 1);
            (
                K::iter_with_props(column, k_type.clone(), k_props)?,
                V::iter_with_props(column, v_type.clone(), 0)?,
            )
        } else {
            return Err(Error::FromSql(FromSqlError::InvalidType {
                src: column_type.to_string(),
                dst: "Map".into(),
            }));
        };

        let mut size: usize = 0;
        let offsets = unsafe {
            let mut ptr = ptr::null();
            column.get_internal(
                &[&mut ptr, &mut size as *mut usize as *mut *const u8],
                column_type.level(),
                props,
            )?;

            assert!(!ptr.is_null());
            slice::from_raw_parts(ptr as *const u64, size)
        };

        Ok(MapIterator {
            inner,
            offsets,
            index: 0,
            size,
        })
    }
}

pub struct ComplexIterator<'a, T>
where
    T: Iterable<'a, Simple>,
{
    column_type: SqlType,

    data: &'a Vec<ArcColumnData>,

    current_index: usize,
    current: Option<<T as Iterable<'a, Simple>>::Iter>,

    _marker: marker::PhantomData<T>,
}

impl<'a, T> Iterator for ComplexIterator<'a, T>
where
    T: Iterable<'a, Simple>,
{
    type Item = <<T as Iterable<'a, Simple>>::Iter as Iterator>::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index == self.data.len() && self.current.is_none() {
            return None;
        }

        if self.current.is_none() {
            loop {
                if self.current_index >= self.data.len() {
                    return None;
                }
                if self.data[self.current_index].len() > 0 {
                    break;
                }
                self.current_index += 1;
            }

            let column: Column<Simple> = Column {
                name: String::new(),
                data: self.data[self.current_index].clone(),
                _marker: marker::PhantomData,
            };

            let iter =
                unsafe { T::iter(mem::transmute(&column), self.column_type.clone()) }.unwrap();

            self.current = Some(iter);
            self.current_index += 1;
        }

        let ret = match self.current {
            None => None,
            Some(ref mut iter) => iter.next(),
        };

        match ret {
            None => {
                self.current = None;
                self.next()
            }
            Some(r) => Some(r),
        }
    }
}

impl<'a, T> Iterable<'a, Complex> for T
where
    T: Iterable<'a, Simple> + 'a,
{
    type Iter = ComplexIterator<'a, T>;

    fn iter_with_props(
        column: &Column<Complex>,
        column_type: SqlType,
        props: u32,
    ) -> Result<Self::Iter> {
        let data: &Vec<ArcColumnData> = unsafe {
            let mut data: *const Vec<ArcColumnData> = ptr::null();

            column.get_internal(
                &[&mut data as *mut *const Vec<ArcColumnData> as *mut *const u8],
                0xff,
                props,
            )?;

            &*data
        };

        Ok(ComplexIterator {
            column_type,
            data,

            current_index: 0,
            current: None,

            _marker: marker::PhantomData,
        })
    }
}

#[cfg(test)]
mod test {
    use crate::types::Block;
    use std::collections::HashMap;

    #[test]
    fn test_complex_iter() {
        let lo = Block::new().column("?", vec![1_u32, 2]);
        let hi = Block::new().column("?", vec![3_u32, 4, 5]);

        let block = Block::concat(&[lo, hi]);

        let columns = block.columns()[0].iter::<u32>().unwrap();
        let actual: Vec<_> = columns.collect();

        assert_eq!(actual, vec![&1_u32, &2, &3, &4, &5])
    }

    #[test]
    fn test_map_iter() {
        let mut hm = HashMap::new();
        hm.insert("a".to_string(), 5_u8);

        let block = Block::new().column("?", vec![hm.clone()]);

        let columns = block.columns()[0].iter::<HashMap<&[u8], u8>>().unwrap();
        let actual: Vec<_> = columns.collect();
        let mut aa = HashMap::new();

        for a in actual[0].iter() {
            aa.insert(*a.0, **a.1);
        }

        let mut expected = HashMap::new();
        for h in &hm {
            expected.insert(h.0.as_bytes(), *h.1);
        }

        assert_eq!(aa, expected)
    }
}
