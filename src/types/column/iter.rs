#![allow(clippy::cast_ptr_alignment)]

use std::{
    iter::FusedIterator,
    marker, mem,
    net::{Ipv4Addr, Ipv6Addr},
    ptr, slice,
};

use chrono::{prelude::*, Date};
use chrono_tz::Tz;

use crate::{
    errors::{Error, FromSqlError, Result},
    types::{column::StringPool, decimal::NoBits, Column, Decimal, Simple, SqlType},
};

macro_rules! simple_num_iterable {
    ( $($t:ty: $k:ident),* ) => {
        $(
            impl<'a> SimpleIterable<'a> for $t {
                type Iter = slice::Iter<'a, $t>;

                fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter> {
                    if column_type != SqlType::$k {
                        return Err(Error::FromSql(FromSqlError::InvalidType {
                            src: column.sql_type().to_string(),
                            dst: SqlType::$k.to_string(),
                        }));
                    }

                    unsafe {
                        let mut ptr: *const u8 = ptr::null();
                        let mut size: usize = 0;
                        column.get_internal(&[&mut ptr, &mut size as *mut usize as *mut *const u8], 0)?;
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
    f64: Float64
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

macro_rules! exact_size_iterator {
    (
        $name:ident: $type:ty
    ) => {
        impl ExactSizeIterator for $name<'_> {
            #[inline(always)]
            fn len(&self) -> usize {
                (self.end as usize - self.ptr as usize) / mem::size_of::<$type>()
            }
        }
    };
}

pub trait SimpleIterable<'a> {
    type Iter: Iterator + 'a;

    fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter>;
}

enum StringInnerIterator<'a> {
    String(&'a StringPool),
    FixedString(*const u8, usize),
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

pub struct DateIterator<'a> {
    ptr: *const u16,
    end: *const u16,
    tz: Tz,
    _marker: marker::PhantomData<&'a ()>,
}

pub struct DateTimeIterator<'a> {
    ptr: *const u32,
    end: *const u32,
    tz: Tz,
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
                    None
                } else {
                    let old_index = self.index;
                    self.index += 1;
                    Some(unsafe { string_pool.get_unchecked(old_index) })
                }
            }
            StringInnerIterator::FixedString(buffer, str_len) => {
                if self.index >= self.size {
                    None
                } else {
                    let shift = self.index * str_len;
                    self.index += 1;
                    unsafe {
                        let ptr = buffer.add(shift);
                        Some(slice::from_raw_parts(ptr, str_len))
                    }
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
        self.ptr = self.ptr.offset(4) as *const u8;

        Ipv4Addr::from(m)
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe {
            self.ptr.add(n * 4);
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
        self.ptr = self.ptr.offset(16) as *const u8;

        Ipv6Addr::from(m)
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe {
            self.ptr.add(n * 16);
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

impl<'a> DateIterator<'a> {
    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> Date<Tz> {
        let current_value = *self.ptr;
        self.ptr = self.ptr.offset(1);

        let time = self.tz.timestamp(i64::from(current_value) * 24 * 3600, 0);
        time.date()
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe { self.ptr = self.ptr.add(n) }
    }
}

impl<'a> DateTimeIterator<'a> {
    #[inline(always)]
    unsafe fn next_unchecked(&mut self) -> DateTime<Tz> {
        let current_value = *self.ptr;
        self.ptr = self.ptr.offset(1);

        self.tz.timestamp(i64::from(current_value), 0)
    }

    #[inline(always)]
    fn post_inc_start(&mut self, n: usize) {
        unsafe { self.ptr = self.ptr.add(n) }
    }
}

exact_size_iterator! { DateIterator: u16 }

exact_size_iterator! { DateTimeIterator: u32 }

iterator! { DateIterator: Date<Tz> }

iterator! { DateTimeIterator: DateTime<Tz> }

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

impl<'a> SimpleIterable<'a> for Ipv4Addr {
    type Iter = Ipv4Iterator<'a>;

    fn iter(column: &'a Column<Simple>, _column_type: SqlType) -> Result<Self::Iter> {
        let inner = unsafe {
            let mut inner: *const u8 = ptr::null();
            column.get_internal(&[&mut inner], 0)?;
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


impl<'a> SimpleIterable<'a> for Ipv6Addr {
    type Iter = Ipv6Iterator<'a>;

    fn iter(column: &'a Column<Simple>, _column_type: SqlType) -> Result<Self::Iter> {
        let inner = unsafe {
            let mut inner: *const u8 = ptr::null();
            column.get_internal(&[&mut inner], 0)?;
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

impl<'a> SimpleIterable<'a> for &[u8] {
    type Iter = StringIterator<'a>;

    fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter> {
        let mut size: usize = 0;
        let inner = match column_type {
            SqlType::String => {
                let string_pool = unsafe {
                    let mut string_pool: *const u8 = ptr::null();
                    column.get_internal(
                        &[&mut string_pool, &mut size as *mut usize as *mut *const u8],
                        0,
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

impl<'a> SimpleIterable<'a> for Decimal {
    type Iter = DecimalIterator<'a>;

    fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter> {
        let (precision, scale) = if let SqlType::Decimal(precision, scale) = column_type {
            (precision, scale)
        } else {
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

impl<'a> SimpleIterable<'a> for DateTime<Tz> {
    type Iter = DateTimeIterator<'a>;

    fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter> {
        let (ptr, end, tz) = date_iter(column, column_type, SqlType::DateTime)?;
        Ok(DateTimeIterator {
            ptr,
            end,
            tz,
            _marker: marker::PhantomData,
        })
    }
}

impl<'a> SimpleIterable<'a> for Date<Tz> {
    type Iter = DateIterator<'a>;

    fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter> {
        let (ptr, end, tz) = date_iter(column, column_type, SqlType::Date)?;
        Ok(DateIterator {
            ptr,
            end,
            tz,
            _marker: marker::PhantomData,
        })
    }
}

fn date_iter<T>(
    column: &Column<Simple>,
    column_type: SqlType,
    sql_type: SqlType,
) -> Result<(*const T, *const T, Tz)> {
    if column_type != sql_type {
        return Err(Error::FromSql(FromSqlError::InvalidType {
            src: column.sql_type().to_string(),
            dst: sql_type.to_string(),
        }));
    };

    let (ptr, end, tz) = unsafe {
        let mut ptr: *const T = ptr::null();
        let mut tz: *const Tz = ptr::null();
        let mut size: usize = 0;
        column.get_internal(
            &[
                &mut ptr as *mut *const T as *mut *const u8,
                &mut tz as *mut *const Tz as *mut *const u8,
                &mut size as *mut usize as *mut *const u8,
            ],
            0,
        )?;
        assert_ne!(ptr, ptr::null());
        assert_ne!(tz, ptr::null());
        let end = ptr.add(size);
        (ptr, end, &*tz)
    };

    Ok((ptr, end, *tz))
}

impl<'a, T> SimpleIterable<'a> for Option<T>
where
    T: SimpleIterable<'a>,
{
    type Iter = NullableIterator<'a, T::Iter>;

    fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter> {
        let inner = if let SqlType::Nullable(inner_type) = column_type {
            T::iter(column, *inner_type)?
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

impl<'a, T> SimpleIterable<'a> for Vec<T>
where
    T: SimpleIterable<'a>,
{
    type Iter = ArrayIterator<'a, T::Iter>;

    fn iter(column: &'a Column<Simple>, column_type: SqlType) -> Result<Self::Iter> {
        let inner = if let SqlType::Array(inner_type) = column_type {
            T::iter(column, *inner_type)?
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
