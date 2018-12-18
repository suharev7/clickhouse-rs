use std::iter;

use crate::binary::Encoder;
use crate::column::column_data::ColumnData;
use crate::column::BoxColumnData;
use crate::types::{SqlType, Value, ValueRef};

pub struct ConcatColumnData {
    data: Vec<BoxColumnData>,
    index: Vec<usize>,
}

impl ConcatColumnData {
    pub fn concat(data: Vec<BoxColumnData>) -> ConcatColumnData {
        ConcatColumnData::check_columns(&data);

        let index = build_index(data.iter().map(|x| x.len()));
        ConcatColumnData { data, index }
    }

    fn check_columns(data: &Vec<BoxColumnData>) {
        match data.first() {
            None => panic!("data should not be empty."),
            Some(first) => {
                for column in data.iter().skip(1) {
                    if first.sql_type() != column.sql_type() {
                        panic!(
                            "all columns should have the same type ({:?} != {:?}).",
                            first.sql_type(),
                            column.sql_type()
                        );
                    }
                }
            }
        }
    }
}

impl ColumnData for ConcatColumnData {
    fn sql_type(&self) -> SqlType {
        self.data[0].sql_type()
    }

    fn save(&self, encoder: &mut Encoder) {
        for chunk in &self.data {
            chunk.save(encoder)
        }
    }

    fn len(&self) -> usize {
        *self.index.last().unwrap()
    }

    fn push(&mut self, _value: Value) {
        unimplemented!()
    }

    fn at(&self, index: usize) -> ValueRef {
        let chunk_index = find_chunk(&self.index, index);
        let chunk = &self.data[chunk_index];
        chunk.at(index - self.index[chunk_index])
    }
}

fn build_index<'a, I>(sizes: I) -> Vec<usize>
where
    I: iter::Iterator<Item = usize> + 'a,
{
    let mut acc = 0;
    let mut index = Vec::new();

    index.push(acc);
    for size in sizes {
        acc += size;
        index.push(acc);
    }

    index
}

fn find_chunk(index: &[usize], ix: usize) -> usize {
    let mut lo = 0_usize;
    let mut hi = index.len() - 1;

    while lo < hi {
        let mid = lo + (hi - lo) / 2;

        if index[lo] == index[lo + 1] {
            lo += 1;
            continue;
        }

        if ix < index[mid] {
            hi = mid;
        } else if ix >= index[mid + 1] {
            lo = mid + 1;
        } else {
            return mid;
        }
    }

    0
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::column::column_data::ColumnDataExt;
    use crate::column::numeric::VectorColumnData;
    use crate::column::string::StringColumnData;

    use super::*;

    #[test]
    fn test_build_index() {
        let sizes = vec![2_usize, 3, 4];
        let index = build_index(sizes.iter().map(|x| *x));
        assert_eq!(index, vec![0, 2, 5, 9])
    }

    #[test]
    fn test_find_chunk() {
        let index = vec![0_usize, 2, 5, 9];
        assert_eq!(find_chunk(&index, 0), 0);
        assert_eq!(find_chunk(&index, 1), 0);
        assert_eq!(find_chunk(&index, 2), 1);
        assert_eq!(find_chunk(&index, 3), 1);
        assert_eq!(find_chunk(&index, 4), 1);
        assert_eq!(find_chunk(&index, 5), 2);
        assert_eq!(find_chunk(&index, 6), 2);

        assert_eq!(find_chunk(&index, 7), 2);
        assert_eq!(find_chunk(&vec![0], 7), 0);
    }

    #[test]
    fn test_find_chunk2() {
        let index = vec![0_usize, 0, 5];
        assert_eq!(find_chunk(&index, 0), 1);
        assert_eq!(find_chunk(&index, 1), 1);
        assert_eq!(find_chunk(&index, 2), 1);
        assert_eq!(find_chunk(&index, 3), 1);
        assert_eq!(find_chunk(&index, 4), 1);
        assert_eq!(find_chunk(&index, 5), 0);
    }

    #[test]
    fn test_find_chunk5() {
        let index = vec![
            0_usize, 0, 3, 6, 9, 12, 15, 18, 21, 24, 27, 30, 33, 36, 39, 42, 45, 48, 51, 54, 57,
            60, 63, 66, 69,
        ];
        for i in 0..69 {
            assert_eq!(find_chunk(&index, i), 1 + i / 3);
        }
    }

    #[test]
    fn test_concat_column() {
        let xs = vec![make_string_column(), make_string_column()];
        let actual = ConcatColumnData::concat(xs);

        assert_eq!(
            actual.at(0).as_str().unwrap(),
            "13298a5f-6a10-4fbe-9644-807f7ebf82cc"
        );
        assert_eq!(
            actual.at(1).as_str().unwrap(),
            "df0e62bb-c0db-4728-a558-821f8e8da38c"
        );
        assert_eq!(
            actual.at(2).as_str().unwrap(),
            "13298a5f-6a10-4fbe-9644-807f7ebf82cc"
        );
        assert_eq!(
            actual.at(3).as_str().unwrap(),
            "df0e62bb-c0db-4728-a558-821f8e8da38c"
        );

        assert_eq!(actual.len(), 4);
    }

    #[test]
    fn test_concat_num_column() {
        let xs = vec![make_num_column(), make_num_column()];
        let actual = ConcatColumnData::concat(xs);

        assert_eq!(u32::from(actual.at(0)), 1_u32);
        assert_eq!(u32::from(actual.at(1)), 2_u32);
        assert_eq!(u32::from(actual.at(2)), 1_u32);
        assert_eq!(u32::from(actual.at(3)), 2_u32);

        assert_eq!(actual.len(), 4);
    }

    fn make_string_column() -> BoxColumnData {
        let mut data = StringColumnData::with_capacity(1);
        data.append("13298a5f-6a10-4fbe-9644-807f7ebf82cc".to_string());
        data.append("df0e62bb-c0db-4728-a558-821f8e8da38c".to_string());
        Arc::new(data)
    }

    fn make_num_column() -> BoxColumnData {
        let mut data = VectorColumnData::<u32>::with_capacity(1);
        data.append(1_u32);
        data.append(2_u32);
        Arc::new(data)
    }
}
