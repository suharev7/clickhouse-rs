use crate::{
    types::{column::ColumnData, FromSql},
    Result,
};

pub(crate) fn extract_nulls_and_values<'a, T, C>(
    column_data: &'a C,
    start: usize,
    end: usize,
) -> Result<(Vec<u8>, Vec<Option<T>>)>
where
    C: ColumnData,
    T: FromSql<'a>,
{
    let size = end - start;
    let mut nulls = vec![0_u8; size];
    let mut values: Vec<Option<T>> = Vec::with_capacity(size);

    for (i, index) in (start..end).enumerate() {
        let value = Option::from_sql(column_data.at(index))?;
        if value.is_none() {
            nulls[i] = 1;
        }
        values.push(value);
    }

    Ok((nulls, values))
}
