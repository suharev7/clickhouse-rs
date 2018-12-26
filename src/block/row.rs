use crate::Block;
use crate::block::block::ColumnIdx;
use crate::types::{FromSql, FromSqlResult};

pub struct Row<'a> {
    row: usize,
    block: &'a Block,
}

impl<'a> Row<'a> {
    /// Get the value of a particular cell of the row.
    pub fn get<T, I>(&self, col: I) -> FromSqlResult<T>
    where
        T: FromSql<'a>,
        I: ColumnIdx,
    {
        self.block.get(self.row, col)
    }
}

pub struct Rows<'a> {
    pub(crate) row: usize,
    pub(crate) block: &'a Block,
}

impl<'a> Iterator for Rows<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.block.row_count() {
            return None;
        }
        let result = Some(Row {
            row: self.row,
            block: self.block,
        });
        self.row += 1;
        result
    }
}
