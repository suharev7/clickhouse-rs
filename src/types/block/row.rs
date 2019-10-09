use std::sync::Arc;

use crate::types::{block::ColumnIdx, Block, ClickhouseResult, Column, FromSql, SqlType};

/// A row from Clickhouse
pub struct Row<'a> {
    pub(crate) row: usize,
    pub(crate) block_ref: BlockRef<'a>,
}

impl<'a> Row<'a> {
    /// Get the value of a particular cell of the row.
    pub fn get<T, I>(&'a self, col: I) -> ClickhouseResult<T>
    where
        T: FromSql<'a>,
        I: ColumnIdx + Copy,
    {
        self.block_ref.get(self.row, col)
    }

    /// Return the number of cells in the current row.
    pub fn len(&self) -> usize {
        self.block_ref.column_count()
    }

    /// Returns `true` if the row contains no cells.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the name of a particular cell of the row.
    pub fn name<I: ColumnIdx + Copy>(&self, col: I) -> ClickhouseResult<&str> {
        Ok(self.block_ref.get_column(col)?.name())
    }

    /// Get the type of a particular cell of the row.
    pub fn sql_type<I: ColumnIdx + Copy>(&self, col: I) -> ClickhouseResult<SqlType> {
        Ok(self.block_ref.get_column(col)?.sql_type())
    }
}

#[derive(Clone)]
pub(crate) enum BlockRef<'a> {
    Borrowed(&'a Block),
    Owned(Arc<Block>),
}

impl<'a> BlockRef<'a> {
    fn row_count(&self) -> usize {
        match self {
            BlockRef::Borrowed(block) => block.row_count(),
            BlockRef::Owned(block) => block.row_count(),
        }
    }

    fn column_count(&self) -> usize {
        match self {
            BlockRef::Borrowed(block) => block.column_count(),
            BlockRef::Owned(block) => block.column_count(),
        }
    }

    fn get<'s, T, I>(&'s self, row: usize, col: I) -> ClickhouseResult<T>
    where
        T: FromSql<'s>,
        I: ColumnIdx + Copy,
    {
        match self {
            BlockRef::Borrowed(block) => block.get(row, col),
            BlockRef::Owned(block) => block.get(row, col),
        }
    }

    fn get_column<I: ColumnIdx + Copy>(&self, col: I) -> ClickhouseResult<&Column> {
        match self {
            BlockRef::Borrowed(block) => {
                let column_index = col.get_index(block.columns())?;
                Ok(&block.columns[column_index])
            }
            BlockRef::Owned(block) => {
                let column_index = col.get_index(block.columns())?;
                Ok(&block.columns[column_index])
            }
        }
    }
}

/// Immutable rows iterator
pub struct Rows<'a> {
    pub(crate) row: usize,
    pub(crate) block_ref: BlockRef<'a>,
}

impl<'a> Iterator for Rows<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.row >= self.block_ref.row_count() {
            return None;
        }
        let result = Some(Row {
            row: self.row,
            block_ref: self.block_ref.clone(),
        });
        self.row += 1;
        result
    }
}

#[cfg(test)]
mod test {
    use crate::{row, types::Block};

    #[test]
    fn test_len() {
        let mut block = Block::new();
        block.push(row!{foo: "bar"}).unwrap();

        for row in block.rows() {
            assert_eq!(row.len(), 1);
        }
    }

    #[test]
    fn test_is_empty() {
        let mut block = Block::new();
        block.push(row!{foo: "bar"}).unwrap();

        for row in block.rows() {
            assert!(!row.is_empty());
        }
    }
}