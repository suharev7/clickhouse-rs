use std::cmp;

use crate::types::{Block, ColumnType};

pub(crate) struct ChunkIterator<K: ColumnType> {
    position: usize,
    size: usize,
    block: Block<K>,
}

impl<K: ColumnType> Iterator for ChunkIterator<K> {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        let m = self.block.row_count();

        if m == 0 && self.position == 0 {
            self.position += 1;
            return Some(Block::default());
        }

        if self.position >= m {
            return None;
        }

        let mut result = Block::new();
        let size = cmp::min(self.size, m - self.position);

        for column in self.block.columns().iter() {
            let range = self.position..self.position + size;
            let data = column.slice(range);
            result = result.column(column.name(), data);
        }

        self.position += size;
        Some(result)
    }
}

impl<K: ColumnType> ChunkIterator<K> {
    pub fn new(size: usize, block: Block<K>) -> ChunkIterator<K> {
        ChunkIterator {
            position: 0,
            size,
            block,
        }
    }
}
