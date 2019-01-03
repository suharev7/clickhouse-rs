use std::cmp;

use crate::types::Block;

pub struct ChunkIterator<'a> {
    position: usize,
    size: usize,
    block: &'a Block,
}

impl<'a> Iterator for ChunkIterator<'a> {
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
            result = result.add_column(column.name(), data);
        }

        self.position += size;
        Some(result)
    }
}

impl<'a> ChunkIterator<'a> {
    pub fn new(size: usize, block: &Block) -> ChunkIterator {
        ChunkIterator {
            position: 0,
            size,
            block,
        }
    }
}
