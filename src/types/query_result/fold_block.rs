use tokio::prelude::*;

use crate::{
    errors::Error,
    types::block::{Block, BlockRef, Row, Rows},
};

use std::{mem, sync::Arc};

enum State<T, Fut> {
    Empty,
    Ready(T),
    Run(Fut),
}

pub(super) struct FoldBlock<T, F, Fut>
where
    F: Fn(T, Row) -> Fut + Send + 'static,
    Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
    Fut::Future: Send,
    T: Send + 'static,
{
    state: State<T, Fut::Future>,
    f: Arc<F>,
    rows: Rows<'static>,
}

impl<T, F, Fut> FoldBlock<T, F, Fut>
where
    F: Fn(T, Row) -> Fut + Send + 'static,
    Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
    Fut::Future: Send,
    T: Send + 'static,
{
    pub(super) fn new(block: Block, init: T, f: Arc<F>) -> FoldBlock<T, F, Fut> {
        let block = Arc::new(block);
        FoldBlock {
            state: State::Ready(init),
            rows: rows(block),
            f,
        }
    }
}

impl<T, F, Fut> Future for FoldBlock<T, F, Fut>
where
    F: Fn(T, Row) -> Fut + Send + 'static,
    Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
    Fut::Future: Send,
    T: Send + 'static,
{
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        loop {
            let mut state = mem::replace(&mut self.state, State::Empty);

            match state {
                State::Empty => unreachable!(),
                State::Ready(acc) => match self.rows.next() {
                    None => return Ok(Async::Ready(acc)),
                    Some(row) => {
                        self.state = State::Run((self.f)(acc, row).into_future());
                    }
                },
                State::Run(ref mut inner) => {
                    let row = try_ready!(inner.poll());
                    self.state = State::Ready(row);
                }
            }
        }
    }
}

fn rows(block: Arc<Block>) -> Rows<'static> {
    Rows {
        row: 0,
        block_ref: BlockRef::Owned(block),
    }
}
