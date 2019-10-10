use tokio::prelude::*;

use crate::{
    errors::Error,
    types::block::{Block, BlockRef, Row, Rows},
};

use std::{mem, sync::Arc, marker};
use crate::types::Simple;

enum State<T, Fut> {
    Empty,
    Ready(T),
    Run(Fut),
}

pub(super) struct FoldBlock<T, F, Fut>
where
    F: Fn(T, Row<Simple>) -> Fut + Send + 'static,
    Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
    Fut::Future: Send,
    T: Send + 'static,
{
    state: State<T, Fut::Future>,
    f: Arc<F>,
    rows: Rows<'static, Simple>,
}

impl<T, F, Fut> FoldBlock<T, F, Fut>
where
    F: Fn(T, Row<Simple>) -> Fut + Send + 'static,
    Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
    Fut::Future: Send,
    T: Send + 'static,
{
    pub(super) fn new(block: Block, init: T, f: Arc<F>) -> FoldBlock<T, F, Fut> {
        let block = Arc::new(block);
        Self {
            state: State::Ready(init),
            rows: rows(block),
            f,
        }
    }
}

impl<T, F, Fut> Future for FoldBlock<T, F, Fut>
where
    F: Fn(T, Row<Simple>) -> Fut + Send + 'static,
    Fut: IntoFuture<Item = T, Error = Error> + Send + 'static,
    Fut::Future: Send,
    T: Send + 'static,
{
    type Item = T;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let mut state;
        loop {
            state = mem::replace(&mut self.state, State::Empty);

            match state {
                State::Empty => unreachable!(),
                State::Ready(acc) => match self.rows.next() {
                    None => return Ok(Async::Ready(acc)),
                    Some(row) => {
                        self.state = State::Run((self.f)(acc, row).into_future());
                    }
                },
                State::Run(ref mut inner) => match inner.poll() {
                    Ok(Async::Ready(item)) => self.state = State::Ready(item),
                    Ok(Async::NotReady) => break,
                    Err(e) => return Err(e),
                },
            }
        }

        self.state = state;
        Ok(Async::NotReady)
    }
}

fn rows(block: Arc<Block>) -> Rows<'static, Simple> {
    Rows {
        row: 0,
        block_ref: BlockRef::Owned(block),
        kind: marker::PhantomData,
    }
}
