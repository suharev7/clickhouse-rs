use std::{mem, pin::Pin, sync::Arc, future::Future, task::{Context, Poll}};

use pin_project::pin_project;

use crate::{
    errors::Result,
    types::block::{Block, BlockRef, Row, Rows},
};

enum State<T, Fut> {
    Empty,
    Ready(T),
    Run(Fut),
}

#[pin_project]
pub(super) struct FoldBlock<T, F, Fut>
where
    F: Fn(T, Row) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>>,
    T: Send + 'static,
{
    state: State<T, Fut>,
    f: Arc<F>,
    rows: Rows<'static>,
}

impl<T, F, Fut> FoldBlock<T, F, Fut>
where
    F: Fn(T, Row) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>>,
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
    F: Fn(T, Row) -> Fut + Send + 'static,
    Fut: Future<Output = Result<T>> + Unpin,
    T: Send + 'static,
{
    type Output = Result<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state;
        loop {
            state = mem::replace(&mut self.state, State::Empty);

            match state {
                State::Empty => unreachable!(),
                State::Ready(acc) => match self.rows.next() {
                    None => return Poll::Ready(Ok(acc)),
                    Some(row) => {
                        self.state = State::Run((self.f)(acc, row));
                    }
                },
                State::Run(ref mut inner) => {
                    match Pin::new(inner).poll(cx) {
                        Poll::Ready(Ok(row)) => self.state = State::Ready(row),
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Pending => break,
                    };
                }
            }
        }

        self.state = state;
        Poll::Pending
    }
}

fn rows(block: Arc<Block>) -> Rows<'static> {
    Rows {
        row: 0,
        block_ref: BlockRef::Owned(block),
    }
}
