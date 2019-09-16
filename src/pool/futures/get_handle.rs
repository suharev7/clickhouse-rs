use std::{future::Future, pin::Pin};

use futures_core::{Poll, task::Context};

use pin_project::pin_project;

use crate::{ClientHandle, errors::Result, pool::Pool};

#[pin_project]
pub struct GetHandle {
    #[pin]
    pool: Pool,
}

impl GetHandle {
    pub fn new(pool: &Pool) -> Self {
        Self { pool: pool.clone() }
    }
}

impl Future for GetHandle {
    type Output = Result<ClientHandle>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().pool.poll(cx) {
            Poll::Ready(Ok(h)) => Poll::Ready(Ok(h)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
