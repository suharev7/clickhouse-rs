use std::{future::Future, pin::Pin};

use futures_core::{task::Context, Poll};

use pin_project::pin_project;

use crate::{errors::Result, pool::Pool, ClientHandle};

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

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().pool.poll(cx)
    }
}
