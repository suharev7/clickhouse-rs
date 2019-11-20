use tokio::prelude::*;

use crate::{errors::Error, pool::Pool, ClientHandle};

/// Future that resolves to a `ClientHandle`.
pub struct GetHandle {
    pool: Pool,
}

impl GetHandle {
    pub(crate) fn new(pool: &Pool) -> Self {
        Self { pool: pool.clone() }
    }
}

impl Future for GetHandle {
    type Item = ClientHandle;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.pool.poll()
    }
}
