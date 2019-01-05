use tokio::prelude::*;

use crate::errors::Error;
use crate::pool::Pool;
use crate::ClientHandle;

pub struct GetHandle {
    pool: Pool,
}

impl GetHandle {
    pub fn new(pool: &Pool) -> Self {
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
