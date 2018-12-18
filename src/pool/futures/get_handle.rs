use std::io;

use tokio::prelude::*;

use ClientHandle;
use pool::Pool;

pub struct GetHandle {
    pool: Pool,
}

impl GetHandle {
    pub fn new(pool: &Pool) -> GetHandle {
        GetHandle { pool: pool.clone() }
    }
}

impl Future for GetHandle {
    type Item = ClientHandle;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.pool.poll()
    }
}
