use std::{fmt, io};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::prelude::*;
use tokio::prelude::task::{self, Task};

use crate::Client;
use crate::pool::futures::GetHandle;

use crate::ClientHandle;
use crate::io::IoFuture;
use crate::Options;

mod futures;

struct Inner {
    new: Vec<IoFuture<ClientHandle>>,
    idle: Vec<ClientHandle>,
    tasks: Vec<Task>,
    ongoing: usize,
}

impl Inner {
    fn conn_count(&self) -> usize {
        self.new.len() + self.idle.len() + self.ongoing
    }
}

#[derive(Clone)]
pub struct Pool {
    options: Options,
    inner: Arc<Mutex<Inner>>,
    min: usize,
    max: usize,
}

struct PoolInfo {
    new_len: usize,
    idle_len: usize,
    tasks_len: usize,
    ongoing: usize,
}

impl fmt::Debug for Pool {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let info = self.info();
        f.debug_struct("Pool")
            .field("min", &self.min)
            .field("max", &self.max)
            .field("new connections count", &info.new_len)
            .field("idle connections count", &info.idle_len)
            .field("tasks count", &info.tasks_len)
            .field("ongoing connections count", &info.ongoing)
            .finish()
    }
}

impl Pool {
    pub fn new(options: Options) -> Pool {
        let inner = Arc::new(Mutex::new(Inner {
            new: Vec::new(),
            idle: Vec::new(),
            tasks: Vec::new(),
            ongoing: 0,
        }));

        Pool {
            options,
            inner,
            min: 5,
            max: 10,
        }
    }

    fn info(&self) -> PoolInfo {
        self.with_inner(|inner| PoolInfo {
            new_len: inner.new.len(),
            idle_len: inner.idle.len(),
            tasks_len: inner.tasks.len(),
            ongoing: inner.ongoing,
        })
    }

    /// Returns future that resolves to `ClientHandle`.
    pub fn get_handle(&self) -> GetHandle {
        GetHandle::new(self)
    }

    fn with_inner<F, T>(&self, fun: F) -> T
    where
        F: FnOnce(MutexGuard<Inner>) -> T,
        T: 'static,
    {
        fun(self.inner.lock().unwrap())
    }

    fn poll(&mut self) -> io::Result<Async<ClientHandle>> {
        self.handle_futures()?;

        match self.take_conn() {
            Some(client) => Ok(Async::Ready(client)),
            None => {
                let new_conn_created = self.with_inner(|mut inner| {
                    if inner.new.len() == 0 && inner.conn_count() < self.max {
                        inner.new.push(self.new_connection());
                        true
                    } else {
                        inner.tasks.push(task::current());
                        false
                    }
                });
                match new_conn_created {
                    true => self.poll(),
                    false => Ok(Async::NotReady),
                }
            }
        }
    }

    fn new_connection(&self) -> IoFuture<ClientHandle> {
        Client::open(self.options.clone())
    }

    fn handle_futures(&mut self) -> io::Result<()> {
        let len = self.with_inner(|inner| inner.new.len());

        for i in 0..len {
            let result = self.with_inner(|mut inner| inner.new[i].poll());
            match result {
                Ok(Async::Ready(client)) => {
                    self.with_inner(|mut inner| {
                        inner.new.swap_remove(i);
                        inner.idle.push(client)
                    });
                }
                Ok(Async::NotReady) => (),
                Err(err) => {
                    self.with_inner(|mut inner| inner.new.swap_remove(i));
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    fn take_conn(&mut self) -> Option<ClientHandle> {
        self.with_inner(|mut inner| {
            while let Some(mut client) = inner.idle.pop() {
                client.pool = Some(self.clone());
                inner.ongoing += 1;
                return Some(client);
            }
            None
        })
    }

    fn return_conn(&mut self, mut client: ClientHandle) {
        let min = self.min;

        self.with_inner(|mut inner| {
            inner.ongoing -= 1;
            if inner.idle.len() < min {
                inner.idle.push(client);
            } else {
                client.pool = None;
            }

            while let Some(task) = inner.tasks.pop() {
                task.notify()
            }
        })
    }
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        match (self.pool.take(), self.inner.take()) {
            (Some(mut pool), Some(inner)) => {
                let context = self.context.clone();
                let client = ClientHandle {
                    inner: Some(inner),
                    context,
                    pool: Some(pool.clone()),
                };
                pool.return_conn(client);
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod test {
    use tokio::prelude::*;

    use crate::Options;

    use super::Pool;
    use crate::io::IoFuture;

    use std::time::{Duration, Instant};

    const HOST: &str = "127.0.0.1:9000";

    /// Same as `tokio::run`, but will panic if future panics and will return the result
    /// of future execution.
    fn run<F, T, U>(future: F) -> Result<T, U>
    where
        F: Future<Item = T, Error = U> + Send + 'static,
        T: Send + 'static,
        U: Send + 'static,
    {
        let mut runtime = tokio::runtime::Runtime::new().unwrap();
        let result = runtime.block_on(future);
        runtime.shutdown_on_idle().wait().unwrap();
        result
    }

    #[test]
    fn test_connect() {
        let options = Options::new(HOST.parse().unwrap());
        let pool = Pool::new(options);

        let done = pool
            .get_handle()
            .and_then(|c| c.ping().map(|_| ()))
            .and_then(move |_| {
                let info = pool.info();
                assert_eq!(info.ongoing, 0);
                assert_eq!(info.idle_len, 1);
                Ok(())
            });

        run(done).unwrap()
    }

    #[test]
    fn test_many_connection() {
        let options = Options::new(HOST.parse().unwrap())
            .pool_min(5)
            .pool_max(10);
        let pool = Pool::new(options);

        fn exec_query(pool: Pool) -> IoFuture<u32> {
            Box::new(
            pool.get_handle()
                .and_then(|c| c.query_all("SELECT toUInt32(1), sleep(1)"))
                .and_then(|(_, block)| {
                    let value: u32 = block.get(0, 0)?;
                    Ok(value)
                })
            )
        }

        let expected = 20_u32;

        let start = Instant::now();

        let mut requests = Vec::new();
        for _ in 0..expected as usize {
            requests.push(exec_query(pool.clone()))
        }

        let done = future::join_all(requests)
            .and_then(move |xs| {
                let actual: u32 = xs.iter().sum();
                assert_eq!(actual, expected);
                Ok(())
            });

        run(done).unwrap();

        let spent = start.elapsed();

        assert!(spent >= Duration::from_millis(2000));
        assert!(spent < Duration::from_millis(2500));

        assert_eq!(pool.info().idle_len, pool.min);
    }
}
