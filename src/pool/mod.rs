use std::{
    fmt, mem,
    sync::{Arc, Mutex, MutexGuard},
};

use tokio::prelude::*;
use tokio::prelude::task::{self, Task};

use crate::{
    Client,
    ClientHandle,
    io::BoxFuture,
    pool::futures::GetHandle, types::{ClickhouseResult, IntoOptions, OptionsSource},
};

mod futures;

struct Inner {
    new: Vec<BoxFuture<ClientHandle>>,
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
pub(crate) enum PoolBinding {
    None,
    Attached(Pool),
    Detached(Pool),
}

impl From<PoolBinding> for Option<Pool> {
    fn from(binding: PoolBinding) -> Self {
        match binding {
            PoolBinding::None => None,
            PoolBinding::Attached(pool) | PoolBinding::Detached(pool) => Some(pool),
        }
    }
}

impl PoolBinding {
    pub(crate) fn take(&mut self) -> Self {
        mem::replace(self, PoolBinding::None)
    }

    fn return_conn(self, client: ClientHandle) {
        if let Some(mut pool) = self.into() {
            Pool::return_conn(&mut pool, client);
        }
    }

    pub(crate) fn release_conn(self) {
        if let Some(mut pool) = self.into() {
            Pool::release_conn(&mut pool);
        }
    }

    pub(crate) fn is_attached(&self) -> bool {
        match self {
            PoolBinding::Attached(_) => true,
            _ => false,
        }
    }

    pub(crate) fn is_some(&self) -> bool {
        match self {
            PoolBinding::None => false,
            _ => true,
        }
    }

    pub(crate) fn attach(&mut self) {
        match self.take() {
            PoolBinding::Detached(pool) => *self = PoolBinding::Attached(pool),
            _ => unreachable!(),
        }
    }

    pub(crate) fn detach(&mut self) {
        match self.take() {
            PoolBinding::Attached(pool) => *self = PoolBinding::Detached(pool),
            _ => unreachable!(),
        }
    }
}

/// Asynchronous pool of Clickhouse connections.
#[derive(Clone)]
pub struct Pool {
    options: OptionsSource,
    inner: Arc<Mutex<Inner>>,
    min: usize,
    max: usize,
}

#[derive(Debug)]
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
    /// Constructs a new Pool.
    pub fn new<O>(options: O) -> Self
    where
        O: IntoOptions,
    {
        let inner = Arc::new(Mutex::new(Inner {
            new: Vec::new(),
            idle: Vec::new(),
            tasks: Vec::new(),
            ongoing: 0,
        }));

        Self {
            options: options.into_options_src(),
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

    fn poll(&mut self) -> ClickhouseResult<Async<ClientHandle>> {
        self.handle_futures()?;

        match self.take_conn() {
            Some(client) => Ok(Async::Ready(client)),
            None => {
                let new_conn_created = self.with_inner(|mut inner| {
                    if inner.new.is_empty() && inner.conn_count() < self.max {
                        inner.new.push(self.new_connection());
                        true
                    } else {
                        inner.tasks.push(task::current());
                        false
                    }
                });
                if new_conn_created {
                    self.poll()
                } else {
                    Ok(Async::NotReady)
                }
            }
        }
    }

    fn new_connection(&self) -> BoxFuture<ClientHandle> {
        Client::open(&self.options)
    }

    fn handle_futures(&mut self) -> ClickhouseResult<()> {
        self.with_inner(|mut inner| {
            let len = inner.new.len();

            for i in 0..len {
                let result = inner.new[i].poll();
                match result {
                    Ok(Async::Ready(client)) => {
                        inner.new.swap_remove(i);
                        inner.idle.push(client)
                    }
                    Ok(Async::NotReady) => (),
                    Err(err) => {
                        inner.new.swap_remove(i);
                        return Err(err);
                    }
                }
            }

            Ok(())
        })
    }

    fn take_conn(&mut self) -> Option<ClientHandle> {
        self.with_inner(|mut inner| {
            if let Some(mut client) = inner.idle.pop() {
                client.pool = PoolBinding::Attached(self.clone());
                inner.ongoing += 1;
                Some(client)
            } else {
                None
            }
        })
    }

    fn return_conn(&mut self, mut client: ClientHandle) {
        let min = self.min;

        self.with_inner(|mut inner| {
            inner.ongoing -= 1;
            if inner.idle.len() < min && client.pool.is_attached() {
                inner.idle.push(client);
            } else {
                client.pool = PoolBinding::None;
            }

            while let Some(task) = inner.tasks.pop() {
                task.notify()
            }
        })
    }

    fn release_conn(&mut self) {
        self.with_inner(|mut inner| {
            inner.ongoing -= 1;

            while let Some(task) = inner.tasks.pop() {
                task.notify()
            }
        })
    }
}

impl Drop for ClientHandle {
    fn drop(&mut self) {
        if let (pool, Some(inner)) = (self.pool.take(), self.inner.take()) {
            if !pool.is_some() {
                return;
            }

            let context = self.context.clone();
            let client = Self {
                inner: Some(inner),
                pool: pool.clone(),
                context,
            };
            pool.return_conn(client);
        }
    }
}

#[cfg(test)]
mod test {
    use std::{
        str::FromStr,
        sync::{Arc, atomic::{AtomicBool, Ordering}},
        thread::spawn,
        time::{Duration, Instant},
    };

    use tokio::prelude::*;

    use crate::{errors::Error, io::BoxFuture, test_misc::DATABASE_URL, types::Options};

    use super::Pool;

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
        let options = Options::from_str(DATABASE_URL.as_str()).unwrap();
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

        run(done).unwrap();
    }

    #[test]
    fn test_detach() {
        let pool = Pool::new(DATABASE_URL.as_str());

        let done = pool.get_handle().and_then(|c| c.ping()).and_then(|mut c| {
            c.pool.detach();
            Ok(())
        });

        run(done).unwrap();
        assert_eq!(pool.info().idle_len, 0);
    }

    #[test]
    fn test_many_connection() {
        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_min(5)
            .pool_max(10);
        let pool = Pool::new(options);

        fn exec_query(pool: &Pool) -> BoxFuture<u32> {
            Box::new(
                pool.get_handle()
                    .and_then(|c| c.query("SELECT toUInt32(1), sleep(1)").fetch_all())
                    .and_then(|(_, block)| {
                        let value: u32 = block.get(0, 0)?;
                        Ok(value)
                    }),
            )
        }

        let expected = 20_u32;

        let start = Instant::now();

        let mut requests = Vec::new();
        for _ in 0..expected as usize {
            requests.push(exec_query(&pool))
        }

        let done = future::join_all(requests).and_then(move |xs| {
            let actual: u32 = xs.iter().sum();
            assert_eq!(actual, expected);
            Ok(())
        });

        run(done).unwrap();

        let spent = start.elapsed();

        assert!(spent >= Duration::from_millis(2000));
        assert!(spent < Duration::from_millis(2500));

        assert_eq!(pool.info().idle_len, 5);
    }

    #[test]
    fn test_error_in_fold() {
        let pool = Pool::new(DATABASE_URL.as_str());

        let done = pool.get_handle().and_then(|c| {
            c.query("SELECT 1").fold((), |_, _| {
                future::err("[test_error_in_fold] It's fine.".into())
            })
        });

        run(done).unwrap_err();

        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
    }

    #[test]
    fn test_race_condition() {
        use tokio::runtime::current_thread;
        use futures::future::lazy;

        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_min(80)
            .pool_max(99);
        let pool = Pool::new(options);

        let done = future::lazy(move || {
            let mut threads = Vec::new();
            let barer = Arc::new(AtomicBool::new(true));

            for _ in 0..100 {
                let local_barer = barer.clone();
                let mut local_pool = pool.clone();

                let thread = spawn(|| {
                    current_thread::block_on_all(lazy(|| {
                        current_thread::spawn(lazy(move || {
                            while local_barer.load(Ordering::SeqCst) {
                            }

                            match local_pool.poll() {
                                Ok(_) => Ok(()),
                                Err(_) => Err(()),
                            }
                        }));

                        Ok::<_, Error>(())
                    }))
                });
                threads.push(thread);
            }

            barer.store(false, Ordering::SeqCst);
            Ok(threads)
        }).and_then(|threads| {

            for thread in threads {
                match thread.join() {
                    Ok(_) => {},
                    Err(_) => return Err(())
                }
            }

            Ok(())
        });

        run(done).unwrap();
    }
}
