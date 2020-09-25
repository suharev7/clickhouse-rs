use std::{
    fmt, mem, sync::atomic::{self, Ordering},
    sync::Arc,
};

use tokio::prelude::{*, task::{self, Task}};

use crate::{
    io::BoxFuture,
    Client, ClientHandle,
    errors::Result,
    types::{IntoOptions, OptionsSource},
};

pub use self::futures::GetHandle;
use url::Url;

mod futures;

pub(crate) struct Inner {
    new: crossbeam::queue::ArrayQueue<BoxFuture<ClientHandle>>,
    idle: crossbeam::queue::ArrayQueue<ClientHandle>,
    tasks: crossbeam::queue::SegQueue<Task>,
    ongoing: atomic::AtomicUsize,
    hosts: Vec<Url>,
    connections_num: atomic::AtomicUsize,
}

impl Inner {
    pub(crate) fn release_conn(&self) {
        self.ongoing.fetch_sub(1, Ordering::AcqRel);
        while let Ok(task) = self.tasks.pop() {
            task.notify()
        }
    }

    fn conn_count(&self) -> usize {
        let is_new_some = self.new.len();
        let ongoing = self.ongoing.load(Ordering::Acquire);
        let idle_count = self.idle.len();
        is_new_some + idle_count + ongoing
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

    pub(crate) fn is_attached(&self) -> bool {
        matches!(self, PoolBinding::Attached(_))
    }

    pub(crate) fn is_some(&self) -> bool {
        !matches!(self, PoolBinding::None)
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
    pub(crate) inner: Arc<Inner>,
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
        let options_src = options.into_options_src();

        let mut min = 5;
        let mut max = 10;
        let mut hosts = vec![];

        match options_src.get() {
            Ok(opt) => {
                min = opt.pool_min;
                max = opt.pool_max;
                hosts.push(opt.addr.clone());
                hosts.extend(opt.alt_hosts.iter().cloned());
            }
            Err(err) => error!("{}", err),
        }

        let inner = Arc::new(Inner {
            new: crossbeam::queue::ArrayQueue::new(1),
            idle: crossbeam::queue::ArrayQueue::new(max),
            tasks: crossbeam::queue::SegQueue::new(),
            ongoing: atomic::AtomicUsize::new(0),
            connections_num: atomic::AtomicUsize::new(0),
            hosts,
        });

        Self {
            options: options_src,
            inner,
            min,
            max,
        }
    }

    fn info(&self) -> PoolInfo {
        PoolInfo {
            new_len: self.inner.new.len(),
            idle_len: self.inner.idle.len(),
            tasks_len: self.inner.tasks.len(),
            ongoing: self.inner.ongoing.load(Ordering::Acquire),
        }
    }

    /// Returns future that resolves to `ClientHandle`.
    pub fn get_handle(&self) -> GetHandle {
        GetHandle::new(self)
    }

    fn poll(&mut self) -> Result<Async<ClientHandle>> {
        self.handle_futures()?;

        match self.take_conn() {
            Some(client) => Ok(Async::Ready(client)),
            None => {
                let new_conn_created = {
                    let conn_count = self.inner.conn_count();

                    if conn_count < self.max && self.inner.new.push(self.new_connection()).is_ok() {
                        true
                    } else {
                        self.inner.tasks.push(task::current());
                        false
                    }
                };
                if new_conn_created {
                    self.poll()
                } else {
                    Ok(Async::NotReady)
                }
            }
        }
    }

    fn new_connection(&self) -> BoxFuture<ClientHandle> {
        Box::new(Client::open(&self.options, Some(self.clone())))
    }

    fn handle_futures(&mut self) -> Result<()> {
        let inner = &self.inner;
        if let Ok(mut new) = self.inner.new.pop() {
            match new.poll() {
                Ok(Async::Ready(client)) => {
                    inner.idle.push(client).unwrap();
                }
                Ok(Async::NotReady) => {
                    // NOTE: it is okay to drop the construction task
                    // because another construction will be attempted
                    // later in Pool::poll
                    let _ = self.inner.new.push(new);
                },
                Err(err) => {
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    fn take_conn(&mut self) -> Option<ClientHandle> {
        if let Ok(mut client) = self.inner.idle.pop() {
            client.pool = PoolBinding::Attached(self.clone());
            client.set_inside(false);
            self.inner.ongoing.fetch_add(1, Ordering::AcqRel);
            Some(client)
        } else {
            None
        }
    }

    fn return_conn(&mut self, mut client: ClientHandle) {
        let min = self.min;

        let is_attached = client.pool.is_attached();
        client.pool = PoolBinding::None;
        client.set_inside(true);

        if self.inner.idle.len() < min && is_attached {
            let _ = self.inner.idle.push(client);
        }
        self.inner.ongoing.fetch_sub(1, Ordering::AcqRel);

        while let Ok(task) = self.inner.tasks.pop() {
            task.notify()
        }
    }

    pub(crate) fn get_addr(&self) -> &Url {
        let n = self.inner.hosts.len();
        let index = self.inner.connections_num.fetch_add(1, Ordering::SeqCst);
        &self.inner.hosts[index % n]
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
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread::spawn,
        time::{Duration, Instant},
    };

    use tokio::prelude::*;

    use crate::{
        errors::Error,
        io::BoxFuture,
        test_misc::DATABASE_URL,
        types::{Block, Options},
        ClientHandle,
    };

    use super::Pool;
    use url::Url;

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
            .and_then(|c| {
                c.ping().map(|_| ())
            })
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

        let done = pool
            .get_handle()
            .and_then(ClientHandle::ping)
            .and_then(|mut c| {
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
            .pool_min(6)
            .pool_max(12);
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

        let expected = 22_u32;

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

        assert_eq!(pool.info().idle_len, 6);
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

    #[allow(clippy::all)]
    #[test]
    fn test_race_condition() {
        use futures::future::lazy;
        use tokio::runtime::current_thread;

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

                threads.push(spawn(|| {
                    current_thread::block_on_all(lazy(|| {
                        current_thread::spawn(lazy(move || {
                            while local_barer.load(Ordering::SeqCst) {}

                            match local_pool.poll() {
                                Ok(_) => Ok(()),
                                Err(_) => Err(()),
                            }
                        }));

                        Ok::<_, Error>(())
                    }))
                }));
            }

            barer.store(false, Ordering::SeqCst);
            Ok(threads)
        })
        .and_then(|threads| {
            for thread in threads {
                match thread.join() {
                    Ok(_) => {}
                    Err(_) => return Err(()),
                }
            }

            Ok(())
        });

        run(done).unwrap();
    }

    #[test]
    fn test_query_timeout() {
        let url = format!("{}{}", DATABASE_URL.as_str(), "&query_timeout=10ms");
        let pool = Pool::new(url);

        let done = pool
            .get_handle()
            .and_then(|c| c.query("SELECT sleep(10)").fetch_all());

        run(done).unwrap_err();

        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
    }

    #[test]
    fn test_query_stream_timeout() {
        let url = format!("{}{}", DATABASE_URL.as_str(), "&query_block_timeout=10ms");
        let pool = Pool::new(url);

        let done = pool
            .get_handle()
            .and_then(|c| {
                c.query("SELECT sleep(10)")
                    .stream_blocks()
                    .for_each(|block| {
                        println!("{:?}\nblock counts: {} rows", block, block.row_count());
                        Ok(())
                    })
            });

        run(done).unwrap_err();

        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
    }

    #[test]
    fn test_wrong_insert() {
        let pool = Pool::new(DATABASE_URL.as_str());

        let done = pool.get_handle().and_then(|c| {
            let block = Block::new();
            c.insert("unexisting", block)
        });

        run(done).unwrap_err();

        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
    }

    #[test]
    fn test_wrong_execute() {
        let pool = Pool::new(DATABASE_URL.as_str());

        let done = pool
            .get_handle()
            .and_then(|c| c.execute("DROP TABLE unexisting"));

        run(done).unwrap_err();

        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
    }

    #[test]
    fn test_wrong_ping() {
        let counter = Arc::new(AtomicUsize::new(0));

        let url = format!("{}{}", DATABASE_URL.as_str(), "&query_block_timeout=10ms");
        let pool = Pool::new(url);

        for i in 0..4 {
            let counter = counter.clone();
            let done = pool
                .get_handle()
                .and_then(move |c| c.ping())
                .and_then(move |_|{
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok(())
                })
                .map_err(|err| eprintln!("database error: {}", err));

            if i % 2 == 0 {
                run(done).unwrap();

                let info = pool.info();
                assert_eq!(info.ongoing, 0);
                assert_eq!(info.tasks_len, 0);
                assert_eq!(info.idle_len, 1);
            } else {
                run(done).unwrap_err();

                let info = pool.info();
                assert_eq!(info.ongoing, 0);
                assert_eq!(info.tasks_len, 0);
                assert_eq!(info.idle_len, 0);
            }
        }

        assert_eq!(2, counter.load(Ordering::Acquire))
    }

    #[test]
    fn test_get_addr() {
        let options = Options::from_str("tcp://host1:9000?alt_hosts=host2:9000,host3:9000").unwrap();
        let pool = Pool::new(options);

        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host1:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host2:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host3:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host1:9000").unwrap())
    }
}
