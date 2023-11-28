use std::{
    fmt, mem,
    pin::Pin,
    sync::atomic::{self, Ordering},
    sync::Arc,
    task::{Context, Poll, Waker},
};

use futures_util::future::BoxFuture;
use log::{error, warn};

use crate::{
    errors::Result,
    types::{IntoOptions, OptionsSource},
    Client, ClientHandle,
};

pub use self::futures::GetHandle;
use futures_util::FutureExt;
use url::Url;

mod futures;

pub(crate) struct Inner {
    new: crossbeam::queue::ArrayQueue<BoxFuture<'static, Result<ClientHandle>>>,
    idle: crossbeam::queue::ArrayQueue<ClientHandle>,
    tasks: crossbeam::queue::SegQueue<Waker>,
    ongoing: atomic::AtomicUsize,
    hosts: Vec<Url>,
    connections_num: atomic::AtomicUsize,
}

impl Inner {
    pub(crate) fn release_conn(&self) {
        self.ongoing.fetch_sub(1, Ordering::AcqRel);
        while let Some(task) = self.tasks.pop() {
            task.wake()
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

        for host in &hosts {
            if host.port() == Some(8123) {
                warn!(
                    "The attempt to establish a connection through the text protocol. clickhouse-rs is for using the binary protocol."
                );
                break;
            }
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

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<ClientHandle>> {
        self.handle_futures(cx)?;

        match self.take_conn() {
            Some(client) => Poll::Ready(Ok(client)),
            None => {
                let new_conn_created = {
                    let conn_count = self.inner.conn_count();

                    if conn_count < self.max && self.inner.new.push(self.new_connection()).is_ok() {
                        true
                    } else {
                        self.inner.tasks.push(cx.waker().clone());
                        false
                    }
                };
                if new_conn_created {
                    self.poll(cx)
                } else {
                    Poll::Pending
                }
            }
        }
    }

    fn new_connection(&self) -> BoxFuture<'static, Result<ClientHandle>> {
        let source = self.options.clone();
        let pool = Some(self.clone());
        Box::pin(async move { Client::open(source, pool).await })
    }

    fn handle_futures(&mut self, cx: &mut Context<'_>) -> Result<()> {
        if let Some(mut new) = self.inner.new.pop() {
            match new.poll_unpin(cx) {
                Poll::Ready(Ok(client)) => {
                    self.inner.idle.push(client).unwrap();
                }
                Poll::Pending => {
                    // NOTE: it is okay to drop the construction task
                    // because another construction will be attempted
                    // later in Pool::poll
                    let _ = self.inner.new.push(new);
                }
                Poll::Ready(Err(err)) => {
                    return Err(err);
                }
            }
        }

        Ok(())
    }

    fn take_conn(&mut self) -> Option<ClientHandle> {
        if let Some(mut client) = self.inner.idle.pop() {
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

        if self.inner.idle.len() < min && is_attached && client.inner.is_some() {
            let _ = self.inner.idle.push(client);
        }
        self.inner.ongoing.fetch_sub(1, Ordering::AcqRel);

        while let Some(task) = self.inner.tasks.pop() {
            task.wake()
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

#[cfg(feature = "tokio_io")]
#[cfg(test)]
mod test {
    use std::{
        str::FromStr,
        time::{Duration, Instant},
    };

    use futures_util::future;

    use crate::{errors::Result, test_misc::DATABASE_URL, Block, Options};

    use super::Pool;
    use url::Url;

    #[tokio::test]
    async fn test_connect() -> Result<()> {
        let options = Options::from_str(DATABASE_URL.as_str()).unwrap();
        let pool = Pool::new(options);
        {
            let mut c = pool.get_handle().await?;
            c.ping().await?;
        }

        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.idle_len, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_detach() -> Result<()> {
        async fn done(pool: Pool) -> Result<()> {
            let p = pool.clone();
            let mut c = p.get_handle().await?;
            c.ping().await?;
            c.pool.detach();
            Ok(())
        }

        let pool = Pool::new(DATABASE_URL.as_str());
        done(pool.clone()).await?;
        assert_eq!(pool.info().idle_len, 0);

        Ok(())
    }

    #[tokio::test]
    async fn test_many_connection() -> Result<()> {
        let options = Options::from_str(DATABASE_URL.as_str())
            .unwrap()
            .pool_min(6)
            .pool_max(12);
        let pool = Pool::new(options);

        async fn exec_query(pool: &Pool) -> Result<u32> {
            let mut c = pool.get_handle().await?;
            let block = c.query("SELECT toUInt32(1), sleep(1)").fetch_all().await?;

            let value: u32 = block.get(0, 0)?;
            Ok(value)
        }

        let expected = 22_u32;

        let start = Instant::now();

        let mut requests = Vec::new();
        for _ in 0..expected as usize {
            requests.push(exec_query(&pool))
        }

        let xs = future::join_all(requests).await;
        let mut actual: u32 = 0;

        for x in xs {
            actual += x?;
        }
        assert_eq!(actual, expected);

        let spent = start.elapsed();

        assert!(spent >= Duration::from_millis(2000));
        assert!(spent < Duration::from_millis(2500));

        assert_eq!(pool.info().idle_len, 6);
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_insert() -> Result<()> {
        let pool = Pool::new(DATABASE_URL.as_str());
        {
            let block = Block::new();
            let mut c = pool.get_handle().await?;
            c.insert("unexisting", block).await.unwrap_err();
        }
        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_wrong_execute() -> Result<()> {
        let pool = Pool::new(DATABASE_URL.as_str());
        {
            let mut c = pool.get_handle().await?;
            c.execute("DROP TABLE unexisting").await.unwrap_err();
        }
        let info = pool.info();
        assert_eq!(info.ongoing, 0);
        assert_eq!(info.tasks_len, 0);
        assert_eq!(info.idle_len, 0);
        Ok(())
    }

    #[test]
    fn test_get_addr() {
        let options =
            Options::from_str("tcp://host1:9000?alt_hosts=host2:9000,host3:9000").unwrap();
        let pool = Pool::new(options);

        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host1:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host2:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host3:9000").unwrap());
        assert_eq!(pool.get_addr(), &Url::from_str("tcp://host1:9000").unwrap())
    }
}
