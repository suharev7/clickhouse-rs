use std::net::SocketAddr;
use std::time::Duration;

const DEFAULT_MIN_CONNS: usize = 5;
const DEFAULT_MAX_CONNS: usize = 10;

/// Clickhouse connection options.
#[derive(Clone, Debug)]
pub struct Options {
    /// Address of clickhouse server (defaults to `127.0.0.1:9000`).
    pub(crate) addr: SocketAddr,

    /// Database name. (defaults to `default`).
    pub(crate) database: String,
    /// User name (defaults to `default`).
    pub(crate) username: String,
    /// Access password (defaults to `""`).
    pub(crate) password: String,

    /// Enable compression (defaults to `false`).
    pub(crate) compression: bool,

    /// Lower bound of opened connections for `Pool` (defaults to 5).
    pub(crate) pool_min: usize,
    /// Upper bound of opened connections for `Pool` (defaults to 10).
    pub(crate) pool_max: usize,

    /// Whether to enable `TCP_NODELAY` (defaults to `true`).
    pub(crate) nodelay: bool,
    /// TCP keep alive timeout in milliseconds (defaults to `None`).
    pub(crate) keepalive: Option<Duration>,

    /// Ping server every time before execute any query. (defaults to `true`)
    pub(crate) ping_before_query: bool,
    /// Count of retry to send request to server. (defaults to `3`)
    pub(crate) send_retries: usize,
    /// Amount of time to wait before next retry. (defaults to `5 sec`)
    pub(crate) retry_timeout: Duration,
    /// Timeout for ping (defaults to `500 ms`)
    pub(crate) ping_timeout: Duration,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            addr: "127.0.0.1:9000".parse().unwrap(),
            database: "default".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            compression: false,
            pool_min: DEFAULT_MIN_CONNS,
            pool_max: DEFAULT_MAX_CONNS,
            nodelay: true,
            keepalive: None,
            ping_before_query: true,
            send_retries: 3,
            retry_timeout: Duration::from_secs(5),
            ping_timeout: Duration::from_millis(500),
        }
    }
}

macro_rules! property {
    ( $k:ident: $t:ty ) => {
        pub fn $k(self, $k: $t) -> Options {
            Options {
                $k: $k.into(),
                ..self
            }
        }
    }
}

impl Options {
    pub fn new(addr: SocketAddr) -> Options {
        Options {
            addr,
            ..Options::default()
        }
    }

    /// Database name. (defaults to `default`).
    property!(database: &str);

    /// User name (defaults to `default`).
    property!(username: &str);

    /// Access password (defaults to `""`).
    property!(password: &str);

    /// Enable compression (defaults to `false`).
    pub fn with_compression(self) -> Options {
        Options {
            compression: true,
            ..self
        }
    }

    /// Lower bound of opened connections for `Pool` (defaults to 5).
    property!(pool_min: usize);

    /// Upper bound of opened connections for `Pool` (defaults to 10).
    property!(pool_max: usize);

    /// Whether to enable `TCP_NODELAY` (defaults to `true`).
    property!(nodelay: bool);

    /// TCP keep alive timeout in milliseconds (defaults to `None`).
    property!(keepalive: Option<Duration>);

    /// Ping server every time before execute any query. (defaults to `true`)
    property!(ping_before_query: bool);
    /// Count of retry to send request to server. (defaults to `3`)
    property!(send_retries: usize);
    /// Amount of time to wait before next retry. (defaults to `5 sec`)
    property!(retry_timeout: Duration);
    /// Timeout for ping (defaults to `500 ms`)
    property!(ping_timeout: Duration);
}
