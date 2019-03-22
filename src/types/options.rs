use std::{
    borrow::Cow,
    fmt, io,
    net::{SocketAddr, ToSocketAddrs},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
    vec,
};

use url::{percent_encoding::percent_decode, Url};

use crate::{
    errors::{Error, UrlError},
    types::ClickhouseResult,
};

const DEFAULT_MIN_CONNS: usize = 10;

const DEFAULT_MAX_CONNS: usize = 20;

#[derive(Debug)]
enum State {
    Raw(Options),
    Url(String),
}

#[derive(Clone)]
pub struct OptionsSource {
    state: Arc<Mutex<State>>,
}

impl fmt::Debug for OptionsSource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let guard = self.state.lock().unwrap();
        match *guard {
            State::Url(ref url) => write!(f, "Url({})", url),
            State::Raw(ref options) => write!(f, "{:?}", options),
        }
    }
}

impl OptionsSource {
    pub(crate) fn get(&self) -> ClickhouseResult<Cow<Options>> {
        let mut state = self.state.lock().unwrap();
        loop {
            let new_state;
            match &*state {
                State::Raw(ref options) => {
                    let ptr = options as *const Options;
                    return unsafe { Ok(Cow::Borrowed(ptr.as_ref().unwrap())) };
                }
                State::Url(url) => {
                    let options = from_url(&url)?;
                    new_state = State::Raw(options);
                }
            }
            *state = new_state;
        }
    }
}

impl Default for OptionsSource {
    fn default() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::Raw(Options::default()))),
        }
    }
}

pub trait IntoOptions {
    fn into_options_src(self) -> OptionsSource;
}

impl IntoOptions for Options {
    fn into_options_src(self) -> OptionsSource {
        OptionsSource {
            state: Arc::new(Mutex::new(State::Raw(self))),
        }
    }
}

impl IntoOptions for &str {
    fn into_options_src(self) -> OptionsSource {
        OptionsSource {
            state: Arc::new(Mutex::new(State::Url(self.into()))),
        }
    }
}

impl IntoOptions for String {
    fn into_options_src(self) -> OptionsSource {
        OptionsSource {
            state: Arc::new(Mutex::new(State::Url(self))),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum Address {
    Url(String),
    SocketAddr(SocketAddr),
}

impl From<SocketAddr> for Address {
    fn from(addr: SocketAddr) -> Self {
        Address::SocketAddr(addr)
    }
}

impl From<String> for Address {
    fn from(url: String) -> Self {
        Address::Url(url)
    }
}

impl From<&str> for Address {
    fn from(url: &str) -> Self {
        Address::Url(url.to_string())
    }
}

impl ToSocketAddrs for Address {
    type Iter = vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> Result<Self::Iter, io::Error> {
        match self {
            Address::SocketAddr(addr) => Ok(vec![*addr].into_iter()),
            Address::Url(url) => url.to_socket_addrs(),
        }
    }
}

/// Clickhouse connection options.
#[derive(Clone, Debug, PartialEq)]
pub struct Options {
    /// Address of clickhouse server (defaults to `127.0.0.1:9000`).
    pub(crate) addr: Address,

    /// Database name. (defaults to `default`).
    pub(crate) database: String,
    /// User name (defaults to `default`).
    pub(crate) username: String,
    /// Access password (defaults to `""`).
    pub(crate) password: String,

    /// Enable compression (defaults to `false`).
    pub(crate) compression: bool,

    /// Lower bound of opened connections for `Pool` (defaults to 10).
    pub(crate) pool_min: usize,
    /// Upper bound of opened connections for `Pool` (defaults to 20).
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

    /// Timeout for connection (defaults to `500 ms`)
    pub(crate) connection_timeout: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            addr: Address::SocketAddr("127.0.0.1:9000".parse().unwrap()),
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
            connection_timeout: Duration::from_millis(500),
        }
    }
}

macro_rules! property {
    ( $k:ident: $t:ty ) => {
        pub fn $k(self, $k: $t) -> Self {
            Self {
                $k: $k.into(),
                ..self
            }
        }
    };
    ( $(#[$attr:meta])* => $k:ident: $t:ty ) => {
        $(#[$attr])*
        pub fn $k(self, $k: $t) -> Self {
            Self {
                $k: $k.into(),
                ..self
            }
        }
    }
}

impl Options {
    /// Constructs a new Options.
    pub fn new<A>(addr: A) -> Self
    where
        Address: From<A>,
    {
        Self {
            addr: From::from(addr),
            ..Self::default()
        }
    }

    property! {
        /// Database name. (defaults to `default`).
        => database: &str
    }

    property! {
        /// User name (defaults to `default`).
        => username: &str
    }

    property! {
        /// Access password (defaults to `""`).
        => password: &str
    }

    /// Enable compression (defaults to `false`).
    pub fn with_compression(self) -> Self {
        Self {
            compression: true,
            ..self
        }
    }

    property! {
        /// Lower bound of opened connections for `Pool` (defaults to `10`).
        => pool_min: usize
    }

    property! {
        /// Upper bound of opened connections for `Pool` (defaults to `20`).
        => pool_max: usize
    }

    property! {
        /// Whether to enable `TCP_NODELAY` (defaults to `true`).
        => nodelay: bool
    }

    property! {
        /// TCP keep alive timeout in milliseconds (defaults to `None`).
        => keepalive: Option<Duration>
    }

    property! {
        /// Ping server every time before execute any query. (defaults to `true`).
        => ping_before_query: bool
    }

    property! {
        /// Count of retry to send request to server. (defaults to `3`).
        => send_retries: usize
    }

    property! {
        /// Amount of time to wait before next retry. (defaults to `5 sec`).
        => retry_timeout: Duration
    }

    property! {
        /// Timeout for ping (defaults to `500 ms`).
        => ping_timeout: Duration
    }

    property! {
        /// Timeout for connection (defaults to `500 ms`).
        => connection_timeout: Duration
    }
}

impl FromStr for Options {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self, Error> {
        from_url(url)
    }
}

fn from_url(url_str: &str) -> ClickhouseResult<Options> {
    let url = Url::parse(url_str)?;

    if url.scheme() != "tcp" {
        return Err(UrlError::UnsupportedScheme {
            scheme: url.scheme().to_string(),
        }
        .into());
    }

    if url.cannot_be_a_base() || !url.has_host() {
        return Err(UrlError::Invalid.into());
    }

    let mut options = Options::default();

    if let Some(username) = get_username_from_url(&url)? {
        options.username = username;
    }

    if let Some(password) = get_password_from_url(&url)? {
        options.password = password
    }

    let host = url
        .host_str()
        .map_or_else(|| "127.0.0.1".into(), String::from);

    let port = url.port().unwrap_or(9000);
    options.addr = format!("{}:{}", host, port).into();

    if let Some(database) = get_database_from_url(&url)? {
        options.database = database;
    }

    set_params(&mut options, url.query_pairs())?;

    Ok(options)
}

fn set_params<'a, I>(options: &mut Options, iter: I) -> std::result::Result<(), UrlError>
where
    I: Iterator<Item = (Cow<'a, str>, Cow<'a, str>)>,
{
    for (key, value) in iter {
        match key.as_ref() {
            "pool_min" => options.pool_min = parse_param(key, value, usize::from_str)?,
            "pool_max" => options.pool_max = parse_param(key, value, usize::from_str)?,
            "nodelay" => options.nodelay = parse_param(key, value, bool::from_str)?,
            "keepalive" => options.keepalive = parse_param(key, value, parse_opt_duration)?,
            "ping_before_query" => {
                options.ping_before_query = parse_param(key, value, bool::from_str)?
            }
            "send_retries" => options.send_retries = parse_param(key, value, usize::from_str)?,
            "retry_timeout" => options.retry_timeout = parse_param(key, value, parse_duration)?,
            "ping_timeout" => options.ping_timeout = parse_param(key, value, parse_duration)?,
            "connection_timeout" => {
                options.connection_timeout = parse_param(key, value, parse_duration)?
            }
            "compression" => options.compression = parse_param(key, value, parse_compression)?,
            _ => return Err(UrlError::UnknownParameter { param: key.into() }),
        };
    }

    Ok(())
}

fn parse_param<'a, F, T, E>(
    param: Cow<'a, str>,
    value: Cow<'a, str>,
    parse: F,
) -> std::result::Result<T, UrlError>
where
    F: Fn(&str) -> std::result::Result<T, E>,
{
    match parse(value.as_ref()) {
        Ok(value) => Ok(value),
        Err(_) => Err(UrlError::InvalidParamValue {
            param: param.into(),
            value: value.into(),
        }),
    }
}

fn get_username_from_url(url: &Url) -> ClickhouseResult<Option<String>> {
    let user = url.username();
    if user.is_empty() {
        return Ok(None);
    }
    Ok(Some(
        percent_decode(user.as_ref())
            .decode_utf8()
            .map_err(|_| Error::Url(UrlError::Invalid))?
            .to_string(),
    ))
}

fn get_password_from_url(url: &Url) -> ClickhouseResult<Option<String>> {
    match url.password() {
        None => Ok(None),
        Some(password) => Ok(Some(
            percent_decode(password.as_ref())
                .decode_utf8()
                .map_err(|_| Error::Url(UrlError::Invalid))?
                .to_string(),
        )),
    }
}

fn get_database_from_url(url: &Url) -> ClickhouseResult<Option<String>> {
    match url.path_segments() {
        None => Ok(None),
        Some(mut segments) => {
            let head = segments.next();

            if segments.next().is_some() {
                return Err(Error::Url(UrlError::Invalid));
            }

            match head {
                Some(database) if !database.is_empty() => Ok(Some(
                    percent_decode(database.as_ref())
                        .decode_utf8()
                        .map_err(|_| Error::Url(UrlError::Invalid))?
                        .to_string(),
                )),
                _ => Ok(None),
            }
        }
    }
}

fn parse_duration(source: &str) -> std::result::Result<Duration, ()> {
    let digits_count = source.chars().take_while(|c| c.is_digit(10)).count();

    let left: String = source.chars().take(digits_count).collect();
    let right: String = source.chars().skip(digits_count).collect();

    let num = match u64::from_str(&left) {
        Ok(value) => value,
        Err(_) => return Err(()),
    };

    match right.as_str() {
        "s" => Ok(Duration::from_secs(num)),
        "ms" => Ok(Duration::from_millis(num)),
        _ => Err(()),
    }
}

fn parse_opt_duration(source: &str) -> std::result::Result<Option<Duration>, ()> {
    let duration = parse_duration(source)?;
    Ok(Some(duration))
}

fn parse_compression(source: &str) -> std::result::Result<bool, ()> {
    match source {
        "none" => Ok(false),
        "lz4" => Ok(true),
        _ => Err(()),
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use super::{from_url, parse_compression, parse_duration, Options};

    #[test]
    fn test_parse_default() {
        let url = "tcp://host1";
        let options = from_url(url).unwrap();
        assert_eq!(options.database, "default");
        assert_eq!(options.username, "default");
        assert_eq!(options.password, "");
    }

    #[test]
    fn test_parse_options() {
        let url = "tcp://username:password@host1:9001/database?ping_timeout=42ms&keepalive=99s&compression=lz4&connection_timeout=10s";
        assert_eq!(
            Options {
                username: "username".into(),
                password: "password".into(),
                addr: "host1:9001".into(),
                database: "database".into(),
                keepalive: Some(Duration::from_secs(99)),
                ping_timeout: Duration::from_millis(42),
                connection_timeout: Duration::from_secs(10),
                compression: true,
                ..Options::default()
            },
            from_url(url).unwrap(),
        );
    }

    #[test]
    #[should_panic]
    fn test_parse_invalid_url() {
        let url = "ʘ_ʘ";
        from_url(url).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_parse_with_unknown_url() {
        let url = "tcp://localhost:9000/foo?bar=baz";
        from_url(url).unwrap();
    }

    #[test]
    #[should_panic]
    fn test_parse_with_multi_databases() {
        let url = "tcp://localhost:9000/foo/bar";
        from_url(url).unwrap();
    }

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("3s").unwrap(), Duration::from_secs(3));
        assert_eq!(parse_duration("123ms").unwrap(), Duration::from_millis(123));

        assert_eq!(parse_duration("ms").unwrap_err(), ());
        assert_eq!(parse_duration("1ss").unwrap_err(), ());
    }

    #[test]
    fn test_parse_compression() {
        assert_eq!(parse_compression("none").unwrap(), false);
        assert_eq!(parse_compression("lz4").unwrap(), true);
        assert_eq!(parse_compression("?").unwrap_err(), ());
    }
}
