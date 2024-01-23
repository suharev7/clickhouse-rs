use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};

use crate::errors::{Error, Result, UrlError};
use percent_encoding::percent_decode;
use url::Url;

const DEFAULT_MIN_CONNS: usize = 10;

const DEFAULT_MAX_CONNS: usize = 20;

#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
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
            State::Url(ref url) => write!(f, "Url({url})"),
            State::Raw(ref options) => write!(f, "{options:?}"),
        }
    }
}

impl OptionsSource {
    pub(crate) fn get(&self) -> Result<Cow<Options>> {
        let mut state = self.state.lock().unwrap();
        loop {
            *state = match &*state {
                State::Raw(ref options) => {
                    let ptr = options as *const Options;
                    return unsafe { Ok(Cow::Borrowed(ptr.as_ref().unwrap())) };
                }
                State::Url(url) => {
                    let options = from_url(url)?;
                    State::Raw(options)
                }
            };
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

/// An X509 certificate for native-tls.
#[cfg(feature = "tls-native-tls")]
#[derive(Clone)]
pub struct Certificate(Arc<native_tls::Certificate>);
#[cfg(feature = "tls-native-tls")]
impl Certificate {
    /// Parses a DER-formatted X509 certificate.
    pub fn from_der(der: &[u8]) -> Result<Certificate> {
        let inner = match native_tls::Certificate::from_der(der) {
            Ok(certificate) => certificate,
            Err(err) => return Err(Error::Other(err.to_string().into())),
        };
        Ok(Certificate(Arc::new(inner)))
    }

    /// Parses a PEM-formatted X509 certificate.
    pub fn from_pem(der: &[u8]) -> Result<Certificate> {
        let inner = match native_tls::Certificate::from_pem(der) {
            Ok(certificate) => certificate,
            Err(err) => return Err(Error::Other(err.to_string().into())),
        };
        Ok(Certificate(Arc::new(inner)))
    }
}
#[cfg(feature = "tls-native-tls")]
impl From<Certificate> for native_tls::Certificate {
    fn from(value: Certificate) -> Self {
        value.0.as_ref().clone()
    }
}

/// An X509 certificate for rustls.
#[cfg(feature = "tls-rustls")]
#[derive(Clone)]
pub struct Certificate(Arc<Vec<rustls::pki_types::CertificateDer<'static>>>);
#[cfg(feature = "tls-rustls")]
impl Certificate {
    /// Parses a DER-formatted X509 certificate.
    pub fn from_der(der: &[u8]) -> Result<Certificate> {
        let der = der.to_vec();
        let inner = match rustls::pki_types::CertificateDer::try_from(der) {
            Ok(certificate) => certificate,
            Err(err) => return Err(Error::Other(err.to_string().into())),
        };
        Ok(Certificate(Arc::new(vec![inner])))
    }

    /// Parses a PEM-formatted X509 certificate.
    pub fn from_pem(der: &[u8]) -> Result<Certificate> {
        let certs = rustls_pemfile::certs(&mut der.as_ref())
            .map(|result| result.unwrap())
            .collect();
        Ok(Certificate(Arc::new(certs)))
    }
}
#[cfg(feature = "tls-rustls")]
impl From<Certificate> for Vec<rustls::pki_types::CertificateDer<'static>> {
    fn from(value: Certificate) -> Self {
        value.0.as_ref().clone()
    }
}

#[cfg(feature = "_tls")]
impl fmt::Debug for Certificate {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[Certificate]")
    }
}
#[cfg(feature = "_tls")]
impl PartialEq for Certificate {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

#[derive(Clone, PartialEq, Debug)]
pub enum SettingType {
    String(String),
    Bool(bool),
    UInt64(u64),
    Float64(f64),
}

impl fmt::Display for SettingType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self {
            SettingType::Bool(val) => write!(f, "{val}"),
            SettingType::UInt64(val) => write!(f, "{val}"),
            SettingType::Float64(val) => write!(f, "{val}"),
            SettingType::String(val) => write!(f, "{val}"),
        }
    }
}

impl From<&str> for SettingType {
    fn from(val: &str) -> Self {
        SettingType::String(val.into())
    }
}

impl From<bool> for SettingType {
    fn from(val: bool) -> Self {
        SettingType::Bool(val)
    }
}

impl From<u64> for SettingType {
    fn from(val: u64) -> Self {
        SettingType::UInt64(val)
    }
}

impl From<i32> for SettingType {
    fn from(val: i32) -> Self {
        SettingType::UInt64(val as u64)
    }
}

impl From<i64> for SettingType {
    fn from(val: i64) -> Self {
        SettingType::UInt64(val as u64)
    }
}

impl From<f64> for SettingType {
    fn from(val: f64) -> Self {
        SettingType::Float64(val)
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct SettingValue {
    pub(crate) value: SettingType,
    pub(crate) is_important: bool,
}
impl fmt::Display for SettingValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

/// Clickhouse connection options.
#[derive(Clone, PartialEq)]
pub struct Options {
    /// Address of clickhouse server (defaults to `127.0.0.1:9000`).
    pub(crate) addr: Url,

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

    /// Timeout for queries (defaults to `180 sec`)
    pub(crate) query_timeout: Duration,

    /// Timeout for inserts (defaults to `180 sec`)
    pub(crate) insert_timeout: Option<Duration>,

    /// Timeout for execute (defaults to `180 sec`)
    pub(crate) execute_timeout: Option<Duration>,

    /// Enable TLS encryption (defaults to `false`)
    #[cfg(feature = "_tls")]
    pub(crate) secure: bool,

    /// Skip certificate verification (default is `false`).
    #[cfg(feature = "_tls")]
    pub(crate) skip_verify: bool,

    /// An X509 certificate.
    #[cfg(feature = "_tls")]
    pub(crate) certificate: Option<Certificate>,

    /// Query settings
    pub(crate) settings: HashMap<String, SettingValue>,

    /// Comma separated list of single address host for load-balancing.
    pub(crate) alt_hosts: Vec<Url>,
}

impl fmt::Debug for Options {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Options")
            .field("addr", &self.addr)
            .field("database", &self.database)
            .field("compression", &self.compression)
            .field("pool_min", &self.pool_min)
            .field("pool_max", &self.pool_max)
            .field("nodelay", &self.nodelay)
            .field("keepalive", &self.keepalive)
            .field("ping_before_query", &self.ping_before_query)
            .field("send_retries", &self.send_retries)
            .field("retry_timeout", &self.retry_timeout)
            .field("ping_timeout", &self.ping_timeout)
            .field("connection_timeout", &self.connection_timeout)
            .field("settings", &self.settings)
            .field("alt_hosts", &self.alt_hosts)
            .finish()
    }
}

impl Default for Options {
    fn default() -> Self {
        Self {
            addr: Url::parse("tcp://default@127.0.0.1:9000").unwrap(),
            database: "default".into(),
            username: "default".into(),
            password: "".into(),
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
            query_timeout: Duration::from_secs(180),
            insert_timeout: Some(Duration::from_secs(180)),
            execute_timeout: Some(Duration::from_secs(180)),
            #[cfg(feature = "_tls")]
            secure: false,
            #[cfg(feature = "_tls")]
            skip_verify: false,
            #[cfg(feature = "_tls")]
            certificate: None,
            settings: HashMap::new(),
            alt_hosts: Vec::new(),
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
        A: Into<Url>,
    {
        Self {
            addr: addr.into(),
            ..Self::default()
        }
    }

    pub fn with_setting<V>(mut self, name: &str, value: V, is_important: bool) -> Self
    where
        V: Into<SettingType>,
    {
        let value: SettingType = value.into();
        self.settings.insert(
            name.into(),
            SettingValue {
                value,
                is_important,
            },
        );
        self
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

    property! {
        /// Timeout for query (defaults to `180,000 ms`).
        => query_timeout: Duration
    }

    property! {
        /// Timeout for insert (defaults to `180,000 ms`).
        => insert_timeout: Option<Duration>
    }

    property! {
        /// Timeout for execute (defaults to `180 sec`).
        => execute_timeout: Option<Duration>
    }

    #[cfg(feature = "_tls")]
    property! {
        /// Establish secure connection (default is `false`).
        => secure: bool
    }

    #[cfg(feature = "_tls")]
    property! {
        /// Skip certificate verification (default is `false`).
        => skip_verify: bool
    }

    #[cfg(feature = "_tls")]
    property! {
        /// An X509 certificate.
        => certificate: Option<Certificate>
    }

    property! {
        /// Query settings
        => settings: HashMap<String, SettingValue>
    }

    property! {
        /// Comma separated list of single address host for load-balancing.
        => alt_hosts: Vec<Url>
    }
}

impl FromStr for Options {
    type Err = Error;

    fn from_str(url: &str) -> Result<Self> {
        from_url(url)
    }
}

fn from_url(url_str: &str) -> Result<Options> {
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

    if let Some(username) = get_username_from_url(&url) {
        options.username = username.into();
    }

    if let Some(password) = get_password_from_url(&url) {
        options.password = password.into()
    }

    let mut addr = url.clone();
    addr.set_path("");
    addr.set_query(None);

    let port = url.port().or(Some(9000));
    addr.set_port(port).map_err(|_| UrlError::Invalid)?;
    options.addr = addr;

    if let Some(database) = get_database_from_url(&url)? {
        options.database = database.into();
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
            "query_timeout" => options.query_timeout = parse_param(key, value, parse_duration)?,
            "insert_timeout" => {
                options.insert_timeout = parse_param(key, value, parse_opt_duration)?
            }
            "execute_timeout" => {
                options.execute_timeout = parse_param(key, value, parse_opt_duration)?
            }
            "compression" => options.compression = parse_param(key, value, parse_compression)?,
            #[cfg(feature = "_tls")]
            "secure" => options.secure = parse_param(key, value, bool::from_str)?,
            #[cfg(feature = "_tls")]
            "skip_verify" => options.skip_verify = parse_param(key, value, bool::from_str)?,
            "alt_hosts" => options.alt_hosts = parse_param(key, value, parse_hosts)?,
            _ => {
                let value = SettingType::String(value.to_string());
                options.settings.insert(
                    key.to_string(),
                    SettingValue {
                        value,
                        is_important: true,
                    },
                );
            }
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
    let source = percent_decode(value.as_bytes()).decode_utf8_lossy();
    match parse(source.as_ref()) {
        Ok(value) => Ok(value),
        Err(_) => Err(UrlError::InvalidParamValue {
            param: param.into(),
            value: value.into(),
        }),
    }
}

fn get_username_from_url(url: &Url) -> Option<Cow<'_, str>> {
    let user = url.username();
    if user.is_empty() {
        return None;
    }
    Some(percent_decode(user.as_bytes()).decode_utf8_lossy())
}

fn get_password_from_url(url: &Url) -> Option<Cow<'_, str>> {
    let password = url.password()?;
    Some(percent_decode(password.as_bytes()).decode_utf8_lossy())
}

fn get_database_from_url(url: &Url) -> Result<Option<&str>> {
    match url.path_segments() {
        None => Ok(None),
        Some(mut segments) => {
            let head = segments.next();

            if segments.next().is_some() {
                return Err(Error::Url(UrlError::Invalid));
            }

            match head {
                Some(database) if !database.is_empty() => Ok(Some(database)),
                _ => Ok(None),
            }
        }
    }
}

fn parse_duration(source: &str) -> std::result::Result<Duration, ()> {
    let digits_count = source.chars().take_while(|c| c.is_ascii_digit()).count();

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
    if source == "none" {
        return Ok(None);
    }

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

fn parse_hosts(source: &str) -> std::result::Result<Vec<Url>, ()> {
    let mut result = Vec::new();
    for host in source.split(',') {
        match Url::from_str(&format!("tcp://{host}")) {
            Ok(url) => result.push(url),
            Err(_) => return Err(()),
        }
    }
    Ok(result)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_hosts() {
        let source = "host2:9000,host3:9000";
        let expected = vec![
            Url::from_str("tcp://host2:9000").unwrap(),
            Url::from_str("tcp://host3:9000").unwrap(),
        ];
        let actual = parse_hosts(source).unwrap();
        assert_eq!(actual, expected)
    }

    #[test]
    fn test_parse_default() {
        let url = "tcp://host1";
        let options = from_url(url).unwrap();
        assert_eq!(options.database, "default");
        assert_eq!(options.username, "default");
        assert_eq!(options.password, "");
    }

    #[test]
    #[cfg(feature = "_tls")]
    fn test_parse_secure_options() {
        let url = "tcp://username:password@host1:9001/database?ping_timeout=42ms&keepalive=99s&compression=lz4&connection_timeout=10s&secure=true&skip_verify=true";
        assert_eq!(
            Options {
                username: "username".into(),
                password: "password".into(),
                addr: Url::parse("tcp://username:password@host1:9001").unwrap(),
                database: "database".into(),
                keepalive: Some(Duration::from_secs(99)),
                ping_timeout: Duration::from_millis(42),
                connection_timeout: Duration::from_secs(10),
                compression: true,
                secure: true,
                skip_verify: true,
                ..Options::default()
            },
            from_url(url).unwrap(),
        );
    }

    #[test]
    fn test_parse_encoded_creds() {
        let url = "tcp://user%20%3Cbar%3E:password%20%3Cbar%3E@host1:9001/database?ping_timeout=42ms&keepalive=99s&compression=lz4&connection_timeout=10s";
        assert_eq!(
            Options {
                username: "user <bar>".into(),
                password: "password <bar>".into(),
                addr: Url::parse("tcp://user%20%3Cbar%3E:password%20%3Cbar%3E@host1:9001").unwrap(),
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
    fn test_parse_options() {
        let url = "tcp://username:password@host1:9001/database?ping_timeout=42ms&keepalive=99s&compression=lz4&connection_timeout=10s";
        assert_eq!(
            Options {
                username: "username".into(),
                password: "password".into(),
                addr: Url::parse("tcp://username:password@host1:9001").unwrap(),
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
    fn test_parse_with_unknown_setting() {
        let url = "tcp://localhost:9000/foo?bar=baz";
        assert_eq!(
            Options {
                addr: Url::parse("tcp://localhost:9000").unwrap(),
                database: "foo".into(),
                settings: HashMap::from([(
                    "bar".into(),
                    SettingValue {
                        value: SettingType::String("baz".into()),
                        is_important: true,
                    }
                ),]),
                ..Options::default()
            },
            from_url(url).unwrap(),
        );
        // TODO: try to run "SELECT 1" with it and got a failure
    }

    #[test]
    fn test_with_setting() {
        {
            let opts = Options::from_str("tcp://localhost:9000")
                .unwrap()
                .with_setting("foo", "bar", true);
            assert_eq!(
                opts.settings,
                HashMap::from([(
                    "foo".into(),
                    SettingValue {
                        value: SettingType::String("bar".into()),
                        is_important: true,
                    }
                )])
            );
        }

        {
            let opts = Options::from_str("tcp://localhost:9000")
                .unwrap()
                .with_setting("foo", "bar", false);
            assert_eq!(
                opts.settings,
                HashMap::from([(
                    "foo".into(),
                    SettingValue {
                        value: SettingType::String("bar".into()),
                        is_important: false,
                    }
                )])
            );
        }

        {
            let opts = Options::from_str("tcp://localhost:9000")
                .unwrap()
                .with_setting("foo", 1, true);
            assert_eq!(
                opts.settings,
                HashMap::from([(
                    "foo".into(),
                    SettingValue {
                        value: SettingType::UInt64(1u64),
                        is_important: true,
                    }
                )])
            );
        }

        {
            let opts = Options::from_str("tcp://localhost:9000")
                .unwrap()
                .with_setting("foo", true, true);
            assert_eq!(
                opts.settings,
                HashMap::from([(
                    "foo".into(),
                    SettingValue {
                        value: SettingType::Bool(true),
                        is_important: true,
                    }
                )])
            );
        }

        {
            let opts = Options::from_str("tcp://localhost:9000")
                .unwrap()
                .with_setting("foo", 1., true);
            assert_eq!(
                opts.settings,
                HashMap::from([(
                    "foo".into(),
                    SettingValue {
                        value: SettingType::Float64(1.),
                        is_important: true,
                    }
                )])
            );
        }
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

        parse_duration("ms").unwrap_err();
        parse_duration("1ss").unwrap_err();
    }

    #[test]
    fn test_parse_opt_duration() {
        assert_eq!(
            parse_opt_duration("3s").unwrap(),
            Some(Duration::from_secs(3))
        );
        assert_eq!(parse_opt_duration("none").unwrap(), None::<Duration>);
    }

    #[test]
    fn test_parse_compression() {
        assert!(!parse_compression("none").unwrap());
        assert!(parse_compression("lz4").unwrap());
        parse_compression("?").unwrap_err();
    }
}
