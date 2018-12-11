extern crate chrono;
extern crate chrono_tz;
extern crate core;
#[macro_use]
extern crate futures;
extern crate hostname;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate clickhouse_rs_cityhash_sys;
extern crate lz4;
#[cfg(test)]
extern crate rand;
extern crate tokio;

use std::fmt;
use std::net::SocketAddr;

use futures::{Future, Stream};
use tokio::net::TcpStream;
use tokio::prelude::*;

pub use crate::block::Block;
use crate::block::BlockEx;
use crate::io::ClickhouseTransport;
pub use crate::io::IoFuture;
use crate::types::query::QueryEx;
pub use crate::types::{ClickhouseError, SqlType};
use crate::types::{ClickhouseResult, Cmd, Context, Packet, Query};

mod binary;
mod block;
mod client_info;
mod column;
mod io;
mod types;

#[derive(Clone)]
pub struct Options {
    addr: SocketAddr,
    database: String,
    username: String,
    password: String,
    compression: bool,
}

pub struct Client {
    _private: (),
}

pub struct ClientHandle {
    inner: ClickhouseTransport,
    context: Context,
}

impl Options {
    pub fn new(addr: SocketAddr) -> Options {
        Options {
            addr,
            database: "default".to_string(),
            username: "default".to_string(),
            password: "".to_string(),
            compression: false,
        }
    }

    pub fn database(self, database: &str) -> Options {
        Options {
            database: database.to_string(),
            ..self
        }
    }

    pub fn username(self, username: &str) -> Options {
        Options {
            username: username.to_string(),
            ..self
        }
    }

    pub fn password(self, password: &str) -> Options {
        Options {
            password: password.to_string(),
            ..self
        }
    }

    pub fn with_compression(self) -> Options {
        Options {
            compression: true,
            ..self
        }
    }
}

impl fmt::Debug for ClientHandle {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "ClientHandle({:?})", self.context.server_info)
    }
}

impl Client {
    pub fn connect(options: Options) -> IoFuture<ClientHandle> {
        let compress = options.compression;

        let context = Context {
            database: options.database,
            username: options.username,
            password: options.password,
            compression: options.compression,
            ..Context::default()
        };

        Box::new(
            TcpStream::connect(&options.addr)
                .and_then(move |stream| {
                    stream.set_nodelay(true)?;

                    let transport = ClickhouseTransport::new(stream, compress);
                    Ok(ClientHandle {
                        inner: transport,
                        context,
                    })
                })
                .and_then(|client| client.hello()),
        )
    }
}

impl ClientHandle {
    pub fn hello(self) -> IoFuture<ClientHandle> {
        let context = self.context;
        Box::new(
            self.inner
                .call(Cmd::Hello(context.clone()))
                .fold(None, move |_, packet| match packet {
                    Packet::Hello(inner, server_info) => {
                        let context = Context {
                            server_info,
                            ..context.clone()
                        };
                        let client = ClientHandle { inner, context };
                        future::ok::<_, std::io::Error>(Some(client))
                    }
                    Packet::Exception(e) => future::err(ClickhouseError::Internal(e).into()),
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(Option::unwrap),
        )
    }

    pub fn ping(self) -> IoFuture<ClientHandle> {
        let context = self.context;
        Box::new(
            self.inner
                .call(Cmd::Ping)
                .fold(None, move |_, packet| match packet {
                    Packet::Pong(inner) => {
                        let client = ClientHandle {
                            inner,
                            context: context.clone(),
                        };
                        future::ok::<_, std::io::Error>(Some(client))
                    }
                    Packet::Exception(e) => future::err(ClickhouseError::Internal(e).into()),
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(Option::unwrap),
        )
    }

    /// Fetch data from table. It returns a block that contains all rows.
    pub fn query_all<Q>(self, sql: Q) -> IoFuture<(ClientHandle, Block)>
    where
        Query: From<Q>,
    {
        let context = self.context;
        let query = Query::from(sql);
        let init = (None, vec![]);
        info!("[send query] {}", query.get_sql());

        Box::new(
            self.inner
                .call(Cmd::SendQuery(query, context.clone()))
                .fold(init, move |(h, mut bs), packet| match packet {
                    Packet::Block(b) => {
                        if !b.is_empty() {
                            bs.push(b);
                        }
                        future::ok::<_, std::io::Error>((h, bs))
                    }
                    Packet::Eof(inner) => {
                        let client = ClientHandle {
                            inner,
                            context: context.clone(),
                        };
                        future::ok((Some(client), bs))
                    }
                    Packet::ProfileInfo(_) | Packet::Progress(_) => future::ok((h, bs)),
                    Packet::Exception(e) => future::err(ClickhouseError::Internal(e).into()),
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(|(client, blocks)| (client.unwrap(), Block::concat(&blocks[..]))),
        )
    }

    /// Convenience method to prepare and execute a single SQL statement.
    pub fn execute<Q>(self, sql: Q) -> IoFuture<ClientHandle>
    where
        Query: From<Q>,
    {
        let context = self.context;
        let query = Query::from(sql);
        trace!("[send query] {}", query.get_sql());
        Box::new(
            self.inner
                .call(Cmd::SendQuery(query, context.clone()))
                .fold(None, move |acc, packet| match packet {
                    Packet::Eof(inner) => {
                        let client = ClientHandle {
                            inner,
                            context: context.clone(),
                        };
                        future::ok::<_, std::io::Error>(Some(client))
                    }
                    Packet::Block(_) | Packet::ProfileInfo(_) | Packet::Progress(_) => {
                        future::ok::<_, std::io::Error>(acc)
                    }
                    Packet::Exception(exception) => {
                        future::err(ClickhouseError::Internal(exception).into())
                    }
                    _ => future::err(ClickhouseError::UnexpectedPacket.into()),
                })
                .map(Option::unwrap),
        )
    }

    /// Convenience method to insert block of data.
    pub fn insert<Q>(self, table: Q, block: Block) -> IoFuture<ClientHandle>
    where
        Query: From<Q>,
    {
        let names: Vec<_> = block
            .as_ref()
            .columns()
            .iter()
            .map(|column| column.name().to_string())
            .collect();
        let fields = names.join(", ");

        let query = Query::from(table)
            .map_sql(|table| format!("INSERT INTO {} ({}) VALUES", table, fields));

        let context = self.context;

        let send_cmd = Cmd::Union(
            Box::new(Cmd::SendData(block, context.clone())),
            Box::new(Cmd::SendData(Block::default(), context.clone())),
        );

        Box::new(
            self.inner
                .call(Cmd::SendQuery(query, context.clone()))
                .read_block(context.clone())
                .and_then(move |c| c.inner.call(send_cmd).read_block(context.clone())),
        )
    }
}
