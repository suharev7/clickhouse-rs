use crate::{
    binary::{protocol, Encoder},
    block::BlockEx,
    client_info,
    types::{query::QueryEx, ClickhouseResult, Context, Query},
    Block,
};

/// Represents clickhouse commands.
pub enum Cmd {
    Hello(Context),
    Ping,
    SendQuery(Query, Context),
    SendData(Block, Context),
    Union(Box<Cmd>, Box<Cmd>),
}

impl Cmd {
    /// Returns the packed command as a byte vector.
    #[inline]
    pub fn get_packed_command(&self) -> ClickhouseResult<Vec<u8>> {
        encode_command(self)
    }
}

fn encode_command(cmd: &Cmd) -> ClickhouseResult<Vec<u8>> {
    match cmd {
        Cmd::Hello(context) => encode_hello(context),
        Cmd::Ping => encode_ping(),
        Cmd::SendQuery(query, context) => encode_query(query, context),
        Cmd::SendData(block, context) => encode_data(&block, context),
        Cmd::Union(first, second) => encode_union(first.as_ref(), second.as_ref()),
    }
}

fn encode_hello(context: &Context) -> ClickhouseResult<Vec<u8>> {
    trace!("[hello]        -> {}", client_info::description());

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_HELLO);
    client_info::write(&mut encoder);

    let options = context.options.get()?;

    encoder.string(&options.database);
    encoder.string(&options.username);
    encoder.string(&options.password);

    Ok(encoder.get_buffer())
}

fn encode_ping() -> ClickhouseResult<Vec<u8>> {
    trace!("[ping]         -> ping");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_PING);
    Ok(encoder.get_buffer())
}

fn encode_query(query: &Query, context: &Context) -> ClickhouseResult<Vec<u8>> {
    trace!("[send query] {}", query.get_sql());

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_QUERY);
    encoder.string("");

    {
        let hostname = &context.hostname;
        encoder.uvarint(1);
        encoder.string("");
        encoder.string(&query.get_id()); //initial_query_id;
        encoder.string("[::ffff:127.0.0.1]:0");
        encoder.uvarint(1); // iface type TCP;
        encoder.string(hostname);
        encoder.string(hostname);
    }
    client_info::write(&mut encoder);

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        encoder.string("");
    }

    encoder.string(""); // settings
    encoder.uvarint(protocol::STATE_COMPLETE);

    let options = context.options.get()?;

    encoder.uvarint(match options.compression {
        true => protocol::COMPRESS_ENABLE,
        false => protocol::COMPRESS_DISABLE,
    });

    let options = context.options.get()?;

    encoder.string(&query.get_sql());
    Block::default().send_data(&mut encoder, options.compression);

    Ok(encoder.get_buffer())
}

fn encode_data(block: &Block, context: &Context) -> ClickhouseResult<Vec<u8>> {
    let mut encoder = Encoder::new();
    let options = context.options.get()?;
    block.send_data(&mut encoder, options.compression);
    Ok(encoder.get_buffer())
}

fn encode_union(first: &Cmd, second: &Cmd) -> ClickhouseResult<Vec<u8>> {
    let mut result = encode_command(first)?;
    result.extend((encode_command(second)?).iter());
    Ok(result)
}
