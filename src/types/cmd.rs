use log::trace;

use crate::{
    binary::{protocol, Encoder},
    client_info,
    errors::Result,
    types::{Context, Query, Simple, Options},
    Block,
};

/// Represents Clickhouse commands.
pub(crate) enum Cmd {
    Hello(Context),
    Ping,
    SendQuery(Query, Context),
    SendData(Block, Context),
    Union(Box<Cmd>, Box<Cmd>),
    Cancel,
}

impl Cmd {
    /// Returns the packed command as a byte vector.
    #[inline(always)]
    pub(crate) fn get_packed_command(&self) -> Result<Vec<u8>> {
        encode_command(self)
    }
}

#[derive(Debug, PartialOrd, PartialEq)]
enum SettingsBinaryFormat {
    Old,
    Strings,
}

fn encode_command(cmd: &Cmd) -> Result<Vec<u8>> {
    match cmd {
        Cmd::Hello(context) => encode_hello(context),
        Cmd::Ping => Ok(encode_ping()),
        Cmd::SendQuery(query, context) => encode_query(query, context),
        Cmd::SendData(block, context) => encode_data(block, context),
        Cmd::Union(first, second) => encode_union(first.as_ref(), second.as_ref()),
        Cmd::Cancel => Ok(encode_cancel()),
    }
}

fn encode_hello(context: &Context) -> Result<Vec<u8>> {
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

fn encode_ping() -> Vec<u8> {
    trace!("[ping]         -> ping");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_PING);
    encoder.get_buffer()
}

fn encode_cancel() -> Vec<u8> {
    trace!("[cancel]");

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_CANCEL);
    encoder.get_buffer()
}

fn encode_query(query: &Query, context: &Context) -> Result<Vec<u8>> {
    trace!("[send query] {}", query.get_sql());

    let mut encoder = Encoder::new();
    encoder.uvarint(protocol::CLIENT_QUERY);
    encoder.string("");

    {
        let hostname = &context.hostname;
        encoder.uvarint(1);
        encoder.string("");
        encoder.string(&query.get_id()); // initial_query_id;
        encoder.string("[::ffff:127.0.0.1]:0");
        encoder.uvarint(1); // iface type TCP;
        encoder.string(hostname);
        encoder.string(hostname);
    }
    client_info::write(&mut encoder);

    if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO {
        encoder.string("");
    }

    let options = context.options.get()?;

    let settings_format = if context.server_info.revision >= protocol::DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS {
        SettingsBinaryFormat::Strings
    } else {
        SettingsBinaryFormat::Old
    };

    serialize_settings(&mut encoder, &options, settings_format);

    encoder.uvarint(protocol::STATE_COMPLETE);

    encoder.uvarint(if options.compression {
        protocol::COMPRESS_ENABLE
    } else {
        protocol::COMPRESS_DISABLE
    });

    let options = context.options.get()?;

    encoder.string(&query.get_sql());
    Block::<Simple>::default().send_data(&mut encoder, options.compression);

    Ok(encoder.get_buffer())
}

fn serialize_settings(encoder: &mut Encoder, options: &Options, format: SettingsBinaryFormat) {

    if let Some(level) = options.readonly {
        encoder.string("readonly");
        if format >= SettingsBinaryFormat::Strings {
            encoder.write(0_u8); // is_important
        }
        serialize_uint(encoder, level as u64, format);
    }

    encoder.string(""); // settings
}

fn serialize_uint(encoder: &mut Encoder, value: u64, format: SettingsBinaryFormat) {
    if format >= SettingsBinaryFormat::Strings {
        encoder.string(format!("{}", value));
        return;
    }

    encoder.uvarint(value);
}

fn encode_data(block: &Block, context: &Context) -> Result<Vec<u8>> {
    let mut encoder = Encoder::new();
    let options = context.options.get()?;
    block.send_data(&mut encoder, options.compression);
    Ok(encoder.get_buffer())
}

fn encode_union(first: &Cmd, second: &Cmd) -> Result<Vec<u8>> {
    let mut result = encode_command(first)?;
    result.extend((encode_command(second)?).iter());
    Ok(result)
}
