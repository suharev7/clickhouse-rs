use std::io::{self, Read};

use chrono_tz::Tz;
use log::{trace, warn};

use crate::{
    binary::{protocol, ReadEx},
    errors::{DriverError, Error, Result, ServerError},
    io::transport::TransportInfo,
    types::{Block, Packet, ProfileInfo, Progress, ServerInfo, TableColumns},
};

/// The internal clickhouse response parser.
pub(crate) struct Parser<'i, T> {
    reader: T,
    info: &'i TransportInfo,
}

/// The parser can be used to parse clickhouse responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the clickhouse responses.
impl<'i, T: Read> Parser<'i, T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub(crate) fn new(reader: T, info: &'i TransportInfo) -> Parser<T> {
        Self { reader, info }
    }

    /// Parses a single value out of the stream. If there are multiple
    /// values you can call this multiple times. If the reader is not yet
    /// ready this will block.
    pub(crate) fn parse_packet(&mut self, revision: u64) -> Result<Packet<()>> {
        let packet = self.reader.read_uvarint()?;
        match packet {
            protocol::SERVER_HELLO => Ok(self.parse_server_info()?),
            protocol::SERVER_PONG => Ok(self.parse_pong()),
            protocol::SERVER_PROGRESS => Ok(self.parse_progress(revision)?),
            protocol::SERVER_PROFILE_INFO => Ok(self.parse_profile_info()?),
            protocol::SERVER_TABLE_COLUMNS => Ok(self.parse_table_columns()?),
            protocol::SERVER_EXCEPTION => Ok(self.parse_exception()?),
            protocol::SERVER_DATA | protocol::SERVER_TOTALS | protocol::SERVER_EXTREMES => {
                Ok(self.parse_block()?)
            }
            protocol::SERVER_END_OF_STREAM => Ok(Packet::Eof(())),
            _ => Err(Error::Driver(DriverError::UnknownPacket { packet })),
        }
    }

    fn parse_block(&mut self) -> Result<Packet<()>> {
        match self.info.timezone {
            None => Err(Error::Driver(DriverError::UnexpectedPacket)),
            Some(tz) => {
                self.reader.skip_string()?;
                let block = Block::load(&mut self.reader, tz, self.info.compress)?;
                Ok(Packet::Block(block))
            }
        }
    }

    fn parse_server_info(&mut self) -> Result<Packet<()>> {
        let name = self.reader.read_string()?;
        let major_version = self.reader.read_uvarint()?;
        let minor_version = self.reader.read_uvarint()?;
        let revision = self.reader.read_uvarint()?;

        let timezone = if revision >= protocol::DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE {
            match self.reader.read_string()?.parse() {
                Ok(tz) => tz,
                Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err).into()),
            }
        } else {
            Tz::UTC
        };

        let display_name = if revision >= protocol::DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME {
            self.reader.read_string()?
        } else {
            "".into()
        };

        let patch_version = if revision >= protocol::DBMS_MIN_REVISION_WITH_VERSION_PATCH {
            self.reader.read_uvarint()?
        } else {
            0
        };

        let server_info = ServerInfo {
            name,
            major_version,
            minor_version,
            revision,
            timezone,
            display_name,
            patch_version,
        };

        trace!("[hello]        <- {:?}", &server_info);
        Ok(Packet::Hello((), server_info))
    }

    fn parse_progress(&mut self, revision: u64) -> Result<Packet<()>> {
        let rows = self.reader.read_uvarint()?;
        let bytes = self.reader.read_uvarint()?;

        let total_rows = if revision >= protocol::DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS {
            self.reader.read_uvarint()?
        } else {
            0
        };

        let (written_rows, written_bytes) =
            if revision >= protocol::DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO {
                (self.reader.read_uvarint()?, self.reader.read_uvarint()?)
            } else {
                (0, 0)
            };

        let progress = Progress {
            rows,
            bytes,
            total_rows,
            written_rows,
            written_bytes,
        };

        trace!(
            "[process] <- Progress: rows={}, bytes={}, total rows={}, written_rows={}, write_bytes={}",
            progress.rows,
            progress.bytes,
            progress.total_rows,
            progress.written_rows,
            progress.written_bytes,
        );

        Ok(Packet::Progress(progress))
    }

    fn parse_profile_info(&mut self) -> Result<Packet<()>> {
        let info = Packet::ProfileInfo(ProfileInfo {
            rows: self.reader.read_uvarint()?,
            blocks: self.reader.read_uvarint()?,
            bytes: self.reader.read_uvarint()?,
            applied_limit: self.reader.read_scalar()?,
            rows_before_limit: self.reader.read_uvarint()?,
            calculated_rows_before_limit: self.reader.read_scalar()?,
        });

        trace!("profile_info: {:?}", info);
        Ok(info)
    }

    fn parse_table_columns(&mut self) -> Result<Packet<()>> {
        let table_columns = Packet::TableColumns(TableColumns {
            table_name: self.reader.read_string()?,
            columns: self.reader.read_string()?,
        });

        trace!("table_columns: {:?}", table_columns);
        Ok(table_columns)
    }

    fn parse_exception(&mut self) -> Result<Packet<()>> {
        let exception = ServerError {
            code: self.reader.read_scalar()?,
            name: self.reader.read_string()?,
            message: self.reader.read_string()?,
            stack_trace: self.reader.read_string()?,
        };

        warn!("server exception: {:?}", exception);
        Ok(Packet::Exception(exception))
    }

    fn parse_pong(&self) -> Packet<()> {
        trace!("[process]      <- pong");
        Packet::Pong(())
    }
}
