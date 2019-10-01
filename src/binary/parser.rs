use std::io::{self, Read};

use chrono_tz::Tz;
use log::{trace, warn};

use crate::{
    binary::{protocol, ReadEx},
    errors::{DriverError, Error, Result, ServerError},
    types::{Block, Packet, ProfileInfo, Progress, ServerInfo},
};

/// The internal clickhouse response parser.
pub(crate) struct Parser<T> {
    reader: T,
    tz: Option<Tz>,
    compress: bool,
}

/// The parser can be used to parse clickhouse responses into values.  Generally
/// you normally do not use this directly as it's already done for you by
/// the client but in some more complex situations it might be useful to be
/// able to parse the clickhouse responses.
impl<'a, T: Read> Parser<T> {
    /// Creates a new parser that parses the data behind the reader.  More
    /// than one value can be behind the reader in which case the parser can
    /// be invoked multiple times.  In other words: the stream does not have
    /// to be terminated.
    pub(crate) fn new(reader: T, tz: Option<Tz>, compress: bool) -> Parser<T> {
        Self {
            reader,
            tz,
            compress,
        }
    }

    /// Parses a single value out of the stream. If there are multiple
    /// values you can call this multiple times. If the reader is not yet
    /// ready this will block.
    pub(crate) fn parse_packet(&mut self) -> Result<Packet<()>> {
        let packet = self.reader.read_uvarint()?;
        match packet {
            protocol::SERVER_HELLO => Ok(self.parse_server_info()?),
            protocol::SERVER_PONG => Ok(self.parse_pong()?),
            protocol::SERVER_PROGRESS => Ok(self.parse_progress()?),
            protocol::SERVER_PROFILE_INFO => Ok(self.parse_profile_info()?),
            protocol::SERVER_EXCEPTION => Ok(self.parse_exception()?),
            protocol::SERVER_DATA | protocol::SERVER_TOTALS | protocol::SERVER_EXTREMES => {
                Ok(self.parse_block()?)
            }
            protocol::SERVER_END_OF_STREAM => Ok(Packet::Eof(())),
            _ => Err(Error::Driver(DriverError::UnknownPacket { packet })),
        }
    }

    fn parse_block(&mut self) -> Result<Packet<()>> {
        match self.tz {
            None => Err(Error::Driver(DriverError::UnexpectedPacket)),
            Some(tz) => {
                self.reader.skip_string()?;
                let block = Block::load(&mut self.reader, tz, self.compress)?;
                Ok(Packet::Block(block))
            }
        }
    }

    fn parse_server_info(&mut self) -> Result<Packet<()>> {
        let server_info = ServerInfo {
            name: self.reader.read_string()?,
            major_version: self.reader.read_uvarint()?,
            minor_version: self.reader.read_uvarint()?,
            revision: self.reader.read_uvarint()?,
            timezone: match self.reader.read_string()?.parse() {
                Ok(tz) => tz,
                Err(err) => return Err(io::Error::new(io::ErrorKind::Other, err).into()),
            },
        };

        trace!("[hello]        <- {:?}", &server_info);
        Ok(Packet::Hello((), server_info))
    }

    fn parse_progress(&mut self) -> Result<Packet<()>> {
        let progress = Progress {
            rows: self.reader.read_uvarint()?,
            bytes: self.reader.read_uvarint()?,
            total_rows: self.reader.read_uvarint()?,
        };

        trace!(
            "[process] <- Progress: rows={}, bytes={}, total rows={}",
            progress.rows,
            progress.bytes,
            progress.total_rows
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

    fn parse_pong(&self) -> Result<Packet<()>> {
        trace!("[process]      <- pong");
        Ok(Packet::Pong(()))
    }
}
