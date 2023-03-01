use derive_more::Display;
use prost::Message;
use tracing::warn;

use crate::kaskada::sparrow::v1alpha::{FlightRecord, FlightRecordHeader};

pub struct FlightRecordWriter<W: std::io::Write> {
    buffer: Vec<u8>,
    writer: W,
}

#[derive(Debug, Display)]
pub enum Error {
    ProstEncodeError(prost::EncodeError),
    IoError(std::io::Error),
}

impl error_stack::Context for Error {}

impl<W: std::io::Write> FlightRecordWriter<W> {
    pub fn try_new(writer: W, header: FlightRecordHeader) -> Result<Self, Error> {
        let mut writer = Self {
            buffer: Vec::with_capacity(64 * 1024),
            writer,
        };

        writer.write_message(header)?;

        Ok(writer)
    }

    pub fn write(&mut self, record: FlightRecord) -> Result<(), Error> {
        self.write_message(record)
    }

    fn write_message(&mut self, message: impl Message) -> Result<(), Error> {
        let capacity = self.buffer.capacity();
        message.encode_length_delimited(&mut self.buffer)?;
        self.writer.write_all(&self.buffer)?;
        self.buffer.clear();

        if self.buffer.capacity() > capacity {
            warn!(
                "Buffer for flight records grew from {} to {} bytes",
                capacity,
                self.buffer.capacity()
            );
        }

        Ok(())
    }

    pub fn flush(mut self) -> Result<(), Error> {
        self.writer.flush()?;
        Ok(())
    }
}

impl From<prost::EncodeError> for Error {
    fn from(err: prost::EncodeError) -> Self {
        Error::ProstEncodeError(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}
