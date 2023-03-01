use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

use fallible_iterator::FallibleIterator;
use prost::bytes::Bytes;
use prost::Message;

use crate::kaskada::sparrow::v1alpha::{FlightRecord, FlightRecordHeader};

pub struct FlightRecordReader {
    header: FlightRecordHeader,
    event_position: u64,
    path: PathBuf,
}

impl FlightRecordReader {
    pub fn try_new(path: &std::path::Path) -> Result<Self, Error> {
        let mut reader = DelimitedProtoReader::try_open(path)?;

        if let Some(header) = reader.read_message()? {
            let event_position = reader.stream_position()?;
            Ok(Self {
                header,
                event_position,
                path: path.to_path_buf(),
            })
        } else {
            Err(Error::MissingHeader)
        }
    }

    pub fn header(&self) -> &FlightRecordHeader {
        &self.header
    }

    pub fn records(
        &self,
    ) -> Result<impl FallibleIterator<Item = FlightRecord, Error = Error>, Error> {
        let reader = DelimitedProtoReader::try_open_offset(&self.path, self.event_position)?;
        let reader = FlightRecordEventReader(reader);
        Ok(reader)
    }
}

#[repr(transparent)]
struct DelimitedProtoReader<R: std::io::Read>(BufReader<R>);

impl DelimitedProtoReader<File> {
    fn try_open(path: &std::path::Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(Self(reader))
    }

    fn try_open_offset(path: &std::path::Path, offset: u64) -> Result<Self, Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset))?;
        Ok(Self(reader))
    }

    fn stream_position(&mut self) -> Result<u64, Error> {
        Ok(self.0.stream_position()?)
    }
}

impl<R: std::io::Read> DelimitedProtoReader<R> {
    /// Reads the given message (if the stream isn't empty).
    fn read_message<T: Message + Default>(&mut self) -> Result<Option<T>, Error> {
        let buf = self.0.fill_buf()?;
        if buf.is_empty() {
            return Ok(None);
        }

        // Try to read the length from the existing buffer.
        let message_length = if let Ok(length) = prost::decode_length_delimiter(buf) {
            self.0.consume(prost::length_delimiter_len(length));
            length
        } else {
            let prefix = Bytes::copy_from_slice(buf);
            let prefix_len = prefix.len();
            self.0.consume(prefix_len);

            let length = prost::decode_length_delimiter(prost::bytes::Buf::chain(
                prefix,
                self.0.fill_buf()?,
            ))?;
            self.0
                .consume(prost::length_delimiter_len(length) - prefix_len);
            length
        };

        // We can't naively fill the buffer, since the message may be larger
        // than the buffer size. We may be able to do something conditionally?
        let mut buffer = vec![0; message_length];
        self.0.read_exact(&mut buffer)?;

        let message = T::decode(buffer.as_slice())?;
        Ok(Some(message))
    }
}

#[repr(transparent)]
struct FlightRecordEventReader<R: std::io::Read>(DelimitedProtoReader<R>);

#[derive(Debug)]
pub enum Error {
    ProstDecode(prost::DecodeError),
    Io(std::io::Error),
    MissingHeader,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::ProstDecode(e) => write!(f, "Error decoding flight record: {e}"),
            Error::Io(e) => write!(f, "I/O Error: {e}"),
            Error::MissingHeader => write!(f, "Missing Header"),
        }
    }
}

impl std::error::Error for Error {}

impl<R: std::io::Read> FallibleIterator for FlightRecordEventReader<R> {
    type Item = FlightRecord;

    type Error = Error;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.0.read_message::<Self::Item>()
    }
}

impl From<prost::DecodeError> for Error {
    fn from(err: prost::DecodeError) -> Self {
        Error::ProstDecode(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::Io(err)
    }
}
