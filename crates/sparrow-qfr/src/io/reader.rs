use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::PathBuf;

use error_stack::{IntoReport, ResultExt};
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
    pub fn try_new(path: &std::path::Path) -> error_stack::Result<Self, Error> {
        let mut reader = DelimitedProtoReader::try_open(path).change_context(Error::Internal)?;

        if let Some(header) = reader.read_message()? {
            let event_position = reader.stream_position().change_context(Error::Internal)?;
            Ok(Self {
                header,
                event_position,
                path: path.to_path_buf(),
            })
        } else {
            error_stack::bail!(Error::MissingHeader)
        }
    }

    pub fn header(&self) -> &FlightRecordHeader {
        &self.header
    }

    pub fn records(
        &self,
    ) -> error_stack::Result<
        impl FallibleIterator<Item = FlightRecord, Error = error_stack::Report<Error>>,
        Error,
    > {
        let reader = DelimitedProtoReader::try_open_offset(&self.path, self.event_position)
            .change_context(Error::Internal)?;
        let reader = FlightRecordEventReader(reader);
        Ok(reader)
    }
}

#[repr(transparent)]
struct DelimitedProtoReader<R: std::io::Read>(BufReader<R>);

impl DelimitedProtoReader<File> {
    fn try_open(path: &std::path::Path) -> error_stack::Result<Self, std::io::Error> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        Ok(Self(reader))
    }

    fn try_open_offset(
        path: &std::path::Path,
        offset: u64,
    ) -> error_stack::Result<Self, std::io::Error> {
        let file = File::open(path).into_report()?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(offset)).into_report()?;
        Ok(Self(reader))
    }

    fn stream_position(&mut self) -> error_stack::Result<u64, std::io::Error> {
        Ok(self.0.stream_position()?)
    }
}

impl<R: std::io::Read> DelimitedProtoReader<R> {
    /// Reads the given message (if the stream isn't empty).
    fn read_message<T: Message + Default>(&mut self) -> error_stack::Result<Option<T>, Error> {
        let buf = self
            .0
            .fill_buf()
            .into_report()
            .change_context(Error::Internal)?;
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
                self.0
                    .fill_buf()
                    .into_report()
                    .change_context(Error::Internal)?,
            ))
            .into_report()
            .change_context(Error::Internal)?;
            self.0
                .consume(prost::length_delimiter_len(length) - prefix_len);
            length
        };

        // We can't naively fill the buffer, since the message may be larger
        // than the buffer size. We may be able to do something conditionally?
        let mut buffer = vec![0; message_length];
        self.0
            .read_exact(&mut buffer)
            .into_report()
            .change_context(Error::Internal)?;

        let message = T::decode(buffer.as_slice())
            .into_report()
            .change_context(Error::Internal)?;
        Ok(Some(message))
    }
}

#[repr(transparent)]
struct FlightRecordEventReader<R: std::io::Read>(DelimitedProtoReader<R>);

impl<R: std::io::Read> FallibleIterator for FlightRecordEventReader<R> {
    type Item = FlightRecord;

    type Error = error_stack::Report<Error>;

    fn next(&mut self) -> Result<Option<Self::Item>, Self::Error> {
        self.0.read_message::<Self::Item>()
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "internal error reading flight records")]
    Internal,
    #[display(fmt = "flight record file missing header")]
    MissingHeader,
}

impl error_stack::Context for Error {}
