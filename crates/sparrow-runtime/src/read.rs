mod parquet_stream;
pub(super) mod sort_in_time;
mod stream_reader;
mod table_reader;
#[cfg(test)]
pub(crate) mod testing;

pub use table_reader::*;
