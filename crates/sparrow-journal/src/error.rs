#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "checkpoint path ({}) is not a directory", "_0.display()")]
    CheckpointDirNotDir(std::path::PathBuf),
    #[display(fmt = "error creating writer")]
    CreatingWriter,
    #[display(fmt = "error recovering write-ahead-log")]
    Recovering,
    #[display(fmt = "error recovering checkpoints")]
    RecoveringCheckpoints,
    #[display(fmt = "error creating entry in write-ahead-log")]
    CreatingEntry,
    #[display(fmt = "error writing batch")]
    WriteBatch,
    #[display(fmt = "error concatenating batch")]
    Concatenate,
    #[display(fmt = "error reading batch")]
    ReadBatch,
    #[display(fmt = "error writing checkpoint")]
    WriteCheckpoint,
    #[display(fmt = "error requesting checkpoint")]
    Checkpointing,
    #[display(fmt = "error reading checkpoint")]
    ReadingCheckpoint,
}

impl error_stack::Context for Error {}
