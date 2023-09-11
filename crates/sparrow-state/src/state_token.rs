use crate::{Error, Keys, ReadState, StateWriter, WriteState};

pub trait StateToken {
    type State;

    /// Return the index of this state token.
    fn state_index(&self) -> u16;
}

pub fn read<T>(
    token: &T,
    keys: &Keys,
    backend: &dyn ReadState<T::State>,
) -> error_stack::Result<T::State, Error>
where
    T: StateToken,
{
    backend.read(token.state_index(), keys)
}

pub fn write<T>(
    token: &T,
    keys: &Keys,
    writer: &mut dyn StateWriter,
    value: T::State,
) -> error_stack::Result<(), Error>
where
    T: StateToken,
    dyn StateWriter: WriteState<T::State>,
{
    writer.write(token.state_index(), keys, value)
}
