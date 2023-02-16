use uuid::Uuid;

/// A wrapper around a UUID identifying a Table.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct TableId(Uuid);

impl TableId {
    pub const fn new(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub fn as_proto(&self) -> sparrow_api::kaskada::v1alpha::Uuid {
        self.0.into()
    }
}

impl std::fmt::Display for TableId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A wrapper around a u32 identifying a distinct grouping.
#[derive(Debug, Eq, PartialEq, Clone, Copy, Hash, Ord, PartialOrd)]
#[repr(transparent)]
pub struct GroupId(u32);

impl GroupId {
    pub fn index(&self) -> usize {
        self.0 as usize
    }
}

impl From<usize> for GroupId {
    fn from(id: usize) -> Self {
        Self(id as u32)
    }
}

impl From<u32> for GroupId {
    fn from(id: u32) -> Self {
        Self(id)
    }
}

impl From<GroupId> for usize {
    fn from(id: GroupId) -> Self {
        id.0 as usize
    }
}

impl From<GroupId> for u32 {
    fn from(id: GroupId) -> Self {
        id.0
    }
}

impl std::fmt::Display for GroupId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
