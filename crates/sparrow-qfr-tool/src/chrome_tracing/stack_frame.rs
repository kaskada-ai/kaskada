use serde::Serialize;

#[derive(Serialize, Eq, PartialEq, Debug)]
pub(crate) struct StackFrame {
    #[serde(skip_serializing)]
    pub(crate) id: StackFrameId,
    #[serde(skip_serializing_if = "str::is_empty")]
    pub(crate) category: &'static str,
    pub(crate) name: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) parent: Option<StackFrameId>,
}

/// Stack Frame ID.
#[repr(transparent)]
#[derive(Serialize, Eq, PartialEq, Debug, Hash, Clone, Copy)]
#[serde(transparent)]
pub(crate) struct StackFrameId(pub u32);
