use serde::ser::SerializeStruct;
use serde::Serialize;
use smallvec::SmallVec;
use sparrow_qfr::kaskada::sparrow::v1alpha::metric_value;

#[derive(PartialEq, Debug)]
#[repr(transparent)]
pub(crate) struct Args(pub SmallVec<[(&'static str, Arg); 4]>);

impl Args {
    pub fn new() -> Self {
        Args(SmallVec::new())
    }

    pub fn push(&mut self, name: &'static str, value: impl Into<Arg>) {
        self.0.push((name, value.into()))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum Arg {
    Unsigned(u64),
    Signed(i64),
    Float(f64),
    StaticString(&'static str),
    String(String),
    #[allow(dead_code)]
    ObjectRef(&'static str),
    Nested(Box<Args>),
}

impl Serialize for Arg {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Arg::Unsigned(n) => serializer.serialize_u64(*n),
            Arg::Signed(n) => serializer.serialize_i64(*n),
            Arg::Float(n) => serializer.serialize_f64(*n),
            Arg::StaticString(str) => serializer.serialize_str(str),
            Arg::String(str) => serializer.serialize_str(str),
            Arg::ObjectRef(object_id) => {
                let mut object_ref = serializer.serialize_struct("Reference", 1)?;
                object_ref.serialize_field("id_ref", &object_id)?;
                object_ref.end()
            }
            Arg::Nested(nested) => nested.serialize(serializer),
        }
    }
}

impl From<SmallVec<[(&'static str, Arg); 4]>> for Args {
    fn from(args: SmallVec<[(&'static str, Arg); 4]>) -> Self {
        Args(args)
    }
}

impl Serialize for Args {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_map(self.0.iter().map(|(k, v)| (k, v)))
    }
}

impl From<&'static str> for Arg {
    fn from(s: &'static str) -> Self {
        Self::StaticString(s)
    }
}

impl From<String> for Arg {
    fn from(s: String) -> Self {
        Self::String(s)
    }
}

impl From<u32> for Arg {
    fn from(n: u32) -> Self {
        Self::Unsigned(n as u64)
    }
}

impl From<i32> for Arg {
    fn from(n: i32) -> Self {
        Self::Signed(n as i64)
    }
}

impl From<u64> for Arg {
    fn from(n: u64) -> Self {
        Self::Unsigned(n)
    }
}

impl From<i64> for Arg {
    fn from(n: i64) -> Self {
        Self::Signed(n)
    }
}

impl From<Args> for Arg {
    fn from(nested: Args) -> Self {
        Self::Nested(Box::new(nested))
    }
}

impl From<metric_value::Value> for Arg {
    fn from(value: metric_value::Value) -> Self {
        match value {
            metric_value::Value::U64Value(n) => Arg::Unsigned(n),
            metric_value::Value::I64Value(n) => Arg::Signed(n),
            metric_value::Value::F64Value(n) => Arg::Float(n),
        }
    }
}
