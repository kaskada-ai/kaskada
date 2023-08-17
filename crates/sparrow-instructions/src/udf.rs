use sparrow_syntax::Signature;
use static_init::StaticInfo;
use std::fmt::Debug;
use std::hash::Hash;

use crate::Evaluator;

// TODO: See if Ord and PartialOrd are necessary
//
/// Defines the interface for user-defined functions.
// pub trait UserDefinedFunction: PartialEq + Eq + PartialOrd + Ord + Hash {
pub trait UserDefinedFunction: Send + Sync + Debug {
    fn signature(&self) -> &Signature;
    fn make_evaluator(&self, static_info: StaticInfo) -> Box<dyn Evaluator>;
    fn uuid(&self) -> &uuid::Uuid;
}

impl PartialOrd for dyn UserDefinedFunction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn UserDefinedFunction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.signature()
            .name()
            .cmp(other.signature().name())
            .then(self.uuid().cmp(other.uuid()))
    }
}

impl Hash for dyn UserDefinedFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.signature().hash(state);
    }
}

impl PartialEq for dyn UserDefinedFunction {
    fn eq(&self, other: &Self) -> bool {
        &*self.signature() == &*other.signature()
    }
}

impl Eq for dyn UserDefinedFunction {}

// TODO:Could use https://github.com/dtolnay/dyn-clone ?
// pub trait BoxClone {
//     fn clone_box<'a>(&self) -> Box<dyn UserDefinedFunction>;
// }

// impl<T> BoxClone for T
// where
//     T: UserDefinedFunction + Clone + Send + Sync + 'static,
// {
//     fn clone_box<'a>(&self) -> Box<dyn UserDefinedFunction> {
//         Box::new(self.clone())
//     }
// }

// impl Clone for Box<dyn UserDefinedFunction> {
//     fn clone(&self) -> Self {
//         self.clone_box()
//     }
// }

// impl PartialEq for dyn UserDefinedFunction + '_ {
//     fn eq(&self, other: &Self) -> bool {
//         &*self.signature() == &*other.signature()
//     }
// }
// impl Eq for dyn UserDefinedFunction + '_ {}

// struct Udf<T: UserDefinedFunction>(T);

// pub(super) trait ErasedUdf {
//     fn signature(&self) -> &Signature;
//     fn make_evaluator(&self, static_info: StaticInfo) -> Box<dyn Evaluator>;

//     fn as_any(&self) -> &dyn Any;
//     fn equals_dyn(&self, other: &dyn ErasedUdf) -> bool;
// }

// impl<T: UserDefinedFunction> ErasedUdf for Udf<T> {
//     fn signature(&self) -> &Signature {
//         self.0.signature()
//     }

//     fn make_evaluator(&self, static_info: StaticInfo) -> Box<dyn Evaluator> {
//         self.0.make_evaluator(static_info)
//     }

//     fn as_any(&self) -> &dyn Any {
//         self
//     }

// fn clone_any(&self) -> Box<dyn ErasedUdf> {
//     todo!()
// }
// }

// impl PartialEq for dyn ErasedUdf {
//     fn eq(&self, other: &Self) -> bool {
//         other
//             .as_any()
//             .downcast_ref::<Self>()
//             .map_or(false, |a| self == a)
//     }
// }

// impl Eq for dyn ErasedUdf {}

// impl Hash for dyn ErasedUdf {}

// #[derive(Eq, PartialEq)]
// struct Foo(Box<dyn ErasedUdf>);
