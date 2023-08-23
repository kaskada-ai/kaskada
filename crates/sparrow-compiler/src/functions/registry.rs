use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use sparrow_syntax::{FeatureSetPart, Signature};
use static_init::dynamic;

use crate::nearest_matches::NearestMatches;

use super::{Function, FunctionBuilder};

pub(super) struct Registry {
    functions: HashMap<String, Function>,
}

impl Registry {
    fn new() -> Self {
        Self {
            functions: HashMap::new(),
        }
    }

    pub(super) fn register(&mut self, signature_str: &'static str) -> FunctionBuilder<'_> {
        let signature =
            Signature::try_from_str(FeatureSetPart::Function(signature_str), signature_str)
                .unwrap_or_else(|e| panic!("Failed to parse signature '{signature_str}': {e:?}"));

        match self.functions.entry(signature.name().to_owned()) {
            Entry::Occupied(_) => {
                // Using panic here is OK because the function registry is initialized
                // statically when Sparrow is loaded.
                panic!(
                    "Function with name '{}' already registered",
                    signature.name()
                )
            }
            Entry::Vacant(vacant) => {
                let function = Function::new(signature, signature_str);
                FunctionBuilder::new(vacant.insert(function))
            }
        }
    }

    /// Get the registered function with the given name.
    fn get_by_name(&self, name: &str) -> Option<&Function> {
        self.functions.get(name)
    }

    /// Return an iterator over the registered functions.
    fn iter(&self) -> impl Iterator<Item = &Function> {
        self.functions.values()
    }
}

/// Get the registered function with the given name.
///
/// The name of the function corresponds to the name from the signature.
/// For instance, the signature `is_valid(input: any) -> bool` has the name
/// `is_valid`.
///
/// # Errors
/// Returns the 5 closest matches from the registry.
pub fn get_function(name: &str) -> Result<&'static Function, NearestMatches<&'static str>> {
    REGISTRY.get_by_name(name).ok_or_else(|| {
        crate::nearest_matches::NearestMatches::new_nearest_strs(
            name,
            REGISTRY
                .iter()
                // Exclude "internal" functions.
                .filter(|i| !i.is_internal())
                .map(|i| i.name())
                // Add the "special" functions that don't yet fit into the registry.
                // `lookup` - Key type can't be specified by the signature. Could be
                //            in the registry using a more general type.
                // `extend` - Record behaviors can't be typed by the signature. Could
                //            be in the registry using a more general type.
                // TODO: Allow custom type-checking logic in the registry, then add
                //       these to the registry.
                .chain(["lookup", "extend", "remove_fields", "select_fields"]),
        )
    })
}

/// Return an iterator over all registered functions.
pub fn registered_functions() -> impl Iterator<Item = &'static Function> {
    REGISTRY.iter()
}

#[dynamic]
static REGISTRY: Registry = {
    let mut registry = Registry::new();
    super::register_functions(&mut registry);
    registry
};
