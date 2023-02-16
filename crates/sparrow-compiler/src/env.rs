//! Provides the environment for managing scope information.

use std::borrow::Borrow;
use std::hash::Hash;

use hashbrown::hash_map::HashMap;

/// Manages a lexical environment mapping keys `K` to values `V`.
///
/// The goals are to (1) make it easy to get the bindings for the current scope
/// and (2) make it relatively easy to enter / exit scopes. The strategy
/// employed is to maintain all of the *active* bindings in a map. Whenever a
/// binding is shadowed it is added to a stack. When a scope is entered, the
/// depth of the "shadow stack" is recorded. When a scope is left, the contents
/// of the shadow stack are applied to undo any bindings that were by that
/// scope.
#[derive(Debug)]
pub struct Env<K, V> {
    bindings: HashMap<K, V>,
    /// Stack of bindings that have been made and the values they shadowed, if
    /// any. Used to restore bindings when exiting scopes.
    shadow_stack: Vec<(K, Option<V>)>,
    /// Stack of scopes. When a new scope is entered, the size of the shadow
    /// stack is recorded. When exiting a scope, the extra items are popped
    /// from the shadow stack and restored in the binding map.
    scope_stack: Vec<usize>,
}

impl<K: Eq + Hash + Clone, V> Default for Env<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Eq + Hash + Clone, V> Env<K, V> {
    pub fn new() -> Self {
        Self {
            bindings: HashMap::new(),
            shadow_stack: Vec::new(),
            scope_stack: Vec::new(),
        }
    }

    /// Return true if the key is defined.
    pub fn contains<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.bindings.contains_key(key)
    }

    /// Retrieve a binding from the environment.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.bindings.get(key)
    }

    /// Adds a binding to the environment.
    ///
    /// Returns true if the environment did not already have a binding for
    /// `key`.
    pub fn insert(&mut self, key: K, val: V) -> bool {
        let previous = self.bindings.insert(key.clone(), val);
        let is_new = previous.is_none();
        self.shadow_stack.push((key, previous));
        is_new
    }

    /// Enters a new scope within the environment.
    ///
    /// A matched call to `exit` will drop all the bindings made after the
    /// corresponding `enter`.
    pub fn enter(&mut self) {
        let shadow_stack_size = self.shadow_stack.len();
        self.scope_stack.push(shadow_stack_size);
    }

    /// Exits the top-most scope, dropping any bindings made since it was
    /// entered.
    pub fn exit(&mut self) {
        let shadow_stack_size = self.scope_stack.pop().expect("No scope to exit");
        let to_restore = self.shadow_stack.split_off(shadow_stack_size);

        // To properly restore the state prior to the scope, we need to restore the
        // bindings in the opposite order.
        for (key, opt_val) in to_restore.into_iter().rev() {
            if let Some(val) = opt_val {
                self.bindings.insert(key, val);
            } else {
                self.bindings.remove(&key);
            }
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &K> {
        self.bindings.keys()
    }

    /// Apply a function to compute updated values in place.
    pub fn foreach_value(&self, f: impl Fn(&V)) {
        self.bindings.values().for_each(|v| {
            f(v);
        });
        self.shadow_stack.iter().for_each(|(_, v)| {
            if let Some(v) = v {
                f(v);
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bindings() {
        let mut env = Env::new();
        env.insert("hello", 5);
        env.insert("world", 6);
        env.insert("hello", 7);
        assert_eq!(env.get(&"hello"), Some(&7));
        assert_eq!(env.get(&"world"), Some(&6));
        assert_eq!(env.get(&"foo"), None);
    }

    #[test]
    fn scoping() {
        let mut env = Env::new();
        env.insert("hello", 5);
        env.enter();
        env.insert("world", 6);
        env.insert("hello", 7);
        assert_eq!(env.get(&"hello"), Some(&7));
        assert_eq!(env.get(&"world"), Some(&6));
        env.exit();
        assert_eq!(env.get(&"hello"), Some(&5));
        assert_eq!(env.get(&"world"), None);
    }

    #[test]
    fn scope_with_duplicate_bindings() {
        let mut env = Env::new();
        env.insert("hello", 5);
        env.enter();
        env.insert("world", 6);
        env.insert("hello", 7);
        env.insert("hello", 8);
        assert_eq!(env.get(&"hello"), Some(&8));
        assert_eq!(env.get(&"world"), Some(&6));
        env.exit();
        assert_eq!(env.get(&"hello"), Some(&5));
        assert_eq!(env.get(&"world"), None);
    }
}
