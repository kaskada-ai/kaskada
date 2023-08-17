use sparrow_instructions::InstOp;

use crate::functions::{Implementation, Registry};

pub(super) fn register(registry: &mut Registry) {
    registry
        .register("is_valid<T: any>(input: T) -> bool")
        .with_implementation(Implementation::Instruction(InstOp::IsValid));

    registry
        .register("hash<K: key>(input: K) -> u64")
        .with_implementation(Implementation::Instruction(InstOp::Hash));

    registry
        .register("with_key<K: key, T: any>(key: K, value: T, const grouping: string = null) -> T")
        .with_implementation(Implementation::new_pattern({
            const MERGED_OP: &str = "(merge_join ?key_op ?value_op)";
            const MERGED_KEY: &str = const_format::formatcp!("(transform ?key_value {MERGED_OP})");
            const MERGED_VALUE: &str =
                const_format::formatcp!("(transform ?value_value {MERGED_OP})");

            const WITH_KEY_OP: &str =
                const_format::formatcp!("(with_key {MERGED_KEY} ?grouping_value)");
            const_format::formatcp!("(transform {MERGED_VALUE} {WITH_KEY_OP})")
        }))
        // Ideally, we would pull this through the implementation and not use the `is_new`
        // of the original `with_key`.
        .with_is_new(Implementation::new_pattern(
            "(transform (logical_or ?value_is_new ?key_is_new) (with_key (transform ?key_value \
             (merge_join ?key_op ?value_op)) ?grouping_value))",
        ));

    registry
        .register("lookup<K: key, T: any>(key: K, value: T) -> T")
        .with_implementation(Implementation::new_pattern({
            // The operation of the lookup request depends on the key value.
            //
            // NOTE: This means the same DFG node may be used for lookup requests
            // to different groupings. This should be OK since the schema is the same.
            const LOOKUP_REQUEST: &str = "(lookup_request ?key_value)";

            // The operation containing the lookup responses.
            const MERGED_LOOKUP_REQUEST: &str =
                const_format::formatcp!("(merge_join ?value_op {LOOKUP_REQUEST})");
            const LOOKUP_RESPONSE: &str = const_format::formatcp!(
                "(lookup_response {LOOKUP_REQUEST} {MERGED_LOOKUP_REQUEST})"
            );

            // We need to move the value from the foreign domain to the foreign-domain
            // merged with the lookup request. This ensures we'll have information about
            // the joined values.
            const JOINED_FOREIGN_VALUE: &str =
                const_format::formatcp!("(transform ?value_value {MERGED_LOOKUP_REQUEST})");

            const VALUE_IN_RESPONSE: &str =
                const_format::formatcp!("(transform {JOINED_FOREIGN_VALUE} {LOOKUP_RESPONSE})");

            // Merge the lookup result with the original key operation.
            const MERGED_RESPONSE: &str =
                const_format::formatcp!("(merge_join ?key_op {LOOKUP_RESPONSE})");

            // Carry the key from it's original operation to the merged response.
            const_format::formatcp!("(transform {VALUE_IN_RESPONSE} {MERGED_RESPONSE})")
        }))
        .with_is_new(Implementation::new_pattern({
            // TODO: Allow sharing these common patterns with the value implementation.
            //
            // The operation of the lookup request depends on the key value.
            //
            // NOTE: This means the same DFG node may be used for lookup requests
            // to different groupings. This should be OK since the schema is the same.
            const LOOKUP_REQUEST: &str = "(lookup_request ?key_value)";

            // The operation containing the lookup responses.
            const MERGED_LOOKUP_REQUEST: &str =
                const_format::formatcp!("(merge_join ?value_op {LOOKUP_REQUEST})");
            const LOOKUP_RESPONSE: &str = const_format::formatcp!(
                "(lookup_response {LOOKUP_REQUEST} {MERGED_LOOKUP_REQUEST})"
            );

            // Merge the lookup result with the original key operation.
            const MERGED_RESPONSE: &str =
                const_format::formatcp!("(merge_join ?key_op {LOOKUP_RESPONSE})");

            // Carry the key from it's original operation to the merged response.
            const_format::formatcp!("(transform ?key_is_new {MERGED_RESPONSE})")
        }));

    registry
        .register("coalesce<T: any>(values+: T) -> T")
        .with_implementation(Implementation::Instruction(InstOp::Coalesce));
}
