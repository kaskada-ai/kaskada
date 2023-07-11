use object_store::ObjectMeta;

use crate::DETERMINISTIC_RUNTIME_HASHER;

pub trait ObjectMetaExt {
    /// Return a deterministic hash of the etag.
    ///
    /// For object stores that don't provide an etag, this will return a hash of
    /// the last modified time.
    fn etag_hash(&self) -> u64;
}

impl ObjectMetaExt for ObjectMeta {
    fn etag_hash(&self) -> u64 {
        match &self.e_tag {
            None => {
                // The underlying store doesn't provide an etag.
                //
                // We generate the hash of the etag by hashing the last_modified
                // time. Technically, we could also generate the etag, but this
                // skips a step.
                //
                // In systems, the entity tag may be generated from the last
                // modified time:
                //
                // > Entity tag that uniquely represents the requested resource.
                // > ... The method by which ETag values are generated is not
                // > specified. Typically, the ETag value is a hash of the
                // > content, a hash of the last modification timestamp, or just
                // > a revision number.
                //
                // -- https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
                DETERMINISTIC_RUNTIME_HASHER.hash_one(self.last_modified)
            }
            Some(e_tag) => DETERMINISTIC_RUNTIME_HASHER.hash_one(e_tag),
        }
    }
}
