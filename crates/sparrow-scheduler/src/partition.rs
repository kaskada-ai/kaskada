index_vec::define_index_type! {
    /// Wrapper around a partition.
    pub struct Partition = u32;

    DISPLAY_FORMAT = "{}";
}

pub type Partitioned<T> = index_vec::IndexVec<Partition, T>;
