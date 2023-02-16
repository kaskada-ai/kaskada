use std::path::{Path, PathBuf};

/// Create a path to testdata.
pub fn testdata_path(relative_path: impl AsRef<Path>) -> PathBuf {
    fn inner(relative_path: &Path) -> PathBuf {
        assert!(
            relative_path.is_relative(),
            "Test data file must be relative the `/testdata` dir: {}",
            relative_path.display()
        );

        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        // the cargo manifest is at `/crates/<crate>/`. testdata is ../../testdata.
        assert!(path.pop());
        assert!(path.pop());
        path.push("testdata");

        // now add the path
        path.push(relative_path);
        path
    }
    inner(relative_path.as_ref())
}
