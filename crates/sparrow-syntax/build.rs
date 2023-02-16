/// Generates code for the parser.
fn main() {
    lalrpop::Configuration::new()
        // Emit rerun directives to avoid re-running the build script unless
        // something changes.
        .emit_rerun_directives(true)
        .use_cargo_dir_conventions()
        .process()
        .unwrap();
}
