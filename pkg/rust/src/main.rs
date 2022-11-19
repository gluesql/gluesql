fn main() {
    #[cfg(feature = "cli")]
    cli::run().unwrap();
}
