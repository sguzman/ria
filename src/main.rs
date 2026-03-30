fn main() {
    if let Err(err) = ria::cli::run() {
        eprintln!("ria: {err}");
        std::process::exit(1);
    }
}
