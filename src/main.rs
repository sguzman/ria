fn main() {
    if let Err(err) = ria::signals::install() {
        eprintln!("ria: {err}");
        std::process::exit(1);
    }
    if let Err(err) = ria::cli::run() {
        eprintln!("ria: {err}");
        std::process::exit(1);
    }
}
