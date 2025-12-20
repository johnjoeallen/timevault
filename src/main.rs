fn main() {
    if let Err(err) = timevault::cli::run() {
        println!("{}", err);
        std::process::exit(2);
    }
}
