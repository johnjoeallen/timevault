fn main() {
    let result = timevault::cli::run();
    timevault::progress::shutdown();
    if let Err(err) = result {
        println!("{}", err);
        std::process::exit(2);
    }
}
