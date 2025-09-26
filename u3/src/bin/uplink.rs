use structopt::StructOpt;

fn main() {
    let args = Cli::from_args();
    // initialize logging
    // parse config and auth

    //
}

#[derive(StructOpt)]
struct Cli {
    #[structopt(short = "v", long = "verbose", parse(from_occurrences))]
    pub log_level: u8,
    #[structopt(short, long)]
    pub config: String,
    #[structopt(short, long)]
    pub authentication: String
}
