mod controllers;
mod crds;
mod fep;
mod ood;
mod utils;
use clap::Parser;
use clap::Subcommand;
use clap::ValueEnum;
use tracing::error;
use tracing_subscriber;

use crate::controllers::pun;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    #[command()]
    Controller { resources: Resources },
    #[command()]
    Crd,
}

#[derive(ValueEnum, Debug, Copy, Clone, PartialEq, Eq)]
enum Resources {
    Pun,
    Fep,
    Ood,
    All,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    match args.command {
        Commands::Controller { resources } => {
            let result = match resources {
                Resources::Pun => pun::controller().await,
                Resources::Ood => ood::controller().await,
                Resources::Fep => fep::controller().await,
                Resources::All => {
                    tokio::join!(pun::controller(), ood::controller(), fep::controller()).0
                }
            };

            error!("Controller {resources:?} with {result:?}");
        }
        Commands::Crd => {
            println!("{}", crds::export_crds());
        }
    }
}
