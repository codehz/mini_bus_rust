use crate::gateway::handle_client;
use crate::registry::*;
use anyhow::Result;
use async_std::net;
use async_std::prelude::*;
use async_std::task;
use pretty_env_logger;
use structopt::StructOpt;

mod broker;
mod entity;
mod gateway;
mod packet;
mod registry;
mod shared;
mod short_text;
mod utils;

#[derive(Debug, StructOpt)]
#[structopt(name = "minibus", about = "MiniBus Server Implemention")]
struct Opt {
    #[structopt(short, long, default_value = "127.0.0.1:4040")]
    listen: String,
}

#[async_std::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();
    pretty_env_logger::init();
    log::info!("option: {:#?}", &opt);
    Registry::init().await;

    let listener = net::TcpListener::bind(opt.listen).await?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        task::spawn(handle_client(stream?));
    }
    Ok(())
}
