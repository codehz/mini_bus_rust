use crate::gateway::handle_client;
use crate::registry::*;
use anyhow::Result;
use async_std::net;
use async_std::prelude::*;
use async_std::task;
use pretty_env_logger;

mod broker;
mod entity;
mod packet;
mod registry;
mod shared;
mod short_text;
mod utils;
mod gateway;

#[async_std::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();
    Registry::init().await;

    let listener = net::TcpListener::bind("127.0.0.1:4040").await?;
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        task::spawn(handle_client(stream?));
    }
    Ok(())
}
