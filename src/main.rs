use crate::broker::*;
use crate::entity::*;
use crate::packet::*;
use crate::registry::*;
use crate::utils::*;
use anyhow::{anyhow, Result};
use async_std::io;
use async_std::io::{BufReader, BufWriter, Read, Write};
use async_std::net;
use async_std::prelude::*;
use async_std::sync::Arc;
use async_std::task;
use log::debug;
use pretty_env_logger;
use std::time::Duration;

mod broker;
mod entity;
mod packet;
mod registry;
mod shared;
mod short_text;
mod utils;

fn errtoresp(e: std::io::Error) -> ResponsePayload {
    ResponsePayload::Failed(format!("{}", e).as_bytes().to_vec())
}

async fn handle_loop<Reader: Read + Unpin + Send, Writer: 'static + Write + Unpin + Send>(
    reader: &mut Reader,
    entity: &Arc<ExternalEntity<Writer>>,
) -> Result<()> {
    loop {
        let request: Request = reader.decode().await?;
        debug!("request: {:?}", &request);
        match request.command.as_bytes() {
            b"STOP" => break,
            b"PING" => {
                entity
                    .send(Response::new_resp(
                        request.reqid,
                        ResponsePayload::SuccessWithData(request.payload),
                    ))
                    .await?;
            }
            b"SET PRIVATE" => {
                let mut payload = request.payload.as_slice();
                let key = payload.decode_short_text().await?;
                entity.set_private(&key, payload.to_vec()).await;
                entity
                    .send(Response::new_resp(request.reqid, ResponsePayload::Success))
                    .await?;
            }
            b"GET PRIVATE" => {
                let mut payload = request.payload.as_slice();
                let key = payload.decode_short_text().await?;
                match entity.get_private(&key).await {
                    Some(data) => entity.send(Response::new_resp(
                        request.reqid,
                        ResponsePayload::SuccessWithData(data),
                    )),
                    None => entity.send(Response::new_resp(
                        request.reqid,
                        ResponsePayload::Failed(b"not found".to_vec()),
                    )),
                }
                .await?;
            }
            b"DEL PRIVATE" => {
                let mut payload = request.payload.as_slice();
                let key = payload.decode_short_text().await?;
                entity.del_private(&key).await;
                entity
                    .send(Response::new_resp(request.reqid, ResponsePayload::Success))
                    .await?;
            }
            b"ACL" => {
                let mut payload = request.payload.as_slice();
                let key = payload.decode_short_text().await?;
                let acl = payload.decode().await?;
                entity.set_acl(&key, acl).await;
                entity
                    .send(Response::new_resp(request.reqid, ResponsePayload::Success))
                    .await?;
            }
            b"SET" => {
                let mut payload = request.payload.as_slice();
                let target = payload.decode_short_text().await?;
                let key = payload.decode_short_text().await?;
                let value = payload.to_vec();
                if let Some(target) = Registry::get_global().find(&target).await {
                    let temp = Arc::clone(entity) as Arc<dyn Entity>;
                    let payload = target
                        .set(&temp, &key, value)
                        .await
                        .map_or_else(errtoresp, |_| ResponsePayload::Success);
                    entity
                        .send(Response::new_resp(request.reqid, payload))
                        .await?;
                } else {
                    entity
                        .send(Response::new_resp(
                            request.reqid,
                            ResponsePayload::Failed(b"target not found".to_vec()),
                        ))
                        .await?;
                }
            }
            b"GET" => {
                let mut payload = request.payload.as_slice();
                let target = payload.decode_short_text().await?;
                let key = payload.decode_short_text().await?;
                if let Some(target) = Registry::get_global().find(&target).await {
                    let temp = Arc::clone(&entity) as Arc<dyn Entity>;
                    let value = target
                        .get(&temp, &key)
                        .await
                        .map_or_else(errtoresp, |data| {
                            data.map_or(ResponsePayload::Success, |data| {
                                ResponsePayload::SuccessWithData(data)
                            })
                        });
                    entity
                        .send(Response::new_resp(request.reqid, value))
                        .await?;
                } else {
                    entity
                        .send(Response::new_resp(
                            request.reqid,
                            ResponsePayload::Failed(b"target not found".to_vec()),
                        ))
                        .await?;
                }
            }
            b"DEL" => {
                let mut payload = request.payload.as_slice();
                let target = payload.decode_short_text().await?;
                let key = payload.decode_short_text().await?;
                if let Some(target) = Registry::get_global().find(&target).await {
                    let temp = Arc::clone(&entity) as Arc<dyn Entity>;
                    let value = target
                        .del(&temp, &key)
                        .await
                        .map_or_else(errtoresp, |_| ResponsePayload::Success);
                    entity
                        .send(Response::new_resp(request.reqid, value))
                        .await?;
                } else {
                    entity
                        .send(Response::new_resp(
                            request.reqid,
                            ResponsePayload::Failed(b"target not found".to_vec()),
                        ))
                        .await?;
                }
            }
            b"KEYS" => {
                let mut payload = request.payload.as_slice();
                let target = payload.decode_short_text().await?;
                if let Some(target) = Registry::get_global().find(&target).await {
                    let temp = Arc::clone(&entity) as Arc<dyn Entity>;
                    match target.keys(&temp).await {
                        Err(e) => {
                            entity
                                .send(Response::new_resp(request.reqid, errtoresp(e)))
                                .await?;
                        }
                        Ok(list) => {
                            let mut buf: Vec<u8> = Vec::new();
                            for (key, tag) in list {
                                buf.encode(tag).await.unwrap();
                                buf.encode_short_text(&key).await.unwrap();
                            }
                            entity
                                .send(Response::new_resp(
                                    request.reqid,
                                    ResponsePayload::SuccessWithData(buf),
                                ))
                                .await?;
                        }
                    }
                } else {
                    entity
                        .send(Response::new_resp(
                            request.reqid,
                            ResponsePayload::Failed(b"target not found".to_vec()),
                        ))
                        .await?;
                }
            }
            b"NOTIFY" => {
                let mut payload = request.payload.as_slice();
                let key = payload.decode_short_text().await?;
                let value = payload.to_vec();
                if let Some(name) = entity.get_name().await {
                    get_notify_broker().send(EventKey(name, key), Some(&value)).await;
                    entity
                        .send(Response::new_resp(request.reqid, ResponsePayload::Success))
                        .await?;
                } else {
                    entity
                        .send(Response::new_resp(
                            request.reqid,
                            ResponsePayload::Failed(b"no name".to_vec()),
                        ))
                        .await?;
                }
            }
            b"LISTEN" => {
                let mut payload = request.payload.as_slice();
                let target = payload.decode_short_text().await?;
                let key = payload.decode_short_text().await?;
                let broker = get_notify_broker();
                let temp = Arc::clone(&entity);
                broker.register(temp, EventKey(target, key)).await;
                entity.register_notify(request.reqid, EventKey(target, key)).await;
                entity
                    .send(Response::new_resp(request.reqid, ResponsePayload::Success))
                    .await?;
            }
            b"OBSERVE" => {
                let mut payload = request.payload.as_slice();
                let target = payload.decode_short_text().await?;
                let key = payload.decode_short_text().await?;
                let broker = get_event_broker();
                let temp = Arc::clone(&entity);
                broker.register(temp, EventKey(target, key)).await;
                entity.register_event(request.reqid, EventKey(target, key)).await;
                entity
                    .send(Response::new_resp(request.reqid, ResponsePayload::Success))
                    .await?;
            }
            b"CALL" => {
                let mut payload = request.payload.as_slice();
                let target = payload.decode_short_text().await?;
                let key = payload.decode_short_text().await?;
                let value = payload;
                if let Some(target) = Registry::get_global().find(&target).await {
                    let temp = Arc::clone(&entity) as Arc<dyn EntityReceiver>;
                    if let Err(e) = target.call(&temp, request.reqid, &key, value).await {
                        entity
                            .send(Response::new_resp(request.reqid, errtoresp(e)))
                            .await?;
                    }
                } else {
                    entity
                        .send(Response::new_resp(
                            request.reqid,
                            ResponsePayload::Failed(b"target not found".to_vec()),
                        ))
                        .await?;
                }
            }
            b"RESPONSE" => {
                entity
                    .recv_call_resp(
                        request.reqid,
                        ResponsePayload::SuccessWithData(request.payload),
                    )
                    .await;
            }
            b"EXCEPTION" => {
                entity
                    .recv_call_resp(
                        request.reqid,
                        ResponsePayload::Failed(request.payload),
                    )
                    .await;
            }
            _ => {
                entity
                    .send(Response::new_resp(
                        request.reqid,
                        ResponsePayload::Failed(b"Unknown command".to_vec()),
                    ))
                    .await?;
                break;
            }
        }
    }
    Ok(())
}

async fn handle_client(stream: net::TcpStream) -> Result<()> {
    let mut reader = BufReader::new(stream.clone());
    let mut writer = BufWriter::new(stream.clone());
    if !io::timeout(Duration::from_secs(1), verify_client(&mut reader)).await? {
        return Err(anyhow!("Fake client detected!"));
    }
    writer.write(b"OK").await?;
    writer.flush().await?;
    let entity = Arc::new(ExternalEntity::new(writer));
    let ret = handle_loop(&mut reader, &entity).await;
    drop(entity);
    drop(stream);
    task::spawn(get_event_broker().cleanup());
    task::spawn(get_notify_broker().cleanup());
    ret
}

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
