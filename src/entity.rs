use crate::broker::{get_event_broker, EventKey, EventReceiver, NotifyReceiver};
use crate::packet::{Response, ResponsePayload};
use crate::short_text::ShortText;
use crate::utils::{strerr, Decoder, DecoderUtils, Encoder, EncoderUtils, RandomKey};
use async_std::io::{Read, Result, Write};
use async_std::prelude::*;
use async_std::sync::{Arc, Mutex, Weak};
use async_trait::async_trait;
use log::debug;
use std::collections::{BTreeMap, HashMap};

#[derive(PartialEq, Clone, Copy)]
pub enum AccessTag {
    Private,
    Protected,
    Public,
}

#[async_trait]
impl<T: Read + Unpin + Send> Decoder<AccessTag> for T {
    async fn decode(&mut self) -> Result<AccessTag> {
        let ret = match self.decode_short_text().await?.as_bytes() {
            b"private" => AccessTag::Private,
            b"protected" => AccessTag::Protected,
            b"public" => AccessTag::Public,
            _ => {
                return strerr("unexcepted");
            }
        };
        Ok(ret)
    }
}

#[async_trait]
impl<T: Write + Unpin + Send> Encoder<AccessTag> for T {
    async fn encode(&mut self, data: AccessTag) -> Result<()> {
        let tag = match data {
            AccessTag::Private => ShortText::build(b"private"),
            AccessTag::Protected => ShortText::build(b"protected"),
            AccessTag::Public => ShortText::build(b"public"),
        };
        self.encode_short_text(&tag).await?;
        Ok(())
    }
}

pub struct ValueWithAccess(Option<Vec<u8>>, AccessTag);

#[async_trait]
pub trait Entity: Sync + Send {
    async fn update_name(&self, _name: Option<&ShortText>) {}
    async fn get(
        &self,
        sender: Option<&Arc<dyn Entity>>,
        key: &ShortText,
    ) -> Result<Option<Vec<u8>>>;
    async fn set(
        &self,
        sender: Option<&Arc<dyn Entity>>,
        key: &ShortText,
        val: Vec<u8>,
    ) -> Result<()>;
    async fn del(&self, sender: Option<&Arc<dyn Entity>>, key: &ShortText) -> Result<()>;
    async fn keys(&self, sender: Option<&Arc<dyn Entity>>) -> Result<Vec<(ShortText, AccessTag)>>;
    async fn call(
        &self,
        _sender: &Arc<dyn EntityReceiver>,
        _reqid: u32,
        _key: &ShortText,
        _val: &[u8],
    ) -> Result<()> {
        strerr("not supported")
    }
}

#[async_trait]
pub trait EntityReceiver: Sync + Send {
    async fn assign_call_ids(&self, _reqid: u32, _resid: u32) {}
    async fn remove_call_id(&self, _resid: u32) {}
    async fn call_resp(&self, reqid: u32, val: ResponsePayload);
}

pub struct ExternalEntity<Writer: 'static + Write + Unpin + Send> {
    name: Mutex<Option<ShortText>>,
    writer: Mutex<Writer>,
    kvstore: Mutex<HashMap<ShortText, ValueWithAccess>>,
    pending_call: Mutex<BTreeMap<u32, u32>>,
    call_record: Mutex<BTreeMap<u32, Weak<dyn EntityReceiver>>>,
    notify_subscribe: Mutex<HashMap<EventKey, u32>>,
    event_subscribe: Mutex<HashMap<EventKey, u32>>,
}

#[async_trait]
impl<Writer: 'static + Write + Unpin + Send> EntityReceiver for ExternalEntity<Writer> {
    async fn assign_call_ids(&self, reqid: u32, resid: u32) {
        let mut guard = self.pending_call.lock().await;
        guard.insert(resid, reqid);
    }
    async fn remove_call_id(&self, resid: u32) {
        let mut guard = self.pending_call.lock().await;
        guard.remove(&resid);
    }
    async fn call_resp(&self, resid: u32, val: ResponsePayload) {
        let mut guard = self.pending_call.lock().await;
        if let Some(reqid) = guard.remove(&resid) {
            let _ = self.send(Response::new_resp(reqid, val)).await;
        }
    }
}

#[async_trait]
impl<Writer: 'static + Write + Unpin + Send> NotifyReceiver for ExternalEntity<Writer> {
    async fn on_notify(&self, key: &EventKey, data: Option<&[u8]>) {
        let guard = self.notify_subscribe.lock().await;
        debug!("notify({:?}): {:?}", key, data);
        if let Some(id) = guard.get(key) {
            let _ = self
                .send(Response::new_next(
                    *id,
                    if let Some(data) = data {
                        ResponsePayload::SuccessWithData(data.to_vec())
                    } else {
                        ResponsePayload::Success
                    },
                ))
                .await;
        }
    }
}

#[async_trait]
impl<Writer: 'static + Write + Unpin + Send> EventReceiver for ExternalEntity<Writer> {
    async fn on_event(&self, key: &EventKey, data: Option<&[u8]>) {
        let guard = self.event_subscribe.lock().await;
        debug!("event({:?}): {:?}", key, data);
        if let Some(id) = guard.get(key) {
            let _ = self
                .send(Response::new_next(
                    *id,
                    if let Some(data) = data {
                        ResponsePayload::SuccessWithData(data.to_vec())
                    } else {
                        ResponsePayload::Success
                    },
                ))
                .await;
        }
    }
}

#[async_trait]
impl<Writer: 'static + Write + Unpin + Send> Entity for ExternalEntity<Writer> {
    async fn update_name(&self, name: Option<&ShortText>) {
        let mut guard = self.name.lock().await;
        *guard = name.map(|x| x.to_owned());
    }

    async fn get(
        &self,
        _sender: Option<&Arc<dyn Entity>>,
        key: &ShortText,
    ) -> Result<Option<Vec<u8>>> {
        let guard = self.kvstore.lock().await;
        match guard.get(key) {
            Some(ValueWithAccess(_, AccessTag::Private)) => strerr("not allowed"),
            Some(ValueWithAccess(value, _)) => Ok(value.to_owned()),
            None => strerr("not found"),
        }
    }
    async fn set(
        &self,
        _sender: Option<&Arc<dyn Entity>>,
        key: &ShortText,
        val: Vec<u8>,
    ) -> Result<()> {
        let mut guard = self.kvstore.lock().await;
        match guard.get_mut(key) {
            Some(ValueWithAccess(value, AccessTag::Public)) => {
                if let Some(name) = self.name.lock().await.to_owned() {
                    get_event_broker()
                        .send(EventKey(name, key.to_owned()), Some(&val[..]))
                        .await;
                }
                value.replace(val);
                Ok(())
            }
            Some(_) => strerr("not allowned"),
            None => strerr("not found"),
        }
    }
    async fn del(&self, _sender: Option<&Arc<dyn Entity>>, key: &ShortText) -> Result<()> {
        let mut guard = self.kvstore.lock().await;
        match guard.get_mut(key) {
            Some(ValueWithAccess(value, AccessTag::Public)) => {
                if let Some(name) = self.name.lock().await.to_owned() {
                    get_event_broker()
                        .send(EventKey(name, key.to_owned()), None)
                        .await;
                }
                value.take();
                Ok(())
            }
            Some(_) => strerr("not allowned"),
            None => strerr("not found"),
        }
    }

    async fn keys(&self, _sender: Option<&Arc<dyn Entity>>) -> Result<Vec<(ShortText, AccessTag)>> {
        let guard = self.kvstore.lock().await;
        let ret = guard
            .iter()
            .map(|(key, ValueWithAccess(_, access))| (key.to_owned(), access.to_owned()))
            .collect();
        Ok(ret)
    }

    async fn call(
        &self,
        sender: &Arc<dyn EntityReceiver>,
        reqid: u32,
        key: &ShortText,
        val: &[u8],
    ) -> Result<()> {
        let mut guard = self.call_record.lock().await;
        let id = guard.gen_random_key();
        let mut buf = Vec::new();
        buf.encode_short_text(key).await?;
        buf.write(val).await?;
        sender.assign_call_ids(reqid, id).await;
        guard.insert(id, Arc::downgrade(sender));
        if let Err(e) = self
            .send(Response::new_call(
                id,
                ResponsePayload::SuccessWithData(buf),
            ))
            .await
        {
            sender.remove_call_id(id).await;
            Err(e)
        } else {
            Ok(())
        }
    }
}

impl<Writer: 'static + Write + Unpin + Send> ExternalEntity<Writer> {
    pub async fn get_name(&self) -> Option<ShortText> {
        self.name.lock().await.to_owned()
    }

    pub async fn send(&self, resp: Response) -> Result<()> {
        let mut guard = self.writer.lock().await;
        guard.encode(resp).await?;
        guard.flush().await?;
        Ok(())
    }

    pub async fn set_acl(&self, key: &ShortText, acl: AccessTag) {
        let mut guard = self.kvstore.lock().await;
        match guard.get_mut(key) {
            Some(va) => {
                let ValueWithAccess(_, ref mut access) = va;
                *access = acl;
            }
            None => {
                guard.insert(key.to_owned(), ValueWithAccess(None, acl));
            }
        }
    }

    pub async fn get_private(&self, key: &ShortText) -> Option<Vec<u8>> {
        let guard = self.kvstore.lock().await;
        match guard.get(key) {
            Some(ValueWithAccess(value, _)) => value.to_owned(),
            None => None,
        }
    }

    pub async fn set_private(&self, key: &ShortText, value: Vec<u8>) {
        let mut guard = self.kvstore.lock().await;
        if let Some(name) = self.name.lock().await.to_owned() {
            get_event_broker()
                .send(EventKey(name, key.to_owned()), Some(&value[..]))
                .await;
        }
        guard.insert(
            key.to_owned(),
            ValueWithAccess(Some(value), AccessTag::Public),
        );
    }

    pub async fn del_private(&self, key: &ShortText) {
        let mut guard = self.kvstore.lock().await;
        if let Some(name) = self.name.lock().await.to_owned() {
            get_event_broker()
                .send(EventKey(name, key.to_owned()), None)
                .await;
        }
        guard.remove(key);
    }

    pub async fn recv_call_resp(&self, reqid: u32, payload: ResponsePayload) {
        let mut call_record = self.call_record.lock().await;
        if let Some(tgt) = call_record.remove(&reqid) {
            if let Some(tgt) = tgt.upgrade() {
                tgt.call_resp(reqid, payload).await;
            }
        }
    }

    pub async fn register_event(&self, reqid: u32, ek: EventKey) {
        let mut guard = self.event_subscribe.lock().await;
        guard.insert(ek, reqid);
    }

    pub async fn register_notify(&self, reqid: u32, ek: EventKey) {
        let mut guard = self.notify_subscribe.lock().await;
        guard.insert(ek, reqid);
    }

    pub fn new(writer: Writer) -> ExternalEntity<Writer> {
        ExternalEntity {
            name: Mutex::new(None),
            writer: Mutex::new(writer),
            kvstore: Mutex::new(HashMap::with_capacity(32)),
            pending_call: Mutex::new(BTreeMap::new()),
            call_record: Mutex::new(BTreeMap::new()),
            event_subscribe: Mutex::new(HashMap::with_capacity(8)),
            notify_subscribe: Mutex::new(HashMap::with_capacity(8)),
        }
    }
}
