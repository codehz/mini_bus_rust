use crate::broker::{get_event_broker, EventKey};
use crate::entity::{AccessTag, Entity};
use crate::registry::StaticRegistryItem;
use crate::short_text::ShortText;
use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use std::collections::HashMap;

pub struct SharedStorage {
    data: Mutex<HashMap<ShortText, Vec<u8>>>,
}

inventory::submit! {
    StaticRegistryItem(b"shared", Arc::new(SharedStorage{
        data: Mutex::new(HashMap::new())
    }))
}

impl SharedStorage {}

#[async_trait]
impl Entity for SharedStorage {
    async fn get(
        &self,
        _sender: &Arc<dyn Entity>,
        key: &ShortText,
    ) -> std::io::Result<Option<Vec<u8>>> {
        let guard = self.data.lock().await;
        let ret = guard.get(key).map(|x| x.clone());
        Ok(ret)
    }
    async fn set(
        &self,
        _sender: &Arc<dyn Entity>,
        key: &ShortText,
        val: Vec<u8>,
    ) -> std::io::Result<()> {
        let mut guard = self.data.lock().await;
        get_event_broker()
            .send(
                EventKey(ShortText::build(b"shared"), key.to_owned()),
                Some(&val[..]),
            )
            .await;
        guard.insert(key.to_owned(), val);
        Ok(())
    }
    async fn del(&self, _sender: &Arc<dyn Entity>, key: &ShortText) -> std::io::Result<()> {
        let mut guard = self.data.lock().await;
        get_event_broker()
            .send(EventKey(ShortText::build(b"shared"), key.to_owned()), None)
            .await;
        guard.remove(key);
        todo!()
    }
    async fn keys(
        &self,
        _sender: &Arc<dyn Entity>,
    ) -> std::io::Result<Vec<(ShortText, AccessTag)>> {
        let guard = self.data.lock().await;
        let ret = guard
            .keys()
            .map(|x| (x.to_owned(), AccessTag::Public))
            .collect();
        Ok(ret)
    }
}
