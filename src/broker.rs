use crate::short_text::ShortText;
use async_std::sync::{Arc, Mutex, Weak};
use async_trait::async_trait;
use std::collections::HashMap;
use weak_table::PtrWeakHashSet;
use log::debug;

#[async_trait]
pub trait NotifyReceiver: Send + Sync {
    async fn on_notify(&self, key: &EventKey, data: Option<&[u8]>);
}

#[async_trait]
pub trait EventReceiver: Send + Sync {
    async fn on_event(&self, key: &EventKey, data: Option<&[u8]>);
}

#[async_trait]
pub trait GeneralReceiver: Send + Sync {
    async fn receive(&self, key: &EventKey, data: Option<&[u8]>);
}

#[async_trait]
impl GeneralReceiver for dyn NotifyReceiver {
    async fn receive(&self, key: &EventKey, data: Option<&[u8]>) {
        self.on_notify(key, data).await
    }
}

#[async_trait]
impl GeneralReceiver for dyn EventReceiver {
    async fn receive(&self, key: &EventKey, data: Option<&[u8]>) {
        self.on_event(key, data).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventKey(pub ShortText, pub ShortText);

pub struct Broker<Receiver: GeneralReceiver + ?Sized + Send + Sync> {
    map: Mutex<HashMap<EventKey, PtrWeakHashSet<Weak<Receiver>>>>,
}

pub fn get_notify_broker() -> &'static Broker<dyn NotifyReceiver> {
    static mut INSTANCE: Option<Broker<dyn NotifyReceiver>> = None;
    unsafe {
        INSTANCE.get_or_insert_with(|| Broker::new());
        INSTANCE.as_ref().unwrap()
    }
}

pub fn get_event_broker() -> &'static Broker<dyn EventReceiver> {
    static mut INSTANCE: Option<Broker<dyn EventReceiver>> = None;
    unsafe {
        INSTANCE.get_or_insert_with(|| Broker::new());
        INSTANCE.as_ref().unwrap()
    }
}

impl<Receiver: GeneralReceiver + ?Sized + Send + Sync> Broker<Receiver> {
    fn new() -> Self {
        Broker {
            map: Mutex::new(HashMap::new()),
        }
    }

    pub async fn send(&self, key: EventKey, data: Option<&[u8]>) {
        debug!("broadcast({:?}): {:?}", &key, &data);
        let guard = self.map.lock().await;
        if let Some(set) = guard.get(&key) {
            for item in set {
                item.receive(&key, data).await;
            }
        }
    }

    pub async fn register(&self, sender: Arc<Receiver>, key: EventKey) {
        let mut guard = self.map.lock().await;
        if let Some(set) = guard.get_mut(&key) {
            set.insert(sender);
        } else {
            let mut temp = PtrWeakHashSet::new();
            temp.insert(sender);
            guard.insert(key, temp);
        }
    }

    pub async fn cleanup(&self) {
        let mut guard = self.map.lock().await;
        let removed: Vec<_> = guard
            .iter_mut()
            .filter_map(|(k, set)| {
                set.remove_expired();
                if set.is_empty() {
                    Some(k.to_owned())
                } else {
                    None
                }
            })
            .collect();
        for k in removed {
            guard.remove(&k);
        }
    }
}
