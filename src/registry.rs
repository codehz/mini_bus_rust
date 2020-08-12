use crate::broker::{get_event_broker, EventKey};
use crate::entity::{AccessTag, Entity};
use crate::short_text::ShortText;
use crate::utils::strerr;
use async_std::io::Result;
use async_std::sync::{Arc, Mutex, Weak};
use async_trait::async_trait;
use std::ptr;
use weak_table::{PtrWeakKeyHashMap, WeakValueHashMap};

struct BidiMap {
    forward: WeakValueHashMap<ShortText, Weak<dyn Entity>>,
    reverse: PtrWeakKeyHashMap<Weak<dyn Entity>, ShortText>,
}

impl BidiMap {
    fn new() -> BidiMap {
        BidiMap {
            forward: WeakValueHashMap::new(),
            reverse: PtrWeakKeyHashMap::new(),
        }
    }
}

pub struct StaticRegistryItem(pub &'static [u8], pub Arc<dyn Entity>);

inventory::collect!(StaticRegistryItem);

pub struct Registry {
    entities: Mutex<BidiMap>,
}

static mut INSTANCE: Option<Arc<Registry>> = None;

impl Registry {
    pub async fn init() {
        let instance = unsafe {
            INSTANCE.replace(Arc::new(Registry {
                entities: Mutex::new(BidiMap::new()),
            }));
            INSTANCE.as_ref().unwrap()
        };
        let mut guard = instance.entities.lock().await;
        guard
            .forward
            .insert(ShortText::build(b"registry"), instance.clone());
        for StaticRegistryItem(key, entity) in inventory::iter {
            guard.forward.insert(ShortText::build(key), entity.clone());
        }
    }
    pub fn get_global() -> &'static Registry {
        unsafe { INSTANCE.as_ref().unwrap() }
    }
    pub async fn find(&self, key: &ShortText) -> Option<Arc<dyn Entity>> {
        let guard = self.entities.lock().await;
        if let Some(target) = guard.forward.get(key) {
            Some(target)
        } else {
            None
        }
    }
}

#[async_trait]
impl Entity for Registry {
    async fn get(&self, _sender: &Arc<dyn Entity>, key: &ShortText) -> Result<Option<Vec<u8>>> {
        let guard = self.entities.lock().await;
        if guard.forward.contains_key(key) {
            Ok(None)
        } else {
            strerr("not found")
        }
    }
    async fn set(&self, sender: &Arc<dyn Entity>, key: &ShortText, val: Vec<u8>) -> Result<()> {
        let mut guard = self.entities.lock().await;
        if guard.forward.contains_key(key) {
            strerr("duplicated")
        } else {
            if let Some(_) = guard.reverse.get(sender) {
                strerr("too many names")
            } else {
                guard.forward.insert(key.to_owned(), sender.to_owned());
                guard.reverse.insert(sender.to_owned(), key.to_owned());
                sender.update_name(Some(key)).await;
                get_event_broker()
                    .send(
                        EventKey(ShortText::build(b"registry"), key.to_owned()),
                        Some(&val),
                    )
                    .await;
                Ok(())
            }
        }
    }
    async fn del(&self, sender: &Arc<dyn Entity>, key: &ShortText) -> Result<()> {
        let mut guard = self.entities.lock().await;
        if let Some(target) = guard.forward.get(key) {
            if ptr::eq(target.as_ref(), sender.as_ref()) {
                get_event_broker()
                    .send(
                        EventKey(ShortText::build(b"registry"), key.to_owned()),
                        None,
                    )
                    .await;
                guard.forward.remove(key);
                guard.reverse.remove(sender);
                Ok(())
            } else {
                strerr("not allowned")
            }
        } else {
            strerr("not found")
        }
    }
    async fn keys(&self, sender: &Arc<dyn Entity>) -> Result<Vec<(ShortText, AccessTag)>> {
        let guard = self.entities.lock().await;
        let ret = guard
            .forward
            .iter()
            .map(|(key, value)| {
                let tag = if ptr::eq(value.as_ref(), sender.as_ref()) {
                    AccessTag::Public
                } else {
                    AccessTag::Protected
                };
                (key.to_owned(), tag)
            })
            .collect();
        Ok(ret)
    }
}
