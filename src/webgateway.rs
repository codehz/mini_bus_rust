use crate::entity::*;
use crate::packet::ResponsePayload;
use crate::registry::Registry;
use async_std::sync::{channel, Arc, Receiver, Sender};
use async_trait::async_trait;

struct CallReceiver(Sender<ResponsePayload>);

impl CallReceiver {
    fn new() -> (CallReceiver, Receiver<ResponsePayload>) {
        let (s, r) = channel(1);
        (CallReceiver(s), r)
    }
}

#[async_trait]
impl EntityReceiver for CallReceiver {
    async fn call_resp(&self, _reqid: u32, val: ResponsePayload) {
        self.0.send(val).await;
    }
}

fn get_access_tag_flag(tag: AccessTag) -> char {
    match tag {
        AccessTag::Private => '!',
        AccessTag::Protected => '-',
        AccessTag::Public => '+',
    }
}

async fn get_bucket(req: tide::Request<()>) -> tide::Result<tide::Response> {
    let bucket = req.param("bucket")?;
    if let Some(bucket) = Registry::get_global().find(&bucket).await {
        let keys = bucket.keys(None).await?;
        let vec: Vec<_> = keys
            .iter()
            .map(|(name, tag)| format!("{}{}", get_access_tag_flag(*tag), name))
            .collect();
        Ok(tide::Response::from(vec.join("\x00")))
    } else {
        Ok(tide::Response::new(400))
    }
}

async fn get_bucket_key(req: tide::Request<()>) -> tide::Result<tide::Response> {
    let bucket = req.param("bucket")?;
    let key = req.param("key")?;
    if let Some(bucket) = Registry::get_global().find(&bucket).await {
        if let Some(data) = bucket.get(None, &key).await? {
            Ok(tide::Response::builder(400).body(data).build())
        } else {
            Ok(tide::Response::new(204))
        }
    } else {
        Ok(tide::Response::new(400))
    }
}

async fn put_bucket_key(mut req: tide::Request<()>) -> tide::Result<tide::Response> {
    let bucket = req.param("bucket")?;
    let key = req.param("key")?;
    if let Some(bucket) = Registry::get_global().find(&bucket).await {
        let value = req.body_bytes().await?;
        bucket.set(None, &key, value).await?;
        Ok(tide::Response::new(204))
    } else {
        Ok(tide::Response::new(400))
    }
}

async fn delete_bucket_key(req: tide::Request<()>) -> tide::Result<tide::Response> {
    let bucket = req.param("bucket")?;
    let key = req.param("key")?;
    if let Some(bucket) = Registry::get_global().find(&bucket).await {
        bucket.del(None, &key).await?;
        Ok(tide::Response::new(204))
    } else {
        Ok(tide::Response::new(400))
    }
}

async fn post_bucket_key(mut req: tide::Request<()>) -> tide::Result<tide::Response> {
    let bucket = req.param("bucket")?;
    let key = req.param("key")?;
    if let Some(bucket) = Registry::get_global().find(&bucket).await {
        let value = req.body_bytes().await?;
        let (s, r) = CallReceiver::new();
        let s = Arc::new(s) as Arc<dyn EntityReceiver>;
        bucket.call(&s, 0, &key, &value[..]).await?;
        match r.recv().await {
            Ok(ResponsePayload::Success) => Ok(tide::Response::new(204)),
            Ok(ResponsePayload::SuccessWithData(data)) => {
                Ok(tide::Response::builder(400).body(data).build())
            }
            Ok(ResponsePayload::Failed(e)) => Ok(tide::Response::builder(400).body(e).build()),
            Err(e) => Ok(tide::Response::builder(500).body(e.to_string()).build()),
        }
    } else {
        Ok(tide::Response::new(400))
    }
}

pub fn init(route: &mut tide::Route<()>) {
    route.at("ping").get(|_| async { Ok("pong") });
    route.at("map/:bucket").get(get_bucket);
    route.at("map/:bucket/:key").get(get_bucket_key);
    route.at("map/:bucket/:key").put(put_bucket_key);
    route.at("map/:bucket/:key").delete(delete_bucket_key);
    route.at("map/:bucket/:key").post(post_bucket_key);
}
