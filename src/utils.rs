use crate::short_text::ShortText;
use async_std::io::{BufRead, Read, ReadExt, Result, Write};
use async_std::prelude::*;
use async_trait::async_trait;
use rand::distributions::{Distribution, Standard};
use rand::random;
use std::borrow::Borrow;
use std::collections::{BTreeMap, HashMap};
use std::hash::{BuildHasher, Hash};
use std::mem::transmute;

pub async fn verify_client<T: BufRead + Unpin>(reader: &mut T) -> Result<bool> {
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf).await?;
    Ok(&buf == b"MINIBUS\x00")
}

#[async_trait]
pub trait EncoderUtils {
    async fn encode_short_text(&mut self, str: &ShortText) -> Result<()>;
    async fn encode_varuint(&mut self, val: usize) -> Result<()>;
    async fn encode_binary(&mut self, str: &[u8]) -> Result<()>;
    async fn encode_reqid(&mut self, id: u32) -> Result<()>;
}

#[async_trait]
pub trait Encoder<T> {
    async fn encode(&mut self, data: T) -> Result<()>;
}

#[async_trait]
pub trait DecoderUtils {
    async fn decode_short_text(&mut self) -> Result<ShortText>;
    async fn decode_varuint(&mut self) -> Result<usize>;
    async fn decode_binary(&mut self) -> Result<Vec<u8>>;
    async fn decode_reqid(&mut self) -> Result<u32>;
}

#[async_trait]
pub trait Decoder<T> {
    async fn decode(&mut self) -> Result<T>;
}

#[async_trait]
impl<T: Write + Unpin + Send> EncoderUtils for T {
    async fn encode_short_text(&mut self, str: &ShortText) -> Result<()> {
        self.write(&[str.len() as u8]).await?;
        self.write(str.as_bytes()).await?;
        Ok(())
    }
    async fn encode_varuint(&mut self, sval: usize) -> Result<()> {
        let mut val = sval;
        loop {
            if val < 128 {
                self.write(&[val as u8]).await?;
                break;
            }
            let n: u8 = ((val & 0b01111111) | 0b10000000) as u8;
            self.write(&[n]).await?;
            val >>= 7;
        }
        Ok(())
    }
    async fn encode_binary(&mut self, str: &[u8]) -> Result<()> {
        self.encode_varuint(str.len()).await?;
        self.write(str).await?;
        Ok(())
    }
    async fn encode_reqid(&mut self, id: u32) -> Result<()> {
        let bytes: [u8; 4] = unsafe { transmute(id) };
        self.write(&bytes).await?;
        Ok(())
    }
}

#[async_trait]
impl<T: Read + Unpin + Send> DecoderUtils for T {
    async fn decode_short_text(&mut self) -> Result<ShortText> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf).await?;
        let len = buf[0];
        unsafe {
            let mut ret = ShortText::new(len);
            self.read_exact(ret.get_buffer()).await?;
            Ok(ret)
        }
    }
    async fn decode_varuint(&mut self) -> Result<usize> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf).await?;
        let ret = buf[0] as usize;
        if ret < 128 {
            return Ok(ret);
        }
        let ret = ret & 0b01111111;
        let ret = ret + (self.decode_varuint().await? << 7);
        Ok(ret)
    }
    async fn decode_binary(&mut self) -> Result<Vec<u8>> {
        let len = self.decode_varuint().await?;
        let mut buf: Vec<u8> = Vec::with_capacity(len);
        unsafe {
            buf.set_len(len);
        }
        self.read_exact(&mut buf).await?;
        Ok(buf)
    }
    async fn decode_reqid(&mut self) -> Result<u32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf).await?;
        Ok(unsafe { transmute(buf) })
    }
}

pub trait RandomKey<K: Hash + Eq>
where
    Standard: Distribution<K>,
{
    fn gen_random_key<Q: ?Sized>(&self) -> K
    where
        K: Borrow<Q>,
        Q: Hash + Ord + Eq;
}

impl<K: Hash + Eq, V, S> RandomKey<K> for HashMap<K, V, S>
where
    Standard: Distribution<K>,
    S: BuildHasher,
{
    fn gen_random_key<Q: ?Sized>(&self) -> K
    where
        K: Borrow<Q>,
        Q: Hash + Ord + Eq,
    {
        let k = random();
        if self.contains_key(k.borrow()) {
            self.gen_random_key()
        } else {
            k
        }
    }
}

impl<K: Hash + Eq, V> RandomKey<K> for BTreeMap<K, V>
where
    Standard: Distribution<K>,
    K: Ord,
{
    fn gen_random_key<Q: ?Sized>(&self) -> K
    where
        K: Borrow<Q>,
        Q: Hash + Ord + Eq,
    {
        let k = random();
        if self.contains_key(k.borrow()) {
            self.gen_random_key()
        } else {
            k
        }
    }
}

pub fn strerr<T, S: Into<String>>(data: S) -> Result<T> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, data.into()))
}
