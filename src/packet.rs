use crate::short_text::ShortText;
use crate::utils::*;
use async_std::io::{Error, ErrorKind, Read, ReadExt, Result, Write};
use async_std::prelude::*;
use async_trait::async_trait;
use log::debug;

#[derive(Debug, Clone)]
pub struct Request {
    pub reqid: u32,
    pub command: ShortText,
    pub payload: Vec<u8>,
}

#[async_trait]
impl<T: Read + Unpin + Send> Decoder<Request> for T {
    async fn decode(&mut self) -> Result<Request> {
        let reqid = self.decode_reqid().await?;
        let command = self.decode_short_text().await?;
        let payload = self.decode_binary().await?;
        Ok(Request {
            reqid,
            command,
            payload,
        })
    }
}

#[async_trait]
impl<T: Write + Unpin + Send> Encoder<Request> for T {
    async fn encode(&mut self, data: Request) -> Result<()> {
        self.encode_reqid(data.reqid).await?;
        self.encode_short_text(&data.command).await?;
        self.encode_binary(&data.payload).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ResponseKind {
    RESP,
    NEXT,
    CALL,
}

#[async_trait]
impl<T: Read + Unpin + Send> Decoder<ResponseKind> for T {
    async fn decode(&mut self) -> Result<ResponseKind> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf).await?;
        let ret = match &buf {
            b"RESP" => ResponseKind::RESP,
            b"NEXT" => ResponseKind::NEXT,
            b"CALL" => ResponseKind::CALL,
            _ => return Err(Error::new(ErrorKind::Other, "not match")),
        };
        Ok(ret)
    }
}

#[async_trait]
impl<T: Write + Unpin + Send> Encoder<ResponseKind> for T {
    async fn encode(&mut self, data: ResponseKind) -> Result<()> {
        let data = match data {
            ResponseKind::RESP => b"RESP",
            ResponseKind::NEXT => b"NEXT",
            ResponseKind::CALL => b"CALL",
        };
        self.write(data).await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ResponsePayload {
    Success,
    SuccessWithData(Vec<u8>),
    Failed(Vec<u8>),
}

#[async_trait]
impl<T: Read + Unpin + Send> Decoder<ResponsePayload> for T {
    async fn decode(&mut self) -> Result<ResponsePayload> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf).await?;
        let typ = buf[0];
        let ret = match typ {
            0 => ResponsePayload::Success,
            1 => ResponsePayload::SuccessWithData(self.decode_binary().await?),
            255 => ResponsePayload::Failed(self.decode_binary().await?),
            _ => return Err(Error::new(ErrorKind::Other, "not match")),
        };
        Ok(ret)
    }
}

#[async_trait]
impl<T: Write + Unpin + Send> Encoder<ResponsePayload> for T {
    async fn encode(&mut self, data: ResponsePayload) -> Result<()> {
        match data {
            ResponsePayload::Success => {
                self.write(&[0]).await?;
            }
            ResponsePayload::SuccessWithData(data) => {
                self.write(&[1]).await?;
                self.encode_binary(&data).await?;
            }
            ResponsePayload::Failed(data) => {
                self.write(&[255]).await?;
                self.encode_binary(&data).await?;
            }
        };
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Response {
    pub reqid: u32,
    pub kind: ResponseKind,
    pub payload: ResponsePayload,
}

impl Response {
    pub fn new_resp(reqid: u32, payload: ResponsePayload) -> Response {
        Response {
            reqid,
            kind: ResponseKind::RESP,
            payload,
        }
    }
    pub fn new_next(reqid: u32, payload: ResponsePayload) -> Response {
        Response {
            reqid,
            kind: ResponseKind::NEXT,
            payload,
        }
    }
    pub fn new_call(reqid: u32, payload: ResponsePayload) -> Response {
        Response {
            reqid,
            kind: ResponseKind::CALL,
            payload,
        }
    }
}

#[async_trait]
impl<T: Read + Unpin + Send> Decoder<Response> for T {
    async fn decode(&mut self) -> Result<Response> {
        let reqid = self.decode_reqid().await?;
        let kind = self.decode().await?;
        let payload = self.decode().await?;
        let ret = Response {
            reqid,
            kind,
            payload,
        };
        Ok(ret)
    }
}

#[async_trait]
impl<T: Write + Unpin + Send> Encoder<Response> for T {
    async fn encode(&mut self, data: Response) -> Result<()> {
        debug!("response: {:?}", &data);
        self.encode_reqid(data.reqid).await?;
        self.encode(data.kind).await?;
        self.encode(data.payload).await?;
        Ok(())
    }
}
