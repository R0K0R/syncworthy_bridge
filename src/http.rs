//! REST side: `/api/tree`, `/api/file`, `/api/rename`, `/api/delete`.
//! Every request carries the optional token both as a bearer header and as `?access_token=`
//! (Syncworthy's auth middleware accepts either; `?token=` on /ws/doc is an unrelated client id).

use crate::ws::Ev;
use serde_json::{Value, json};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Semaphore, mpsc::UnboundedSender};

#[derive(Debug)]
pub enum Err {
    Auth(String),
    Net(String),
    Other(String),
}

impl std::fmt::Display for Err {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Err::Auth(s) | Err::Net(s) | Err::Other(s) => write!(f, "{s}"),
        }
    }
}

pub struct Tree {
    pub root_name: String,
    pub files: Vec<String>,
    pub dirs: Vec<String>,
}

pub enum Fetch {
    NotModified,
    Gone,
    Body {
        bytes: Vec<u8>,
        etag: Option<String>,
    },
}

#[derive(Clone, Debug)]
pub enum OpKind {
    Create { path: String, text: String },
    Rename { from: String, to: String },
    Delete { path: String },
}

pub enum Rest {
    Tree(Instant, Result<Tree, Err>),
    Fetched(String, Result<Fetch, Err>),
    Op(OpKind, Result<(), String>),
    /// The role the token grants ("edit" or "view"); None when the server does not say.
    Role(Result<Option<String>, Err>),
}

#[derive(Clone)]
pub struct Api {
    pub client: reqwest::Client,
    pub base: String,
    pub token: Option<String>,
    pub sem: Arc<Semaphore>,
}

const MAX_FILE: u64 = 64 * 1024 * 1024;

impl Api {
    pub fn new(base: String, token: Option<String>) -> Self {
        Api {
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(60))
                .user_agent("syncworthy_bridge")
                .build()
                .expect("http client"),
            base,
            token,
            sem: Arc::new(Semaphore::new(8)),
        }
    }

    fn req(&self, m: reqwest::Method, path: &str) -> reqwest::RequestBuilder {
        let mut r = self.client.request(m, format!("{}{}", self.base, path));
        if let Some(t) = &self.token {
            r = r.bearer_auth(t).query(&[("access_token", t.as_str())]);
        }
        r
    }

    fn classify(e: reqwest::Error) -> Err {
        Err::Net(e.to_string())
    }

    fn status_err(s: reqwest::StatusCode) -> Option<Err> {
        if s == reqwest::StatusCode::UNAUTHORIZED || s == reqwest::StatusCode::FORBIDDEN {
            Some(Err::Auth(format!("server answered {s}")))
        } else if s.is_server_error() {
            Some(Err::Net(format!("server answered {s}")))
        } else {
            None
        }
    }

    pub async fn tree(&self) -> Result<Tree, Err> {
        let r = self
            .req(reqwest::Method::GET, "/api/tree")
            .send()
            .await
            .map_err(Self::classify)?;
        if let Some(e) = Self::status_err(r.status()) {
            return Err(e);
        }
        let v: Value = r
            .json()
            .await
            .map_err(|e| Err::Other(format!("bad /api/tree: {e}")))?;
        let mut t = Tree {
            root_name: v["root"].as_str().unwrap_or("").to_string(),
            files: vec![],
            dirs: vec![],
        };
        fn walk(items: &Value, t: &mut Tree) {
            for it in items.as_array().into_iter().flatten() {
                let Some(p) = it["path"].as_str() else {
                    continue;
                };
                if it["is_dir"].as_bool().unwrap_or(false) {
                    t.dirs.push(p.to_string());
                    walk(&it["children"], t);
                } else {
                    t.files.push(p.to_string());
                }
            }
        }
        walk(&v["items"], &mut t);
        Ok(t)
    }

    /// Fetch a file's raw bytes, with a conditional request when an etag is known.
    pub async fn fetch(&self, path: &str, etag: Option<&str>) -> Result<Fetch, Err> {
        let _permit = self.sem.acquire().await;
        let mut r = self
            .req(reqwest::Method::GET, "/api/file")
            .query(&[("path", path), ("raw", "1")]);
        if let Some(e) = etag {
            r = r.header("If-None-Match", e);
        }
        let r = r.send().await.map_err(Self::classify)?;
        let s = r.status();
        if s == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(Fetch::NotModified);
        }
        if s == reqwest::StatusCode::NOT_FOUND {
            return Ok(Fetch::Gone);
        }
        if let Some(e) = Self::status_err(s) {
            return Err(e);
        }
        if !s.is_success() {
            return Err(Err::Other(format!("GET {path}: {s}")));
        }
        if r.content_length().map(|n| n > MAX_FILE).unwrap_or(false) {
            return Err(Err::Other(format!("{path} is too large to mirror")));
        }
        let etag = r
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let bytes = r.bytes().await.map_err(Self::classify)?.to_vec();
        // the server answers errors as 200 + a small JSON object
        if bytes == br#"{"error":"File not found"}"# || bytes == br#"{"error":"Invalid path"}"# {
            return Ok(Fetch::Gone);
        }
        Ok(Fetch::Body { bytes, etag })
    }

    pub async fn role(&self) -> Result<Option<String>, Err> {
        let r = self
            .req(reqwest::Method::GET, "/api/app")
            .send()
            .await
            .map_err(Self::classify)?;
        if let Some(e) = Self::status_err(r.status()) {
            return Err(e);
        }
        let v: Value = r.json().await.unwrap_or(Value::Null);
        Ok(v["auth"]["role"].as_str().map(String::from))
    }

    async fn post(&self, path: &str, body: Value) -> Result<(), String> {
        let r = self
            .req(reqwest::Method::POST, path)
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let s = r.status();
        if !s.is_success() {
            return Err(format!("server answered {s}"));
        }
        let v: Value = r.json().await.map_err(|e| e.to_string())?;
        if v["success"].as_bool().unwrap_or(false) {
            Ok(())
        } else {
            Err(v["error"]
                .as_str()
                .unwrap_or("the server refused")
                .to_string())
        }
    }

    pub async fn op(&self, op: &OpKind) -> Result<(), String> {
        match op {
            OpKind::Create { path, text } => {
                self.post("/api/file", json!({"path": path, "content": text}))
                    .await
            }
            OpKind::Rename { from, to } => {
                self.post("/api/rename", json!({"path": from, "newPath": to}))
                    .await
            }
            OpKind::Delete { path } => self.post("/api/delete", json!({"path": path})).await,
        }
    }
}

pub fn spawn_tree(api: &Api, ev: &UnboundedSender<Ev>) {
    let (api, ev) = (api.clone(), ev.clone());
    tokio::spawn(async move {
        let started = Instant::now();
        let r = api.tree().await;
        let _ = ev.send(Ev::Rest(Rest::Tree(started, r)));
    });
}

pub fn spawn_fetch(api: &Api, ev: &UnboundedSender<Ev>, path: String, etag: Option<String>) {
    let (api, ev) = (api.clone(), ev.clone());
    tokio::spawn(async move {
        let r = api.fetch(&path, etag.as_deref()).await;
        let _ = ev.send(Ev::Rest(Rest::Fetched(path, r)));
    });
}

pub fn spawn_op(api: &Api, ev: &UnboundedSender<Ev>, op: OpKind) {
    let (api, ev) = (api.clone(), ev.clone());
    tokio::spawn(async move {
        let r = api.op(&op).await;
        let _ = ev.send(Ev::Rest(Rest::Op(op, r)));
    });
}

pub fn spawn_role(api: &Api, ev: &UnboundedSender<Ev>) {
    let (api, ev) = (api.clone(), ev.clone());
    tokio::spawn(async move {
        let r = api.role().await;
        let _ = ev.send(Ev::Rest(Rest::Role(r)));
    });
}
