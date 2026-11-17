//! One reconnecting websocket per connection (a Yjs room, or the /ws/doc socket).
//! The task only moves frames; all protocol logic lives in the bridge actor.
//!
//! Structure derived from TeXlyre_bridge/src/ws.rs (GPL-3.0-or-later, same author).

use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};

pub enum Frame {
    Bin(Vec<u8>),
    Text(String),
}

pub enum NetEv {
    Up,
    Down,
    Frame(Frame),
}

/// What the bridge actor's event loop receives.
pub enum Ev {
    Emacs(serde_json::Value),
    EmacsEof,
    /// A Yjs room connection (by connection id).
    Room(usize, NetEv),
    /// The /ws/doc connection.
    Doc(NetEv),
    Rest(crate::http::Rest),
    Tick,
}

pub async fn run(
    label: String,
    url: String,
    token: Option<String>,
    mut out: UnboundedReceiver<Frame>,
    emit: Box<dyn Fn(NetEv) -> bool + Send>,
) {
    let mut delay = Duration::from_millis(500);
    loop {
        let req = url.as_str().into_client_request().map(|mut r| {
            if let Some(t) = &token {
                if let Ok(v) = format!("Bearer {t}").parse() {
                    r.headers_mut().insert("Authorization", v);
                }
            }
            r
        });
        let conn = match req {
            Ok(r) => tokio_tungstenite::connect_async(r)
                .await
                .map_err(|e| e.to_string()),
            Err(e) => Err(e.to_string()),
        };
        match conn {
            Ok((ws, _)) => {
                delay = Duration::from_millis(500);
                // frames queued while offline are stale: the sync handshake re-sends state
                while out.try_recv().is_ok() {}
                if !emit(NetEv::Up) {
                    return;
                }
                let (mut w, mut r) = ws.split();
                loop {
                    tokio::select! {
                        m = r.next() => match m {
                            Some(Ok(Message::Binary(b))) => {
                                if !emit(NetEv::Frame(Frame::Bin(b.to_vec()))) { return; }
                            }
                            Some(Ok(Message::Text(t))) => {
                                if !emit(NetEv::Frame(Frame::Text(t.to_string()))) { return; }
                            }
                            Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                            Some(Ok(_)) => {}
                        },
                        o = out.recv() => match o {
                            Some(Frame::Bin(b)) => if w.send(Message::Binary(b.into())).await.is_err() { break },
                            Some(Frame::Text(t)) => if w.send(Message::Text(t.into())).await.is_err() { break },
                            None => { let _ = w.close().await; return; }
                        },
                    }
                }
                if !emit(NetEv::Down) {
                    return;
                }
            }
            Err(e) => {
                // never log the query string: it can carry the access token
                let shown = url.split('?').next().unwrap_or("");
                eprintln!("[ws {label}] connect {shown}: {e}")
            }
        }
        // wait out the backoff, discarding frames (they are re-derived on resync)
        let sleep = tokio::time::sleep(delay);
        tokio::pin!(sleep);
        loop {
            tokio::select! {
                _ = &mut sleep => break,
                o = out.recv() => if o.is_none() { return; },
            }
        }
        delay = (delay * 2).min(Duration::from_secs(15));
    }
}

pub fn spawn_room(
    id: usize,
    url: String,
    token: Option<String>,
    ev: UnboundedSender<Ev>,
) -> UnboundedSender<Frame> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run(
        format!("room{id}"),
        url,
        token,
        rx,
        Box::new(move |n| ev.send(Ev::Room(id, n)).is_ok()),
    ));
    tx
}

pub fn spawn_doc(
    url: String,
    token: Option<String>,
    ev: UnboundedSender<Ev>,
) -> UnboundedSender<Frame> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(run(
        "doc".into(),
        url,
        token,
        rx,
        Box::new(move |n| ev.send(Ev::Doc(n)).is_ok()),
    ));
    tx
}
