//! syncworthy_bridge: an emacs-collab bridge (stdio) that joins a Syncworthy
//! server: Yjs rooms over `/yjs/<path>`, presence and chat over `/ws/doc`, and
//! the file tree over REST.
//!
//! Structure derived from TeXlyre_bridge (GPL-3.0-or-later, same author).
#![allow(clippy::collapsible_if, clippy::type_complexity)]

mod bridge;
mod http;
mod ot;
mod ws;

use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use ws::Ev;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        println!(
            "syncworthy_bridge [--test]\nemacs-collab bridge for Syncworthy. Speaks JSON lines on stdin/stdout.\n\
             open.params: {{server, token?, name?, mirror_dir?, poll_secs?}}"
        );
        return;
    }
    let test = args.iter().any(|a| a == "--test");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Ev>();

    let t2 = tx.clone();
    tokio::spawn(async move {
        let mut lines = BufReader::new(tokio::io::stdin()).lines();
        while let Ok(Some(l)) = lines.next_line().await {
            if l.trim().is_empty() {
                continue;
            }
            match serde_json::from_str(&l) {
                Ok(v) => {
                    if t2.send(Ev::Emacs(v)).is_err() {
                        return;
                    }
                }
                Err(e) => eprintln!("bad JSON from Emacs: {e}"),
            }
        }
        let _ = t2.send(Ev::EmacsEof);
    });
    let t3 = tx.clone();
    tokio::spawn(async move {
        let mut i = tokio::time::interval(Duration::from_millis(200));
        loop {
            i.tick().await;
            if t3.send(Ev::Tick).is_err() {
                return;
            }
        }
    });

    let mut b = bridge::Bridge::new(test, tx);
    while let Some(ev) = rx.recv().await {
        match ev {
            Ev::Emacs(v) => b.on_emacs(v),
            Ev::EmacsEof => b.quit = true,
            Ev::Room(i, n) => b.on_room(i, n),
            Ev::Doc(n) => b.on_doc(n),
            Ev::Rest(r) => b.on_rest(r),
            Ev::Tick => b.tick(),
        }
        if b.quit {
            break;
        }
    }
    b.shutdown();
    tokio::time::sleep(Duration::from_millis(150)).await;
    std::process::exit(0);
}
