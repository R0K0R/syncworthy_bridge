//! The bridge actor: owns the /ws/doc connection, one Yjs room per *attached* file,
//! the file mirror, and speaks emacs-collab (PROTOCOL.md) on stdout.
//!
//! Everything runs on one task, so no locking is needed except for the small
//! queue the yrs text observers fill.
//!
//! The y-sync / Y.Text <-> TextOperation / two-party-sync code is derived from
//! TeXlyre_bridge/src/bridge.rs (GPL-3.0-or-later, same author).

use crate::http::{self, Api, Err, Fetch, OpKind, Rest};
use crate::ot::{self, C, Op};
use crate::ws::{Frame, NetEv};
use serde_json::{Value, json};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::{BuildHasher, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedSender;
use yrs::sync::{Message, SyncMessage};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{
    Any, Doc, GetString, Observable, OffsetKind, Options, Origin, Out, ReadTxn, Text, TextRef,
    Transact, Update,
};

const EMACS: &str = "emacs";
const ACK_QUIET: Duration = Duration::from_millis(300);
const READY_TIMEOUT: Duration = Duration::from_secs(20);
const WELCOME_WAIT: Duration = Duration::from_secs(4);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// A file that changed this recently is re-checked every `poll`, others every 4 x `poll`.
const HOT: Duration = Duration::from_secs(60);
const PALETTE: [&str; 8] = [
    "#FF6B6B", "#4ECDC4", "#FFE66D", "#95E1D3", "#F38181", "#AA96DA", "#FCBAD3", "#A8D8EA",
];

#[derive(Clone, Debug)]
enum D {
    Ret(u32),
    Del(u32),
    Ins(String),
}

/// The server and the other browser peers, collapsed into one yrs doc (`--test`).
struct Fake {
    doc: Doc,
    text: TextRef,
}

impl Fake {
    fn handle(&mut self, frame: &[u8]) -> Vec<Vec<u8>> {
        let mut out = vec![];
        match Message::decode_v1(frame) {
            Ok(Message::Sync(SyncMessage::SyncStep1(sv))) => {
                let u = self.doc.transact().encode_diff_v1(&sv);
                out.push(Message::Sync(SyncMessage::SyncStep2(u)).encode_v1());
            }
            Ok(Message::Sync(SyncMessage::SyncStep2(u)))
            | Ok(Message::Sync(SyncMessage::Update(u))) => {
                if let Ok(u) = Update::decode_v1(&u) {
                    let _ = self.doc.transact_mut_with("relay").apply_update(u);
                }
            }
            _ => {}
        }
        out
    }
    fn hello(&self) -> Vec<u8> {
        Message::Sync(SyncMessage::SyncStep1(self.doc.transact().state_vector())).encode_v1()
    }
    fn string(&self) -> String {
        self.text.get_string(&self.doc.transact())
    }
}

enum Transport {
    Ws(UnboundedSender<Frame>),
    Fake(Rc<RefCell<Fake>>),
}

/// A live Yjs room for an attached file.
struct Room {
    conn: usize,
    path: String,
    doc: Doc,
    text: TextRef,
    tr: Transport,
    q: Arc<Mutex<Vec<Vec<D>>>>,
    /// The bridge's current text, as code points: what Emacs's operations apply to.
    old: Vec<char>,
    up: bool,
    synced: bool,
    attached: bool,
    // PROTOCOL section 4 state
    sent: u64,
    recv_seen: u64,
    acked: u64,
    pending: Vec<(u64, Op)>,
    broken: bool,
    last_activity: Instant,
}

/// A file of the project tree, mirrored on disk.
struct FileInfo {
    etag: Option<String>,
    next_check: Instant,
    hold_until: Option<Instant>,
    last_change: Instant,
    inflight: bool,
    announced: bool,
}

impl FileInfo {
    fn new(announced: bool) -> Self {
        FileInfo {
            etag: None,
            next_check: Instant::now(),
            hold_until: None,
            last_change: Instant::now(),
            inflight: false,
            announced,
        }
    }
}

#[derive(PartialEq, Clone, Debug)]
struct Peer {
    name: String,
    color: String,
    path: Option<String>,
    pos: Option<usize>,
    mark: Option<usize>,
}

#[derive(PartialEq)]
enum Stage {
    Idle,
    Opening,
    Ready,
}

pub struct Bridge {
    test: bool,
    ev_tx: UnboundedSender<crate::ws::Ev>,
    api: Option<Api>,
    ws_base: String,
    token: Option<String>,
    /// Sent as `?token=` on /ws/doc: the hub broadcasts it, so it must not be the secret.
    client_token: String,
    read_only: bool,
    stage: Stage,
    opened_at: Instant,
    root: PathBuf,
    project_name: String,
    // identity
    uid: String,
    uname: String,
    color: String,
    self_id: Option<String>,
    // files
    files: BTreeMap<String, FileInfo>,
    touched: HashMap<String, Instant>,
    init_pending: HashSet<String>,
    tree_seen: Option<Instant>,
    tree_inflight: bool,
    next_tree: Instant,
    tree_fail: u32,
    poll: Duration,
    // rooms
    rooms: HashMap<String, Room>,
    conns: HashMap<usize, String>,
    next_conn: usize,
    pending_attach: Vec<String>,
    // /ws/doc
    docsock: Option<UnboundedSender<Frame>>,
    doc_up: bool,
    welcomed: bool,
    joined: Option<String>,
    peers: HashMap<String, Peer>,
    last_status: String,
    fakes: HashMap<String, Rc<RefCell<Fake>>>,
    pub quit: bool,
}

// ---------------------------------------------------------------- helpers

fn out(v: Value) {
    let mut o = std::io::stdout().lock();
    let _ = writeln!(o, "{v}");
    let _ = o.flush();
}

fn rand64() -> u64 {
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.finish()
}

fn u16len(cs: &[char]) -> u32 {
    cs.iter().map(|c| c.len_utf16() as u32).sum()
}

fn delta_to_op(old: &[char], ds: &[D]) -> Result<Op, String> {
    let mut b = ot::Builder::default();
    let mut i = 0usize;
    for d in ds {
        match d {
            D::Ins(s) => b.insert(s),
            D::Ret(n) | D::Del(n) => {
                let mut acc = 0u32;
                let start = i;
                while acc < *n {
                    let c = old.get(i).ok_or("delta past end of text")?;
                    acc += c.len_utf16() as u32;
                    i += 1;
                }
                if acc != *n {
                    return Err("delta splits a surrogate pair".into());
                }
                if matches!(d, D::Ret(_)) {
                    b.retain(i - start)
                } else {
                    b.delete(i - start)
                }
            }
        }
    }
    b.retain(old.len().saturating_sub(i));
    Ok(b.ops)
}

/// Apply `op` (code points, based on `old`) to a Y.Text with UTF-16 offsets.
fn apply_to_text(doc: &Doc, text: &TextRef, old: &[char], op: &[C], origin: &str) {
    let mut txn = doc.transact_mut_with(origin);
    let (mut pos, mut i) = (0u32, 0usize);
    for c in op {
        match c {
            C::Ret(n) => {
                pos += u16len(&old[i..i + n]);
                i += n;
            }
            C::Ins(s) => {
                text.insert(&mut txn, pos, s);
                pos += s.encode_utf16().count() as u32;
            }
            C::Del(n) => {
                text.remove_range(&mut txn, pos, u16len(&old[i..i + n]));
                i += n;
            }
        }
    }
}

fn new_doc() -> Doc {
    let mut o = Options::with_client_id(yrs::block::ClientID::new(rand64() & 0xffff_ffff | 1));
    o.offset_kind = OffsetKind::Utf16;
    Doc::with_options(o)
}

fn safe_path(p: &str) -> Option<String> {
    let p = p.trim_start_matches('/');
    if p.is_empty() || p.contains('\\') || p.contains('\0') {
        return None;
    }
    if p.split('/').any(|c| c.is_empty() || c == "." || c == "..") {
        return None;
    }
    Some(p.to_string())
}

fn write_atomic(path: &Path, data: &[u8]) -> std::io::Result<()> {
    if let Some(d) = path.parent() {
        std::fs::create_dir_all(d)?;
    }
    let tmp = path.with_file_name(format!(
        ".{}.tmp{}",
        path.file_name().and_then(|s| s.to_str()).unwrap_or("f"),
        std::process::id()
    ));
    std::fs::write(&tmp, data)?;
    std::fs::rename(&tmp, path)
}

fn expand_home(p: &str) -> PathBuf {
    if let Some(r) = p.strip_prefix("~/") {
        if let Ok(h) = std::env::var("HOME") {
            return Path::new(&h).join(r);
        }
    }
    PathBuf::from(p)
}

fn walk(dir: &Path, base: &Path, out: &mut Vec<String>) {
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, base, out);
            } else if let Ok(r) = p.strip_prefix(base) {
                let s: Vec<_> = r
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().to_string())
                    .collect();
                if !s.last().map(|n| n.starts_with('.')).unwrap_or(false) {
                    out.push(s.join("/"));
                }
            }
        }
    }
}

/// Percent-encode one URL component (keeps `/` when `slash` is set).
fn enc(s: &str, slash: bool) -> String {
    let mut o = String::new();
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) || (slash && b == b'/') {
            o.push(b as char);
        } else {
            o.push_str(&format!("%{b:02X}"));
        }
    }
    o
}

/// 0-based code-point offset to a Monaco-style (1-based line, 1-based UTF-16 column).
fn pos_to_linecol(cs: &[char], pos: usize) -> (usize, usize) {
    let pos = pos.min(cs.len());
    let mut line = 1;
    let mut ls = 0;
    for (i, c) in cs[..pos].iter().enumerate() {
        if *c == '\n' {
            line += 1;
            ls = i + 1;
        }
    }
    (line, 1 + u16len(&cs[ls..pos]) as usize)
}

fn linecol_to_pos(cs: &[char], line: i64, col: i64) -> usize {
    let mut ln = 1;
    let mut i = 0;
    while ln < line && i < cs.len() {
        if cs[i] == '\n' {
            ln += 1;
        }
        i += 1;
    }
    let target = (col - 1).max(0) as u32;
    let mut units = 0u32;
    while i < cs.len() && cs[i] != '\n' && units < target {
        units += cs[i].len_utf16() as u32;
        i += 1;
    }
    i
}

// ---------------------------------------------------------------- bridge

impl Bridge {
    pub fn new(test: bool, ev_tx: UnboundedSender<crate::ws::Ev>) -> Self {
        let id = rand64();
        let user = std::env::var("USER").unwrap_or_default();
        Bridge {
            test,
            ev_tx,
            api: None,
            ws_base: String::new(),
            token: None,
            client_token: format!("emacs-tok-{:016x}", rand64()),
            read_only: false,
            stage: Stage::Idle,
            opened_at: Instant::now(),
            root: PathBuf::new(),
            project_name: String::new(),
            uid: format!("emacs-{:08x}", id as u32),
            uname: if user.is_empty() {
                "Emacs".into()
            } else {
                format!("{user} (Emacs)")
            },
            color: PALETTE[(id >> 40) as usize % PALETTE.len()].to_string(),
            self_id: None,
            files: BTreeMap::new(),
            touched: HashMap::new(),
            init_pending: HashSet::new(),
            tree_seen: None,
            tree_inflight: false,
            next_tree: Instant::now(),
            tree_fail: 0,
            poll: Duration::from_secs(2),
            rooms: HashMap::new(),
            conns: HashMap::new(),
            next_conn: 1,
            pending_attach: vec![],
            docsock: None,
            doc_up: false,
            welcomed: false,
            joined: None,
            peers: HashMap::new(),
            last_status: String::new(),
            fakes: HashMap::new(),
            quit: false,
        }
    }

    fn emit(&self, v: Value) {
        out(v);
    }

    fn error(&self, code: &str, msg: &str, path: Option<&str>, fatal: bool) {
        let mut v = json!({"t":"error","code":code,"message":msg});
        if let Some(p) = path {
            v["path"] = json!(p);
        }
        if fatal {
            v["fatal"] = json!(true);
        }
        self.emit(v);
    }

    fn ready(&self) -> bool {
        self.stage == Stage::Ready
    }

    fn mid(&self) -> &str {
        self.self_id.as_deref().unwrap_or(&self.uid)
    }

    // ------------------------------------------------------------ open

    fn open(&mut self, m: &Value) {
        if self.stage != Stage::Idle {
            return;
        }
        let p = &m["params"];
        self.opened_at = Instant::now();
        if let Some(n) = p["name"].as_str().filter(|n| !n.is_empty()) {
            self.uname = n.to_string();
        }
        if self.test && p["dir"].is_string() {
            self.root = PathBuf::from(p["dir"].as_str().unwrap());
            self.project_name = "test".into();
            let mut fs = vec![];
            walk(&self.root, &self.root, &mut fs);
            for f in fs {
                self.files.insert(f, FileInfo::new(true));
            }
            self.stage = Stage::Ready;
            self.send_ready();
            self.last_status.clear();
            self.update_status();
            return;
        }
        let Some(server) = p["server"].as_str().filter(|s| !s.is_empty()) else {
            return self.error(
                "bad_params",
                "open.params.server is required (e.g. http://host:8000)",
                None,
                true,
            );
        };
        let cred = &m["credentials"];
        self.token = ["token", "secret", "password"]
            .iter()
            .find_map(|k| cred[*k].as_str())
            .or_else(|| p["token"].as_str())
            .filter(|t| !t.is_empty())
            .map(String::from);
        let s = server.trim().trim_end_matches('/');
        let http_base = if s.contains("://") {
            s.replace("ws://", "http://").replace("wss://", "https://")
        } else {
            format!("http://{s}")
        };
        self.ws_base = http_base.replacen("http", "ws", 1);
        let hostport: String = http_base
            .split("://")
            .nth(1)
            .unwrap_or("server")
            .split('/')
            .next()
            .unwrap_or("server")
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '-' {
                    c
                } else {
                    '-'
                }
            })
            .collect();
        self.root = match p["mirror_dir"].as_str() {
            Some(d) => expand_home(d),
            None => Path::new(&std::env::var("HOME").unwrap_or_else(|_| ".".into()))
                .join("collab/syncworthy")
                .join(hostport),
        };
        if let Err(e) = std::fs::create_dir_all(&self.root) {
            return self.error(
                "mirror",
                &format!("cannot create {}: {e}", self.root.display()),
                None,
                true,
            );
        }
        if let Some(s) = p["poll_secs"].as_f64().filter(|s| *s >= 0.2) {
            self.poll = Duration::from_secs_f64(s);
        }
        let api = Api::new(http_base, self.token.clone());
        http::spawn_role(&api, &self.ev_tx);
        self.api = Some(api);
        self.stage = Stage::Opening;
        self.emit(json!({"t":"status","state":"connecting"}));
        self.connect_doc();
        self.start_tree();
    }

    fn connect_doc(&mut self) {
        let mut url = format!(
            "{}/ws/doc?name={}&id={}&token={}",
            self.ws_base,
            enc(&self.uname, false),
            enc(&self.uid, false),
            enc(&self.client_token, false)
        );
        if let Some(t) = &self.token {
            url.push_str(&format!("&access_token={}", enc(t, false)));
        }
        self.docsock = Some(crate::ws::spawn_doc(
            url,
            self.token.clone(),
            self.ev_tx.clone(),
        ));
    }

    fn start_tree(&mut self) {
        if let Some(api) = &self.api {
            if !self.tree_inflight {
                self.tree_inflight = true;
                http::spawn_tree(api, &self.ev_tx);
            }
        }
    }

    fn send_ready(&mut self) {
        let mut files: Vec<&String> = self
            .files
            .iter()
            .filter(|(_, f)| f.announced)
            .map(|(p, _)| p)
            .collect();
        files.sort();
        let name = if self.project_name.is_empty() {
            "project".to_string()
        } else {
            self.project_name.clone()
        };
        self.emit(json!({"t":"ready",
            "project":{"name":name,"root":self.root.to_string_lossy(),"files":files},
            "self":{"id":self.mid(),"name":self.uname,"color":self.color}}));
    }

    fn try_ready(&mut self) {
        if self.stage != Stage::Opening {
            return;
        }
        let Some(seen) = self.tree_seen else {
            if self.opened_at.elapsed() > CONNECT_TIMEOUT {
                self.error(
                    "connect",
                    "cannot reach the Syncworthy server (is open.params.server right, and the server up?)",
                    None,
                    true,
                );
                self.quit = true;
            }
            return;
        };
        let files_done = self.init_pending.is_empty() || seen.elapsed() > READY_TIMEOUT;
        let welcome_done = self.welcomed
            || !self.doc_up && seen.elapsed() > WELCOME_WAIT
            || self.opened_at.elapsed() > WELCOME_WAIT;
        if !(files_done && welcome_done) {
            return;
        }
        self.stage = Stage::Ready;
        self.send_ready();
        self.last_status.clear();
        self.update_status();
        // peers learned before ready are announced now
        let peers: Vec<(String, Peer)> = self
            .peers
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, p) in peers {
            self.emit_peer(&k, &p);
        }
    }

    fn update_status(&mut self) {
        if !self.ready() {
            return;
        }
        let s = if self.test {
            "connected"
        } else if self.tree_fail > 0 || !self.doc_up || self.rooms.values().any(|r| !r.up) {
            "offline"
        } else if self.rooms.values().any(|r| !r.synced) {
            "resyncing"
        } else {
            "connected"
        };
        if s != self.last_status {
            self.last_status = s.into();
            self.emit(json!({"t":"status","state":s}));
        }
    }

    /// Credentials missing or rejected: ask Emacs, and wait for a fresh `open`.
    fn auth_failed(&mut self, why: &str) {
        if self.stage == Stage::Idle {
            return;
        }
        self.emit(json!({"t":"need_auth","fields":["token"],"reason":why}));
        self.rooms.clear();
        self.conns.clear();
        self.pending_attach.clear();
        self.files.clear();
        self.peers.clear();
        self.docsock = None;
        self.doc_up = false;
        self.api = None;
        self.read_only = false;
        self.tree_inflight = false;
        self.tree_seen = None;
        self.init_pending.clear();
        self.stage = Stage::Idle;
    }

    // ------------------------------------------------------------ REST results

    pub fn on_rest(&mut self, r: Rest) {
        match r {
            Rest::Tree(started, res) => self.on_tree(started, res),
            Rest::Fetched(path, res) => self.on_fetched(&path, res),
            Rest::Op(op, res) => self.on_op_result(op, res),
            Rest::Role(Ok(role)) => self.read_only = role.as_deref() == Some("view"),
            Rest::Role(Err(Err::Auth(m))) => self.auth_failed(&m),
            Rest::Role(Err(_)) => {}
        }
        self.try_ready();
    }

    fn touched_since(&self, p: &str, started: Instant) -> bool {
        self.touched.get(p).map(|t| *t >= started).unwrap_or(false)
    }

    fn on_tree(&mut self, started: Instant, res: Result<http::Tree, Err>) {
        self.tree_inflight = false;
        self.next_tree = Instant::now()
            + if self.tree_seen.is_some() {
                self.poll
            } else {
                Duration::from_secs(1)
            };
        match res {
            Err(Err::Auth(m)) => self.auth_failed(&m),
            Err(e) => {
                if self.tree_fail == 0 {
                    eprintln!("server unreachable: {e}");
                }
                self.tree_fail += 1;
                self.update_status();
            }
            Ok(t) => {
                if self.tree_fail > 0 {
                    eprintln!("server reachable again");
                }
                self.tree_fail = 0;
                if self.tree_seen.is_none() {
                    self.tree_seen = Some(Instant::now());
                }
                if !t.root_name.is_empty() {
                    self.project_name = t.root_name.clone();
                }
                let now_set: HashSet<String> =
                    t.files.iter().filter_map(|f| safe_path(f)).collect();
                let gone: Vec<String> = self
                    .files
                    .keys()
                    .filter(|k| !now_set.contains(*k) && !self.touched_since(k, started))
                    .cloned()
                    .collect();
                for p in gone {
                    self.server_deleted(&p);
                }
                let mut added = vec![];
                for p in &now_set {
                    if !self.files.contains_key(p) && !self.touched_since(p, started) {
                        self.files.insert(p.clone(), FileInfo::new(false));
                        added.push(p.clone());
                    }
                }
                if self.stage == Stage::Opening {
                    self.init_pending.extend(added);
                }
                for d in t.dirs {
                    if let Some(d) = safe_path(&d) {
                        let _ = std::fs::create_dir_all(self.root.join(d));
                    }
                }
                self.schedule_fetches();
                self.update_status();
            }
        }
    }

    fn server_deleted(&mut self, p: &str) {
        let attached = self.rooms.get(p).map(|r| r.attached).unwrap_or(false);
        if attached {
            self.error(
                "file_deleted",
                "this file was deleted on the server",
                Some(p),
                false,
            );
        }
        self.drop_room(p);
        let announced = self.files.remove(p).map(|f| f.announced).unwrap_or(false);
        let _ = std::fs::remove_file(self.root.join(p));
        self.init_pending.remove(p);
        if announced && self.ready() {
            self.emit(json!({"t":"file_event","op":"delete","path":p}));
        }
    }

    fn schedule_fetches(&mut self) {
        let Some(api) = self.api.clone() else { return };
        let now = Instant::now();
        let mut n = 0;
        for (p, f) in self.files.iter_mut() {
            if n >= 256 {
                break;
            }
            if f.inflight
                || now < f.next_check
                || self.rooms.get(p).map(|r| r.attached).unwrap_or(false)
            {
                continue;
            }
            f.inflight = true;
            n += 1;
            http::spawn_fetch(&api, &self.ev_tx, p.clone(), f.etag.clone());
        }
    }

    fn on_fetched(&mut self, path: &str, res: Result<Fetch, Err>) {
        let now = Instant::now();
        let attached = self.rooms.get(path).map(|r| r.attached).unwrap_or(false);
        let ready = self.ready();
        let Some(f) = self.files.get_mut(path) else {
            return;
        };
        f.inflight = false;
        let interval = |f: &FileInfo| {
            if now.duration_since(f.last_change) < HOT {
                Duration::from_secs(3)
            } else {
                Duration::from_secs(15)
            }
        };
        match res {
            Err(Err::Auth(m)) => self.auth_failed(&m),
            Err(e) => {
                eprintln!("fetch {path}: {e}");
                f.next_check = now + Duration::from_secs(3);
            }
            Ok(Fetch::NotModified) => f.next_check = now + interval(f),
            Ok(Fetch::Gone) => f.next_check = now + Duration::from_secs(5),
            Ok(Fetch::Body { bytes, etag }) => {
                if attached || f.hold_until.map(|h| now < h).unwrap_or(false) {
                    f.next_check = f.hold_until.unwrap_or(now) + Duration::from_millis(300);
                    return;
                }
                let first = !f.announced;
                f.etag = etag;
                f.announced = true;
                let dest = self.root.join(path);
                let same = std::fs::read(&dest).map(|c| c == bytes).unwrap_or(false);
                if !same {
                    if let Err(e) = write_atomic(&dest, &bytes) {
                        eprintln!("cannot write {}: {e}", dest.display());
                    }
                    f.last_change = now;
                }
                f.next_check = now + interval(f);
                self.init_pending.remove(path);
                if ready && (first || !same) {
                    self.emit(json!({"t":"file_event","op": if first {"create"} else {"change"},"path":path}));
                }
            }
        }
    }

    // ------------------------------------------------------------ Emacs file ops

    fn on_file(&mut self, m: &Value) {
        if self.read_only {
            return self.error(
                "read_only",
                "this token is view-only",
                m["path"].as_str(),
                false,
            );
        }
        let (Some(op), Some(path)) = (m["op"].as_str(), m["path"].as_str().and_then(safe_path))
        else {
            return self.error(
                "bad_file_op",
                "file needs op and a safe path",
                m["path"].as_str(),
                false,
            );
        };
        let is_dir = |s: &Self, p: &str| s.files.keys().any(|k| k.starts_with(&format!("{p}/")));
        let kind = match op {
            "create" => {
                if self.files.contains_key(&path) || is_dir(self, &path) {
                    return self.error(
                        "exists",
                        "a file with that name already exists",
                        Some(&path),
                        false,
                    );
                }
                OpKind::Create {
                    path: path.clone(),
                    text: m["text"].as_str().unwrap_or("").to_string(),
                }
            }
            "rename" => {
                let Some(to) = m["to"].as_str().and_then(safe_path) else {
                    return self.error(
                        "bad_file_op",
                        "rename needs a safe `to`",
                        Some(&path),
                        false,
                    );
                };
                if !self.files.contains_key(&path) && !is_dir(self, &path) {
                    return self.error("no_such_file", "no such file", Some(&path), false);
                }
                if self.files.contains_key(&to) || is_dir(self, &to) {
                    return self.error(
                        "exists",
                        "a file with that name already exists",
                        Some(&to),
                        false,
                    );
                }
                OpKind::Rename {
                    from: path.clone(),
                    to,
                }
            }
            "delete" => {
                if !self.files.contains_key(&path) && !is_dir(self, &path) {
                    return self.error("no_such_file", "no such file", Some(&path), false);
                }
                OpKind::Delete { path: path.clone() }
            }
            _ => return,
        };
        match &self.api {
            Some(api) if !self.test => http::spawn_op(api, &self.ev_tx, kind),
            _ => {
                let _ = self.ev_tx.send(crate::ws::Ev::Rest(Rest::Op(kind, Ok(()))));
            }
        }
    }

    fn on_op_result(&mut self, op: OpKind, res: Result<(), String>) {
        let now = Instant::now();
        let (code, path) = match &op {
            OpKind::Create { path, .. } => ("create", path.clone()),
            OpKind::Rename { from, .. } => ("rename", from.clone()),
            OpKind::Delete { path } => ("delete", path.clone()),
        };
        if let Err(e) = res {
            return self.error(
                "file_op_failed",
                &format!("{code}: {e}"),
                Some(&path),
                false,
            );
        }
        match op {
            OpKind::Create { path, text } => {
                let _ = write_atomic(&self.root.join(&path), text.as_bytes());
                let mut f = FileInfo::new(true);
                f.next_check = now + Duration::from_millis(1500);
                self.files.insert(path.clone(), f);
                self.touched.insert(path.clone(), now);
                self.emit(json!({"t":"file_event","op":"create","path":path}));
            }
            OpKind::Rename { from, to } => {
                let prefix = format!("{from}/");
                let moves: Vec<(String, String)> = if self.files.contains_key(&from) {
                    vec![(from.clone(), to.clone())]
                } else {
                    self.files
                        .keys()
                        .filter(|k| k.starts_with(&prefix))
                        .map(|k| (k.clone(), format!("{to}/{}", &k[prefix.len()..])))
                        .collect()
                };
                let dest = self.root.join(&to);
                if let Some(d) = dest.parent() {
                    let _ = std::fs::create_dir_all(d);
                }
                let _ = std::fs::rename(self.root.join(&from), &dest);
                for (a, b) in moves {
                    let was = self.rooms.get(&a).map(|r| r.attached).unwrap_or(false);
                    self.drop_room(&a);
                    if let Some(mut f) = self.files.remove(&a) {
                        f.etag = None;
                        f.inflight = false;
                        f.next_check = now + Duration::from_millis(1500);
                        self.files.insert(b.clone(), f);
                    }
                    self.touched.insert(a.clone(), now);
                    self.touched.insert(b.clone(), now);
                    self.emit(json!({"t":"file_event","op":"rename","path":a,"to":b}));
                    if was {
                        // the room is named by the path: rejoin under the new name
                        self.error("resync", "the attached file was renamed", Some(&b), false);
                        self.start_room(&b);
                        if !self.pending_attach.contains(&b) {
                            self.pending_attach.push(b);
                        }
                    }
                }
            }
            OpKind::Delete { path } => {
                let prefix = format!("{path}/");
                let gone: Vec<String> = self
                    .files
                    .keys()
                    .filter(|k| **k == path || k.starts_with(&prefix))
                    .cloned()
                    .collect();
                for p in gone {
                    self.drop_room(&p);
                    self.files.remove(&p);
                    self.touched.insert(p.clone(), now);
                    self.emit(json!({"t":"file_event","op":"delete","path":p}));
                }
                let t = self.root.join(&path);
                let _ = if t.is_dir() {
                    std::fs::remove_dir_all(t)
                } else {
                    std::fs::remove_file(t)
                };
            }
        }
    }

    // ------------------------------------------------------------ rooms

    fn fake_for(&mut self, path: &str) -> Rc<RefCell<Fake>> {
        if let Some(f) = self.fakes.get(path) {
            return f.clone();
        }
        let s = std::fs::read_to_string(self.root.join(path)).unwrap_or_default();
        let doc = new_doc();
        let text = doc.get_or_insert_text("content");
        text.insert(&mut doc.transact_mut(), 0, &s.replace("\r\n", "\n"));
        let f = Rc::new(RefCell::new(Fake { doc, text }));
        self.fakes.insert(path.to_string(), f.clone());
        f
    }

    fn start_room(&mut self, path: &str) {
        if self.rooms.contains_key(path) {
            return;
        }
        let doc = new_doc();
        let text = doc.get_or_insert_text("content");
        let q: Arc<Mutex<Vec<Vec<D>>>> = Arc::default();
        let q2 = q.clone();
        text.observe("bridge", move |txn, ev| {
            if txn.origin() == Some(&Origin::from(EMACS)) {
                return;
            }
            let ds = ev
                .delta(txn)
                .iter()
                .map(|d| match d {
                    yrs::types::Delta::Inserted(Out::Any(Any::String(s)), _) => {
                        D::Ins(s.to_string())
                    }
                    yrs::types::Delta::Inserted(_, _) => D::Ins("\u{fffc}".into()),
                    yrs::types::Delta::Deleted(n) => D::Del(*n),
                    yrs::types::Delta::Retain(n, _) => D::Ret(*n),
                })
                .collect();
            q2.lock().unwrap().push(ds);
        });
        let conn = self.next_conn;
        self.next_conn += 1;
        let tr = if self.test {
            Transport::Fake(self.fake_for(path))
        } else {
            let mut url = format!("{}/yjs/{}", self.ws_base, enc(path, true));
            if let Some(t) = &self.token {
                url.push_str(&format!("?access_token={}", enc(t, false)));
            }
            Transport::Ws(crate::ws::spawn_room(
                conn,
                url,
                self.token.clone(),
                self.ev_tx.clone(),
            ))
        };
        let is_fake = matches!(tr, Transport::Fake(_));
        self.rooms.insert(
            path.to_string(),
            Room {
                conn,
                path: path.to_string(),
                doc,
                text,
                tr,
                q,
                old: vec![],
                up: false,
                synced: false,
                attached: false,
                sent: 0,
                recv_seen: 0,
                acked: 0,
                pending: vec![],
                broken: false,
                last_activity: Instant::now(),
            },
        );
        self.conns.insert(conn, path.to_string());
        if is_fake {
            self.on_up(path);
        }
    }

    /// Forget a room (its connection task ends when the sender is dropped).
    fn drop_room(&mut self, path: &str) {
        if let Some(r) = self.rooms.remove(path) {
            self.conns.remove(&r.conn);
        }
        self.pending_attach.retain(|p| p != path);
    }

    fn send(&mut self, path: &str, m: Message) {
        let bytes = m.encode_v1();
        let replies = match self.rooms.get(path).map(|r| &r.tr) {
            Some(Transport::Ws(tx)) => {
                let _ = tx.send(Frame::Bin(bytes));
                return;
            }
            Some(Transport::Fake(f)) => f.clone().borrow_mut().handle(&bytes),
            None => return,
        };
        for r in replies {
            self.on_frame(path, &r);
        }
    }

    fn on_up(&mut self, path: &str) {
        let Some(r) = self.rooms.get_mut(path) else {
            return;
        };
        r.up = true;
        r.synced = false;
        let sv = r.doc.transact().state_vector();
        let fake = match &r.tr {
            Transport::Fake(f) => Some(f.borrow().hello()),
            _ => None,
        };
        self.send(path, Message::Sync(SyncMessage::SyncStep1(sv)));
        if let Some(h) = fake {
            self.on_frame(path, &h);
        }
        self.update_status();
    }

    pub fn on_room(&mut self, conn: usize, ev: NetEv) {
        let Some(path) = self.conns.get(&conn).cloned() else {
            return;
        };
        match ev {
            NetEv::Up => self.on_up(&path),
            NetEv::Down => {
                if let Some(r) = self.rooms.get_mut(&path) {
                    r.up = false;
                    r.synced = false;
                }
                self.update_status();
            }
            NetEv::Frame(Frame::Bin(b)) => self.on_frame(&path, &b),
            NetEv::Frame(_) => {}
        }
        self.try_ready();
    }

    fn on_frame(&mut self, path: &str, bytes: &[u8]) {
        let msg = match Message::decode_v1(bytes) {
            Ok(m) => m,
            Err(e) => return eprintln!("[{path}] undecodable frame: {e}"),
        };
        match msg {
            Message::Sync(SyncMessage::SyncStep1(sv)) => {
                let Some(r) = self.rooms.get(path) else {
                    return;
                };
                let u = r.doc.transact().encode_diff_v1(&sv);
                self.send(path, Message::Sync(SyncMessage::SyncStep2(u)));
            }
            Message::Sync(SyncMessage::SyncStep2(u)) => {
                self.apply_update(path, &u);
                if let Some(r) = self.rooms.get_mut(path) {
                    r.synced = true;
                }
                self.process_text(path);
                self.update_status();
                self.flush_attaches();
            }
            Message::Sync(SyncMessage::Update(u)) => {
                self.apply_update(path, &u);
                self.process_text(path);
            }
            // cursors ride /ws/doc on Syncworthy; Yjs awareness is not used
            _ => {}
        }
    }

    fn apply_update(&mut self, path: &str, u: &[u8]) {
        let Some(r) = self.rooms.get(path) else {
            return;
        };
        match Update::decode_v1(u) {
            Ok(u) => {
                if let Err(e) = r.doc.transact_mut_with("net").apply_update(u) {
                    eprintln!("[{path}] bad update: {e}");
                }
            }
            Err(e) => eprintln!("[{path}] undecodable update: {e}"),
        }
    }

    fn process_text(&mut self, path: &str) {
        let Some(r) = self.rooms.get_mut(path) else {
            return;
        };
        let batches = std::mem::take(&mut *r.q.lock().unwrap());
        if batches.is_empty() {
            return;
        }
        let mut cur = r.old.clone();
        let mut ops = vec![];
        let mut ok = true;
        for b in &batches {
            match delta_to_op(&cur, b).and_then(|op| ot::apply(&cur, &op).map(|n| (op, n))) {
                Ok((op, n)) => {
                    ops.push(op);
                    cur = n;
                }
                Err(e) => {
                    eprintln!("delta mismatch ({e}); falling back to a diff");
                    ok = false;
                    break;
                }
            }
        }
        let actual: Vec<char> = r.text.get_string(&r.doc.transact()).chars().collect();
        if !ok || cur != actual {
            ops = vec![ot::diff(&r.old, &actual)];
        }
        r.old = actual;
        ops.retain(|o| !ot::is_noop(o));
        if ops.is_empty() || !r.attached || r.broken {
            return;
        }
        for op in ops {
            r.sent += 1;
            r.acked = r.recv_seen;
            r.last_activity = Instant::now();
            r.pending.push((r.sent, op.clone()));
            out(
                json!({"t":"remote","path":r.path,"seq":r.sent,"seen":r.recv_seen,"op":ot::to_json(&op)}),
            );
        }
    }

    // ------------------------------------------------------------ attach / edit

    fn flush_attaches(&mut self) {
        let list = std::mem::take(&mut self.pending_attach);
        for path in list {
            match self.rooms.get(&path) {
                Some(r) if r.synced => self.do_attach(&path),
                Some(_) => self.pending_attach.push(path),
                None => {}
            }
        }
    }

    fn on_attach(&mut self, path: &str) {
        if !self.ready() {
            return self.error("not_ready", "attach before ready", Some(path), false);
        }
        if !self.files.contains_key(path) {
            return self.error(
                "no_such_file",
                "no such file in this project",
                Some(path),
                false,
            );
        }
        if self.read_only {
            return self.error(
                "read_only",
                "this token is view-only: the server drops live edits (the mirror stays current); open with an edit token to edit",
                Some(path),
                false,
            );
        }
        self.start_room(path);
        self.pending_attach.retain(|p| p != path);
        self.pending_attach.push(path.to_string());
        self.flush_attaches();
    }

    fn do_attach(&mut self, path: &str) {
        // CRLF is normalised in the shared text, so wire positions match the CRDT
        let cr: Vec<usize> = match self.rooms.get(path) {
            Some(r) => (0..r.old.len().saturating_sub(1))
                .filter(|&i| r.old[i] == '\r' && r.old[i + 1] == '\n')
                .collect(),
            None => return,
        };
        if !cr.is_empty() {
            self.local_edit_ops(path, |old| {
                let mut b = ot::Builder::default();
                let mut at = 0;
                for &i in &cr {
                    b.retain(i - at);
                    b.delete(1);
                    at = i + 1;
                }
                b.retain(old.len() - at);
                b.ops
            });
        }
        let Some(r) = self.rooms.get_mut(path) else {
            return;
        };
        r.attached = true;
        r.broken = false;
        r.sent = 0;
        r.recv_seen = 0;
        r.acked = 0;
        r.pending.clear();
        let text: String = r.old.iter().collect();
        self.emit(json!({"t":"doc","path":path,"text":text,"seen":0}));
        self.join_doc(path);
    }

    /// Change the shared text with an op computed from its current content, and send it on.
    fn local_edit_ops(&mut self, path: &str, f: impl FnOnce(&[char]) -> Op) {
        let Some(r) = self.rooms.get_mut(path) else {
            return;
        };
        let op = f(&r.old);
        let Ok(new) = ot::apply(&r.old, &op) else {
            return;
        };
        let before = r.doc.transact().state_vector();
        apply_to_text(&r.doc, &r.text, &r.old, &op, EMACS);
        r.old = new;
        let u = r.doc.transact().encode_diff_v1(&before);
        self.send(path, Message::Sync(SyncMessage::Update(u)));
    }

    fn on_detach(&mut self, path: &str) {
        self.pending_attach.retain(|p| p != path);
        let Some(r) = self.rooms.get(path) else {
            return;
        };
        if r.attached {
            let text: String = r.old.iter().collect();
            let _ = write_atomic(&self.root.join(path), text.as_bytes());
            if let Some(f) = self.files.get_mut(path) {
                // the server saves a moment after the last edit: do not mirror its older copy over ours
                f.hold_until = Some(Instant::now() + Duration::from_millis(1500));
                f.next_check = Instant::now() + Duration::from_millis(1800);
                f.etag = None;
            }
        }
        self.drop_room(path);
    }

    fn desync(&mut self, path: &str, why: &str) {
        if let Some(r) = self.rooms.get_mut(path) {
            r.broken = true;
        }
        self.error("desync", why, Some(path), false);
    }

    fn on_edit(&mut self, m: &Value) {
        let (Some(path), Some(seq), Some(seen)) =
            (m["path"].as_str(), m["seq"].as_u64(), m["seen"].as_u64())
        else {
            return;
        };
        let Some(r) = self.rooms.get_mut(path) else {
            return;
        };
        if !r.attached || r.broken {
            return;
        }
        let want = r.recv_seen + 1;
        if seq != want {
            return self.desync(path, &format!("expected edit seq {want}, got {seq}"));
        }
        if seen > r.sent {
            return self.desync(path, "edit.seen is ahead of what was sent");
        }
        let mut op = match ot::from_json(&m["op"]) {
            Ok(o) => o,
            Err(e) => return self.desync(path, &e),
        };
        r.pending.retain(|(q, _)| *q > seen);
        let mut err = None;
        for (_, p) in r.pending.iter_mut() {
            match ot::transform(p, &op) {
                Ok((p2, op2)) => {
                    *p = p2;
                    op = op2;
                }
                Err(e) => {
                    err = Some(e);
                    break;
                }
            }
        }
        if err.is_none() && ot::base_len(&op) != r.old.len() {
            err = Some(format!(
                "op base length {} != text length {}",
                ot::base_len(&op),
                r.old.len()
            ));
        }
        if let Some(e) = err {
            return self.desync(path, &e);
        }
        r.recv_seen = seq;
        r.last_activity = Instant::now();
        self.local_edit_ops(path, |_| op);
    }

    // ------------------------------------------------------------ /ws/doc

    fn send_doc(&self, v: Value) -> bool {
        match (&self.docsock, self.doc_up) {
            (Some(tx), true) => tx.send(Frame::Text(v.to_string())).is_ok(),
            _ => false,
        }
    }

    fn join_doc(&mut self, path: &str) {
        if self.joined.as_deref() != Some(path) {
            self.joined = Some(path.to_string());
            self.send_doc(json!({"type":"join","path":path}));
        }
    }

    pub fn on_doc(&mut self, ev: NetEv) {
        match ev {
            NetEv::Up => {
                self.doc_up = true;
                if let Some(p) = self.joined.clone() {
                    self.send_doc(json!({"type":"join","path":p}));
                }
                self.update_status();
            }
            NetEv::Down => {
                self.doc_up = false;
                self.welcomed = false;
                let ids: Vec<String> = self.peers.keys().cloned().collect();
                for id in ids {
                    self.remove_peer(&id);
                }
                self.update_status();
            }
            NetEv::Frame(Frame::Text(t)) => {
                if let Ok(v) = serde_json::from_str::<Value>(&t) {
                    self.on_doc_msg(&v);
                }
            }
            NetEv::Frame(_) => {}
        }
        self.try_ready();
    }

    fn on_doc_msg(&mut self, m: &Value) {
        match m["type"].as_str().unwrap_or("") {
            "welcome" => {
                if let Some(id) = m["userId"].as_str() {
                    self.self_id = Some(id.to_string());
                }
                if let Some(c) = m["color"].as_str() {
                    self.color = c.to_string();
                }
                self.welcomed = true;
                for u in m["users"].as_array().into_iter().flatten() {
                    self.user_info(u);
                }
            }
            "user_joined" | "user_updated" => self.user_info(&m["user"]),
            "user_left" => {
                if let Some(id) = m["userId"].as_str() {
                    self.remove_peer(id);
                }
            }
            "chat" => {
                let ts = m["timestamp"].as_f64().unwrap_or(0.0);
                let ts = if ts > 1e11 { ts / 1000.0 } else { ts };
                let ts = if ts <= 0.0 {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or(0.0)
                } else {
                    ts
                };
                self.emit(json!({"t":"chat","from":m["name"].as_str().unwrap_or("?"),
                    "from_id":m["userId"],"text":m["text"].as_str().unwrap_or(""),"ts":ts as u64}));
            }
            "cursor" => self.remote_cursor(m),
            // rendered preview pages (SVG) are for the web editor; compile output is a log line
            "preview_log" => eprintln!("[preview] {}", m["message"].as_str().unwrap_or("")),
            _ => {}
        }
    }

    fn is_me(&self, id: &str) -> bool {
        id == self.uid || self.self_id.as_deref() == Some(id)
    }

    fn emit_peer(&self, id: &str, p: &Peer) {
        let mut v = json!({"t":"peer","id":id,"name":p.name,"color":p.color});
        if let (Some(path), Some(pos)) = (&p.path, p.pos) {
            v["path"] = json!(path);
            v["pos"] = json!(pos);
            if let Some(m) = p.mark {
                v["mark"] = json!(m);
            }
        }
        self.emit(v);
    }

    fn upsert_peer(&mut self, id: &str, f: impl FnOnce(&mut Peer)) {
        let old = self.peers.get(id).cloned();
        let mut p = old.clone().unwrap_or(Peer {
            name: "?".into(),
            color: "#888888".into(),
            path: None,
            pos: None,
            mark: None,
        });
        f(&mut p);
        if old.as_ref() != Some(&p) {
            if self.ready() {
                self.emit_peer(id, &p);
            }
            self.peers.insert(id.to_string(), p);
        }
    }

    fn remove_peer(&mut self, id: &str) {
        if self.peers.remove(id).is_some() && self.ready() {
            self.emit(json!({"t":"peer_left","id":id}));
        }
    }

    fn user_info(&mut self, u: &Value) {
        let Some(id) = u["id"].as_str() else { return };
        if self.is_me(id) {
            return;
        }
        let file = u["file"]
            .as_str()
            .filter(|f| !f.is_empty())
            .map(String::from);
        let (name, color) = (
            u["name"].as_str().map(String::from),
            u["color"].as_str().map(String::from),
        );
        self.upsert_peer(id, |p| {
            if let Some(n) = name {
                p.name = n;
            }
            if let Some(c) = color {
                p.color = c;
            }
            if p.path != file {
                p.path = None;
                p.pos = None;
                p.mark = None;
            }
        });
    }

    /// Somebody is working in this file: look at it again soon.
    fn mark_hot(&mut self, path: &str) {
        let soon = Instant::now() + Duration::from_millis(400);
        if let Some(f) = self.files.get_mut(path) {
            f.last_change = Instant::now();
            if f.next_check > soon {
                f.next_check = soon;
            }
        }
    }

    fn text_of(&self, path: &str) -> Vec<char> {
        if let Some(r) = self.rooms.get(path).filter(|r| r.attached) {
            return r.old.clone();
        }
        std::fs::read_to_string(self.root.join(path))
            .map(|s| s.replace("\r\n", "\n").chars().collect())
            .unwrap_or_default()
    }

    fn remote_cursor(&mut self, m: &Value) {
        let (Some(id), Some(file)) = (m["userId"].as_str(), m["file"].as_str().and_then(safe_path))
        else {
            return;
        };
        if self.is_me(id) {
            return;
        }
        self.mark_hot(&file);
        let cs = self.text_of(&file);
        let g = |k: &str, d: i64| {
            m[k].as_i64()
                .or_else(|| m[k].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(d)
        };
        let (line, col) = (g("line", 1), g("col", 1));
        let pos = linecol_to_pos(&cs, line, col);
        let a = linecol_to_pos(&cs, g("selStartLine", line), g("selStartCol", col));
        let b = linecol_to_pos(&cs, g("selEndLine", line), g("selEndCol", col));
        let other = if pos == b { a } else { b };
        let mark = if other != pos { Some(other) } else { None };
        let (name, color) = (
            m["name"].as_str().map(String::from),
            m["color"].as_str().map(String::from),
        );
        self.upsert_peer(id, |p| {
            if let Some(n) = name {
                p.name = n;
            }
            if let Some(c) = color {
                p.color = c;
            }
            p.path = Some(file);
            p.pos = Some(pos);
            p.mark = mark;
        });
    }

    fn on_cursor(&mut self, m: &Value) {
        let Some(path) = m["path"].as_str() else {
            return;
        };
        let Some(r) = self.rooms.get(path).filter(|r| r.attached) else {
            return;
        };
        let Some(pos) = m["pos"].as_u64() else { return };
        let mark = m["mark"]
            .as_u64()
            .map(|x| x as usize)
            .unwrap_or(pos as usize);
        let (line, col) = pos_to_linecol(&r.old, pos as usize);
        let (ml, mc) = pos_to_linecol(&r.old, mark);
        let ((sl, sc), (el, ec)) = if (ml, mc) <= (line, col) {
            ((ml, mc), (line, col))
        } else {
            ((line, col), (ml, mc))
        };
        let path = path.to_string();
        self.join_doc(&path);
        self.send_doc(json!({"type":"cursor","file":path,"line":line,"col":col,
            "selStartLine":sl,"selStartCol":sc,"selEndLine":el,"selEndCol":ec,"color":self.color}));
    }

    fn on_chat(&mut self, text: &str) {
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if self.test {
            return self.emit(
                json!({"t":"chat","from":self.uname,"from_id":self.uid,"text":text,"ts":ms / 1000}),
            );
        }
        if !self.send_doc(json!({"type":"chat","text":text,"timestamp":ms})) {
            self.error(
                "chat_offline",
                "chat not delivered: the doc socket is down",
                None,
                false,
            );
        }
    }

    // ------------------------------------------------------------ ticking

    pub fn tick(&mut self) {
        self.try_ready();
        self.flush_attaches();
        if self.api.is_some() && self.stage != Stage::Idle {
            if !self.tree_inflight && Instant::now() >= self.next_tree {
                self.start_tree();
            }
            self.schedule_fetches();
        }
        let cutoff = Instant::now() - Duration::from_secs(30);
        self.touched.retain(|_, t| *t >= cutoff);
        let mut acks = vec![];
        for r in self.rooms.values_mut() {
            if r.attached
                && !r.broken
                && r.recv_seen > r.acked
                && r.last_activity.elapsed() >= ACK_QUIET
            {
                r.acked = r.recv_seen;
                acks.push(json!({"t":"ack","path":r.path,"seen":r.recv_seen}));
            }
        }
        for a in acks {
            self.emit(a);
        }
    }

    pub fn shutdown(&mut self) {
        self.docsock = None;
        self.rooms.clear();
    }

    // ------------------------------------------------------------ emacs messages

    pub fn on_emacs(&mut self, m: Value) {
        let t = m["t"].as_str().unwrap_or("");
        match t {
            "hello" => self.emit(json!({"t":"hello","bridge":"syncworthy_bridge","version":0,
                "capabilities":["cursors","chat","files","binary"]})),
            "open" => self.open(&m),
            "attach" => {
                if let Some(p) = m["path"].as_str() {
                    self.on_attach(p)
                }
            }
            "detach" => {
                if let Some(p) = m["path"].as_str() {
                    self.on_detach(p)
                }
            }
            "edit" => self.on_edit(&m),
            "ack" => {
                if let (Some(p), Some(s)) = (m["path"].as_str(), m["seen"].as_u64()) {
                    if let Some(r) = self.rooms.get_mut(p) {
                        if r.attached && s <= r.sent {
                            r.pending.retain(|(q, _)| *q > s);
                        }
                    }
                }
            }
            "cursor" => self.on_cursor(&m),
            "file" => self.on_file(&m),
            "chat" => {
                if let Some(x) = m["text"].as_str() {
                    self.on_chat(x)
                }
            }
            "close" => self.quit = true,
            _ if self.test && t.starts_with("x-test-") => self.on_test(t, &m),
            _ => {}
        }
        self.try_ready();
    }

    // ------------------------------------------------------------ --test

    fn on_test(&mut self, t: &str, m: &Value) {
        let path = m["path"].as_str().unwrap_or("");
        match t {
            "x-test-text" => {
                let text: String = match self.rooms.get(path) {
                    Some(r) => r.old.iter().collect(),
                    None => match self.fakes.get(path) {
                        Some(f) => f.borrow().string(),
                        None => std::fs::read_to_string(self.root.join(path)).unwrap_or_default(),
                    },
                };
                self.emit(json!({"t":"x-test-text","path":path,"text":text}));
            }
            "x-test-pending" => {
                let n = self
                    .rooms
                    .get(path)
                    .map(|r| if r.attached { r.pending.len() } else { 0 })
                    .unwrap_or(0);
                self.emit(json!({"t":"x-test-pending","path":path,"count":n}));
            }
            "x-test-peer" => {
                let mut v = json!({"t":"peer","id":m["id"],"name":m["name"],"color":m["color"]});
                for k in ["path", "pos"] {
                    if !m[k].is_null() {
                        v[k] = m[k].clone();
                    }
                }
                self.emit(v);
            }
            "x-test-remote" => {
                if !self.files.contains_key(path) {
                    return;
                }
                let f = self.fake_for(path);
                let attached = self.rooms.get(path).map(|r| r.attached).unwrap_or(false);
                let old: Vec<char> = match self.rooms.get(path).filter(|_| attached) {
                    Some(r) => r.old.clone(),
                    None => f.borrow().string().chars().collect(),
                };
                let op = match ot::from_json(&m["op"]) {
                    Ok(o) if ot::base_len(&o) == old.len() => o,
                    _ => {
                        return self.error(
                            "bad_op",
                            "x-test-remote op does not fit the text",
                            Some(path),
                            false,
                        );
                    }
                };
                let u = {
                    let fk = f.borrow();
                    let before = fk.doc.transact().state_vector();
                    apply_to_text(&fk.doc, &fk.text, &old, &op, "fake");
                    fk.doc.transact().encode_diff_v1(&before)
                };
                if attached {
                    let frame = Message::Sync(SyncMessage::Update(u)).encode_v1();
                    self.on_frame(path, &frame);
                } else {
                    let text = f.borrow().string();
                    let _ = write_atomic(&self.root.join(path), text.as_bytes());
                    self.emit(json!({"t":"file_event","op":"change","path":path}));
                }
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cs(s: &str) -> Vec<char> {
        s.chars().collect()
    }

    #[test]
    fn line_col_are_utf16_columns() {
        let t = cs("ab\n😀x\n");
        // before `x`: after an astral char, two UTF-16 units
        assert_eq!(pos_to_linecol(&t, 4), (2, 3));
        assert_eq!(linecol_to_pos(&t, 2, 3), 4);
        assert_eq!(pos_to_linecol(&t, 0), (1, 1));
        assert_eq!(linecol_to_pos(&t, 3, 1), 6);
        // out-of-range column clamps to the end of the line
        assert_eq!(linecol_to_pos(&t, 1, 99), 2);
        assert_eq!(linecol_to_pos(&t, 99, 1), 6);
    }

    #[test]
    fn delta_units_are_utf16() {
        let old = cs("a😀b");
        let op = delta_to_op(&old, &[D::Ret(3), D::Ins("X".into()), D::Del(1)]).unwrap();
        assert_eq!(
            ot::apply(&old, &op).unwrap().iter().collect::<String>(),
            "a😀X"
        );
    }

    #[test]
    fn enc_paths() {
        assert_eq!(enc("a b/é.txt", true), "a%20b/%C3%A9.txt");
        assert_eq!(enc("a/b", false), "a%2Fb");
    }
}
