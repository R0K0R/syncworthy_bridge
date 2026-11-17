# syncworthy_bridge

An emacs-collab bridge (protocol v0, JSON lines on stdio) for a
[Syncworthy](../syncworthy) server. It replaces the old Python Emacs bridge of Noteworthy.
Rust, `yrs`, `tokio`, `reqwest`. GPL-3.0-or-later.

Parts of the y-sync / OT code come from the author's own `TeXlyre_bridge` (also GPL-3.0-or-later).

## Use

```
syncworthy_bridge          # Emacs starts it and speaks emacs-collab on stdin/stdout
syncworthy_bridge --test   # PROTOCOL.md section 8 x-test-* against an in-process fake peer
```

`open.params`:

| key | meaning |
|---|---|
| `server` | `http://host:port` (`https://`, or `ws(s)://`, also accepted; a bare `host:port` means http) |
| `token` | optional access token. `credentials.token` wins when both are given |
| `name` | optional display name (default `$USER (Emacs)`) |
| `mirror_dir` | optional (default `~/collab/syncworthy/<host>-<port>/`) |
| `poll_secs` | optional tree poll interval, default 2 (files that changed recently are re-checked at the same rate, others at 4x) |

## How it maps onto Syncworthy

- **Files** (REST): `/api/tree` is polled; each file is fetched with `GET /api/file?raw=1` and an
  `If-None-Match` etag, so an unchanged file costs a 304. Text and binaries alike are written to the
  mirror byte for byte (capability `binary`). Changes become `file_event`. Rooms are joined only for
  attached files. A file somebody has a cursor in is re-checked within about half a second.
- **Attached files**: websocket `/yjs/<path>`, Y.Text `content`, y-sync over `yrs` (UTF-16 offsets
  converted to code points), the two-party sync and `ack` of PROTOCOL section 4. CRLF is normalised
  in the shared text on attach. Reconnect keeps the same Y.Doc, so a restart resyncs with state
  vectors and never duplicates text.
- **Presence / chat / cursors**: `/ws/doc?name=&id=&token=`. `welcome`, `user_joined`, `user_updated`,
  `user_left` become `peer` / `peer_left`; `cursor` (line, 1-based UTF-16 column, selection) is
  converted from and to code-point `pos` / `mark` using the attached text (or the mirror file for
  other files). `chat` both ways; the hub echoes your own message back, and the bridge forwards it.
- **File ops from Emacs**: `file create|rename|delete` go to `POST /api/file`, `/api/rename`,
  `/api/delete` (directories too, for rename and delete).
- **Auth**: the token goes out as `Authorization: Bearer` and `?access_token=` on every request and
  websocket. `?token=` on `/ws/doc` is the hub's client identity and is broadcast to everyone, so it
  is a random per-run value, never the secret. A 401/403 from the server gives `need_auth`; Emacs
  sends `open` again. A view-only token mirrors files and shows peers, but `attach` and `file`
  answer `error read_only` (the server would drop the writes).
- **Preview**: not offered. The hub's `preview` messages carry rendered SVG pages for the web editor,
  not a URL (`preview_log` goes to stderr).

## Tests

```
cargo test
python3 /path/to/emacs-collab.el/conformance/run.py --bridge "$PWD/target/debug/syncworthy_bridge"
python3 tests/integration.py     # needs pycrdt + websockets and a Syncworthy checkout (SYNCWORTHY_SRC)
```

The integration test starts a real `python -m syncworthy serve` on a scratch project on 127.0.0.1
(also with auth tokens) and plays a browser with pycrdt. See the header of `tests/integration.py`.
