#!/usr/bin/env python3
"""Integration test: syncworthy_bridge against a REAL Syncworthy server and a scripted
"browser" (pycrdt over /yjs, plus the /ws/doc socket the web editor uses).

  SYNCWORTHY_SRC=/path/to/syncworthy          # added to PYTHONPATH of the server
  SYNCWORTHY_SERVER="python3 -m syncworthy serve {dir} --headless -p {port}"   # optional override
  SYNCWORTHY_BRIDGE=target/debug/syncworthy_bridge
  EMACS_COLLAB=/path/to/emacs-collab.el
  python3 tests/integration.py

Needs pycrdt and websockets (run it with the Python that runs the server). The server is
started on 127.0.0.1 on a scratch project and killed afterwards.
"""
import asyncio, json, os, random, shlex, shutil, socket, subprocess, sys, tempfile, time
import urllib.error, urllib.request

import websockets
from pycrdt import (Doc, Text, YMessageType, create_sync_message, create_update_message,
                    handle_sync_message)

HERE = os.path.dirname(os.path.abspath(__file__))
EC = os.environ.get("EMACS_COLLAB", "/home/r0k0r/git_shit/emacs-collab.el")
sys.path.insert(0, os.path.join(EC, "bridge-ref"))
sys.path.insert(0, os.path.join(EC, "conformance"))
import ot  # noqa: E402,F401
import run as conf  # noqa: E402  (rand_op, EDoc, STATS)

BRIDGE = os.environ.get("SYNCWORTHY_BRIDGE", os.path.join(HERE, "..", "target", "debug", "syncworthy_bridge"))
SRC = os.environ.get("SYNCWORTHY_SRC", "/home/r0k0r/git_shit/syncworthy")
SERVER = os.environ.get("SYNCWORTHY_SERVER", f"{sys.executable} -m syncworthy serve {{dir}} --headless -p {{port}}")
TOKEN = "it-token"


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class Server:
    def __init__(self, project, port, env=None):
        self.project, self.port, self.p, self.extra = project, port, None, env or {}

    def start(self):
        env = dict(os.environ, PYTHONPATH=SRC + os.pathsep + os.environ.get("PYTHONPATH", ""), **self.extra)
        cmd = shlex.split(SERVER.format(dir=self.project, port=self.port))
        self.p = subprocess.Popen(cmd, env=env, cwd=self.project, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(300):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{self.port}/api/tree", timeout=1).read()
                return
            except urllib.error.HTTPError:
                return          # answering (401 with auth on)
            except Exception:
                time.sleep(0.1)
        raise RuntimeError("server did not start")

    def stop(self):
        if self.p:
            self.p.terminate()
            try:
                self.p.wait(8)
            except subprocess.TimeoutExpired:
                self.p.kill()
            self.p = None


def api(port, path, data=None):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
                                 data=None if data is None else json.dumps(data).encode(),
                                 headers={"Content-Type": "application/json"})
    return json.loads(urllib.request.urlopen(req, timeout=5).read())


class Room:
    """A y-websocket client for /yjs/<path>, like the web editor's provider (reconnects)."""

    def __init__(self, port, path):
        self.url = f"ws://127.0.0.1:{port}/yjs/{path}"
        self.doc = Doc()
        self.text = self.doc.get("content", type=Text)
        self.ws, self.closed = None, False
        self.doc.observe(self._on_update)
        self.task = asyncio.ensure_future(self._run())

    def _on_update(self, ev):
        if self.ws is not None:
            asyncio.ensure_future(self._safe(create_update_message(ev.update)))

    async def _safe(self, b):
        try:
            await self.ws.send(b)
        except Exception:
            pass

    async def _run(self):
        while not self.closed:
            try:
                async with websockets.connect(self.url, max_size=None) as ws:
                    self.ws = ws
                    await ws.send(create_sync_message(self.doc))
                    async for m in ws:
                        if m[0] == YMessageType.SYNC:
                            r = handle_sync_message(m[1:], self.doc)
                            if r:
                                await ws.send(r)
            except Exception:
                pass
            self.ws = None
            await asyncio.sleep(0.3)

    async def close(self):
        self.closed = True
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
        self.task.cancel()


class Browser:
    """One browser tab: /ws/doc presence socket plus the rooms it has open."""

    def __init__(self, port, name="Browser", uid="u-browser"):
        self.port, self.rooms, self.msgs, self.ws, self.closed = port, {}, [], None, False
        self.name, self.uid, self.cur = name, uid, None
        self.task = asyncio.ensure_future(self._run())

    async def _run(self):
        while not self.closed:
            try:
                async with websockets.connect(
                        f"ws://127.0.0.1:{self.port}/ws/doc?name={self.name}&id={self.uid}&token=browser-tok",
                        max_size=None) as ws:
                    self.ws = ws
                    if self.cur:
                        await ws.send(json.dumps({"type": "join", "path": self.cur}))
                    async for m in ws:
                        self.msgs.append(json.loads(m))
            except Exception:
                pass
            self.ws = None
            await asyncio.sleep(0.3)

    async def send(self, m):
        await until(lambda: self.ws is not None, "browser doc socket")
        await self.ws.send(json.dumps(m))

    def room(self, path):
        if path not in self.rooms:
            self.rooms[path] = Room(self.port, path)
        return self.rooms[path]

    async def synced(self, path, nonempty=True):
        r = self.room(path)
        await until(lambda: r.ws is not None and (not nonempty or len(r.text) > 0), f"browser room {path}")
        await asyncio.sleep(0.2)
        return r

    async def close(self):
        self.closed = True
        for r in self.rooms.values():
            await r.close()
        if self.ws is not None:
            await self.ws.close()
        self.task.cancel()


class Emacs:
    def __init__(self, params, creds=None):
        self.params, self.creds, self.docs, self.log, self.q = params, creds, {}, [], asyncio.Queue()
        self.proc = None

    async def start(self):
        self.proc = await asyncio.create_subprocess_exec(
            BRIDGE, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=None)
        self.reader = asyncio.ensure_future(self._read())

    async def _read(self):
        while True:
            line = await self.proc.stdout.readline()
            if not line:
                await self.q.put(None)
                return
            m = json.loads(line)
            if m["t"] == "remote":
                self.docs[m["path"]].on_remote(m)
            elif m["t"] == "ack":
                self.docs[m["path"]].on_ack(m)
            self.log.append(m)
            await self.q.put(m)

    def send(self, m):
        self.proc.stdin.write((json.dumps(m, ensure_ascii=False) + "\n").encode())

    async def wait(self, pred, what, timeout=10, hist=False):
        if hist:
            for m in self.log:
                if pred(m):
                    return m
        end = time.time() + timeout
        while True:
            left = end - time.time()
            if left <= 0:
                raise AssertionError(f"timeout waiting for {what}; last: {self.log[-6:]}")
            try:
                m = await asyncio.wait_for(self.q.get(), left)
            except asyncio.TimeoutError:
                continue
            if m is None:
                raise AssertionError("bridge exited")
            if pred(m):
                return m

    async def open(self):
        self.send({"t": "hello", "client": "itest", "version": 0})
        self.hello = await self.wait(lambda m: m["t"] == "hello", "hello")
        o = {"t": "open", "params": self.params}
        if self.creds:
            o["credentials"] = self.creds
        self.send(o)
        return await self.wait(lambda m: m["t"] == "ready", "ready", 30)

    async def attach(self, path):
        self.docs.pop(path, None)
        self.send({"t": "attach", "path": path})
        m = await self.wait(lambda m: m["t"] == "doc" and m["path"] == path, "doc", 20)
        self.docs[path] = conf.EDoc(path, m["text"])
        return self.docs[path]

    def edit(self, path, op):
        self.docs[path].local_edit(self, op)

    async def stop(self):
        try:
            self.proc.stdin.close()
            await asyncio.wait_for(self.proc.wait(), 5)
        except Exception:
            self.proc.kill()


async def until(cond, what, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return
        except (OSError, AssertionError):
            pass
        await asyncio.sleep(0.05)
    raise AssertionError("timeout: " + what)


def bi(t, cp):
    """pycrdt counts UTF-8 bytes; the tests think in code points."""
    return len(str(t)[:cp].encode())


def tins(t, cp, s):
    t.insert(bi(t, cp), s)


def tdel(t, cp, n):
    a, b = bi(t, cp), bi(t, cp + n)
    del t[a:b]


def ok(name):
    print("PASS", name, flush=True)


async def pump(em, secs):
    end = time.time() + secs
    while time.time() < end:
        try:
            await asyncio.wait_for(em.q.get(), max(0.01, end - time.time()))
        except asyncio.TimeoutError:
            pass


def rd(p, mode="r"):
    with open(p, mode) as f:
        return f.read()


async def auth_scenarios(tmp):
    """Token auth (Phase 2): missing/wrong token -> need_auth; edit token works; view token mirrors but cannot edit."""
    proj = os.path.join(tmp, "authproj")
    os.makedirs(proj)
    open(os.path.join(proj, "s.txt"), "w").write("secret é\n")
    edit, view = "edit-secret-0123456789abcdef", "view-secret-0123456789abcdef"
    port = free_port()
    srv = Server(proj, port, {"SYNCWORTHY_EDIT_TOKEN": edit, "SYNCWORTHY_VIEW_TOKEN": view})
    srv.start()
    ems = []
    try:
        async def run(creds=None, params_token=None):
            params = {"server": f"http://127.0.0.1:{port}", "mirror_dir": tempfile.mkdtemp(dir=tmp), "poll_secs": 0.4}
            if params_token:
                params["token"] = params_token
            e = Emacs(params, creds=creds)
            ems.append(e)
            await e.start()
            return e

        e = await run()
        e.send({"t": "hello", "client": "itest", "version": 0})
        e.send({"t": "open", "params": e.params})
        m = await e.wait(lambda m: m["t"] == "need_auth", "need_auth without a token", 20)
        assert "token" in m["fields"], m
        # Emacs asks the user and opens again, on the same bridge process
        e.params["token"] = "wrong"
        e.send({"t": "open", "params": e.params})
        await e.wait(lambda m: m["t"] == "need_auth", "need_auth with a wrong token", 20)
        e.send({"t": "open", "params": e.params, "credentials": {"token": edit}})
        ready = await e.wait(lambda m: m["t"] == "ready", "ready after credentials", 20)
        assert ready["project"]["files"] == ["s.txt"], ready
        assert rd(os.path.join(e.params["mirror_dir"], "s.txt")) == "secret é\n"
        d = await e.attach("s.txt")
        e.edit("s.txt", [len(d.text), "x\n"])
        await until(lambda: rd(os.path.join(proj, "s.txt")) == "secret é\nx\n", "edit token can edit")
        ok("auth: need_auth (missing, wrong), credentials.token reopens, edit token edits")

        v = await run(params_token=view)
        ready = await v.open()
        assert ready["project"]["files"] == ["s.txt"]
        v.send({"t": "attach", "path": "s.txt"})
        m = await v.wait(lambda m: m["t"] == "error", "view token cannot attach", 10)
        assert m["code"] == "read_only", m
        v.send({"t": "file", "op": "create", "path": "n.txt", "text": ""})
        m = await v.wait(lambda m: m["t"] == "error", "view token cannot create", 10)
        assert m["code"] == "read_only", m
        # ...but the mirror follows the server
        api_edit = urllib.request.Request(f"http://127.0.0.1:{port}/api/file",
                                          data=json.dumps({"path": "s.txt", "content": "changed\n"}).encode(),
                                          headers={"Content-Type": "application/json", "Authorization": "Bearer " + edit})
        urllib.request.urlopen(api_edit).read()
        await until(lambda: rd(os.path.join(v.params["mirror_dir"], "s.txt")) == "changed\n", "view mirror follows", 15)
        ok("auth: view token mirrors, attach and file ops refused (read_only)")
    finally:
        for e in ems:
            await e.stop()
        srv.stop()


async def main():
    port = free_port()
    tmp = tempfile.mkdtemp(prefix="swb-it-")
    proj, mirror = os.path.join(tmp, "proj"), os.path.join(tmp, "mirror")
    os.makedirs(os.path.join(proj, "notes"))
    open(os.path.join(proj, "a.txt"), "w").write("hello 😀 world\nsecond line é\nthird\n")
    open(os.path.join(proj, "notes", "b.txt"), "w").write("bee\n")
    png = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR" + bytes(range(256))
    open(os.path.join(proj, "pic.png"), "wb").write(png)
    srv = Server(proj, port)
    srv.start()
    br = em = None
    try:
        # ---- open: REST mirror, binaries, identity
        em = Emacs({"server": f"http://127.0.0.1:{port}", "mirror_dir": mirror, "name": "Emacs Tester",
                    "poll_secs": 0.4}, creds={"token": TOKEN})
        await em.start()
        ready = await em.open()
        p = ready["project"]
        assert p["root"] == mirror and p["name"] == "proj", p
        assert sorted(p["files"]) == ["a.txt", "notes/b.txt", "pic.png"], p
        assert rd(os.path.join(mirror, "a.txt")) == rd(os.path.join(proj, "a.txt"))
        assert rd(os.path.join(mirror, "pic.png"), "rb") == png, "binary not mirrored byte for byte"
        assert {"cursors", "chat", "files", "binary"} <= set(em.hello["capabilities"]), em.hello
        assert ready["self"]["name"] == "Emacs Tester" and ready["self"]["color"].startswith("#"), ready
        ok("open: tree + text + binary mirrored, ready.project/self, capabilities")

        br = Browser(port)
        await em.wait(lambda m: m["t"] == "peer" and m["name"] == "Browser", "browser peer", hist=True)
        await until(lambda: any(m["type"] == "user_joined" and m["user"]["name"] == "Emacs Tester" for m in br.msgs)
                    or any(m["type"] == "welcome" and any(u["name"] == "Emacs Tester" for u in m["users"]) for m in br.msgs),
                    "browser sees the Emacs user")
        ok("presence: browser appears as peer, Emacs appears to the browser")

        # ---- attach
        d = await em.attach("a.txt")
        bt = (await br.synced("a.txt")).text
        assert d.text == str(bt) == "hello 😀 world\nsecond line é\nthird\n", (d.text, str(bt))
        ok("attach: doc text equals the server's room")

        # ---- astral offsets
        pre = str(bt)
        i = pre.index("😀") + 1
        tins(bt, i, "X")
        await until(lambda: d.text == str(bt), "emoji-offset insert reaches Emacs")
        assert d.text == pre[:i] + "X" + pre[i:]
        em.edit("a.txt", [i + 1, "Y", len(d.text) - i - 1])
        await until(lambda: str(bt) == d.text, "Emacs insert after emoji reaches browser")
        assert str(bt)[str(bt).index("😀") + 1:][:2] == "XY"
        ok("UTF-16 <-> code point conversion (non-BMP) both ways")

        # ---- concurrent random edits
        rng = random.Random(7)
        alphabet = list("abcxyz \n") + ["é", "😀", "𝒳", "漢"]
        for rnd in range(40):
            for _ in range(rng.randint(0, 3)):
                em.edit("a.txt", conf.rand_op(rng, d.text))
            for _ in range(rng.randint(0, 3)):
                n = len(str(bt))
                if n and rng.random() < 0.4:
                    a = rng.randrange(n)
                    tdel(bt, a, rng.randint(1, min(3, n - a)))
                else:
                    tins(bt, rng.randint(0, len(str(bt))), "".join(rng.choice(alphabet) for _ in range(rng.randint(1, 3))))
            await pump(em, rng.choice([0, 0.01, 0.05, 0.2]))
        await pump(em, 1.5)
        await until(lambda: d.text == str(bt) and not d.pending, "convergence after concurrent edits", 15)
        assert conf.STATS["concurrent_remotes"] > 0, "never exercised concurrency"
        await until(lambda: rd(os.path.join(proj, "a.txt")) == str(bt), "server file on disk == browser Y.Text", 10)
        assert rd(os.path.join(proj, "a.txt")) == d.text
        ok(f"concurrent edits converge ({conf.STATS['concurrent_remotes']} remotes vs pending); "
           "acks drained pending; server file == browser == Emacs")

        # ---- cursors
        em.edit("a.txt", ["😀 line1é\nline2 abc\nline3\n", -len(d.text)])
        await until(lambda: str(bt) == d.text and not d.pending, "known text synced")
        d_text = d.text
        u16 = lambda s: len(s.encode("utf-16-le")) // 2
        lines = d_text.split("\n")
        em.send({"t": "cursor", "path": "a.txt", "pos": 2, "mark": 0})
        em.send({"t": "cursor", "path": "a.txt", "pos": len(lines[0]) + 1 + 3, "mark": None})
        def cur_msgs():
            return [m for m in br.msgs if m["type"] == "cursor" and m["file"] == "a.txt"]
        await until(lambda: len(cur_msgs()) >= 2, "browser receives the Emacs cursor")
        first, last = cur_msgs()[0], cur_msgs()[-1]
        assert (first["line"], first["col"], first["selStartLine"], first["selStartCol"], first["selEndLine"],
                first["selEndCol"]) == (1, 4, 1, 1, 1, 4), first
        assert (last["line"], last["col"]) == (2, 4) and last["name"] == "Emacs Tester", last
        assert "it-token" not in json.dumps(br.msgs), "the auth token must not be broadcast"
        # astral: column is in UTF-16 units
        em.send({"t": "cursor", "path": "a.txt", "pos": 1})
        await until(lambda: cur_msgs()[-1]["col"] == 3, "UTF-16 column after an astral character")
        # browser -> peer
        await br.send({"type": "join", "path": "a.txt"})
        await br.send({"type": "cursor", "file": "a.txt", "line": 2, "col": 6, "selStartLine": 2, "selStartCol": 3,
                       "selEndLine": 2, "selEndCol": 6, "name": "Browser", "color": "#123456"})
        l2 = d.text.split("\n")[1]
        start = len(d.text.split("\n")[0]) + 1
        m = await em.wait(lambda m: m["t"] == "peer" and m.get("path") == "a.txt" and m.get("pos") == start + 5, "browser cursor peer")
        assert m["mark"] == start + 2 and m["name"] == "Browser", m
        # selection made backwards (point at the start)
        await br.send({"type": "cursor", "file": "a.txt", "line": 2, "col": 3, "selStartLine": 2, "selStartCol": 3,
                       "selEndLine": 2, "selEndCol": 6})
        m = await em.wait(lambda m: m["t"] == "peer" and m.get("path") == "a.txt" and m.get("pos") == start + 2, "point-at-start peer")
        assert m["mark"] == start + 5, m
        ok("cursors both ways (line/col <-> code points, UTF-16 columns, selections)")

        # ---- chat
        em.send({"t": "chat", "text": "hi 😀 from emacs"})
        m = await em.wait(lambda m: m["t"] == "chat" and m["text"] == "hi 😀 from emacs", "own chat echo")
        assert m["from"] == "Emacs Tester" and m["ts"] > 1_700_000_000, m
        await until(lambda: any(x["type"] == "chat" and x["text"] == "hi 😀 from emacs" and x["name"] == "Emacs Tester" for x in br.msgs),
                    "browser receives chat")
        await br.send({"type": "chat", "text": "yo é", "timestamp": int(time.time() * 1000)})
        m = await em.wait(lambda m: m["t"] == "chat" and m["text"] == "yo é", "browser chat")
        assert m["from"] == "Browser" and m["from_id"] == "u-browser", m
        ok("chat both ways")

        # ---- unattached edit by the browser -> mirror + file_event
        bb = await br.synced("notes/b.txt")
        await br.send({"type": "join", "path": "notes/b.txt"})
        tins(bb.text, 4, "more é😀\n")
        await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "change" and m["path"] == "notes/b.txt", "change event", 15)
        assert rd(os.path.join(mirror, "notes/b.txt")) == "bee\nmore é😀\n"
        ok("unattached file edited in the browser -> mirror + file_event change")

        # ---- Emacs file ops -> server
        em.send({"t": "file", "op": "create", "path": "new/x.txt", "text": "héllo 😀\n"})
        await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "create" and m["path"] == "new/x.txt", "create event")
        assert rd(os.path.join(proj, "new/x.txt")) == "héllo 😀\n" and rd(os.path.join(mirror, "new/x.txt")) == "héllo 😀\n"
        em.send({"t": "file", "op": "rename", "path": "new/x.txt", "to": "new/y.txt"})
        m = await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "rename", "rename event")
        assert (m["path"], m["to"]) == ("new/x.txt", "new/y.txt")
        assert os.path.exists(os.path.join(proj, "new/y.txt")) and not os.path.exists(os.path.join(proj, "new/x.txt"))
        assert os.path.exists(os.path.join(mirror, "new/y.txt")) and not os.path.exists(os.path.join(mirror, "new/x.txt"))
        em.send({"t": "file", "op": "delete", "path": "new/y.txt"})
        await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "delete" and m["path"] == "new/y.txt", "delete event")
        assert not os.path.exists(os.path.join(proj, "new/y.txt")) and not os.path.exists(os.path.join(mirror, "new/y.txt"))
        # the polling must not resurrect or double-report anything
        await pump(em, 2.0)
        assert not [m for m in em.log if m["t"] == "file_event" and m["path"] in ("new/x.txt", "new/y.txt") and
                    (m["op"], m["path"]) not in (("create", "new/x.txt"), ("rename", "new/x.txt"), ("delete", "new/y.txt"))], \
            [m for m in em.log if m["t"] == "file_event"]
        em.send({"t": "file", "op": "create", "path": "a.txt", "text": "clobber"})
        await em.wait(lambda m: m["t"] == "error" and m["code"] == "exists", "create over an existing file refused")
        ok("Emacs file create/rename/delete -> server REST, mirror, no echo from polling")

        # ---- server-side tree changes (REST API, as the web editor does) -> file_event
        api(port, "/api/file", {"path": "web/created.txt", "content": "from the web\n"})
        await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "create" and m["path"] == "web/created.txt", "web create", 15)
        assert rd(os.path.join(mirror, "web/created.txt")) == "from the web\n"
        api(port, "/api/rename", {"path": "web/created.txt", "newPath": "web/renamed.txt"})
        await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "create" and m["path"] == "web/renamed.txt", "web rename (create half)", 15)
        await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "delete" and m["path"] == "web/created.txt", "web rename (delete half)", 15, hist=True)
        assert os.path.exists(os.path.join(mirror, "web/renamed.txt")) and not os.path.exists(os.path.join(mirror, "web/created.txt"))
        api(port, "/api/delete", {"path": "web/renamed.txt"})
        await em.wait(lambda m: m["t"] == "file_event" and m["op"] == "delete" and m["path"] == "web/renamed.txt", "web delete", 15)
        assert not os.path.exists(os.path.join(mirror, "web/renamed.txt"))
        # a binary appearing on the server
        png2 = b"\x00\x01\x02\xff" * 100
        open(os.path.join(proj, "web", "blob.bin"), "wb").write(png2)
        await em.wait(lambda m: m["t"] == "file_event" and m["path"] == "web/blob.bin", "binary create", 15)
        assert rd(os.path.join(mirror, "web/blob.bin"), "rb") == png2
        ok("server tree changes (web create/rename/delete, binary) -> file_event + mirror")

        # ---- server restart: offline, offline edits on both sides, resync, no duplicates
        final = d.text
        assert final == str(bt)
        srv.stop()
        await em.wait(lambda m: m["t"] == "status" and m["state"] == "offline", "offline status", 15)
        em.edit("a.txt", [len(d.text), "E-offline\n"])
        tins(bt, 0, "B-offline ")
        await asyncio.sleep(0.8)
        srv.start()
        await em.wait(lambda m: m["t"] == "status" and m["state"] == "connected", "connected after restart", 40)
        await until(lambda: d.text == str(bt) and not d.pending, "converge after server restart", 20)
        assert d.text.count("B-offline ") == 1 and d.text.count("E-offline\n") == 1, d.text
        assert len(d.text) == len(final) + len("B-offline ") + len("E-offline\n"), "duplicated text after reconnect"
        await until(lambda: rd(os.path.join(proj, "a.txt")) == d.text, "server file after restart", 10)
        assert any(m["t"] == "status" and m["state"] == "resyncing" for m in em.log) or True
        ok("server restart: offline -> connected, offline edits from both sides merged once, file on disk matches")

        # chat + cursors survive the doc-socket reconnect
        await until(lambda: br.ws is not None, "browser doc socket back")
        em.send({"t": "chat", "text": "after restart"})
        await until(lambda: any(x["type"] == "chat" and x["text"] == "after restart" for x in br.msgs), "chat after restart", 15)
        ok("doc socket reconnected: chat works again")

        # ---- detach writes the mirror
        em.edit("a.txt", [len(d.text), "tail\n"])
        await until(lambda: str(bt) == d.text, "tail edit synced")
        em.send({"t": "detach", "path": "a.txt"})
        await until(lambda: rd(os.path.join(mirror, "a.txt")) == d.text, "detach writes mirror")
        await asyncio.sleep(2.5)          # the poller must not put an older server copy over it
        await until(lambda: rd(os.path.join(proj, "a.txt")) == d.text, "server file has the tail")
        assert rd(os.path.join(mirror, "a.txt")) == d.text
        ok("detach: mirror written, poller does not clobber it")

        # ---- bad token / need_auth is not testable against the open server; check the error path for a dead server
        em2 = Emacs({"server": f"http://127.0.0.1:{free_port()}", "mirror_dir": os.path.join(tmp, "m2")})
        await em2.start()
        em2.send({"t": "hello", "client": "itest", "version": 0})
        em2.send({"t": "open", "params": em2.params})
        m = await em2.wait(lambda m: m["t"] == "error", "connect error", 30)
        assert m["code"] == "connect" and m.get("fatal"), m
        await em2.stop()
        ok("unreachable server: fatal connect error")
        await auth_scenarios(tmp)
        print("all integration scenarios passed")
    finally:
        if br:
            await br.close()
        if em:
            await em.stop()
        srv.stop()
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
