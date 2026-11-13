# -*- coding: utf-8 -*-
"""
walcdc — SQLite WAL 逐事务变更采集引擎（CDC）。

直接解析 SQLite 数据库 b-tree 与 WAL 帧格式（采集器禁用 SQLite 解码源库，
这里只用 sqlite3 模块维护自身的状态库），把 RPA 留存的 WAL 字节流还原为
每个已提交事务对受跟踪表的净行级变更。

接口：
    eng = walcdc.Engine(state, base, tables)
    changes = eng.feed(offset, data)   # -> [[提交帧号, 表名, 键, 旧行, 新行], ...]
    rows    = eng.read(table)          # -> [[键, 整行], ...]

参数约定：
    state   状态库路径（进度、未提交页、行状态、回执全部原子持久化于此）；
    base    WAL 起点基库（数据库镜像）路径，仅首次初始化时读取；
    tables  {表名: [根页号, 列数, 主键列号列表或 None]}，列号从 0 起；
            主键列号列表为 None 表示无别名 rowid 表，否则为 WITHOUT ROWID 表；
            行一律按列序返回。

语义：
  * feed 从偏移 0 接收连续 WAL 字节，允许任意分片；不完整帧等待续传；
  * 重试同调用返回原结果（回执持久化）；旧偏移异请求、跳段、坏校验都会
    把当前未提交事务整次回滚；
  * 仅完整提交可见；每个提交内每个键只输出净变化（[7]->[8]->[7] 不输出）；
  * 键、行按“类型 + 值”判等（整数 1 与浮点 1.0 不相等）；
  * 缺行（插入的旧行 / 删除的新行）用 None 表示；
  * 键为 rowid 整数或主键值列表；
  * 只扫描新增帧与受影响页的依赖闭包，不做逐提交全库扫描。
"""

import hashlib
import pickle
import sqlite3
import struct
from collections import deque

__all__ = ["Engine", "CorruptError"]

_WAL_MAGIC_LE = 0x377F0682   # 小端校验和
_WAL_MAGIC_BE = 0x377F0683   # 大端校验和
_WAL_VERSION = 3007000
_WAL_HDR = 32                # WAL 头长度
_FRAME_HDR = 24              # 帧头长度


class CorruptError(Exception):
    """源库 / WAL 字节流损坏，或与已持久化的字节流冲突。"""


# ---------------------------------------------------------------------------
# 类型化判等与排序：键、行按“类型 + 值”判等
# ---------------------------------------------------------------------------

def _tkey(v):
    if v is None:
        return (0, None)
    if isinstance(v, int):          # 不会出现 bool（记录格式无该类型）
        return (1, v)
    if isinstance(v, float):
        return (2, v)
    if isinstance(v, str):
        return (3, v)
    if isinstance(v, bytes):
        return (4, v)
    raise TypeError("不支持的值类型: %r" % type(v))


def _mapkey(key):
    """键（rowid 整数或主键值列表）-> 可哈希、类型可辨的映射键。"""
    if isinstance(key, list):
        return tuple(_tkey(v) for v in key)
    return (_tkey(key),)


def _rows_equal(a, b):
    if a is b:
        return True
    if len(a) != len(b):
        return False
    for x, y in zip(a, b):
        if _tkey(x) != _tkey(y):
            return False
    return True


def _sortval(v):
    # SQLite 值序：NULL < 数值 < 文本 < 二进制（仅用于输出的确定性排序）
    if v is None:
        return (0, 0)
    if isinstance(v, (int, float)):
        return (1, v)
    if isinstance(v, str):
        return (2, v)
    return (3, v)


def _keysort(key):
    if isinstance(key, list):
        return tuple(_sortval(v) for v in key)
    return _sortval(key)


# ---------------------------------------------------------------------------
# 基础解析：变长整数、记录、页负载、b-tree 页
# ---------------------------------------------------------------------------

def _u16(b, o):
    return (b[o] << 8) | b[o + 1]


def _u32(b, o):
    return struct.unpack_from(">I", b, o)[0]


def _varint(b, o):
    r = 0
    for i in range(9):
        x = b[o + i]
        if i == 8:
            return ((r << 8) | x), o + 9
        r = (r << 7) | (x & 0x7F)
        if not (x & 0x80):
            return r, o + i + 1
    return r, o + 9  # 不可达


def _i64(v):
    return v - (1 << 64) if v >= (1 << 63) else v


_INT_SZ = {1: 1, 2: 2, 3: 3, 4: 4, 5: 6, 6: 8}


def _record(payload, encoding):
    """解析记录（记录头 + 记录体）为值列表。"""
    try:
        hsz, p = _varint(payload, 0)
        types = []
        while p < hsz:
            t, p = _varint(payload, p)
            types.append(t)
        vals = []
        d = hsz
        for t in types:
            if t == 0:
                vals.append(None)
            elif t in _INT_SZ:
                n = _INT_SZ[t]
                vals.append(int.from_bytes(payload[d:d + n], "big", signed=True))
                d += n
            elif t == 7:
                vals.append(struct.unpack_from(">d", payload, d)[0])
                d += 8
            elif t == 8:
                vals.append(0)
            elif t == 9:
                vals.append(1)
            elif t >= 12:
                n = (t - 12) // 2 if t % 2 == 0 else (t - 13) // 2
                raw = payload[d:d + n]
                d += n
                vals.append(raw if t % 2 == 0 else raw.decode(encoding))
            else:
                raise CorruptError("保留序列类型 %d" % t)
        return vals
    except (IndexError, struct.error, UnicodeDecodeError) as e:
        raise CorruptError("记录解析失败: %r" % (e,))


def _wal_cksum(data, s1, s2, be):
    """WAL 校验和：按魔数指定端序解释 32 位字，成对累加。"""
    n = len(data) // 8 * 2
    w = struct.unpack((">" if be else "<") + "%dI" % n, data[:n * 4])
    for i in range(0, n, 2):
        s1 = (s1 + w[i] + s2) & 0xFFFFFFFF
        s2 = (s2 + w[i + 1] + s1) & 0xFFFFFFFF
    return s1, s2


def _btree_info(img, pgno):
    """若 img 是 b-tree 页，返回 {'interior': bool, 'children': [...]}，否则 None。"""
    base = 100 if pgno == 1 else 0
    if img is None or len(img) < base + 8:
        return None
    t = img[base]
    if t not in (2, 5, 10, 13):
        return None
    interior = t in (2, 5)
    info = {"interior": interior, "children": []}
    if interior:
        ncells = _u16(img, base + 3)
        pa = base + 12
        ch = info["children"]
        for i in range(ncells):
            ch.append(_u32(img, _u16(img, pa + 2 * i)))
        ch.append(_u32(img, base + 8))   # 最右指针
    return info


def _payload(img, off, plen, usable, table_leaf, src):
    """读取单元负载（含溢出链）。返回 (payload, [溢出页号...])。"""
    U = usable
    max_local = (U - 35) if table_leaf else ((U - 12) * 64) // 255 - 23
    if plen <= max_local:
        return img[off:off + plen], []
    min_local = ((U - 12) * 32) // 255 - 23
    k = min_local + (plen - min_local) % (U - 4)
    nlocal = k if k <= max_local else min_local
    parts = [img[off:off + nlocal]]
    ovf = []
    nxt = _u32(img, off + nlocal)
    remaining = plen - nlocal
    while remaining > 0:
        if nxt == 0:
            raise CorruptError("溢出链断裂")
        ovf.append(nxt)
        oimg = src.get(nxt)
        if oimg is None:
            raise CorruptError("溢出页缺失 %d" % nxt)
        take = min(remaining, U - 4)
        parts.append(oimg[4:4 + take])
        remaining -= take
        nxt = _u32(oimg, 0)
    return b"".join(parts), ovf


def _wr_row(vals, pk, ncols):
    """WITHOUT ROWID 叶记录 -> 按列序的整行。记录布局：主键列（主键序）+ 其余列（列序）。"""
    npk = len(pk)
    pkset = set(pk)
    nonpk = [c for c in range(ncols) if c not in pkset]
    row = [None] * ncols
    for i, v in enumerate(vals):
        if i < npk:
            row[pk[i]] = v
        elif i - npk < len(nonpk):
            row[nonpk[i - npk]] = v
    return row


def _extract(img, pgno, tspec, usable, encoding, src):
    """解析页内承载的行 -> [(键, 行, [溢出页...])]。行按列序。

    rowid 表仅叶页（13）承载行；WITHOUT ROWID 表为索引 b-tree，
    内页（2）与叶页（10）的每个单元都承载一整行。
    """
    _, ncols, pk = tspec
    base = 100 if pgno == 1 else 0
    t = img[base]
    ncells = _u16(img, base + 3)
    out = []
    if t == 13:                      # rowid 表叶页
        if pk is not None:
            raise CorruptError("WITHOUT ROWID 表出现 rowid 叶页")
        pa = base + 8
        for i in range(ncells):
            cp = _u16(img, pa + 2 * i)
            plen, o = _varint(img, cp)
            rowid, o = _varint(img, o)
            payload, ovf = _payload(img, o, plen, usable, True, src)
            vals = _record(payload, encoding)
            row = (vals + [None] * ncols)[:ncols]
            out.append((_i64(rowid), row, ovf))
    elif t == 10:                    # WITHOUT ROWID 叶页（索引叶）
        if pk is None:
            raise CorruptError("rowid 表出现索引叶页")
        pa = base + 8
        for i in range(ncells):
            cp = _u16(img, pa + 2 * i)
            plen, o = _varint(img, cp)
            payload, ovf = _payload(img, o, plen, usable, False, src)
            vals = _record(payload, encoding)
            row = _wr_row(vals, pk, ncols)
            out.append(([row[c] for c in pk], row, ovf))
    elif t == 2:                     # WITHOUT ROWID 内页（索引内页，单元含整行）
        if pk is None:
            raise CorruptError("rowid 表出现索引内页")
        pa = base + 12
        for i in range(ncells):
            cp = _u16(img, pa + 2 * i)
            plen, o = _varint(img, cp + 4)          # 前 4 字节为子页指针
            payload, ovf = _payload(img, o, plen, usable, False, src)
            vals = _record(payload, encoding)
            row = _wr_row(vals, pk, ncols)
            out.append(([row[c] for c in pk], row, ovf))
    elif t == 5:                     # rowid 表内页：不承载行
        if pk is not None:
            raise CorruptError("WITHOUT ROWID 表出现 rowid 内页")
        return []
    else:
        raise CorruptError("页 %d 非 b-tree 页（类型 %d）" % (pgno, t))
    return out


def _carries_rows(tspec, role):
    """该表该角色的页是否承载行：rowid 表仅叶页；WITHOUT ROWID 表内页亦承载。"""
    return role == "l" or tspec[2] is not None


def _first_diff(a, b):
    for i in range(min(len(a), len(b))):
        if a[i] != b[i]:
            return i
    return min(len(a), len(b))


class _Src:
    """页像访问器：优先覆盖层（本事务新帧），否则读状态库 pages 表。"""

    def __init__(self, cur, overlay=None):
        self.cur = cur
        self.overlay = overlay if overlay is not None else {}
        self.cache = {}

    def get(self, pgno):
        if pgno in self.overlay:
            return self.overlay[pgno]
        if pgno not in self.cache:
            r = self.cur.execute("SELECT data FROM pages WHERE no=?", (pgno,)).fetchone()
            self.cache[pgno] = r[0] if r else None
        return self.cache[pgno]


# ---------------------------------------------------------------------------
# 引擎
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v BLOB);
CREATE TABLE IF NOT EXISTS pages(no INTEGER PRIMARY KEY, data BLOB);
CREATE TABLE IF NOT EXISTS pmap(no INTEGER PRIMARY KEY, tbl TEXT, role TEXT, parent INT);
CREATE INDEX IF NOT EXISTS pmap_parent ON pmap(parent);
CREATE TABLE IF NOT EXISTS ovf(no INTEGER PRIMARY KEY, tbl TEXT, key BLOB, leaf INT);
CREATE TABLE IF NOT EXISTS pending(no INTEGER PRIMARY KEY, pgno INT, csize INT,
                                   ck1 INT, ck2 INT, data BLOB);
CREATE TABLE IF NOT EXISTS stream_parts(seq INTEGER PRIMARY KEY, data BLOB);
CREATE TABLE IF NOT EXISTS receipts(off INT, len INT, hash BLOB, result BLOB,
                                    PRIMARY KEY(off, len, hash));
"""

_STREAM_CHUNK = 1 << 20

_META_KEYS = ("page_size", "reserved", "encoding", "db_size", "committed_len",
              "committed_fno", "ck1", "ck2", "header_seen", "salt1", "salt2",
              "be", "commit_seq")


class Engine:
    """WAL 逐事务变更采集引擎。"""

    def __init__(self, state, base, tables):
        self.tables = {}
        for name, spec in tables.items():
            root, ncols, pk = spec[0], spec[1], spec[2]
            self.tables[str(name)] = (int(root), int(ncols),
                                      None if pk is None else [int(c) for c in pk])
        self.root_of = {}
        for name, (root, _, _) in self.tables.items():
            self.root_of[root] = name

        self.db = sqlite3.connect(state, isolation_level=None,
                                  check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        for stmt in _SCHEMA.strip().split(";"):
            if stmt.strip():
                self.db.execute(stmt)
        row = self.db.execute("SELECT v FROM meta WHERE k='initialized'").fetchone()
        if row is None:
            self._init_from_base(base)
        self._load_state()

    # ------------------------------------------------------------------ 初始化

    def _init_from_base(self, base):
        with open(base, "rb") as f:
            data = f.read()
        if len(data) < 100 or data[:16] != b"SQLite format 3\x00":
            raise CorruptError("基库不是 SQLite 数据库")
        page_size = _u16(data, 16)
        if page_size == 1:
            page_size = 65536
        if page_size < 512 or page_size > 65536 or page_size & (page_size - 1):
            raise CorruptError("非法页尺寸 %d" % page_size)
        if len(data) % page_size:
            raise CorruptError("基库长度非整页")
        reserved = data[20]
        enc = _u32(data, 56)
        if enc not in (1, 2, 3):
            raise CorruptError("非法编码 %d" % enc)
        self.page_size = page_size
        self.reserved = reserved
        self.usable = page_size - reserved
        self.encoding = {1: "utf-8", 2: "utf-16le", 3: "utf-16be"}[enc]
        self.db_size = len(data) // page_size
        self.header_seen = 0
        self.salt1 = self.salt2 = 0
        self.be = True
        self.ck1 = self.ck2 = 0
        self.committed_len = 0
        self.committed_fno = 0
        self.commit_seq = 0
        self.stream = bytearray()
        self._persisted_len = 0
        self._parts_dirty = False
        self.parse_pos = 0
        self.run1 = self.run2 = 0

        cur = self.db.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            cur.execute("INSERT INTO meta VALUES('initialized',?)", (pickle.dumps(1),))
            for i in range(self.db_size):
                cur.execute("INSERT INTO pages VALUES(?,?)",
                            (i + 1, data[i * page_size:(i + 1) * page_size]))
            src = _Src(cur)
            for name, (root, _, _) in self.tables.items():
                self._init_walk(cur, src, name, root)
            self._persist_meta(cur)
            cur.execute("COMMIT")
        except Exception:
            if self.db.in_transaction:
                cur.execute("ROLLBACK")
            raise

    def _init_walk(self, cur, src, tname, root):
        """首次初始化：从根页遍历整棵 b-tree，建立页映射与溢出属主（仅此一次）。"""
        tspec = self.tables[tname]
        seen = set()
        stack = [(root, None)]
        while stack:
            pg, parent = stack.pop()
            if pg in seen:
                raise CorruptError("b-tree 成环")
            seen.add(pg)
            img = src.get(pg)
            info = _btree_info(img, pg)
            if info is None:
                raise CorruptError("表 %s 根/内页 %d 非 b-tree 页" % (tname, pg))
            role = "i" if info["interior"] else "l"
            cur.execute("INSERT INTO pmap VALUES(?,?,?,?)", (pg, tname, role, parent))
            if info["interior"]:
                for c in info["children"]:
                    stack.append((c, pg))
            if _carries_rows(tspec, role):
                for key, _row, ovfl in _extract(img, pg, tspec, self.usable,
                                                self.encoding, src):
                    for o in ovfl:
                        cur.execute("INSERT INTO ovf VALUES(?,?,?,?)",
                                    (o, tname, pickle.dumps(key), pg))

    # ------------------------------------------------------------------ 状态存取

    def _persist_meta(self, cur):
        for k in _META_KEYS:
            cur.execute("INSERT OR REPLACE INTO meta VALUES(?,?)",
                        (k, pickle.dumps(getattr(self, k))))
        # WAL 字节流增量持久化（避免每次重写全量）
        if self._parts_dirty:
            cur.execute("DELETE FROM stream_parts")
            data = bytes(self.stream)
            seq = 0
            for i in range(0, len(data), _STREAM_CHUNK):
                seq += 1
                cur.execute("INSERT INTO stream_parts VALUES(?,?)",
                            (seq, data[i:i + _STREAM_CHUNK]))
            self._parts_dirty = False
        elif len(self.stream) > self._persisted_len:
            seq = cur.execute("SELECT COALESCE(MAX(seq),0) FROM stream_parts").fetchone()[0]
            cur.execute("INSERT INTO stream_parts VALUES(?,?)",
                        (seq + 1, bytes(self.stream[self._persisted_len:])))
        self._persisted_len = len(self.stream)

    def _load_state(self):
        m = {k: pickle.loads(v) for k, v in self.db.execute("SELECT k,v FROM meta")}
        for k in _META_KEYS:
            setattr(self, k, m[k])
        self.usable = self.page_size - self.reserved
        self.stream = bytearray()
        for (part,) in self.db.execute("SELECT data FROM stream_parts ORDER BY seq"):
            self.stream += part
        self._persisted_len = len(self.stream)
        self._parts_dirty = False
        self.npending = self.db.execute("SELECT COUNT(*) FROM pending").fetchone()[0]
        fsz = _FRAME_HDR + self.page_size
        self.parse_pos = self.committed_len + self.npending * fsz
        if self.npending:
            self.run1, self.run2 = self.db.execute(
                "SELECT ck1,ck2 FROM pending ORDER BY no DESC LIMIT 1").fetchone()
        else:
            self.run1, self.run2 = self.ck1, self.ck2

    # ------------------------------------------------------------------ 对外接口

    def feed(self, offset, data):
        """接收 WAL 字节。返回本次调用新完成的提交的变更列表。"""
        offset = int(offset)
        data = bytes(data)
        if offset < 0:
            raise CorruptError("负偏移")
        h = hashlib.sha256(data).digest()
        cur = self.db.cursor()
        cur.execute("BEGIN IMMEDIATE")
        try:
            r = cur.execute(
                "SELECT result FROM receipts WHERE off=? AND len=? AND hash=?",
                (offset, len(data), h)).fetchone()
            if r is not None:                       # 重试同调用：返回原结果
                cur.execute("COMMIT")
                return pickle.loads(r[0])
            result = self._feed(cur, offset, data)
            cur.execute("INSERT OR REPLACE INTO receipts VALUES(?,?,?,?)",
                        (offset, len(data), h, pickle.dumps(result)))
            self._persist_meta(cur)
            cur.execute("COMMIT")
            return result
        except Exception:
            if self.db.in_transaction:
                cur.execute("ROLLBACK")
            self._load_state()                      # 内存状态与库内状态重新对齐
            raise

    def read(self, table):
        """返回表当前（已提交）内容：[[键, 整行], ...]，按键序。"""
        root, _, _ = self.tables[table]
        cur = self.db.cursor()
        src = _Src(cur)
        out = []

        def walk(pg):
            img = src.get(pg)
            info = _btree_info(img, pg)
            if info is None:
                raise CorruptError("页 %d 非 b-tree 页" % pg)
            tspec = self.tables[table]
            rows = []
            if info["interior"] and tspec[2] is None:
                pass                                # rowid 表内页不承载行
            else:
                rows = _extract(img, pg, tspec, self.usable, self.encoding, src)
            if info["interior"]:
                for i, c in enumerate(info["children"]):
                    walk(c)
                    if i < len(rows):               # 内页单元行按键序穿插
                        out.append([rows[i][0], rows[i][1]])
            else:
                for key, row, _ in rows:
                    out.append([key, row])

        walk(root)
        return out

    def close(self):
        if self.db is not None:
            try:
                self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            except Exception:
                pass
            self.db.close()
            self.db = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # ------------------------------------------------------------------ 流处理

    def _feed(self, cur, offset, data):
        while True:
            valid = len(self.stream)
            if offset > valid:
                # 跳段：当前未提交事务整次回滚
                self._rollback_txn(cur)
                if offset > len(self.stream):
                    return []                       # 仍不连续：等待从续传点重发
                continue
            if offset < valid:
                n = min(len(data), valid - offset)
                if data[:n] != self.stream[offset:offset + n]:
                    # 旧偏移异请求：未提交事务整次回滚
                    i = _first_diff(data[:n], self.stream[offset:offset + n])
                    if offset + i < self.committed_len:
                        raise CorruptError("已提交 WAL 字节在 %d 处冲突" % (offset + i))
                    self._rollback_txn(cur)
                    continue
                if len(data) <= valid - offset:
                    return []                       # 纯重传（无回执）：无新内容
                data = data[valid - offset:]
                offset = valid
                continue
            # offset == valid：追加并尽量推进
            if data:
                self.stream += data
            return self._process_stream(cur)

    def _process_stream(self, cur):
        out = []
        if not self.header_seen:
            if len(self.stream) < _WAL_HDR:
                return out                          # 头未齐：等待续传
            hdr = bytes(self.stream[:_WAL_HDR])
            magic, version, pgsz, _ckpt, s1, s2, c1, c2 = struct.unpack(">8I", hdr)
            if magic == _WAL_MAGIC_LE:
                self.be = False
            elif magic == _WAL_MAGIC_BE:
                self.be = True
            else:
                raise CorruptError("WAL 魔数非法")
            if version != _WAL_VERSION:
                raise CorruptError("WAL 版本非法 %d" % version)
            if pgsz == 1:
                pgsz = 65536
            if pgsz != self.page_size:
                raise CorruptError("WAL 页尺寸与基库不符")
            if _wal_cksum(hdr[:24], 0, 0, self.be) != (c1, c2):
                raise CorruptError("WAL 头校验和错误")
            self.salt1, self.salt2 = s1, s2
            self.ck1, self.ck2 = c1, c2
            self.run1, self.run2 = c1, c2
            self.header_seen = 1
            self.committed_len = _WAL_HDR
            self.parse_pos = _WAL_HDR
        fsz = _FRAME_HDR + self.page_size
        while self.parse_pos + fsz <= len(self.stream):
            off = self.parse_pos
            fhdr = bytes(self.stream[off:off + _FRAME_HDR])
            pgno, csize, s1, s2, c1, c2 = struct.unpack(">6I", fhdr)
            if (s1, s2) != (self.salt1, self.salt2):
                self._rollback_txn(cur)             # 异代帧：整次回滚
                return out
            body = bytes(self.stream[off + _FRAME_HDR:off + fsz])
            e1, e2 = _wal_cksum(fhdr[:8] + body, self.run1, self.run2, self.be)
            if (e1, e2) != (c1, c2):
                self._rollback_txn(cur)             # 坏校验：整次回滚
                return out
            self.run1, self.run2 = e1, e2
            fno = self.committed_fno + self.npending + 1
            cur.execute("INSERT INTO pending VALUES(?,?,?,?,?,?)",
                        (fno, pgno, csize, c1, c2, body))
            self.npending += 1
            self.parse_pos += fsz
            if csize:                               # 提交帧：应用整个事务
                frames = [(r[0], r[1], r[2]) for r in cur.execute(
                    "SELECT no,pgno,data FROM pending ORDER BY no")]
                out.extend(self._apply_commit(cur, frames, csize, fno))
                cur.execute("DELETE FROM pending")
                self.npending = 0
                self.committed_len = self.parse_pos
                self.committed_fno = fno
                self.ck1, self.ck2 = e1, e2
                self.db_size = csize
                self.commit_seq += 1
        return out

    def _rollback_txn(self, cur):
        """整次回滚当前未提交事务：丢弃未提交页与续传缓冲，回到最近提交点。"""
        cur.execute("DELETE FROM pending")
        self.npending = 0
        del self.stream[self.committed_len:]
        self.parse_pos = self.committed_len
        self.run1, self.run2 = self.ck1, self.ck2
        self._parts_dirty = True
        cur.execute("DELETE FROM receipts WHERE off + len > ?", (self.committed_len,))

    # ------------------------------------------------------------------ 提交应用

    def _apply_commit(self, cur, frames, new_db_size, commit_fno):
        # 同页多帧取最后（每提交净变化）
        changed_all = {}
        for _fno, pgno, data in frames:
            changed_all[pgno] = data
        dead = {p for p in changed_all if p > new_db_size}   # 提交截短的死帧
        changed = {p: d for p, d in changed_all.items() if p <= new_db_size}

        old_pm = {}
        for p in changed:
            r = cur.execute("SELECT tbl,role,parent FROM pmap WHERE no=?", (p,)).fetchone()
            if r:
                old_pm[p] = (r[0], r[1], r[2])

        new_bt = {}
        for p, img in changed.items():
            info = _btree_info(img, p)
            if info is not None:
                new_bt[p] = info

        # ---- 归属判定：根页 / 未变更父页沿用 -> 变更内页 BFS（依赖闭包）
        owner, parent_of = {}, {}
        dq = deque()
        for p in changed:
            t = self.root_of.get(p)
            if t is not None:
                owner[p] = t
                parent_of[p] = None
                dq.append(p)
        for p in new_bt:
            if p not in owner:
                om = old_pm.get(p)
                if (om and om[1] in ("i", "l")
                        and (om[2] is None
                             or (om[2] not in changed_all and om[2] <= new_db_size))):
                    owner[p] = om[0]                # 父页未变：沿用旧归属
                    parent_of[p] = om[2]
                    dq.append(p)                    # 继续向下传播
        while dq:
            x = dq.popleft()
            info = new_bt.get(x)
            if info and info["interior"]:
                for c in info["children"]:
                    if c in new_bt and c not in owner:
                        owner[c] = owner[x]
                        parent_of[c] = x
                        dq.append(c)
        claimed = set()
        for info in new_bt.values():
            if info["interior"]:
                claimed.update(info["children"])
        for p in new_bt:
            if p not in owner:
                owner[p] = None                     # 游离 / 复用出跟踪集
                parent_of[p] = None

        # ---- 被丢弃的未变更页：父页重写后不再被引用、提交截短、祖先被丢弃的闭包
        dropped = {}
        for p in dead:
            r = cur.execute("SELECT tbl,role FROM pmap WHERE no=?", (p,)).fetchone()
            if r:
                dropped[p] = (r[0], r[1])
        if changed_all:
            keys = list(changed_all)
            for i in range(0, len(keys), 500):          # 分批避免变量数上限
                batch = keys[i:i + 500]
                qs = ",".join("?" * len(batch))
                for p, t, role in cur.execute(
                        "SELECT no,tbl,role FROM pmap WHERE parent IN (%s)" % qs,
                        batch):
                    if p not in changed_all and p not in claimed and p not in dropped:
                        dropped[p] = (t, role)
        for p, t, role in cur.execute(
                "SELECT no,tbl,role FROM pmap WHERE no > ?", (new_db_size,)):
            if p not in changed_all and p not in dropped:
                dropped[p] = (t, role)
        old_src = _Src(cur)
        wq = deque([p for p, (_, r) in dropped.items() if r == "i"])
        while wq:                                   # 内点被丢弃 => 整棵子树被丢弃
            x = wq.popleft()
            info = _btree_info(old_src.get(x), x)
            if not info or not info["interior"]:
                continue
            for c in info["children"]:
                if c in changed_all or c in claimed or c in dropped:
                    continue
                r = cur.execute("SELECT tbl,role FROM pmap WHERE no=?", (c,)).fetchone()
                if not r:
                    continue
                dropped[c] = (r[0], r[1])
                if r[1] == "i":
                    wq.append(c)

        # ---- 行提取：旧侧（变更/被丢弃叶的旧像）与新侧（变更叶的新像）
        new_src = _Src(cur, changed)
        old_rows, new_rows = {}, {}
        ovf_del, ovf_add = set(), {}

        def scan(img, pgno, tname, src, sink, add):
            for key, row, ovfl in _extract(img, pgno, self.tables[tname],
                                           self.usable, self.encoding, src):
                sink.setdefault(tname, {})[_mapkey(key)] = (key, row)
                if add:
                    for o in ovfl:
                        ovf_add[o] = (tname, key, pgno)
                else:
                    ovf_del.update(ovfl)

        for p in changed:
            om = old_pm.get(p)
            if om and _carries_rows(self.tables[om[0]], om[1]):
                img = old_src.get(p)
                if img is not None:
                    scan(img, p, om[0], old_src, old_rows, False)
            t = owner.get(p)
            info = new_bt.get(p)
            if (t is not None and info is not None
                    and _carries_rows(self.tables[t], "i" if info["interior"] else "l")):
                scan(changed[p], p, t, new_src, new_rows, True)
        for p, (t, role) in dropped.items():
            if _carries_rows(self.tables[t], role):
                img = old_src.get(p)
                if img is not None:
                    scan(img, p, t, old_src, old_rows, False)

        # ---- 仅溢出页变更（属主叶未变）：按属主键重读该行的旧/新溢出链
        for p in changed:
            if p in new_bt:
                ovf_del.add(p)                      # 溢出页被复用为 b-tree 页
                continue
            r = cur.execute("SELECT tbl,key,leaf FROM ovf WHERE no=?", (p,)).fetchone()
            if not r:
                continue
            tname, kblob, leaf = r
            if leaf in changed_all or leaf in dropped:
                continue                            # 已由叶级 diff 处理
            key = pickle.loads(kblob)
            tspec = self.tables[tname]
            img = old_src.get(leaf)                 # 叶未变：新旧像相同
            old_row = new_row = None
            mk = _mapkey(key)
            for k2, row2, _ in _extract(img, leaf, tspec, self.usable,
                                        self.encoding, old_src):
                if _mapkey(k2) == mk:
                    old_row = row2
                    break
            for k2, row2, _ in _extract(img, leaf, tspec, self.usable,
                                        self.encoding, new_src):
                if _mapkey(k2) == mk:
                    new_row = row2
                    break
            if old_row is None or new_row is None:
                raise CorruptError("溢出属主行缺失")
            if not _rows_equal(old_row, new_row):
                old_rows.setdefault(tname, {})[mk] = (key, old_row)
                new_rows.setdefault(tname, {})[mk] = (key, new_row)

        # ---- 每提交每键净变化
        changes = []
        for tname in set(old_rows) | set(new_rows):
            om = old_rows.get(tname, {})
            nm = new_rows.get(tname, {})
            for mk in set(om) | set(nm):
                o = om.get(mk)
                n = nm.get(mk)
                if o is not None and n is not None:
                    if _rows_equal(o[1], n[1]):
                        continue
                    changes.append([commit_fno, tname, o[0], o[1], n[1]])
                elif o is not None:
                    changes.append([commit_fno, tname, o[0], o[1], None])
                else:
                    changes.append([commit_fno, tname, n[0], None, n[1]])
        changes.sort(key=lambda c: (c[1], _keysort(c[2])))

        # ---- 原子持久化：页像、页映射、溢出属主
        for p, img in changed.items():
            cur.execute("INSERT OR REPLACE INTO pages VALUES(?,?)", (p, img))
        cur.execute("DELETE FROM pages WHERE no > ?", (new_db_size,))
        for p in changed:
            t = owner.get(p)
            info = new_bt.get(p)
            if t is not None and info is not None:
                cur.execute("INSERT OR REPLACE INTO pmap VALUES(?,?,?,?)",
                            (p, t, "i" if info["interior"] else "l", parent_of.get(p)))
            else:
                cur.execute("DELETE FROM pmap WHERE no=?", (p,))
        for p in dropped:
            cur.execute("DELETE FROM pmap WHERE no=?", (p,))
        for o in ovf_del:
            cur.execute("DELETE FROM ovf WHERE no=?", (o,))
        cur.execute("DELETE FROM ovf WHERE no > ?", (new_db_size,))
        for o, (tname, key, leaf) in ovf_add.items():
            cur.execute("INSERT OR REPLACE INTO ovf VALUES(?,?,?,?)",
                        (o, tname, pickle.dumps(key), leaf))
        if 1 in changed:                            # 库头页可能带来保留区/编码变化
            img1 = changed[1]
            self.reserved = img1[20]
            self.usable = self.page_size - self.reserved
            enc = _u32(img1, 56)
            if enc in (1, 2, 3):
                self.encoding = {1: "utf-8", 2: "utf-16le", 3: "utf-16be"}[enc]
        return changes
