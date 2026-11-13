#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
walcdc 真实核账（verify）。

用 sqlite3 造出真实的多页尺寸 / 多编码 / 多值类型数据库与 WAL，
把 WAL 以随机分片喂给引擎（夹杂重试、重开、回滚异常），
逐提交比对引擎输出与真值快照差异，并用 read() 对账终态。

覆盖：页拆并、页复用、仅溢出页变更（增量 blob I/O）、提交截短
（auto_vacuum 截断）、WITHOUT ROWID 复合主键、rowid 表、
UTF-8 / UTF-16BE、NULL/整数/浮点/文本/二进制全值类型、
[7]->[8]->[7] 提交内净变化抵消、未跟踪表提交、
重试同调用、旧偏移异请求、跳段、坏校验、重开续传。
"""

import os
import random
import shutil
import sqlite3
import struct
import sys
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import walcdc
import walcdc_fixtures as fx

WORKDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "work_verify")


class CheckError(Exception):
    pass


def check(cond, msg):
    if not cond:
        raise CheckError(msg)


def feed_all(eng, wal, rnd, got, dup_rate=0.3):
    """随机分片喂完全部 WAL；随机重试同调用；收集 {提交帧号: [变更...]}。"""
    off = 0
    while off < len(wal):
        n = rnd.choice([1, 3, 7, 64, 500, 1500, 8192, len(wal)])
        n = min(n, len(wal) - off)
        chunk = wal[off:off + n]
        res = eng.feed(off, chunk)
        for c in res:
            got.setdefault(c[0], []).append(c)
        if rnd.random() < dup_rate:
            res2 = eng.feed(off, chunk)          # 重试同调用 -> 原结果
            check(res2 == res, "重试同调用结果不一致")
        off += n


def reconcile(eng, fixture, got, tag):
    """逐提交比对 + 终态 read() 对账。"""
    tables = fixture["tables"]
    exp = fx.expected_changes(fixture["snaps"], fixture["commit_fnos"], tables)
    for fno in fixture["commit_fnos"]:
        e = fx.canon_changes(exp.get(fno, []))
        g = fx.canon_changes(got.get(fno, []))
        check(e == g, "%s 提交帧 %d 变更不一致：\n期望 %s\n实际 %s" % (tag, fno, e, g))
    for name in tables:
        r = fx.canon_rows(eng.read(name))
        s = fx.canon_snap(fixture["snaps"][-1][name])
        check(r == s, "%s 表 %s 终态 read() 对账失败" % (tag, name))


def check_initial_read(eng, fixture, tag):
    for name in fixture["tables"]:
        r = fx.canon_rows(eng.read(name))
        s = fx.canon_snap(fixture["snaps"][0][name])
        check(r == s, "%s 表 %s 基库态 read() 对账失败" % (tag, name))


# ---------------------------------------------------------------- 场景 A
# 512 字节页 / UTF-8：页拆并、溢出、仅溢出页变更、类型全谱、净变化抵消

def scenario_a():
    tag = "A(512页/UTF-8)"
    dirp = os.path.join(WORKDIR, "a")
    ddl = [
        "CREATE TABLE t1(a INTEGER, b TEXT, c REAL, d BLOB)",
        "CREATE TABLE t2(k1 TEXT, k2 INTEGER, v1 TEXT, v2 BLOB,"
        " PRIMARY KEY(k1, k2)) WITHOUT ROWID",
        "CREATE TABLE t3(x, y, z, PRIMARY KEY(y)) WITHOUT ROWID",
        "CREATE TABLE aux(q)",
    ]
    initial = [
        "INSERT INTO t1 VALUES(100,'init',1.5,X'00')",
        "INSERT INTO t2 VALUES('seed',1,'v',X'0102')",
        "INSERT INTO aux VALUES(0)",
    ]
    rec80 = ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<80)")

    def txn_blob(db):
        # 增量 blob I/O：只脏溢出页（x=3 行的 d 是 zeroblob(4000)）
        rid = db.execute("SELECT rowid FROM t1 WHERE length(d)=4000").fetchone()[0]
        b = db.blobopen("t1", "d", rid)
        b.seek(3900)
        b.write(bytes(range(100)))
        b.close()

    txns = [
        [rec80 + " INSERT INTO t1(a,b,c,d) SELECT x,'txt'||x,x+0.5,"
         " CASE WHEN x=3 THEN zeroblob(4000) ELSE randomblob(30) END FROM c"],
        [rec80.replace("80", "30") +
         " INSERT INTO t2 SELECT 'k'||x,x,'v'||x,"
         " CASE WHEN x%5=0 THEN randomblob(2000) ELSE randomblob(40) END FROM c"],
        ["UPDATE t1 SET a=NULL,b=X'0102',c=7.25 WHERE rowid BETWEEN 1 AND 5",
         "UPDATE t1 SET d=randomblob(2500) WHERE rowid BETWEEN 6 AND 10",
         "UPDATE t2 SET v2=randomblob(2000) WHERE k2%5=0",   # 等长溢出重写
         "UPDATE t2 SET v1=NULL WHERE k2%3=0"],
        [txn_blob],                                          # 仅溢出页变更
        ["DELETE FROM t1 WHERE rowid BETWEEN 20 AND 60"],    # 合并
        ["INSERT INTO t3 SELECT x,'y'||x,x*1.5 FROM"
         " (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<20)"
         "  SELECT x FROM c)",
         "UPDATE t3 SET z=NULL WHERE y='y3'",
         "DELETE FROM t3 WHERE y='y7'"],
        ["INSERT INTO aux VALUES(1)", "DELETE FROM aux"],    # 未跟踪表
        ["UPDATE t1 SET a=8 WHERE rowid=70",                 # 净变化抵消
         "UPDATE t1 SET a=70 WHERE rowid=70",                # 70->8->70：不输出
         "UPDATE t1 SET a=8 WHERE rowid=71",
         "UPDATE t1 SET a=9 WHERE rowid=71",                 # 71->8->9：净 71->9
         "INSERT INTO t1(rowid,a) VALUES(500,7)",
         "UPDATE t1 SET a=8 WHERE rowid=500"],               # 插入后改：净插入 a=8
        ["INSERT INTO t1(a) VALUES(1),(2),(3)",
         "DELETE FROM t2 WHERE k2 BETWEEN 10 AND 15",
         "UPDATE t3 SET x=x+100 WHERE y='y10'",
         "INSERT INTO aux VALUES(2)"],
        ["DELETE FROM t1", "DELETE FROM t2", "DELETE FROM t3"],
    ]
    tracked = {"t1": None, "t2": [0, 1], "t3": [1]}
    f = fx.build_fixture(dirp, 512, ddl, initial, txns, tracked)
    state = os.path.join(dirp, "state.db")
    eng = walcdc.Engine(state, f["base"], f["tables"])
    check_initial_read(eng, f, tag)
    got = {}
    feed_all(eng, f["wal"], random.Random(11), got)
    reconcile(eng, f, got, tag)
    eng.close()


# ---------------------------------------------------------------- 场景 B
# 65536 字节页 / UTF-16BE：大页、非 UTF-8 文本、主键乱序的 WITHOUT ROWID

def scenario_b():
    tag = "B(64K页/UTF-16BE)"
    dirp = os.path.join(WORKDIR, "b")
    ddl = [
        "CREATE TABLE tt(a TEXT, b INTEGER, c BLOB)",
        "CREATE TABLE ww(p TEXT, q TEXT, r INTEGER, PRIMARY KEY(q, p)) WITHOUT ROWID",
    ]
    initial = ["INSERT INTO tt VALUES('种子',1,X'FF')"]
    han = "'中文測試𠀀'"
    txns = [
        ["WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<200)"
         " INSERT INTO tt SELECT '行'||x||'-'||%s, x, randomblob(x%%97) FROM c" % han],
        ["WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<150)"
         " INSERT INTO ww SELECT 'p'||x, 'q'||(x%30), x FROM c"],
        ["INSERT INTO tt VALUES('大对象',-1,randomblob(70000))",   # 64K 页也溢出
         "UPDATE tt SET a='改'||a WHERE b BETWEEN 10 AND 40",
         "UPDATE ww SET r=r+1000 WHERE q='q3'"],
        ["DELETE FROM tt WHERE b BETWEEN 50 AND 150",
         "DELETE FROM ww WHERE q='q7'"],
        ["UPDATE tt SET c=randomblob(80000) WHERE b=-1",           # 溢出链整换
         "DELETE FROM tt WHERE b=-1"],
    ]
    tracked = {"tt": None, "ww": [1, 0]}
    f = fx.build_fixture(dirp, 65536, ddl, initial, txns, tracked,
                         encoding="UTF-16be")
    state = os.path.join(dirp, "state.db")
    eng = walcdc.Engine(state, f["base"], f["tables"])
    check_initial_read(eng, f, tag)
    got = {}
    feed_all(eng, f["wal"], random.Random(22), got)
    reconcile(eng, f, got, tag)
    eng.close()


# ---------------------------------------------------------------- 场景 C
# 1024 字节页 / auto_vacuum=FULL：提交截短、页复用

def scenario_c():
    tag = "C(1K页/截短)"
    dirp = os.path.join(WORKDIR, "c")
    ddl = [
        "CREATE TABLE big(a BLOB, b TEXT)",
        "CREATE TABLE sm(x)",
    ]
    initial = ["INSERT INTO sm VALUES(1)"]
    ins400 = ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<400)"
              " INSERT INTO big SELECT randomblob(300),'r'||x FROM c")
    txns = [
        [ins400],                                   # 库膨胀
        ["DELETE FROM big", "UPDATE sm SET x=2"],   # 提交截短 + 跟踪表变更
        ["WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<50)"
         " INSERT INTO big SELECT randomblob(300),'n'||x FROM c"],  # 页复用
        ["DELETE FROM big WHERE rowid%2=0", "UPDATE sm SET x=3"],
        ["INSERT INTO big VALUES(zeroblob(5000),'ovf')",
         "UPDATE big SET a=randomblob(5000) WHERE b='ovf'"],
    ]
    tracked = {"big": None, "sm": None}
    f = fx.build_fixture(dirp, 1024, ddl, initial, txns, tracked, auto_vacuum=1)
    # 确认确实发生了提交截短（第 2 个事务的提交帧 db_size 应变小）
    state = os.path.join(dirp, "state.db")
    eng = walcdc.Engine(state, f["base"], f["tables"])
    check_initial_read(eng, f, tag)
    got = {}
    feed_all(eng, f["wal"], random.Random(33), got)
    reconcile(eng, f, got, tag)
    check(eng.db_size < 400, "%s 未观察到提交截短（db_size=%d）" % (tag, eng.db_size))
    eng.close()


# ---------------------------------------------------------------- 场景 D
# 异常路径：重试、旧偏移异请求、跳段、坏校验、重开续传、响应丢失

def scenario_d():
    tag = "D(异常路径)"
    dirp = os.path.join(WORKDIR, "d")
    ddl = [
        "CREATE TABLE d1(a INTEGER, b TEXT)",
        "CREATE TABLE d2(k TEXT, v BLOB, PRIMARY KEY(k)) WITHOUT ROWID",
    ]
    initial = ["INSERT INTO d1 VALUES(1,'一')"]
    txns = [
        ["INSERT INTO d1 VALUES(2,'二')", "INSERT INTO d2 VALUES('a',randomblob(100))"],
        ["UPDATE d1 SET b='贰' WHERE a=2", "INSERT INTO d2 VALUES('b',randomblob(3000))"],
        ["DELETE FROM d1 WHERE a=1", "UPDATE d2 SET v=randomblob(3000) WHERE k='b'"],
        ["INSERT INTO d1 VALUES(3,'三')", "DELETE FROM d2 WHERE k='a'"],
        ["UPDATE d1 SET a=4 WHERE a=3", "INSERT INTO d2 VALUES('c',X'00')"],
    ]
    tracked = {"d1": None, "d2": [0]}
    f = fx.build_fixture(dirp, 4096, ddl, initial, txns, tracked)
    state = os.path.join(dirp, "state.db")
    wal = f["wal"]
    framesz = f["framesz"]
    # 各事务在 WAL 中的字节区间
    bounds = [0]
    for fno in f["commit_fnos"]:
        bounds.append(32 + fno * framesz)

    eng = walcdc.Engine(state, f["base"], f["tables"])
    check_initial_read(eng, f, tag)
    got = {}

    def collect(res):
        for c in res:
            got.setdefault(c[0], []).append(c)

    # 事务 1：整段喂入
    collect(eng.feed(bounds[0], wal[bounds[0]:bounds[1]]))
    check(len(eng.stream) == bounds[1], "%s 事务1后流长不符" % tag)

    # 事务 2：先喂前半（未提交），重试同调用；再“旧偏移异请求”触发整次回滚
    seg = wal[bounds[1]:bounds[2]]
    half = len(seg) // 2
    r1 = eng.feed(bounds[1], seg[:half])
    check(r1 == [], "%s 未提交帧不应有输出" % tag)
    check(eng.feed(bounds[1], seg[:half]) == r1, "%s 重试同调用结果不一致" % tag)
    bad = bytearray(seg[:half])
    bad[10] ^= 0xFF                                   # 破坏帧头盐值
    r2 = eng.feed(bounds[1], bytes(bad))
    check(r2 == [], "%s 旧偏移异请求应回滚并返回空" % tag)
    check(len(eng.stream) == bounds[1], "%s 回滚后流长应回到提交点" % tag)
    r3 = eng.feed(bounds[1], seg)                     # 从事务起点重发
    collect(r3)
    check(eng.feed(bounds[1], seg) == r3, "%s 重发后重试结果不一致" % tag)

    # 跳段：空洞调用 -> 回滚并等待重发
    r4 = eng.feed(bounds[2] + 64, b"\x00" * 64)
    check(r4 == [], "%s 跳段应返回空" % tag)
    check(len(eng.stream) == bounds[2], "%s 跳段不应推进流" % tag)

    # 事务 3：分片喂入，中途“杀进程”式重开，重放最后调用（响应丢失）
    seg = wal[bounds[2]:bounds[3]]
    third = len(seg) // 3
    collect(eng.feed(bounds[2], seg[:third]))
    last_off, last_data = bounds[2] + third, seg[third:2 * third]
    r5 = eng.feed(last_off, last_data)
    eng.close()                                       # 模拟重开
    eng = walcdc.Engine(state, f["base"], f["tables"])
    r6 = eng.feed(last_off, last_data)                # 响应丢失重放
    check(r6 == r5, "%s 重开后重放结果不一致" % tag)
    collect(r5)
    collect(eng.feed(bounds[2] + 2 * third, seg[2 * third:]))

    # 坏校验：篡改事务 4 首帧数据体 -> 整次回滚；再重发正确字节
    seg = wal[bounds[3]:bounds[4]]
    bad = bytearray(seg)
    bad[24 + 100] ^= 0xFF
    r7 = eng.feed(bounds[3], bytes(bad))
    check(r7 == [], "%s 坏校验应回滚并返回空" % tag)
    check(len(eng.stream) == bounds[3], "%s 坏校验回滚后流长应回到提交点" % tag)
    collect(eng.feed(bounds[3], seg))

    # 事务 5：一次性喂入剩余全部
    collect(eng.feed(bounds[4], wal[bounds[4]:]))

    reconcile(eng, f, got, tag)
    eng.close()


# ---------------------------------------------------------------- 场景 E
# 真实回滚重写：未提交事务溢出到 WAL 后 ROLLBACK，后续事务覆盖其字节，
# 采集端看到“旧的未提交帧被改写”——引擎必须整次回滚未提交事务再续传。

def _wal_valid_length(wal, page_size):
    """解析 WAL 字节流，返回 (有效长度, 提交帧号, 提交数)（校验和验证）。"""
    magic, _ver, _ps, _ck, s1, s2, c1, c2 = struct.unpack(">8I", wal[:32])
    be = magic == 0x377F0683
    check(_wal_ok(wal[:24], c1, c2, be), "WAL 头校验和错误")
    fsz = 24 + page_size
    off, run_ck, valid, commit_fno, ncommit = 32, (c1, c2), 32, 0, 0
    fno = 0
    while off + fsz <= len(wal):
        fno += 1
        pgno, csize, fs1, fs2, fc1, fc2 = struct.unpack(">6I", wal[off:off + 24])
        if (fs1, fs2) != (s1, s2):
            break
        run_ck = walcdc._wal_cksum(wal[off:off + 8] + wal[off + 24:off + fsz],
                                   run_ck[0], run_ck[1], be)
        if run_ck != (fc1, fc2):
            break
        off += fsz
        if csize:
            valid, commit_fno, ncommit = off, fno, ncommit + 1
    return valid, commit_fno, ncommit


def _wal_ok(hdr24, c1, c2, be):
    return walcdc._wal_cksum(hdr24, 0, 0, be) == (c1, c2)


def scenario_e():
    tag = "E(回滚重写)"
    dirp = os.path.join(WORKDIR, "e")
    os.makedirs(dirp)
    dbp = os.path.join(dirp, "src.db")
    db = sqlite3.connect(dbp, isolation_level=None)
    db.execute("PRAGMA page_size=512")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_autocheckpoint=0")
    db.execute("CREATE TABLE t(a INTEGER, b BLOB)")
    db.execute("CREATE TABLE w(k TEXT, v INTEGER, PRIMARY KEY(k)) WITHOUT ROWID")
    db.execute("INSERT INTO t VALUES(1, X'01')")
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    tables = {}
    for name, pk in (("t", None), ("w", [0])):
        root = db.execute("SELECT rootpage FROM sqlite_master WHERE name=?",
                          (name,)).fetchone()[0]
        ncols = len(db.execute('PRAGMA table_info("%s")' % name).fetchall())
        tables[name] = [root, ncols, pk]
    base = os.path.join(dirp, "base.db")
    shutil.copyfile(dbp, base)
    snaps = [fx.snap_db(db, tables)]
    commit_fnos = []
    walp = dbp + "-wal"

    def wal_bytes():
        with open(walp, "rb") as f:
            return f.read()

    # 未提交大事务：溢出到 WAL 后回滚
    db.execute("PRAGMA cache_size=5")
    db.execute("BEGIN")
    db.execute("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<300)"
               " INSERT INTO t SELECT x, randomblob(200) FROM c")
    stream1 = wal_bytes()                       # 含未提交帧的字节流
    check(len(stream1) > 32 + 24 + 512, "%s 回滚事务未溢出到 WAL" % tag)
    db.execute("ROLLBACK")
    check(wal_bytes() == stream1, "%s 回滚后 WAL 字节不应变化" % tag)

    # 后续事务覆盖未提交帧区域（两个独立提交）
    db.execute("INSERT INTO t VALUES(2, randomblob(100))")
    snaps.append(fx.snap_db(db, tables))
    db.execute("INSERT INTO w VALUES('x', 1)")
    snaps.append(fx.snap_db(db, tables))
    stream2 = wal_bytes()[:_wal_valid_length(wal_bytes(), 512)[0]]
    # 再一个正常提交
    db.execute("INSERT INTO t VALUES(3, randomblob(600))")
    snaps.append(fx.snap_db(db, tables))
    stream3 = wal_bytes()[:_wal_valid_length(wal_bytes(), 512)[0]]
    db.close()

    # 各提交帧号：stream2 内含提交 1、2；stream3 追加提交 3
    fsz = 24 + 512
    commit_fnos = []
    off = 32
    fno = 0
    while off + fsz <= len(stream2):
        fno += 1
        if struct.unpack(">I", stream2[off + 4:off + 8])[0]:
            commit_fnos.append(fno)
        off += fsz
    commit_fnos.append((len(stream3) - 32) // fsz)

    # 引擎：先喂含未提交帧的 stream1，再喂被改写的 stream2/3
    state = os.path.join(dirp, "state.db")
    eng = walcdc.Engine(state, base, tables)
    got = {}
    off = 0
    rnd = random.Random(44)
    while off < len(stream1):                    # 未提交帧：全部 pending
        n = min(rnd.choice([17, 300, 2000]), len(stream1) - off)
        r = eng.feed(off, stream1[off:off + n])
        check(r == [], "%s 未提交帧不应有输出" % tag)
        off += n
    r = eng.feed(32, stream2[32:])               # 旧偏移异请求：回滚 + 接受
    for c in r:
        got.setdefault(c[0], []).append(c)
    check(len(eng.stream) == len(stream2), "%s 重写后流长不符" % tag)
    r = eng.feed(len(stream2), stream3[len(stream2):])
    for c in r:
        got.setdefault(c[0], []).append(c)
    # 核账
    exp = fx.expected_changes(snaps, commit_fnos, tables)
    for fno in commit_fnos:
        e = fx.canon_changes(exp.get(fno, []))
        g = fx.canon_changes(got.get(fno, []))
        check(e == g, "%s 提交帧 %d 不一致：\n期望 %s\n实际 %s" % (tag, fno, e, g))
    for name in tables:
        check(fx.canon_rows(eng.read(name)) == fx.canon_snap(snaps[-1][name]),
              "%s 表 %s 终态对账失败" % (tag, name))
    eng.close()


# ---------------------------------------------------------------- 场景 F
# 小端校验和 WAL + 保留字节页 + 仅页 1 提交（PRAGMA user_version）

def _precreate_reserved(dbp, page_size, reserve):
    """用 C 级 file_control 预建带保留字节的库（Python 层无此接口）。"""
    import ctypes
    lib = ctypes.CDLL("libsqlite3.so.0")
    h = ctypes.c_void_p()
    check(lib.sqlite3_open(dbp.encode(), ctypes.byref(h)) == 0, "sqlite3_open 失败")
    try:
        check(lib.sqlite3_exec(h, b"PRAGMA page_size=%d" % page_size,
                               None, None, None) == 0, "page_size 失败")
        n = ctypes.c_int(reserve)
        check(lib.sqlite3_file_control(h, b"main", 38, ctypes.byref(n)) == 0,
              "reserve_bytes 失败")                    # SQLITE_FCNTL_RESERVE_BYTES=38
        check(lib.sqlite3_exec(h, b"PRAGMA journal_mode=WAL", None, None, None) == 0,
              "journal_mode 失败")
    finally:
        lib.sqlite3_close(h)


def _wal_to_le(wal, page_size):
    """把 WAL 改写为小端校验和（魔数 0x377f0682），内容不变。"""
    hdr = bytearray(wal[:32])
    hdr[0:4] = struct.pack(">I", 0x377F0682)
    s1, s2 = walcdc._wal_cksum(bytes(hdr[:24]), 0, 0, False)
    hdr[24:32] = struct.pack(">2I", s1, s2)
    out = bytearray(hdr)
    fsz = 24 + page_size
    off = 32
    while off + fsz <= len(wal):
        fh = bytearray(wal[off:off + 24])
        body = wal[off + 24:off + fsz]
        s1, s2 = walcdc._wal_cksum(bytes(fh[:8]) + body, s1, s2, False)
        fh[16:24] = struct.pack(">2I", s1, s2)
        out += fh
        out += body
        off += fsz
    return bytes(out)


def scenario_f():
    tag = "F(小端校验/保留字节)"
    dirp = os.path.join(WORKDIR, "f")
    os.makedirs(dirp)
    dbp = os.path.join(dirp, "src.db")
    _precreate_reserved(dbp, 512, 32)
    ddl = [
        "CREATE TABLE r1(a INTEGER, b TEXT, c BLOB)",
        "CREATE TABLE w1(k TEXT, v BLOB, PRIMARY KEY(k)) WITHOUT ROWID",
    ]
    initial = ["INSERT INTO r1 VALUES(1,'一',randomblob(700))"]   # 480 可用字节下必溢出
    txns = [
        ["INSERT INTO r1 VALUES(2,'二',randomblob(900))",
         "INSERT INTO w1 VALUES('甲',randomblob(1200))"],
        ["PRAGMA user_version=7"],                   # 仅页 1 的提交
        ["UPDATE r1 SET c=randomblob(900) WHERE a=2",
         "UPDATE w1 SET v=randomblob(600) WHERE k='甲'",
         "INSERT INTO w1 VALUES('乙',X'00')"],
        ["DELETE FROM r1 WHERE a=1", "DELETE FROM w1 WHERE k='乙'"],
    ]
    tracked = {"r1": None, "w1": [0]}
    f = fx.build_fixture(dirp, 512, ddl, initial, txns, tracked, precreated=True)
    wal_le = _wal_to_le(f["wal"], 512)
    state = os.path.join(dirp, "state.db")
    eng = walcdc.Engine(state, f["base"], f["tables"])
    check_initial_read(eng, f, tag)
    got = {}
    feed_all(eng, wal_le, random.Random(55), got)
    reconcile(eng, f, got, tag)
    check(eng.reserved == 32 and eng.usable == 512 - 32, "%s 保留字节未生效" % tag)
    eng.close()


# ---------------------------------------------------------------- 场景 G
# 深层树：三级 b-tree 的拆分与整层合并（内点子树传递丢弃）

def scenario_g():
    tag = "G(深层树)"
    dirp = os.path.join(WORKDIR, "g")
    ddl = [
        "CREATE TABLE t1(a INTEGER, b TEXT)",
        "CREATE TABLE w1(k INTEGER, v TEXT, PRIMARY KEY(k)) WITHOUT ROWID",
    ]
    initial = []
    big1 = ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<3000)"
            " INSERT INTO t1 SELECT x,'r'||x FROM c")
    big2 = ("WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<2000)"
            " INSERT INTO w1 SELECT x,'v'||x FROM c")
    txns = [
        [big1, big2],                                # 一次性长出三级树
        ["DELETE FROM t1 WHERE a%3!=0", "DELETE FROM w1 WHERE k%3!=0"],
        ["DELETE FROM t1 WHERE a%3==0 AND a<2400",
         "DELETE FROM w1 WHERE k%3==0 AND k<1600"],  # 整层合并
        ["INSERT INTO t1 SELECT a+10000,'n'||a FROM t1",
         "INSERT INTO w1 SELECT k+10000,'n'||k FROM w1"],  # 复用再长
        ["DELETE FROM t1", "DELETE FROM w1"],        # 清空
    ]
    tracked = {"t1": None, "w1": [0]}
    f = fx.build_fixture(dirp, 512, ddl, initial, txns, tracked)
    state = os.path.join(dirp, "state.db")
    eng = walcdc.Engine(state, f["base"], f["tables"])
    got = {}
    feed_all(eng, f["wal"], random.Random(66), got)
    reconcile(eng, f, got, tag)
    eng.close()


# ---------------------------------------------------------------- 场景 H
# 随机压力：随机类型、随机事务、随机分片、随机重开，全程逐提交核账

def scenario_h():
    tag = "H(随机压力)"
    dirp = os.path.join(WORKDIR, "h")
    rnd = random.Random(int(os.environ.get("VERIFY_H_SEED", "2026")))
    ddl = [
        "CREATE TABLE r1(a, b, c)",
        "CREATE TABLE w1(p, q, r, PRIMARY KEY(q, p)) WITHOUT ROWID",
        "CREATE TABLE w2(k, v, PRIMARY KEY(k)) WITHOUT ROWID",
    ]
    tracked = {"r1": None, "w1": [1, 0], "w2": [0]}
    f = fx.build_fixture(dirp, 2048, ddl, [], [], tracked)
    dbp = os.path.join(dirp, "src.db")
    db = sqlite3.connect(dbp, isolation_level=None)
    db.execute("PRAGMA wal_autocheckpoint=0")
    tables = f["tables"]
    snaps = [fx.snap_db(db, tables)]
    walp = dbp + "-wal"
    wal = b""
    commit_fnos = []
    framesz = f["framesz"]
    live_r1, live_w1, live_w2 = set(), set(), set()
    rid = [0]

    def rval():
        return rnd.choice([None, rnd.randint(-2**40, 2**40),
                           rnd.random() * 1000, "s%d" % rnd.randint(0, 50),
                           "文%d" % rnd.randint(0, 9),
                           rnd.randbytes(rnd.choice([0, 5, 40, 500, 3000]))])

    for ti in range(40):
        sqls = []
        for _ in range(rnd.randint(1, 12)):
            op = rnd.random()
            t = rnd.choice(["r1", "w1", "w2"])
            if t == "r1":
                if op < 0.45 or not live_r1:
                    rid[0] += 1
                    sqls.append("INSERT INTO r1(rowid,a,b,c) VALUES(%d,%s,%s,%s)"
                                % (rid[0], fx.sqlval(rval()), fx.sqlval(rval()),
                                   fx.sqlval(rval())))
                    live_r1.add(rid[0])
                elif op < 0.8:
                    k = rnd.choice(list(live_r1))
                    sqls.append("UPDATE r1 SET a=%s,b=%s,c=%s WHERE rowid=%d"
                                % (fx.sqlval(rval()), fx.sqlval(rval()),
                                   fx.sqlval(rval()), k))
                else:
                    k = rnd.choice(list(live_r1))
                    sqls.append("DELETE FROM r1 WHERE rowid=%d" % k)
                    live_r1.discard(k)
            elif t == "w1":
                if op < 0.45 or not live_w1:
                    k = ("p%d" % rnd.randint(0, 400), rnd.randint(0, 400))
                    if k in live_w1:
                        continue
                    sqls.append("INSERT INTO w1 VALUES(%s,%s,%s)"
                                % (fx.sqlval(k[0]), fx.sqlval(k[1]), fx.sqlval(rval())))
                    live_w1.add(k)
                elif op < 0.8:
                    k = rnd.choice(list(live_w1))
                    sqls.append("UPDATE w1 SET r=%s WHERE p=%s AND q=%s"
                                % (fx.sqlval(rval()), fx.sqlval(k[0]), fx.sqlval(k[1])))
                else:
                    k = rnd.choice(list(live_w1))
                    sqls.append("DELETE FROM w1 WHERE p=%s AND q=%s"
                                % (fx.sqlval(k[0]), fx.sqlval(k[1])))
                    live_w1.discard(k)
            else:
                if op < 0.45 or not live_w2:
                    k = rnd.randint(0, 500)
                    if k in live_w2:
                        continue
                    sqls.append("INSERT INTO w2 VALUES(%d,%s)" % (k, fx.sqlval(rval())))
                    live_w2.add(k)
                elif op < 0.8:
                    k = rnd.choice(list(live_w2))
                    sqls.append("UPDATE w2 SET v=%s WHERE k=%d" % (fx.sqlval(rval()), k))
                else:
                    k = rnd.choice(list(live_w2))
                    sqls.append("DELETE FROM w2 WHERE k=%d" % k)
                    live_w2.discard(k)
        if not sqls:
            continue
        db.execute("BEGIN")
        for s in sqls:
            db.execute(s)
        db.execute("COMMIT")
        with open(walp, "rb") as fp:
            fp.seek(len(wal))
            wal += fp.read()
        commit_fnos.append((len(wal) - 32) // framesz)
        snaps.append(fx.snap_db(db, tables))
    db.close()

    state = os.path.join(dirp, "state.db")
    eng = walcdc.Engine(state, f["base"], tables)
    got = {}
    off = 0
    while off < len(wal):
        n = min(rnd.choice([1, 9, 100, 2048, 30000]), len(wal) - off)
        res = eng.feed(off, wal[off:off + n])
        for c in res:
            got.setdefault(c[0], []).append(c)
        if rnd.random() < 0.2:
            check(eng.feed(off, wal[off:off + n]) == res, "%s 重试不一致" % tag)
        if rnd.random() < 0.08:                      # 随机重开 + 重放
            eng.close()
            eng = walcdc.Engine(state, f["base"], tables)
            check(eng.feed(off, wal[off:off + n]) == res, "%s 重开后重放不一致" % tag)
        off += n
    exp = fx.expected_changes(snaps, commit_fnos, tables)
    for fno in commit_fnos:
        e = fx.canon_changes(exp.get(fno, []))
        g = fx.canon_changes(got.get(fno, []))
        check(e == g, "%s 提交帧 %d 不一致" % (tag, fno))
    for name in tables:
        check(fx.canon_rows(eng.read(name)) == fx.canon_snap(snaps[-1][name]),
              "%s 表 %s 终态对账失败" % (tag, name))
    eng.close()


# ---------------------------------------------------------------- 主流程

def main():
    if os.path.exists(WORKDIR):
        shutil.rmtree(WORKDIR)
    os.makedirs(WORKDIR)
    scenarios = [scenario_a, scenario_b, scenario_c, scenario_d,
                 scenario_e, scenario_f, scenario_g, scenario_h]
    failed = 0
    for sc in scenarios:
        try:
            sc()
            print("PASS %s" % sc.__name__)
        except Exception:
            failed += 1
            print("FAIL %s" % sc.__name__)
            traceback.print_exc()
    if failed:
        print("核账失败：%d 个场景未通过" % failed)
        return 1
    print("ALL OK：逐提交核账与终态对账全部通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
