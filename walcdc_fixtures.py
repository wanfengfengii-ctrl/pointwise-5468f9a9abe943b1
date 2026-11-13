# -*- coding: utf-8 -*-
"""
verify / demo 共用夹具：用 sqlite3 产出真实 WAL、真值快照与独立比对工具。

注意：这里的 sqlite3 只用于“制造源库与 WAL”以及“真值核账”，
被测引擎 walcdc 自身解析 b-tree / WAL，不用 SQLite 解码源库。
"""

import os
import shutil
import sqlite3


# ---------------------------------------------------------------- 独立判等工具
# （与引擎实现分离，避免同源错误相互抵消）

def tval(v):
    if v is None:
        return (0,)
    if isinstance(v, int):
        return (1, v)
    if isinstance(v, float):
        return (2, v)
    if isinstance(v, str):
        return (3, v)
    if isinstance(v, bytes):
        return (4, v)
    raise TypeError(type(v))


def mkey(key):
    if isinstance(key, list):
        return tuple(tval(v) for v in key)
    return (tval(key),)


def rows_equal(a, b):
    if len(a) != len(b):
        return False
    return all(tval(x) == tval(y) for x, y in zip(a, b))


def ckey(k):
    return (1, tuple(tval(v) for v in k)) if isinstance(k, list) else (0, tval(k))


def crow(r):
    return (0,) if r is None else (1, tuple(tval(v) for v in r))


def canon_changes(changes):
    """变更列表 -> 可排序比较的规范形。"""
    return sorted((c[0], c[1], ckey(c[2]), crow(c[3]), crow(c[4])) for c in changes)


def canon_rows(rows):
    """[[键, 行], ...] -> 可排序比较的规范形。"""
    return sorted((ckey(k), crow(r)) for k, r in rows)


def canon_snap(snap):
    """{mkey: (key, row)} -> 规范形。"""
    return sorted((ckey(k), crow(r)) for k, r in snap.values())


# ---------------------------------------------------------------- 快照与差异

def snap_db(db, tables):
    """经由 sqlite3 取跟踪表真值：{表名: {mkey: (键, 行)}}。"""
    out = {}
    for name, (root, ncols, pk) in tables.items():
        m = {}
        if pk is None:
            for r in db.execute('SELECT rowid, * FROM "%s"' % name):
                m[mkey(r[0])] = (r[0], list(r[1:]))
        else:
            for r in db.execute('SELECT * FROM "%s"' % name):
                row = list(r)
                m[mkey([row[c] for c in pk])] = ([row[c] for c in pk], row)
        out[name] = m
    return out


def expected_changes(snaps, commit_fnos, tables):
    """由相邻真值快照计算每个提交的净变化：{提交帧号: [[fno, 表, 键, 旧, 新], ...]}。"""
    out = {}
    for i, fno in enumerate(commit_fnos):
        prev, cur = snaps[i], snaps[i + 1]
        lst = []
        for name in tables:
            om, nm = prev[name], cur[name]
            for mk in set(om) | set(nm):
                o, n = om.get(mk), nm.get(mk)
                if o is not None and n is not None:
                    if rows_equal(o[1], n[1]):
                        continue
                    lst.append([fno, name, o[0], o[1], n[1]])
                elif o is not None:
                    lst.append([fno, name, o[0], o[1], None])
                else:
                    lst.append([fno, name, n[0], None, n[1]])
        out[fno] = lst
    return out


def sqlval(v):
    if v is None:
        return "NULL"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        return repr(v)
    if isinstance(v, str):
        return "'" + v.replace("'", "''") + "'"
    if isinstance(v, bytes):
        return "X'" + v.hex() + "'"
    raise TypeError(type(v))


# ---------------------------------------------------------------- 夹具构建

def build_fixture(dirp, page_size, ddl, initial, txns, tracked,
                  encoding=None, auto_vacuum=0, precreated=False):
    """
    建库、跑事务、收集 WAL 与真值快照。

    tracked:    {表名: 主键列号列表或 None}
    txns:       [ [SQL 或 callable(db), ...], ... ]，每个子列表是一个事务。
    precreated: 库文件已由调用方预建（页尺寸/保留字节等已定型），跳过相关 pragma。
    返回 dict(dir, base, wal, tables, snaps, commit_fnos, framesz)
      wal          从偏移 0 开始的完整 WAL 字节（单代次）
      snaps[i]     第 i 个提交后的真值（snaps[0] 为基库态）
      commit_fnos  每个事务的提交帧号（1 起）
    """
    os.makedirs(dirp, exist_ok=True)
    dbp = os.path.join(dirp, "src.db")
    if not precreated:
        for suffix in ("", "-wal", "-shm"):
            if os.path.exists(dbp + suffix):
                os.remove(dbp + suffix)
    db = sqlite3.connect(dbp, isolation_level=None)
    if not precreated:
        db.execute("PRAGMA page_size=%d" % page_size)
        if encoding:
            db.execute("PRAGMA encoding='%s'" % encoding)
        if auto_vacuum:
            db.execute("PRAGMA auto_vacuum=%d" % auto_vacuum)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA wal_autocheckpoint=0")
    for s in ddl:
        db.execute(s)
    for s in initial:
        db.execute(s)
    db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    tables = {}
    for name, pk in tracked.items():
        root = db.execute(
            "SELECT rootpage FROM sqlite_master WHERE name=?", (name,)).fetchone()[0]
        ncols = len(db.execute('PRAGMA table_info("%s")' % name).fetchall())
        tables[name] = [root, ncols, pk]

    base = os.path.join(dirp, "base.db")
    shutil.copyfile(dbp, base)

    snaps = [snap_db(db, tables)]
    walp = dbp + "-wal"
    wal = b""
    commit_fnos = []
    framesz = 24 + page_size
    for sqls in txns:
        db.execute("BEGIN")
        for s in sqls:
            if callable(s):
                s(db)
            else:
                db.execute(s)
        db.execute("COMMIT")
        with open(walp, "rb") as f:
            f.seek(len(wal))
            seg = f.read()
        assert seg, "事务未产生 WAL 帧"
        wal += seg
        commit_fnos.append((len(wal) - 32) // framesz)
        snaps.append(snap_db(db, tables))
    db.close()
    return {"dir": dirp, "base": base, "wal": wal, "tables": tables,
            "snaps": snaps, "commit_fnos": commit_fnos, "framesz": framesz}
