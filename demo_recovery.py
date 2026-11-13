#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
walcdc 恢复演示（demo）。

真实演示：子进程在喂 WAL 途中被 SIGKILL 杀死，重开后续传；
已确认调用重放（响应丢失）必须返回原结果；最终以真值快照核账。

用法：
    python3 -I demo_recovery.py            # 父进程：搭夹具、杀子进程、重开、核账
    python3 -I demo_recovery.py child DIR  # 子进程：喂流（被父进程控制生死）
"""

import os
import pickle
import random
import shutil
import signal
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import walcdc
import walcdc_fixtures as fx

WORKDIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "work_demo")


def build(dirp):
    """构造确定性夹具并把调用计划落盘。"""
    ddl = [
        "CREATE TABLE r1(a INTEGER, b TEXT, c BLOB)",
        "CREATE TABLE w1(k TEXT, v INTEGER, PRIMARY KEY(k)) WITHOUT ROWID",
    ]
    initial = ["INSERT INTO r1 VALUES(1,'init',X'00')"]
    txns = []
    for i in range(12):                       # 12 个事务，大小形态各异
        txns.append([
            "INSERT INTO r1 VALUES(%d,'t%d',randomblob(%d))" % (100 + i, i, 20 + 37 * i),
            "INSERT INTO w1 VALUES('k%d',%d)" % (i, i),
        ])
        txns.append([
            "UPDATE r1 SET b='u%d' WHERE a=%d" % (i, 100 + i),
            "UPDATE w1 SET v=v+10 WHERE k='k%d'" % i,
        ])
        if i % 3 == 2:
            txns.append(["DELETE FROM r1 WHERE a=%d" % (100 + i)])
    tracked = {"r1": None, "w1": [0]}
    f = fx.build_fixture(dirp, 1024, ddl, initial, txns, tracked)

    # 把 WAL 切成一串调用（大小混合、覆盖跨提交分片）
    rnd = random.Random(7)
    calls = []
    off = 0
    while off < len(f["wal"]):
        n = rnd.choice([13, 200, 1024, 3000, 9000])
        n = min(n, len(f["wal"]) - off)
        calls.append((off, f["wal"][off:off + n]))
        off += n
    plan = {"calls": calls, "tables": f["tables"], "snaps": f["snaps"],
            "commit_fnos": f["commit_fnos"], "base": f["base"],
            "state": os.path.join(dirp, "state.db")}
    with open(os.path.join(dirp, "plan.pkl"), "wb") as fp:
        pickle.dump(plan, fp)
    return plan


def child(dirp):
    """子进程：逐调用喂流；每个调用先把结果落盘（回执）再上报 ACK。"""
    with open(os.path.join(dirp, "plan.pkl"), "rb") as fp:
        plan = pickle.load(fp)
    resdir = os.path.join(dirp, "results")
    os.makedirs(resdir, exist_ok=True)
    eng = walcdc.Engine(plan["state"], plan["base"], plan["tables"])
    alog = []
    for i, (off, data) in enumerate(plan["calls"]):
        rp = os.path.join(resdir, "%05d.pkl" % i)
        if os.path.exists(rp):
            # 响应丢失重放：该调用已确认过，重试必须返回原结果
            with open(rp, "rb") as fp:
                exp = pickle.load(fp)
            got = eng.feed(off, data)
            assert got == exp, "调用 %d 重放结果不一致" % i
            print("REPLAY %d ok" % i, flush=True)
        else:
            got = eng.feed(off, data)
            with open(rp, "wb") as fp:
                pickle.dump(got, fp)
                fp.flush()
                os.fsync(fp.fileno())
            print("ACK %d (%d changes)" % (i, len(got)), flush=True)
            time.sleep(0.02)                  # 给父进程一个杀进程窗口
        alog.append(got)

    # ---- 终态核账：以变更流重放 + 真值快照 + read() 三方对账
    state = {t: dict(m) for t, m in plan["snaps"][0].items()}
    per_commit = {}
    for got in alog:
        for c in got:
            per_commit.setdefault(c[0], []).append(c)
    nch = 0
    for fno in plan["commit_fnos"]:
        for c in per_commit.get(fno, []):
            _fno, t, key, old, new = c
            mk = fx.mkey(key)
            cur = state[t].get(mk)
            if old is None:
                assert cur is None, "插入键已存在"
            else:
                assert cur is not None and fx.rows_equal(cur[1], old), "旧行不符"
            if new is None:
                state[t].pop(mk, None)
            else:
                state[t][mk] = (key, new)
            nch += 1
    for t in plan["tables"]:
        assert fx.canon_snap(state[t]) == fx.canon_snap(plan["snaps"][-1][t]), \
            "表 %s 重放终态与真值不符" % t
        assert fx.canon_rows(eng.read(t)) == fx.canon_snap(plan["snaps"][-1][t]), \
            "表 %s read() 与真值不符" % t
    eng.close()
    print("RECOVERY OK：%d 次调用、%d 条变更，重放/read 与真值一致" % (len(alog), nch),
          flush=True)


def main():
    if os.path.exists(WORKDIR):
        shutil.rmtree(WORKDIR)
    os.makedirs(WORKDIR)
    plan = build(WORKDIR)
    ncalls = len(plan["calls"])
    kill_after = max(3, ncalls * 2 // 5)
    print("夹具：%d 字节 WAL，%d 次调用；计划在第 %d 次 ACK 后 SIGKILL"
          % (len(plan["calls"]) and sum(len(d) for _, d in plan["calls"]),
             ncalls, kill_after))

    me = os.path.abspath(__file__)
    # ---- 阶段 1：子进程喂流，中途杀死
    p = subprocess.Popen([sys.executable, "-I", me, "child", WORKDIR],
                         stdout=subprocess.PIPE, text=True)
    seen = 0
    for line in p.stdout:
        line = line.strip()
        print("  [阶段1] " + line)
        if line.startswith("ACK"):
            seen += 1
            if seen >= kill_after:
                p.send_signal(signal.SIGKILL)
                break
    p.wait()
    assert p.returncode != 0
    print("阶段1：子进程已被 SIGKILL 杀死（returncode=%d）" % p.returncode)

    # ---- 阶段 2：重开，重放已确认调用（响应丢失），续传至结束并核账
    r = subprocess.run([sys.executable, "-I", me, "child", WORKDIR],
                       capture_output=True, text=True)
    for line in r.stdout.splitlines():
        print("  [阶段2] " + line)
    if r.returncode != 0:
        print(r.stderr)
        print("DEMO FAILED")
        return 1
    if "RECOVERY OK" not in r.stdout:
        print("DEMO FAILED：未看到 RECOVERY OK")
        return 1
    print("DEMO OK：杀进程、重开续传、响应丢失重放、核账全部通过")
    return 0


if __name__ == "__main__":
    if len(sys.argv) >= 2 and sys.argv[1] == "child":
        child(sys.argv[2])
    else:
        sys.exit(main())
