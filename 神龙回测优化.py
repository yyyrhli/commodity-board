#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
神龙策略 · 2021~2025 五年黄金历史回测优化
=========================================
复用 gold_predictor.py 的 shenlong_signals 信号引擎,
用 5 年 M1 数据重采样到 M5/M15/H1, 网格搜索 止损/止盈/超时 组合。

用法: python3 神龙回测优化.py
"""
import csv
import importlib.util
import os
import time
import sys
from collections import OrderedDict

BASE = os.path.dirname(os.path.abspath(__file__))
# 历史 M1 数据目录：优先读环境变量，否则用脚本同级的 ./数据
DATA = os.environ.get("QUANT_DATA_DIR") or os.path.join(BASE, "数据")

# ---- 加载 gold_predictor 模块(复用信号引擎) ----
spec = importlib.util.spec_from_file_location("gp", os.path.join(BASE, "gold_predictor.py"))
gp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gp)


def load_m1(year):
    path = os.path.join(DATA, "DAT_MT_XAUUSD_M1_%d.csv" % year)
    bars = []
    with open(path) as f:
        for row in csv.reader(f):
            if len(row) < 6:
                continue
            try:
                o, h, l, c = float(row[2]), float(row[3]), float(row[4]), float(row[5])
            except (ValueError, IndexError):
                continue
            bars.append(["%s %s" % (row[0], row[1]), o, h, l, c, 0])
    return bars


def make_tf(m1, group):
    kl = gp.resample_bars(m1, group)
    eps = []
    for b in kl:
        try:
            eps.append(time.mktime(time.strptime(b[0], "%Y.%m.%d %H:%M")))
        except (ValueError, TypeError):
            eps.append(0)
    return kl, eps


def simulate(kl, eps, sig_by_idx, sl, tp, timeout_h):
    """按真实账户口径: 黄金乘数100, 每笔(往返)成本约30美元(点差+手续费)"""
    N = len(kl)
    MULT = 100.0
    COST = 30.0
    pos = None  # {side, entry, open_ep}
    realized = 0.0
    trades = wins = losses = 0
    gross_w = gross_l = 0.0
    peak = 100000.0
    max_dd = 0.0

    def close_pnl(entry, side, price):
        return (price - entry) * MULT if side == "buy" else (entry - price) * MULT

    for i in range(1, N):
        price = kl[i][4]
        if pos:
            entry = pos["entry"]
            chg = (price - entry) / entry * 100 if pos["side"] == "buy" else (entry - price) / entry * 100
            timed_out = False
            if timeout_h and eps[i] and pos["open_ep"]:
                timed_out = (eps[i] - pos["open_ep"]) > timeout_h * 3600
            if chg <= -sl or chg >= tp or timed_out:
                pnl = close_pnl(entry, pos["side"], price) - COST
                realized += pnl
                trades += 1
                if pnl > 0:
                    wins += 1; gross_w += pnl
                else:
                    losses += 1; gross_l += abs(pnl)
                pos = None
        for (stype, sp) in sig_by_idx.get(i, []):
            if stype in ("buy", "sell"):
                side = "buy" if stype == "buy" else "sell"
                if pos and pos["side"] != side:
                    pnl = close_pnl(pos["entry"], pos["side"], price) - COST
                    realized += pnl; trades += 1
                    if pnl > 0: wins += 1; gross_w += pnl
                    else: losses += 1; gross_l += abs(pnl)
                    pos = None
                if not pos:
                    pos = {"side": side, "entry": price, "open_ep": eps[i]}
            elif stype == "clsBuy" and pos and pos["side"] == "buy":
                pnl = close_pnl(pos["entry"], "buy", price) - COST
                realized += pnl; trades += 1
                if pnl > 0: wins += 1; gross_w += pnl
                else: losses += 1; gross_l += abs(pnl)
                pos = None
            elif stype == "clsSell" and pos and pos["side"] == "sell":
                pnl = close_pnl(pos["entry"], "sell", price) - COST
                realized += pnl; trades += 1
                if pnl > 0: wins += 1; gross_w += pnl
                else: losses += 1; gross_l += abs(pnl)
                pos = None
        equity = 100000 + realized
        if equity > peak:
            peak = equity
        dd = (peak - equity) / peak * 100
        if dd > max_dd:
            max_dd = dd
    pf = (gross_w / gross_l) if gross_l else (None if gross_w else 0)
    return {"trades": trades, "win_rate": round(wins * 100.0 / trades, 1) if trades else 0,
            "pnl": round(realized, 0), "pf": round(pf, 2) if pf is not None else None,
            "max_dd": round(max_dd, 2), "gross_w": round(gross_w, 0), "gross_l": round(gross_l, 0)}


def main():
    years = [2021, 2022, 2023, 2024, 2025]
    tfs = [("M5", 5), ("M15", 15), ("H1", 60)]
    sls = [1.0, 1.5, 2.0, 2.5, 3.0]
    tps = [1.5, 2.0, 3.0, 4.0, 6.0]
    tos = [0, 6, 8, 12]  # 0 = 不限超时

    print("加载数据...")
    m1_all = {}
    for y in years:
        m1_all[y] = load_m1(y)
        print("  %d: %d 根 M1" % (y, len(m1_all[y])))

    results = []
    for tname, group in tfs:
        print("\n===== 周期 %s =====" % tname)
        # 汇总 5 年信号
        sigs_by_year = {}
        kl_by_year = {}
        eps_by_year = {}
        for y in years:
            kl, eps = make_tf(m1_all[y], group)
            kl_by_year[y] = kl
            eps_by_year[y] = eps
            r = gp.shenlong_signals(kl, 25, 90)
            sig = {}
            if r and r.get("sigs"):
                for (idx, stype, price, tstr) in r["sigs"]:
                    sig.setdefault(idx, []).append((stype, price))
            sigs_by_year[y] = sig
            print("  %d: %d 根 %s, 信号 %d 个" % (y, len(kl), tname, sum(len(v) for v in sig.values())))
        for sl in sls:
            for tp in tps:
                if tp <= sl:
                    continue
                for to in tos:
                    agg = {"trades": 0, "pnl": 0.0, "max_dd": 0.0,
                           "gross_w": 0.0, "gross_l": 0.0, "wins": 0}
                    for y in years:
                        s = simulate(kl_by_year[y], eps_by_year[y], sigs_by_year[y], sl, tp, to)
                        agg["trades"] += s["trades"]
                        agg["pnl"] += s["pnl"]
                        agg["max_dd"] = max(agg["max_dd"], s["max_dd"])
                        agg["gross_w"] += s["gross_w"]
                        agg["gross_l"] += s["gross_l"]
                        agg["wins"] += int(round(s["win_rate"] / 100.0 * s["trades"])) if s["trades"] else 0
                    agg["pf"] = round(agg["gross_w"] / agg["gross_l"], 2) if agg["gross_l"] else (None if agg["gross_w"] else 0)
                    agg["win_rate"] = round(agg["wins"] * 100.0 / agg["trades"], 1) if agg["trades"] else 0
                    results.append((tname, sl, tp, to, agg))

    # 排序: 交易数>=30 且 盈亏比>1, 按 盈亏*盈亏比 - 回撤*0.3 打分
    print("\n===== 结果排名(Top 15, 要求交易>=30 且盈亏比>1) =====")
    valid = [r for r in results if r[4]["trades"] >= 30 and (r[4]["pf"] or 0) > 1]
    valid.sort(key=lambda r: r[4]["pnl"] * (r[4]["pf"] or 0) - r[4]["max_dd"] * 0.3, reverse=True)
    if not valid:
        print("没有满足条件的组合，放宽条件看 Top 15")
        results.sort(key=lambda r: r[4]["pnl"], reverse=True)
        valid = results[:15]
    print(f"{'周期':<5}{'止损':<6}{'止盈':<6}{'超时h':<7}{'交易':<7}{'胜率':<8}{'盈亏':<10}{'盈亏比':<7}{'最大回撤'}")
    for tname, sl, tp, to, a in valid[:15]:
        print(f"{tname:<5}{sl:<6}{tp:<6}{str(to if to else '不限'):<7}{a['trades']:<7}"
              f"{a['win_rate']:<8}{a['pnl']:<10}{str(a['pf'] if a['pf'] is not None else '-'):<7}{a['max_dd']}")

    if valid:
        tname, sl, tp, to, a = valid[0]
        print("\n★ 最佳组合: 周期 %s, 止损 %.1f%%, 止盈 %.1f%%, 超时 %sh" % (tname, sl, tp, to))
        print("  5年合计: 交易 %d 笔, 胜率 %.1f%%, 总盈亏 %.0f, 盈亏比 %s, 最大回撤 %.2f%%"
              % (a["trades"], a["win_rate"], a["pnl"], a["pf"], a["max_dd"]))


if __name__ == "__main__":
    main()
