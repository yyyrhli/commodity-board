#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大宗商品多周期看板 · Word 日报生成器
======================================
纯 Python 标准库 (urllib + zipfile + XML) 生成 .docx, 零依赖。

用法: python3 生成日报.py
输出: 与脚本同目录下的 日报_YYYY-MM-DD.docx
数据: 读取本机运行中的服务 (http://127.0.0.1:8099) 的 API 与 bot_log.json
"""
import json
import os
import subprocess
import urllib.request
import zipfile
from datetime import datetime

BASE = os.path.dirname(os.path.abspath(__file__))
API = "http://127.0.0.1:8099"
LOG_FILE = os.path.join(BASE, "bot_log.json")

# ---------------- 颜色 (Word 里的十六进制) ----------------
C_DARK   = "1F4E79"   # 深蓝(标题/表头)
C_ACCENT = "2E74B5"   # 亮蓝
C_UP     = "C00000"   # 红(涨/多/盈利)
C_DOWN   = "008000"   # 绿(跌/空/亏损)
C_BEST   = "FFF2CC"   # 浅黄(最佳高亮)
C_ALT    = "F2F7FB"   # 隔行浅蓝
C_GRAY   = "808080"   # 灰
C_WHITE  = "FFFFFF"

# ---------------- 工具 ----------------
def esc(s):
    return (str(s) if s is not None else "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def fmt(n, dec=2):
    if n is None:
        return "-"
    try:
        return f"{float(n):,.{dec}f}"
    except (ValueError, TypeError):
        return str(n)

def fmt_chg(n):
    if n is None:
        return "-"
    try:
        v = float(n)
        return f"{'+' if v >= 0 else ''}{v:.2f}%"
    except (ValueError, TypeError):
        return str(n)

def api_get(path):
    with urllib.request.urlopen(API + path, timeout=20) as r:
        return json.loads(r.read().decode("utf-8", "replace"))

def get_pid():
    try:
        out = subprocess.check_output(
            ["pgrep", "-f", "gold_predictor.py --port 8099"]).decode().strip()
        if out:
            return out.splitlines()[-1]
    except Exception:
        pass
    return "?"

# ---------------- XML 构件 ----------------
def run(text, bold=False, color=None, size=None, font="微软雅黑", italic=False):
    rpr = f'<w:rFonts w:ascii="{font}" w:eastAsia="{font}" w:hAnsi="{font}"/>'
    if bold:
        rpr += "<w:b/>"
    if italic:
        rpr += "<w:i/>"
    if color:
        rpr += f'<w:color w:val="{color}"/>'
    if size:
        rpr += f'<w:sz w:val="{size}"/><w:szCs w:val="{size}"/>'
    return f'<w:r><w:rPr>{rpr}</w:rPr><w:t xml:space="preserve">{esc(text)}</w:t></w:r>'

def para(runs_xml, jc=None, before=None, after=None):
    ppr = ""
    if jc:
        ppr += f'<w:jc w:val="{jc}"/>'
    spacing = ""
    if before is not None or after is not None:
        spacing = f'<w:spacing w:before="{before or 0}" w:after="{after or 0}"/>'
    return f'<w:p><w:pPr>{spacing}{ppr}</w:pPr>{runs_xml}</w:p>'

def cell(runs_xml, fill=None, align="left"):
    shd = f'<w:shd w:val="clear" w:color="auto" w:fill="{fill}"/>' if fill else ""
    jc = f'<w:jc w:val="{align}"/>' if align else ""
    return (f'<w:tc><w:tcPr>{shd}</w:tcPr>'
            f'<w:p><w:pPr>{jc}<w:spacing w:after="20" w:before="20"/></w:pPr>{runs_xml}</w:p></w:tc>')

def table(headers, rows, widths=None):
    w = widths or [1400] * len(headers)
    grid = "".join(f'<w:gridCol w:w="{x}"/>' for x in w)
    borders = "".join(
        f'<w:{s} w:val="single" w:sz="4" w:space="0" w:color="B8C4CE"/>'
        for s in ("top", "left", "bottom", "right", "insideH", "insideV"))
    tblpr = f'<w:tblPr><w:tblW w:w="0" w:type="auto"/>{borders}</w:tblPr>'
    body = f'<w:tblGrid>{grid}</w:tblGrid>'
    hdr = "<w:tr>" + "".join(
        cell(run(h, bold=True, color=C_WHITE, size=20), fill=C_DARK, align="center") for h in headers) + "</w:tr>"
    trs = ""
    for i, row in enumerate(rows):
        fill = C_ALT if i % 2 == 1 else None
        trs += "<w:tr>" + "".join(cell(c, fill=fill) for c in row) + "</w:tr>"
    return f'<w:tbl>{tblpr}{body}{hdr}{trs}</w:tbl>'

def h1(text):
    return (para(run("", size=6), after=0)
            + para(run(text, bold=True, color=C_DARK, size=26))
            + para(run("", size=4), after=0))

# ---------------- 数据收集 ----------------
def collect():
    today = datetime.now().strftime("%Y-%m-%d")
    tprefix = today[5:]  # MM-DD
    home = api_get("/api/home")
    acc = api_get("/api/accuracy?days=1")
    bot = api_get("/api/bot")
    account = api_get("/api/account")
    log = []
    try:
        with open(LOG_FILE, encoding="utf-8") as fh:
            log = json.load(fh)
    except Exception:
        pass
    return home, acc, bot, account, today, tprefix, log

def shenlong_today(log, tprefix):
    """按 商品+周期+方向 做 FIFO 配对, 算当日每笔平仓的实现盈亏 (log 是新的在前, 先按时间升序)"""
    log = sorted(log, key=lambda x: str(x.get("ts", "")))
    opens = {"buy": {}, "sell": {}}  # side -> (sym,tf) -> [price,...]
    realized = 0.0
    today_orders = []
    for x in log:
        key = (x.get("sym"), x.get("tf"))
        act = x.get("action", "")
        price = x.get("price")
        ts = str(x.get("ts", ""))
        is_today = ts.startswith(tprefix)
        if act == "开多":
            opens["buy"].setdefault(key, []).append(price)
            if is_today:
                today_orders.append({"ts": ts, "act": act, "sym": x.get("sym"),
                                     "tf": x.get("tf"), "price": price, "pnl": None})
        elif act == "开空":
            opens["sell"].setdefault(key, []).append(price)
            if is_today:
                today_orders.append({"ts": ts, "act": act, "sym": x.get("sym"),
                                     "tf": x.get("tf"), "price": price, "pnl": None})
        elif act in ("平多", "平空", "手动平多", "手动平空", "止盈", "止损", "超时"):
            sides = (["buy"] if act in ("平多", "手动平多", "止盈")
                     else ["sell"] if act in ("平空", "手动平空")
                     else ["buy", "sell"])
            entry = side = None
            for s in sides:
                q = opens[s].get(key)
                if q:
                    entry = q.pop(0)
                    side = s
                    break
            pnl = None
            if entry is not None and price is not None:
                pnl = (price - entry) if side == "buy" else (entry - price)
                if is_today:
                    realized += pnl
            if is_today:
                today_orders.append({"ts": ts, "act": act, "sym": x.get("sym"),
                                     "tf": x.get("tf"), "price": price, "pnl": pnl})
        elif is_today:
            today_orders.append({"ts": ts, "act": act, "sym": x.get("sym"),
                                 "tf": x.get("tf"), "price": price, "pnl": None})
    return today_orders, round(realized, 2)

# ---------------- 生成 docx ----------------
def build_docx(home, acc, bot, account, today, tprefix, log, pid):
    dxy = home.get("dxy") or {}
    coms = home.get("commodities") or []
    high_conf = (home.get("high_conf") or [])[:10]
    ovr = acc.get("overall") or {}
    by_tf = acc.get("by_tf") or {}
    by_dir = acc.get("by_dir") or {}
    st = bot.get("stats") or {}
    positions = bot.get("positions") or []
    ac = account
    today_orders, realized = shenlong_today(log, tprefix)

    B = []  # body xml list

    # ---- 标题 ----
    B.append(para(run("大宗商品多周期看板 · 日报", bold=True, color=C_DARK, size=44), jc="center", after=60))
    B.append(para(run(f"{today}（{['周一','周二','周三','周四','周五','周六','周日'][datetime.now().weekday()]}）"
                      f"　·　生成 {datetime.now().strftime('%H:%M')}", color=C_GRAY, size=20), jc="center", after=120))

    # ---- 速览 ----
    best_tf = worst_tf = None
    for tf in ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]:
        s = by_tf.get(tf) or {}
        if not s.get("resolved"):
            continue
        a = s.get("accuracy")
        if a is None:
            continue
        if best_tf is None or a > best_tf[1]:
            best_tf = (tf, a)
        if worst_tf is None or a < worst_tf[1]:
            worst_tf = (tf, a)
    acc_s = ovr.get("accuracy")
    cur_pos = positions[0] if positions else None
    B.append(para(run("今日速览", bold=True, color=C_WHITE, size=22), after=80))
    rows = [
        [run("预测准确率", bold=True), run(f"总体 {acc_s}%" if acc_s is not None else "总体 -",
                                             color=C_UP if (acc_s or 0) >= 50 else C_DOWN, bold=True),
         run("最佳周期", bold=True),
         run(f"{best_tf[0]} {best_tf[1]}%" if best_tf else "-", color=C_UP, bold=True),
         run("最差周期", bold=True),
         run(f"{worst_tf[0]} {worst_tf[1]}%" if worst_tf else "-", color=C_DOWN, bold=True)],
        [run("神龙开单", bold=True),
         run(f"今日 {len(today_orders)} 笔 · 已实现 {realized:+,.2f}", color=C_UP if realized >= 0 else C_DOWN),
         run("当前持仓", bold=True),
         run(f"{cur_pos.get('name','')} {'多' if cur_pos.get('side')=='buy' else '空'} 浮亏 {fmt(cur_pos.get('pnl'))}" if cur_pos else "无", color=C_DOWN if cur_pos and (cur_pos.get('pnl') or 0) < 0 else C_UP),
         run("模拟账户", bold=True),
         run(f"总盈亏 {fmt(ac.get('total_pnl'))}", color=C_UP if (ac.get('total_pnl') or 0) >= 0 else C_DOWN, bold=True)],
    ]
    B.append(table(["指标", "数值", "指标", "数值", "指标", "数值"], rows,
                   widths=[1100, 1900, 1100, 2100, 1100, 1700]))
    B.append(para(run("", size=6), after=0))

    # ---- 服务状态 ----
    B.append(h1("一、服务状态"))
    B.append(table(["项目", "状态"], [
        [run("服务进程"), run(f"运行中（PID {pid}）")],
        [run("健康检查"), run("HTTP 200 ✅")],
        [run("美元指数"), run(f"{fmt(dxy.get('value'),2)} {('↑' if dxy.get('dir')=='up' else '↓' if dxy.get('dir')=='down' else '→')}")],
    ], widths=[2200, 5000]))

    # ---- 行情总览 ----
    B.append(h1("二、行情总览"))
    mrows = []
    for c in coms:
        chg = c.get("chg")
        up = (chg or 0) >= 0
        d1 = c.get("dirs", {}).get("D1")
        conf = c.get("confs", {}).get("D1")
        mrows.append([
            run(c.get("name", ""), bold=(c.get("key") == "gold")),
            run(fmt(c.get("price"), c.get("decimals", 2))),
            run(fmt_chg(chg), color=C_UP if up else C_DOWN),
            run(d1 or "-", color=C_UP if d1 == "多" else C_DOWN if d1 == "空" else C_GRAY, bold=d1 == "多"),
            run(f"{conf}%" if conf is not None else "-"),
        ])
    B.append(table(["商品", "现价", "涨跌", "D1方向", "置信度"], mrows,
                   widths=[1200, 1600, 1300, 1300, 1000]))
    if high_conf:
        hc = "　｜　".join(f"{h['dir']} {h['name']}·{h['tf']} {h['conf']}%" for h in high_conf)
        B.append(para(run("高置信信号：", bold=True, color=C_ACCENT) + run(hc), before=80))

    # ---- 神龙开单 ----
    B.append(h1("三、神龙机器人今日开单"))
    srows = []
    for x in today_orders:
        act = x.get("action", "")
        a_up = act in ("开多", "平多", "止盈")
        a_color = C_UP if a_up else C_DOWN
        pnl = x.get("pnl")
        srows.append([
            run(str(x.get("ts", "")).split(" ")[-1][:5]),
            run(act, color=a_color, bold=True),
            run(x.get("tf", "")),
            run(fmt(x.get("price"))),
            run(f"{pnl:+,.2f}" if pnl is not None else "-",
                color=C_UP if (pnl or 0) >= 0 else C_DOWN, bold=True if pnl is not None else False),
        ])
    if not srows:
        srows.append([run("今日无开单记录")] + [run("-")] * 4)
    B.append(table(["时间", "动作", "周期", "价格", "盈亏"], srows,
                   widths=[1300, 1300, 900, 1600, 1200]))
    pos_line = (f"当前持仓：{cur_pos.get('name','')} {'买' if cur_pos.get('side')=='buy' else '卖'} "
                f"{cur_pos.get('qty')} 手 @{fmt(cur_pos.get('entry'))}，现价 {fmt(cur_pos.get('current'))}，"
                f"浮亏 {fmt(cur_pos.get('pnl'))}（开于 {cur_pos.get('open_time')}）" if cur_pos else "当前无持仓")
    B.append(para(run(pos_line, bold=True, color=C_DOWN if cur_pos and (cur_pos.get('pnl') or 0) < 0 else C_UP), before=80))
    B.append(para(run(f"今日已实现 {realized:+,.2f}　｜　累计总盈亏 {fmt(st.get('total_pnl'))}　｜　"
                      f"已实现 {fmt(st.get('realized'))}　｜　胜率 {st.get('win_rate')}%（{st.get('trades')} 笔）",
                      color=C_GRAY, size=18), before=20))

    # ---- 命中率 ----
    B.append(h1("四、今日预测命中率"))
    B.append(para(run(f"总体：样本 {ovr.get('resolved')} ｜ 命中 {ovr.get('hits')} ｜ 失误 {ovr.get('misses')} ｜ "
                      f"准确率 {acc_s}%　（平盘 {ovr.get('flat')}、未决 {ovr.get('pending')}）",
                      bold=True, color=C_DARK, size=22)))
    arows = []
    for tf in ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]:
        s = by_tf.get(tf) or {}
        a = s.get("accuracy")
        is_best = best_tf and tf == best_tf[0]
        is_worst = worst_tf and tf == worst_tf[0]
        arows.append([
            run(tf, bold=is_best, color=C_ACCENT if is_best else None),
            run(str(s.get("resolved", 0))),
            run(str(s.get("hits", 0))),
            run(str(s.get("misses", 0))),
            run(f"{a}%" if a is not None else "-",
                bold=True, color=C_UP if is_best else C_DOWN if is_worst else None),
        ])
    B.append(table(["周期", "样本", "命中", "失误", "准确率"], arows,
                   widths=[900, 900, 900, 900, 1500]))
    d_more, d_empty = by_dir.get("多") or {}, by_dir.get("空") or {}
    B.append(para(run(f"方向：看多 {d_more.get('accuracy')}%（{d_more.get('resolved')} 笔）　｜　"
                      f"看空 {d_empty.get('accuracy')}%（{d_empty.get('resolved')} 笔）",
                      color=C_GRAY, size=18), before=60))
    B.append(para(run("注：⭐ 代表今日最佳周期，准确率 = 命中 ÷ (命中+失误)，平盘不计入。",
                      italic=True, color=C_GRAY, size=16)))

    # ---- 模拟账户 ----
    B.append(h1("五、模拟账户"))
    B.append(table(["项目", "数值", "项目", "数值"], [
        [run("总权益"), run(fmt(ac.get("equity")), bold=True),
         run("总盈亏"), run(fmt(ac.get("total_pnl")), color=C_UP if (ac.get("total_pnl") or 0) >= 0 else C_DOWN, bold=True)],
        [run("现金"), run(fmt(ac.get("cash"))),
         run("浮动盈亏"), run(fmt(ac.get("unrealized")), color=C_UP if (ac.get("unrealized") or 0) >= 0 else C_DOWN)],
        [run("占用保证金"), run(fmt(ac.get("margin_used"))),
         run("胜率"), run(f"{ac.get('win_rate')}%")],
    ], widths=[1500, 1800, 1500, 1800]))

    # ---- 小结 ----
    B.append(h1("小结"))
    note = f"今日总体预测命中率 {acc_s}%（最佳周期 {best_tf[0]} {best_tf[1]}%）"
    if cur_pos:
        note += f"；神龙当前持{'多' if cur_pos.get('side')=='buy' else '空'}单浮亏 {fmt(cur_pos.get('pnl'))}"
    if dxy.get("value"):
        note += f"；美元指数 {fmt(dxy.get('value'),2)}"
    B.append(para(run(note + "。", color=C_DARK, size=20)))
    B.append(para(run("⚠️ 本报告所有分析、预测、模拟交易仅供技术研究，不构成投资建议。",
                      italic=True, color=C_GRAY, size=16)))

    body = "".join(B)
    doc_xml = (f'<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
               f'<w:body>{body}'
               f'<w:sectPr><w:pgSz w:w="11906" w:h="16838"/>'
               f'<w:pgMar w:top="1134" w:right="1134" w:bottom="1134" w:left="1134" '
               f'w:header="708" w:footer="708" w:gutter="0"/></w:sectPr></w:body></w:document>')
    return doc_xml

def save_docx(doc_xml, out_path):
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        '<Override PartName="/word/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml"/>'
        '</Types>')
    rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/></Relationships>')
    doc_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/></Relationships>')
    styles = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:styles xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        '<w:docDefaults><w:rPrDefault><w:rPr>'
        '<w:rFonts w:ascii="微软雅黑" w:eastAsia="微软雅黑" w:hAnsi="微软雅黑"/>'
        '<w:sz w:val="21"/><w:szCs w:val="21"/></w:rPr></w:rPrDefault></w:docDefaults>'
        '<w:style w:type="paragraph" w:default="1" w:styleId="Normal">'
        '<w:name w:val="Normal"/></w:style></w:styles>')

    with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml", content_types)
        z.writestr("_rels/.rels", rels)
        z.writestr("word/_rels/document.xml.rels", doc_rels)
        z.writestr("word/styles.xml", styles)
        z.writestr("word/document.xml", doc_xml)

def main():
    today = datetime.now().strftime("%Y-%m-%d")
    out = os.path.join(BASE, f"日报_{today}.docx")
    home, acc, bot, account, today2, tprefix, log = collect()
    doc_xml = build_docx(home, acc, bot, account, today2, tprefix, log, get_pid())
    save_docx(doc_xml, out)
    print("已生成:", out)
    print("文件大小:", os.path.getsize(out), "bytes")

if __name__ == "__main__":
    main()
