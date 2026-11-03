#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gold_predictor.py — 大宗商品多周期走势预测看板
==============================================
用法: python3 gold_predictor.py [--port 8099] [--interval 45]

数据源(无需Key):
  - 实时行情   : 新浪 hq.sinajs.cn (hf_XX) 批量抓取, 3秒一次, 合成各商品M1/M5
  - K线       : 东方财富 push2his.eastmoney.com (各商品secid) M15/M30/H1/H4/D1
  - 美元指数   : 新浪 DINIW
  - 新闻       : 新浪财经滚动新闻

刷新模型:
  - 全部商品价格: 每3秒批量实时
  - 当前查看商品: 每45秒完整分析
  - 其他商品   : 轮流完整分析(每个约10分钟轮询)
"""
import argparse
import html
import http.server
import json
import os
import re
import smtplib
import socketserver
import threading
import time
import urllib.request
import collections
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from email.header import Header
from email.mime.text import MIMEText

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
SINA_HEADERS = {"User-Agent": UA["User-Agent"], "Referer": "https://finance.sina.com.cn/"}

ORDER = ["M1", "M5", "M15", "M30", "H1", "H4", "D1"]
DIRECTION_THRESHOLD = 0.2
KLINE_LIMIT = 100
INTERVAL = 45
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SAVE_DIR = os.path.join(BASE_DIR, "commodity_state")

# ---- 预测准确率统计 ----
ACC_MIN_CONF = 70          # 只记录置信度>=此值的多/空预测
ACC_TOL = 0.0002           # 判定容忍度(0.02%), 变动低于此记为平盘
ACC_HORIZONS = {"M1": 1, "M5": 5, "M15": 15, "M30": 30, "H1": 60, "H4": 240, "D1": 1440}
ACC_FILE = os.path.join(BASE_DIR, "accuracy_state.json")
ACC_MAX = 30000
ACC_REPLAY_MIN = 26   # 回放最少需要的历史K线根数(MACD等指标可用)
ACC_HIST_MAX = 10000  # 内存价格历史采样数(约10s一次, 覆盖D1的24h判定窗口)
ACC_PRICE_HIST = {}


class Commodity(object):
    def __init__(self, key, name, cat, secid, sina, shift=False, ref_sina=None,
                 scale=1, decimals=2, unit="美元", sign="$"):
        self.key = key
        self.name = name
        self.cat = cat
        self.secid = secid
        self.sina = sina
        self.shift = shift          # 是否平移到现货口径(黄金专用)
        self.ref_sina = ref_sina    # 计算价差用的参考合约
        self.scale = scale          # 东财K线单位换算系数
        self.decimals = decimals
        self.unit = unit
        self.sign = sign
        self.lock = threading.Lock()
        self.build_lock = threading.Lock()
        self.m1_bars = []           # 已收盘1分钟K线 [epoch, o,h,l,c,v]
        self.cur_bar = None
        self.last_spot = None
        self.spread = 0.0
        self.state = {}             # frames/klines/futures/spikes
        self.last_update = 0.0
        self.building = False


COMMODITIES = [
    Commodity("gold", "黄金", "贵金属", "101.GC00Y", "hf_XAU", shift=True, ref_sina="hf_GC",
              decimals=2, unit="美元/盎司", sign="$"),
    Commodity("silver", "白银", "贵金属", "101.SI00Y", "hf_SI", decimals=3, unit="美元/盎司", sign="$"),
    Commodity("oil", "WTI原油", "能源", "102.CL00Y", "hf_CL", decimals=2, unit="美元/桶", sign="$"),
    Commodity("brent", "布伦特", "能源", "112.B00Y", "hf_OIL", decimals=2, unit="美元/桶", sign="$"),
    Commodity("natgas", "天然气", "能源", "102.NG00Y", "hf_NG", decimals=3, unit="美元/MMBtu", sign="$"),
    Commodity("copper", "铜", "金属", "101.HG00Y", "hf_HG", scale=100, decimals=1, unit="美分/磅", sign="¢"),
    Commodity("soybean", "大豆", "农产品", "103.ZS00Y", "hf_S", decimals=2, unit="美分/蒲式耳", sign="¢"),
    Commodity("corn", "玉米", "农产品", "103.ZC00Y", "hf_C", decimals=2, unit="美分/蒲式耳", sign="¢"),
    Commodity("wheat", "小麦", "农产品", "103.ZW00Y", "hf_W", decimals=2, unit="美分/蒲式耳", sign="¢"),
    Commodity("cotton", "棉花", "农产品", "108.CT00Y", "hf_CT", decimals=2, unit="美分/磅", sign="¢"),
    Commodity("soyoil", "豆油", "农产品", "103.ZL00Y", "hf_BO", decimals=2, unit="美分/磅", sign="¢"),
    Commodity("soymeal", "豆粕", "农产品", "103.ZM00Y", "hf_SM", decimals=2, unit="美元/短吨", sign="$"),
    Commodity("coffee", "咖啡", "农产品", "108.KC00Y", "hf_KC", decimals=1, unit="美分/磅", sign="¢"),
    Commodity("cocoa", "可可", "农产品", "108.CC00Y", "hf_CC", decimals=1, unit="美元/吨", sign="$"),
]

BY_KEY = {c.key: c for c in COMMODITIES}
BY_SINA = {c.sina: c for c in COMMODITIES}

GLOBAL_NEWS = []
GLOBAL_ALERTS = []
SEEN_NEWS = set()
DXY = {"value": None, "dir": "flat"}
ACTIVE = "gold"
ACTIVE_LOCK = threading.Lock()
NEWS_LOCK = threading.Lock()


# ---------------------------------------------------------------- HTTP 抓取
def http_get(url, headers=None, timeout=12):
    h = dict(UA)
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def curl_get(url, timeout=10):
    import subprocess
    p = subprocess.Popen(["curl", "-s", "--max-time", str(timeout),
                          "-H", "User-Agent: " + UA["User-Agent"], url],
                         stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    out, _ = p.communicate()
    return out.decode("utf-8", errors="replace")


def fetch_klines(secid, klt, limit, retries=3):
    url = ("https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s"
           "&klt=%d&fqt=0&lmt=%d&end=20500101&fields1=f1,f2,f3"
           "&fields2=f51,f52,f53,f54,f55,f56" % (secid, klt, limit))
    last_err = None
    for attempt in range(retries):
        raw = None
        try:
            raw = http_get(url, timeout=8)
        except Exception as e:
            last_err = e
            try:
                raw = curl_get(url, timeout=8)
            except Exception as e2:
                last_err = e2
        if raw:
            try:
                data = json.loads(raw)
                bars = []
                klines = (data or {}).get("data") or {}
                for line in klines.get("klines") or []:
                    p = line.split(",")
                    # 时间, 开, 收, 高, 低, 量
                    bars.append((p[0], float(p[1]), float(p[3]), float(p[4]), float(p[2]), float(p[5])))
                if bars:
                    return bars
            except Exception as e:
                last_err = e
        time.sleep(0.5 + 1.0 * attempt)
    raise last_err or IOError("klines empty")


def fetch_dxy():
    raw = http_get("https://hq.sinajs.cn/list=DINIW", SINA_HEADERS)
    m = re.search(r'"([^"]*)"', raw)
    return float(m.group(1).split(",")[1])


def fetch_futures_quote(secid):
    """东财实时quote: 成交量/持仓量/日增"""
    try:
        url = ("https://push2.eastmoney.com/api/qt/stock/get?secid=%s"
               "&fields=f43,f47,f58,f85,f86,f170" % secid)
        raw = curl_get(url, timeout=7)
        d = json.loads(raw).get("data") or {}
        out = {}
        if d.get("f47") is not None:
            out["vol"] = int(d["f47"])
        if d.get("f85") is not None:
            out["oi"] = int(round(d["f85"] / 100.0))
        if d.get("f86") is not None:
            out["oi_chg"] = int(round(d["f86"] / 100.0))
        return out
    except Exception:
        return {}


NEWS_KEYWORDS = ["黄金", "金价", "白银", "原油", "铜", "大豆", "玉米", "棉花", "咖啡", "可可",
                 "商品", "贵金属", "美元指数", "金属"]
IMPORTANT_KEYWORDS = ["美联储", "鲍威尔", "降息", "加息", "非农", "CPI", "PCE", "通胀", "关税", "央行",
                      "地缘", "中东", "避险", "战争", "贸易战", "债务", "收益率", "数据", "决议", "会议纪要", "OPEC"]


def fetch_news():
    try:
        url = "https://feed.mix.sina.com.cn/api/roll/get?pageid=153&lid=2516&num=30&page=1"
        raw = http_get(url, {"Referer": "https://finance.sina.com.cn"}, timeout=8)
        data = json.loads(raw)
        items = ((data or {}).get("result") or {}).get("data") or []
        gold, rest = [], []
        for it in items:
            t = html.unescape((it.get("title") or "")).strip()
            if not t:
                continue
            item = {"title": t, "important": any(k in t for k in IMPORTANT_KEYWORDS),
                    "ctime": it.get("ctime"), "url": it.get("url") or ""}
            (gold if any(k in t for k in NEWS_KEYWORDS) else rest).append(item)
        return (gold + rest)[:8]
    except Exception:
        return []


def check_news_alerts(items):
    global SEEN_NEWS
    alerts = []
    for n in items:
        if not n.get("important"):
            continue
        key = n.get("ctime") or n.get("title")
        if key is not None and key not in SEEN_NEWS:
            SEEN_NEWS.add(key)
            alerts.append(n["title"])
    if len(SEEN_NEWS) > 400:
        SEEN_NEWS = set(list(SEEN_NEWS)[-250:])
    return alerts[:2]


def fetch_sina_batch():
    """一条请求抓取所有商品实时价, 返回 {sina_symbol: {price,high,low,prev,ts}}"""
    syms = list(dict.fromkeys([c.sina for c in COMMODITIES] + [c.ref_sina for c in COMMODITIES if c.ref_sina]))
    raw = http_get("https://hq.sinajs.cn/list=" + ",".join(syms), SINA_HEADERS)
    out = {}
    for sym in syms:
        m = re.search(r'hq_str_%s="([^"]*)"' % sym, raw)
        if not m or not m.group(1).strip():
            continue
        p = m.group(1).split(",")
        try:
            out[sym] = {"price": float(p[0]), "high": float(p[4]), "low": float(p[5]),
                        "prev": float(p[7]), "ts": (p[12] if len(p) > 12 else "") + " " + p[6]}
        except (ValueError, IndexError):
            continue
    return out


SINA_DAILY = {"ts": 0.0, "bars": []}
SINA_DAILY_LOCK = threading.Lock()


# -------- Twelve Data 备用数据源(东财限流时兜底) --------
API_CONFIG_FILE = os.path.join(BASE_DIR, "api_config.json")
TD_KEY = ""
TD_LOCK = threading.Lock()
TD_LAST = [0.0]
TD_MIN = [0]
TD_MIN_T = [0.0]
TD_DAY = [0]
TD_DAY_T = [0.0]
TWELVE_MAP = {"gold": "XAU/USD", "silver": "XAG/USD", "oil": "CL.F", "brent": "BZ.F",
              "natgas": "NG.F", "copper": "HG.F", "soybean": "S.F", "corn": "C.F",
              "wheat": "W.F", "cotton": "CT.F", "soyoil": "ZL.F", "soymeal": "ZM.F",
              "coffee": "KC.F", "cocoa": "CC.F"}
TWELVE_INTERVAL = {"M1": "1min", "M5": "5min", "M15": "15min", "M30": "30min",
                   "H1": "1h", "H4": "4h", "D1": "1day"}


def load_api_config():
    global TD_KEY
    try:
        with open(API_CONFIG_FILE) as fh:
            d = json.load(fh)
        TD_KEY = d.get("twelve_key", "")
    except Exception:
        TD_KEY = ""


def save_api_config(data):
    global TD_KEY
    try:
        d = {}
        if os.path.exists(API_CONFIG_FILE):
            with open(API_CONFIG_FILE) as fh:
                d = json.load(fh)
        d.update(data)
        with open(API_CONFIG_FILE, "w") as fh:
            json.dump(d, fh)
        if "twelve_key" in data:
            TD_KEY = data["twelve_key"]
    except Exception:
        pass


def fetch_twelve_klines(ckey, tf, limit):
    """Twelve Data K线(备用源), 限频5秒/次、6次/分、750次/天"""
    if not TD_KEY:
        return None
    sym = TWELVE_MAP.get(ckey)
    interval = TWELVE_INTERVAL.get(tf)
    if not sym or not interval:
        return None
    now = time.time()
    with TD_LOCK:
        if now - TD_LAST[0] < 5:
            return None
        if now - TD_MIN_T[0] > 60:
            TD_MIN[0] = 0
            TD_MIN_T[0] = now
        if now - TD_DAY_T[0] > 86400:
            TD_DAY[0] = 0
            TD_DAY_T[0] = now
        if TD_MIN[0] >= 6 or TD_DAY[0] >= 750:
            return None
    try:
        url = ("https://api.twelvedata.com/time_series?symbol=%s&interval=%s&outputsize=%d&apikey=%s"
               % (sym.replace("/", "%2F"), interval, min(limit, 200), TD_KEY))
        raw = http_get(url, timeout=10)
        d = json.loads(raw)
        if d.get("status") == "error":
            return None
        values = d.get("values") or []
        bars = []
        for v in values:
            try:
                bars.append((v["datetime"], float(v["open"]), float(v["high"]),
                             float(v["low"]), float(v["close"]), float(v.get("volume") or 0)))
            except Exception:
                continue
        if bars:
            bars.reverse()
            with TD_LOCK:
                TD_LAST[0] = time.time()
                TD_MIN[0] += 1
                TD_DAY[0] += 1
            return bars
    except Exception:
        return None
    return None


def fetch_sina_daily_xau():
    """新浪现货黄金日K(2006至今,5184根), 5分钟缓存"""
    global SINA_DAILY
    with SINA_DAILY_LOCK:
        if SINA_DAILY["bars"] and time.time() - SINA_DAILY["ts"] < 300:
            return SINA_DAILY["bars"]
    try:
        url = ("https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_/"
               "GlobalFuturesService.getGlobalFuturesDailyKLine?symbol=XAU")
        raw = http_get(url, {"Referer": "https://finance.sina.com.cn"}, timeout=10)
        m = re.search(r"\((\[.*\])\)", raw, re.S)
        if not m:
            return SINA_DAILY["bars"]
        data = json.loads(m.group(1))
        bars = []
        for it in data:
            try:
                bars.append((it["date"], float(it["open"]), float(it["high"]),
                             float(it["low"]), float(it["close"]), float(it.get("volume") or 0)))
            except Exception:
                continue
        with SINA_DAILY_LOCK:
            if bars:
                SINA_DAILY["bars"] = bars
                SINA_DAILY["ts"] = time.time()
            return SINA_DAILY["bars"]
    except Exception:
        with SINA_DAILY_LOCK:
            return SINA_DAILY["bars"]


def fetch_sina_min(c):
    """新浪外盘分钟K线(当天1分钟)备用源, 返回东财原始单位的 [(time_str,o,h,l,c,v)] 或 None.
    新浪分钟线无高低价, 以 open/close 近似 H/L; 仅覆盖当天交易时段."""
    sym = c.sina[3:] if c.sina.startswith("hf_") else c.sina
    url = ("https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%%20_/"
           "GlobalFuturesService.getGlobalFuturesMinLine?symbol=%s&type=1" % sym)
    try:
        raw = http_get(url, {"Referer": "https://finance.sina.com.cn"}, timeout=10)
    except Exception:
        return None
    m = re.search(r"\((\{.*\})\)", raw, re.S)
    if not m:
        return None
    try:
        d = json.loads(m.group(1))
    except Exception:
        return None
    rows = d.get("minLine_1d") or []
    out = []
    for it in rows:
        if not isinstance(it, list) or len(it) < 6:
            continue
        try:
            if "-" in str(it[0]) and len(it) >= 10:
                o = float(it[5]); cc = float(it[8]); t = it[9]
            else:
                o = float(it[1]); cc = float(it[4]); t = it[5]
        except (ValueError, IndexError):
            continue
        h = max(o, cc)
        l = min(o, cc)
        out.append((t, o, h, l, cc, 0))
    if not out:
        return None
    if c.scale and c.scale > 1:
        s = float(c.scale)
        out = [(t, o / s, h / s, l / s, cc / s, v) for (t, o, h, l, cc, v) in out]
    return out


# ---------------------------------------------------------------- 技术指标
def ema(values, n):
    k = 2.0 / (n + 1)
    out = []
    prev = values[0]
    for v in values:
        if out:
            prev = v * k + prev * (1 - k)
        else:
            prev = v
        out.append(prev)
    return out


def sma(values, n):
    out = []
    for i in range(len(values)):
        if i + 1 < n:
            out.append(None)
        else:
            out.append(sum(values[i - n + 1:i + 1]) / n)
    return out


def macd(closes):
    e12 = ema(closes, 12)
    e26 = ema(closes, 26)
    dif = [a - b for a, b in zip(e12, e26)]
    dea = ema(dif, 9)
    hist = [2 * (a - b) for a, b in zip(dif, dea)]
    return dif, dea, hist


def rsi(closes, n=14):
    if len(closes) <= n:
        return 50.0
    gains = losses = 0.0
    for i in range(1, n + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    avg_gain, avg_loss = gains / n, losses / n
    for i in range(n + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        gain = d if d > 0 else 0.0
        loss = -d if d < 0 else 0.0
        avg_gain = (avg_gain * (n - 1) + gain) / n
        avg_loss = (avg_loss * (n - 1) + loss) / n
    if avg_loss == 0:
        return 100.0
    return 100 - 100 / (1 + avg_gain / avg_loss)


def bollinger(closes, n=20, k=2.0):
    mid = sma(closes, n)
    up, low = [], []
    for i in range(len(closes)):
        if mid[i] is None:
            up.append(None)
            low.append(None)
        else:
            window = closes[i - n + 1:i + 1]
            std = (sum((x - mid[i]) ** 2 for x in window) / n) ** 0.5
            up.append(mid[i] + k * std)
            low.append(mid[i] - k * std)
    return up, mid, low


def analyze_bars(bars):
    n = len(bars)
    closes = [b[4] for b in bars]
    highs = [b[2] for b in bars]
    lows = [b[3] for b in bars]
    vols = [b[5] for b in bars]
    last = closes[-1]
    prev = closes[-2] if n > 1 else last
    chg = (last / prev - 1) * 100 if prev else 0.0

    cur_vol = vols[-1] if vols else 0
    avg_vol = (sum(vols[-21:-1]) / 20) if len(vols) >= 21 else (sum(vols) / max(1, len(vols)))
    vol_ratio = (cur_vol / avg_vol) if avg_vol else 0.0
    spike = vol_ratio > 2.0

    e5 = ema(closes, 5)[-1]
    e10 = ema(closes, 10)[-1]
    e20 = ema(closes, 20)[-1]
    e60 = ema(closes, 60)[-1] if n >= 60 else None

    dif, dea, hist = macd(closes)
    dif_c, dea_c, hist_c = dif[-1], dea[-1], hist[-1]
    hist_p = hist[-2] if len(hist) > 1 else 0.0

    r = rsi(closes, 14)
    bup, bmid, blow = bollinger(closes, 20, 2.0)
    b_up, b_mid, b_low = bup[-1], bmid[-1], blow[-1]

    score = 0.0
    if e5 > e10 > e20:
        score += 1.0
        es = "多头排列"
    elif e5 < e10 < e20:
        score -= 1.0
        es = "空头排列"
    else:
        es = "均线交织"
    if e60 is not None:
        score += 0.5 if e20 > e60 else -0.5

    score += 0.8 if hist_c > 0 else -0.8
    score += 0.4 if dif_c > dea_c else -0.4
    if hist_p <= 0 < hist_c:
        score += 0.5
    elif hist_p >= 0 > hist_c:
        score -= 0.5

    if r >= 70:
        score -= 0.5
    elif r <= 30:
        score += 0.5
    elif r >= 55:
        score += 0.5
    elif r <= 45:
        score -= 0.3

    if b_up is not None:
        if last >= b_up:
            score += 0.6
            bp = "上轨外"
        elif last >= b_mid:
            score += 0.4
            bp = "中轨上方"
        elif last <= b_low:
            score -= 0.6
            bp = "下轨外"
        elif last <= b_mid:
            score -= 0.4
            bp = "中轨下方"
        else:
            bp = "中轨附近"
    else:
        bp = "-"

    if n >= 21:
        chg20 = (last / closes[-21] - 1) * 100
        score += 0.5 if chg20 > 0.1 else -0.5 if chg20 < -0.1 else 0.0

    rh = max(highs[-40:])
    sl = min(lows[-40:])
    if last > rh:
        score += 1.0
    elif last < sl:
        score -= 1.0
    else:
        if (last - sl) / sl < 0.001:
            score += 0.3
        if (rh - last) / rh < 0.001:
            score -= 0.3

    if score >= DIRECTION_THRESHOLD:
        direction = "多"
    elif score <= -DIRECTION_THRESHOLD:
        direction = "空"
    else:
        direction = "震荡"

    conf = min(100.0, abs(score) * 18 + 15)

    if hist_c > 0:
        macd_txt = "MACD红柱"
        if hist_p > 0 and hist_c > hist_p:
            macd_txt += "放大"
        elif hist_p > 0 and hist_c < hist_p:
            macd_txt += "收窄"
    else:
        macd_txt = "MACD绿柱"
        if hist_p < 0 and hist_c < hist_p:
            macd_txt += "放大"
        elif hist_p < 0 and hist_c > hist_p:
            macd_txt += "收窄"
    if r >= 70:
        rsi_txt = "RSI超买"
    elif r <= 30:
        rsi_txt = "RSI超卖"
    elif r >= 55:
        rsi_txt = "RSI偏强"
    elif r <= 45:
        rsi_txt = "RSI偏弱"
    else:
        rsi_txt = "RSI中性"
    summary = "，".join([x for x in [es, macd_txt, rsi_txt, bp] if x and x != "-"])
    summary += "；阻力%s/支撑%s" % (round(rh, 1), round(sl, 1))

    return {
        "direction": direction, "score": round(score, 2), "conf": round(conf),
        "close": round(last, 2), "chg": round(chg, 2), "ema_state": es,
        "ema5": round(e5, 1), "ema10": round(e10, 1), "ema20": round(e20, 1),
        "ema60": round(e60, 1) if e60 else None,
        "dif": round(dif_c, 2), "dea": round(dea_c, 2), "hist": round(hist_c, 2),
        "rsi": round(r, 1),
        "boll_up": round(b_up, 1) if b_up is not None else None,
        "boll_mid": round(b_mid, 1) if b_mid is not None else None,
        "boll_low": round(b_low, 1) if b_low is not None else None,
        "boll_pos": bp,
        "resistance": round(rh, 1), "support": round(sl, 1),
        "time": bars[-1][0], "bars": n,
        "vol": int(cur_vol), "vol_ratio": round(vol_ratio, 2), "spike": spike,
        "summary": summary,
    }


def infer_sentiment(direction, chg, vol_ratio, oi_chg):
    if oi_chg is not None and abs(oi_chg) >= 0:
        if oi_chg > 0:
            if chg is not None and chg > 0.05:
                return "多方增仓占优", "bull"
            if chg is not None and chg < -0.05:
                return "空方增仓占优", "bear"
            return "增仓对峙", "neutral"
        elif oi_chg < 0:
            if chg is not None and chg > 0.05:
                return "空头回补推涨", "bull"
            if chg is not None and chg < -0.05:
                return "多头离场/减仓", "bear"
            return "减仓观望", "neutral"
    if vol_ratio and vol_ratio > 1.5:
        if chg is not None and chg > 0:
            return "放量上行,多方占优", "bull"
        if chg is not None and chg < 0:
            return "放量下行,空方占优", "bear"
    return "多空均衡", "neutral"


# ---------------------------------------------------------------- 每商品K线工具
def resample_bars(bars, group):
    out = []
    i = 0
    n = len(bars)
    while i < n:
        chunk = bars[i:i + group]
        i += len(chunk)
        out.append([chunk[0][0], chunk[0][1], max(c[2] for c in chunk),
                    min(c[3] for c in chunk), chunk[-1][4], sum(x[5] for x in chunk)])
    return out


def short_time(t):
    if len(t) >= 16:
        return t[5:16]
    if len(t) >= 10:
        return t[5:10]
    return str(t)


def bar_time_epoch(t):
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return int(time.mktime(time.strptime(t, fmt)))
        except (ValueError, TypeError):
            continue
    return None


def scale_bars(bars, scale):
    if scale == 1:
        return bars
    return [(t, o * scale, h * scale, l * scale, c * scale, v) for t, o, h, l, c, v in bars]


def shift_bars(bars, spread):
    if not spread:
        return bars
    return [(t, round(o + spread, 2), round(h + spread, 2),
             round(l + spread, 2), round(c + spread, 2), v) for t, o, h, l, c, v in bars]


# ---------------------------------------------------------------- 实时tick与M1合成
def tick_loop():
    """每3秒批量抓取所有商品, 更新last_spot并合成M1"""
    while True:
        try:
            quotes = fetch_sina_batch()
            now = int(time.time())
            bucket = now - (now % 60)
            for c in COMMODITIES:
                q = quotes.get(c.sina)
                if not q:
                    continue
                c.last_spot = q
                if c.shift and c.ref_sina and quotes.get(c.ref_sina):
                    c.spread = round(q["price"] - quotes[c.ref_sina]["price"], 2)
                with c.lock:
                    if c.cur_bar is None or c.cur_bar[0] != bucket:
                        if c.cur_bar is not None:
                            c.m1_bars.append(c.cur_bar)
                            if len(c.m1_bars) > 1500:
                                del c.m1_bars[:len(c.m1_bars) - 1500]
                        c.cur_bar = [bucket, q["price"], q["price"], q["price"], q["price"], 0]
                    else:
                        c.cur_bar[2] = max(c.cur_bar[2], q["price"])
                        c.cur_bar[3] = min(c.cur_bar[3], q["price"])
                        c.cur_bar[4] = q["price"]
        except Exception:
            pass
        try:
            check_limit_orders()
        except Exception:
            pass
        time.sleep(3)


def c_m1_series(c):
    with c.lock:
        bars = list(c.m1_bars)
        cur = list(c.cur_bar) if c.cur_bar else None
    if cur:
        bars.append(cur)
    return bars


def seed_m1(c):
    if c.shift:
        with c.lock:
            sp = c.spread
        if abs(sp) < 0.01:
            return  # 等待tick_loop算出价差再种, 防止M1在COMEX/现货间跳变
    else:
        sp = 0.0
    try:
        bars = fetch_klines(c.secid, 1, 600, retries=2)
        bars = scale_bars(bars, c.scale)
        if c.shift and sp:
            bars = shift_bars(bars, sp)
    except Exception:
        # 东财失败 → Twelve Data 1min → 新浪外盘分钟K线(备用源)
        bars = fetch_twelve_klines(c.key, "M1", 600)
        if not bars:
            bars = fetch_sina_min(c)
        if not bars:
            return
        bars = scale_bars(bars, c.scale)
        if c.shift and sp:
            bars = shift_bars(bars, sp)
    now_bucket = int(time.time()) - (int(time.time()) % 60)
    out = []
    for t, o, h, l, cc, v in bars:
        ts = None
        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
            try:
                ts = int(time.mktime(time.strptime(t, fmt)))
                break
            except ValueError:
                continue
        if ts is None or ts >= now_bucket:
            continue
        out.append([ts, o, h, l, cc, v])
    with c.lock:
        c.m1_bars = out
    print("M1种子 %s: %d根 (价差%.1f)" % (c.name, len(out), sp), flush=True)


def seeder_loop():
    idx = 0
    while True:
        try:
            with ACTIVE_LOCK:
                active_key = ACTIVE
            ac = BY_KEY.get(active_key)
            seeded = False
            if ac:
                with ac.lock:
                    enough = len(ac.m1_bars) >= 50
                if not enough:
                    seed_m1(ac)
                    seeded = True
            if not seeded:
                c = COMMODITIES[idx % len(COMMODITIES)]
                idx += 1
                with c.lock:
                    enough = len(c.m1_bars) >= 50
                if not enough:
                    seed_m1(c)
        except Exception:
            pass
        time.sleep(8)


def fallback_from_m1(c, group, min_bars):
    bars = c_m1_series(c)
    if len(bars) < min_bars:
        return None
    return resample_bars(bars, group)


# ---------------------------------------------------------------- 构建单商品状态
TF_SPECS = [("M15", 15, 200, None, 15), ("M30", 30, 200, None, 30),
            ("H1", 60, 240, None, 60), ("H4", 60, 240, 4, 240), ("D1", 101, 200, None, 1440)]


def _fetch_tf(c, spec):
    """并行抓取单个周期K线; 黄金D1优先用新浪现货日K; 东财失败用Twelve Data兜底"""
    name, klt, limit, group, m1group = spec
    try:
        if name == "D1" and c.key == "gold":
            sdb = fetch_sina_daily_xau()
            if len(sdb) >= 20:
                return (name, sdb, None)
        bars = fetch_klines(c.secid, klt, limit)
        with c.lock:
            spread = c.spread if c.shift else 0.0
        bars = scale_bars(bars, c.scale)
        if c.shift and spread:
            bars = shift_bars(bars, spread)
        if group:
            bars = resample_bars(bars, group)
        if len(bars) < 20:
            raise ValueError("bars too few")
        # 东财部分数据(<35根)且M1合成能提供更多时, 优先用M1合成(保证神龙等有足够历史)
        if len(bars) < 35 and m1group and m1group <= 30:
            fb = fallback_from_m1(c, m1group, m1group * 20)
            if fb is not None and len(fb) > len(bars):
                return (name, fb, None)
        return (name, bars, None)
    except Exception as e:
        # 东财失败 → Twelve Data 备源 (限频, 黄金XAU/USD为现货无需平移)
        td = fetch_twelve_klines(c.key, name, limit)
        if td:
            td = scale_bars(td, c.scale)
            return (name, td, None)
        fb = fallback_from_m1(c, m1group, m1group * 20)
        return (name, None, (e, fb))


def build_commodity_state(c):
    state = {"meta": {"key": c.key, "name": c.name, "cat": c.cat,
                      "unit": c.unit, "sign": c.sign, "decimals": c.decimals},
             "frames": {}, "klines": {}, "spikes": []}
    prev_frames = dict(c.state.get("frames") or {})
    prev_klines = dict(c.state.get("klines") or {})

    spot = c.last_spot
    if spot:
        state["spot"] = {"price": spot["price"], "high": spot["high"], "low": spot["low"],
                         "chg": round((spot["price"] / spot["prev"] - 1) * 100, 2) if spot["prev"] else 0.0,
                         "ts": spot["ts"]}
    else:
        state["spot"] = {"price": None, "chg": None, "high": None, "low": None}

    futures_q = fetch_futures_quote(c.secid)
    state["futures"] = futures_q
    news_items = GLOBAL_NEWS
    spikes = []
    fail_count = 0

    results = {}
    with ThreadPoolExecutor(max_workers=3) as ex:
        for name, bars, err in ex.map(lambda s: _fetch_tf(c, s), TF_SPECS):
            results[name] = (bars, err)

    for name, klt, limit, group, m1group in TF_SPECS:
        bars, err = results[name]
        spike_ts = None
        if bars is not None:
            state["klines"][name] = [(short_time(b[0]), b[1], b[2], b[3], b[4], b[5]) for b in bars[-KLINE_LIMIT:]]
            state["frames"][name] = analyze_bars(bars)
            spike_ts = bar_time_epoch(bars[-1][0])
        else:
            e, fb = err
            if fb is not None:
                state["klines"][name] = [(time.strftime("%m-%d %H:%M", time.localtime(b[0])), b[1], b[2], b[3], b[4], b[5])
                                         for b in fb[-KLINE_LIMIT:]]
                fr = analyze_bars(fb)
                fr["error"] = "东财暂不可用,已用M1合成"
                state["frames"][name] = fr
                spike_ts = fb[-1][0]
            else:
                old = dict(prev_frames.get(name) or {})
                old["error"] = "数据延迟(旧数据)"
                state["frames"][name] = old
                if name in prev_klines:
                    state["klines"][name] = prev_klines[name]
                fail_count += 1
            print("%s %s: %s" % (c.name, name, e), flush=True)

    for name in ["M1", "M5"]:
        group = 1 if name == "M1" else 5
        bars = c_m1_series(c)
        kline_bars = resample_bars(bars, group) if group > 1 else bars
        state["klines"][name] = [(time.strftime("%m-%d %H:%M", time.localtime(b[0])), b[1], b[2], b[3], b[4], b[5])
                                 for b in kline_bars[-KLINE_LIMIT:]]
        if len(kline_bars) >= 20:
            std = [(time.strftime("%m-%d %H:%M", time.localtime(b[0])), b[1], b[2], b[3], b[4], b[5])
                   for b in kline_bars]
            state["frames"][name] = analyze_bars(std)
        else:
            state["frames"][name] = {"direction": "震荡", "conf": None, "close": state["spot"]["price"],
                                     "accumulate": True, "error": "数据积累中 %d 根" % len(kline_bars)}

    for name in ORDER:
        fr = state["frames"].get(name)
        if not fr or fr.get("accumulate"):
            continue
        prev_dir = (prev_frames.get(name) or {}).get("direction")
        if prev_dir and prev_dir != fr.get("direction"):
            fr["flip"] = True
        st, scls = infer_sentiment(fr.get("direction"), fr.get("chg"), fr.get("vol_ratio"),
                                   futures_q.get("oi_chg"))
        fr["sentiment"] = st
        fr["sentiment_class"] = scls
        if fr.get("spike"):
            related = [n["title"] for n in news_items if n.get("ctime") and spike_ts and
                       abs(int(n["ctime"]) - spike_ts) <= 1800]
            spikes.append({"tf": name, "time": fr.get("time"), "vol_ratio": fr.get("vol_ratio"),
                           "chg": fr.get("chg"), "direction": fr.get("direction"),
                           "vol": fr.get("vol"), "news": related[:3]})

    state["spikes"] = spikes[-10:]
    c.last_update = time.time()
    return state, fail_count


# ---------------------------------------------------------------- 持久化
def save_state(c):
    try:
        os.makedirs(SAVE_DIR, exist_ok=True)
        with open(os.path.join(SAVE_DIR, c.key + ".json"), "w") as fh:
            json.dump({"ts": time.time(), "frames": c.state.get("frames") or {},
                       "klines": c.state.get("klines") or {}, "spot": c.state.get("spot")}, fh)
    except Exception:
        pass


def load_state(c):
    try:
        with open(os.path.join(SAVE_DIR, c.key + ".json")) as fh:
            d = json.load(fh)
        return d.get("frames") or {}, d.get("klines") or {}, d.get("spot")
    except Exception:
        return {}, {}, None


# ---------------------------------------------------------------- 预测准确率统计
ACC_RECORDS = []
ACC_SEQ = [0]
ACC_SEEN = set()
ACC_DIRTY = [False]
ACC_LAST_SAVE = [0.0]


def load_accuracy():
    global ACC_RECORDS, ACC_SEQ
    try:
        with open(ACC_FILE) as fh:
            d = json.load(fh)
        ACC_RECORDS = d.get("records") or []
        ACC_SEQ[0] = int(d.get("seq") or 0)
    except Exception:
        ACC_RECORDS = []
        ACC_SEQ[0] = 0
    ACC_SEEN.clear()
    for r in ACC_RECORDS:
        ACC_SEEN.add("%s|%s|%s" % (r.get("sym"), r.get("tf"), r.get("time")))


def save_accuracy(force=False):
    now = time.time()
    if not force and (not ACC_DIRTY[0] or now - ACC_LAST_SAVE[0] < 30):
        return
    try:
        with open(ACC_FILE, "w") as fh:
            json.dump({"seq": ACC_SEQ[0], "records": ACC_RECORDS[-ACC_MAX:]}, fh)
        ACC_LAST_SAVE[0] = now
        ACC_DIRTY[0] = False
    except Exception:
        pass


def _acc_record(sym, name, tf, bar_time, direction, conf, score, close,
                state, next_close=None, eval_ts=None, t0=None):
    key = "%s|%s|%s" % (sym, tf, bar_time)
    if key in ACC_SEEN:
        return None
    ACC_SEEN.add(key)
    ACC_SEQ[0] += 1
    rec = {"seq": ACC_SEQ[0], "sym": sym, "name": name, "tf": tf, "time": bar_time,
           "direction": direction, "conf": conf, "score": score, "close": close,
           "state": state, "next_close": next_close, "eval_ts": eval_ts,
           "t0": t0 if t0 is not None else time.time()}
    ACC_RECORDS.append(rec)
    if len(ACC_RECORDS) > ACC_MAX:
        drop = len(ACC_RECORDS) - ACC_MAX
        for r in ACC_RECORDS[:drop]:
            ACC_SEEN.discard("%s|%s|%s" % (r["sym"], r["tf"], r["time"]))
        del ACC_RECORDS[:drop]
    ACC_DIRTY[0] = True
    return rec


def _acc_outcome(direction, pred_close, next_close):
    if direction == "多":
        if next_close > pred_close * (1 + ACC_TOL):
            return "hit"
        if next_close < pred_close * (1 - ACC_TOL):
            return "miss"
        return "flat"
    if direction == "空":
        if next_close < pred_close * (1 - ACC_TOL):
            return "hit"
        if next_close > pred_close * (1 + ACC_TOL):
            return "miss"
        return "flat"
    return "flat"


def acc_sample_prices():
    now = time.time()
    for c in COMMODITIES:
        sp = c.last_spot
        if sp and sp.get("price") is not None:
            lst = ACC_PRICE_HIST.setdefault(c.key, [])
            if lst and now - lst[-1][0] < 5:
                continue
            lst.append([now, sp["price"]])
            if len(lst) > ACC_HIST_MAX:
                del lst[:len(lst) - ACC_HIST_MAX]


def acc_price_at(sym, target):
    """返回内存价格历史中距离目标时刻最近的一次采样 [epoch, price]"""
    lst = ACC_PRICE_HIST.get(sym)
    if not lst:
        return None
    best, bd = None, None
    for it in lst:
        d = abs(it[0] - target)
        if bd is None or d < bd:
            bd, best = d, it
    return best


def accuracy_loop():
    """后台线程: 采样价格并按时间判定未决预测"""
    while True:
        try:
            acc_sample_prices()
            now = time.time()
            for r in ACC_RECORDS:
                if r.get("state") != "pending":
                    continue
                t0 = r.get("t0") or 0
                if not t0:
                    continue
                horizon = ACC_HORIZONS.get(r.get("tf"), 60) * 60
                if now >= t0 + horizon:
                    pr = acc_price_at(r.get("sym"), t0 + horizon)
                    if pr is None:
                        continue
                    r["next_close"] = pr[1]
                    r["state"] = _acc_outcome(r.get("direction"), r.get("close"), pr[1])
                    r["eval_ts"] = now
                    ACC_DIRTY[0] = True
            save_accuracy()
        except Exception:
            pass
        time.sleep(10)


def replay_accuracy(c):
    """用当前K线窗口回放该商品各周期历史预测并判定; 最新一根留待后续判定"""
    klines = (c.state or {}).get("klines") or {}
    frames = (c.state or {}).get("frames") or {}
    for tf in ORDER:
        kl = klines.get(tf)
        if not kl or len(kl) < ACC_REPLAY_MIN + 1:
            continue
        n = len(kl)
        last_idx = n - 1  # 最后一根视为当前/未收盘, 不参与判定
        for i in range(ACC_REPLAY_MIN, last_idx):
            try:
                fr = analyze_bars(kl[:i + 1])
            except Exception:
                continue
            if not fr or fr.get("conf") is None or fr.get("conf") < ACC_MIN_CONF:
                continue
            if fr.get("direction") not in ("多", "空"):
                continue
            out = _acc_outcome(fr["direction"], fr["close"], kl[i + 1][4])
            _acc_record(c.key, c.name, tf, kl[i][0], fr["direction"], fr["conf"],
                        fr.get("score"), fr["close"], out, kl[i + 1][4], time.time())
        fr = frames.get(tf) or {}
        if (fr.get("conf") is not None and fr.get("conf") >= ACC_MIN_CONF
                and fr.get("direction") in ("多", "空")):
            _acc_record(c.key, c.name, tf, kl[last_idx][0], fr["direction"],
                        fr["conf"], fr.get("score"), kl[last_idx][4], "pending", t0=time.time())
    save_accuracy()


def _acc_day_of(rec):
    """把记录里的K线时间解析为预测日期(datetime); 短格式无年份时按当前年, 并处理跨年"""
    t = rec.get("time") or ""
    now = datetime.now()
    dt = None
    for fmt, inject in (("%m-%d %H:%M", True), ("%m-%d", True),
                        ("%Y-%m-%d %H:%M", False), ("%Y-%m-%d", False)):
        try:
            if inject:
                base = datetime.strptime(t, fmt).replace(year=now.year)
                if base - now > timedelta(days=1):
                    base = base.replace(year=now.year - 1)
                dt = base
            else:
                dt = datetime.strptime(t, fmt)
            break
        except (ValueError, TypeError):
            continue
    return dt


def _acc_stat(recs):
    hits = misses = flat = pending = expired = 0
    for r in recs:
        s = r.get("state")
        if s == "hit":
            hits += 1
        elif s == "miss":
            misses += 1
        elif s == "flat":
            flat += 1
        elif s == "pending":
            pending += 1
        elif s == "expired":
            expired += 1
    tot = hits + misses
    return {"samples": len(recs), "resolved": tot, "hits": hits, "misses": misses,
            "flat": flat, "pending": pending, "expired": expired,
            "accuracy": round(hits / tot * 100, 1) if tot else None}


def accuracy_payload(min_conf=ACC_MIN_CONF, days=None, start=None, end=None):
    now = datetime.now()
    cutoff = None
    if days:
        try:
            cutoff = (now - timedelta(days=int(days) - 1)).replace(hour=0, minute=0, second=0, microsecond=0)
        except (ValueError, TypeError):
            cutoff = None
    start_dt = end_dt = None
    try:
        if start:
            start_dt = datetime.strptime(start, "%Y-%m-%d")
    except (ValueError, TypeError):
        start_dt = None
    try:
        if end:
            end_dt = datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    except (ValueError, TypeError):
        end_dt = None
    if start_dt is not None or end_dt is not None:
        cutoff = None
    recs = []
    for r in ACC_RECORDS:
        if (r.get("conf") or 0) < min_conf:
            continue
        if start_dt is not None or end_dt is not None or cutoff is not None:
            dt = _acc_day_of(r)
            if dt is None:
                continue
            if cutoff is not None and dt < cutoff:
                continue
            if start_dt is not None and dt < start_dt:
                continue
            if end_dt is not None and dt > end_dt:
                continue
        recs.append(r)
    by_tf = {tf: [] for tf in ORDER}
    by_dir = {"多": [], "空": []}
    by_conf = {"70": [], "80": [], "90": []}
    by_sym = {}
    for r in recs:
        by_tf.setdefault(r.get("tf"), []).append(r)
        if r.get("direction") in by_dir:
            by_dir[r["direction"]].append(r)
        cf = r.get("conf", 0)
        bucket = "70" if cf < 80 else "80" if cf < 90 else "90"
        by_conf[bucket].append(r)
        by_sym.setdefault(r.get("sym"), []).append(r)
    recent = sorted(recs, key=lambda x: -x.get("seq", 0))[:40]
    return {
        "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
        "min_conf": min_conf,
        "days": days,
        "start": start,
        "end": end,
        "overall": _acc_stat(recs),
        "by_tf": {tf: _acc_stat(by_tf.get(tf) or []) for tf in ORDER},
        "by_dir": {d: _acc_stat(by_dir[d]) for d in ("多", "空")},
        "by_conf": {b: _acc_stat(by_conf[b]) for b in ("70", "80", "90")},
        "by_sym": {sym: _acc_stat(lst) for sym, lst in by_sym.items()},
        "names": {c.key: c.name for c in COMMODITIES},
        "recent": recent,
    }



# ---------------------------------------------------------------- 后台更新(轮询)
ROTATE_IDX = [0]
ADAPTIVE = [0]        # 0=15s并行 1=30s 2=60s串行
ADAPTIVE_FAILS = [0]


def update_commodity(c):
    with c.build_lock:
        st, fails = build_commodity_state(c)
    c.state = st
    save_state(c)
    try:
        replay_accuracy(c)
    except Exception as e:
        print("accuracy error:", e, flush=True)
    return fails


def _build_async(c):
    try:
        update_commodity(c)
    finally:
        c.building = False


def updater_loop():
    global GLOBAL_NEWS, GLOBAL_ALERTS, DXY
    last_dxy = None
    counter = 0
    while True:
        try:
            with NEWS_LOCK:
                news = fetch_news()
                alerts = check_news_alerts(news)
            GLOBAL_NEWS = news
            GLOBAL_ALERTS = alerts
            try:
                dxy = fetch_dxy()
                DXY = {"value": round(dxy, 2),
                       "dir": "up" if last_dxy is not None and dxy > last_dxy else
                              "down" if last_dxy is not None and dxy < last_dxy else "flat"}
                last_dxy = dxy
            except Exception:
                pass

            with ACTIVE_LOCK:
                active = ACTIVE
            ac = BY_KEY.get(active) or COMMODITIES[0]
            fails = update_commodity(ac)
            counter += 1
            if counter % 3 == 0:
                bg = COMMODITIES[ROTATE_IDX[0] % len(COMMODITIES)]
                ROTATE_IDX[0] += 1
                if bg.key != active:
                    update_commodity(bg)
            # 自适应降级: 连续失败→降速; 恢复→提速
            if fails >= 3:
                ADAPTIVE_FAILS[0] += 1
            elif fails == 0 and ADAPTIVE_FAILS[0] > 0:
                ADAPTIVE_FAILS[0] -= 1
            if ADAPTIVE_FAILS[0] >= 4:
                ADAPTIVE[0] = 2
            elif ADAPTIVE_FAILS[0] >= 2:
                ADAPTIVE[0] = 1
            elif ADAPTIVE_FAILS[0] == 0 and ADAPTIVE[0] > 0:
                ADAPTIVE[0] -= 1
            print("%s 更新 %s (%d失败/自适应%d)" % (time.strftime("%H:%M:%S"), ac.name, fails, ADAPTIVE[0]), flush=True)
        except Exception as e:
            print("update error:", e, flush=True)
        time.sleep([15, 30, 60][ADAPTIVE[0]])


# ---------------------------------------------------------------- 首页聚合
def home_payload():
    coms = []
    high_conf = []
    for c in COMMODITIES:
        spot = c.last_spot
        frames = c.state.get("frames") or {}
        chg = round((spot["price"] / spot["prev"] - 1) * 100, 2) if spot and spot["prev"] else None
        dirs = {k: (frames.get(k) or {}).get("direction") for k in ORDER}
        confs = {k: (frames.get(k) or {}).get("conf") for k in ORDER}
        has = bool(spot) or bool(frames)
        for k in ORDER:
            f = frames.get(k)
            if f and f.get("conf") is not None and f["conf"] >= 70 and f.get("direction") in ("多", "空"):
                high_conf.append({"name": c.name, "key": c.key, "tf": k, "dir": f["direction"],
                                  "conf": f["conf"]})
        coms.append({"key": c.key, "name": c.name, "cat": c.cat, "unit": c.unit, "sign": c.sign,
                     "decimals": c.decimals,
                     "price": spot["price"] if spot else None,
                     "chg": chg,
                     "high": spot["high"] if spot else None,
                     "low": spot["low"] if spot else None,
                     "dirs": dirs, "confs": confs, "has": has,
                     "last_update": round(c.last_update, 0),
                     "stale_ok": bool(c.state.get("frames"))})
    high_conf.sort(key=lambda x: -x["conf"])
    return {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "dxy": DXY,
            "news": GLOBAL_NEWS, "alerts": GLOBAL_ALERTS,
            "commodities": coms, "high_conf": high_conf[:10]}


def detail_payload(c):
    st = dict(c.state)
    st["meta"] = {"key": c.key, "name": c.name, "cat": c.cat,
                  "unit": c.unit, "sign": c.sign, "decimals": c.decimals}
    st["dxy"] = DXY
    st["news"] = GLOBAL_NEWS
    st["alerts"] = GLOBAL_ALERTS
    st["ts"] = time.strftime("%Y-%m-%d %H:%M:%S")
    if c.last_spot and not st.get("spot"):
        st["spot"] = {"price": c.last_spot["price"], "high": c.last_spot["high"],
                      "low": c.last_spot["low"],
                      "chg": round((c.last_spot["price"] / c.last_spot["prev"] - 1) * 100, 2)
                      if c.last_spot["prev"] else 0.0,
                      "ts": c.last_spot["ts"]}
    # 合并最新M1/M5本地tick(不触发全量重建, 短周期实时)
    try:
        for name, grp in (("M1", 1), ("M5", 5)):
            bars = c_m1_series(c)
            kb = resample_bars(bars, grp) if grp > 1 else bars
            if kb:
                st.setdefault("klines", {})[name] = [
                    (time.strftime("%m-%d %H:%M", time.localtime(b[0])), b[1], b[2], b[3], b[4], b[5])
                    for b in kb[-KLINE_LIMIT:]]
                if len(kb) >= 20:
                    std = [(time.strftime("%m-%d %H:%M", time.localtime(b[0])), b[1], b[2], b[3], b[4], b[5])
                           for b in kb]
                    st.setdefault("frames", {})[name] = analyze_bars(std)
    except Exception:
        pass
    return st


# ---------------------------------------------------------------- 模拟交易引擎
TRADE_FILE = os.path.join(BASE_DIR, "trade_state.json")
TRADE_LOCK = threading.Lock()
INITIAL_CASH = 100000.0
COMMISSION_RATE = 0.0003          # 手续费: 名义价值的0.03%/笔
MULT = {"gold": 100, "silver": 5000, "oil": 1000, "brent": 1000, "natgas": 10000,
        "copper": 250, "soybean": 50, "corn": 50, "wheat": 50, "cotton": 500,
        "soyoil": 600, "soymeal": 100, "coffee": 375, "cocoa": 10}
ORDER_CNT = [0]


# ---------------------------------------------------------------- 资金流向(逐笔主动买卖)
# 主力/散户动态划分: 取当日单笔成交额的第80分位作为主力阈值(最大20%大单=主力, 其余=散户)
FUNDFLOW_INTERVAL = [15]            # 轮询间隔秒, 自适应 15→30→60
FUNDFLOW_FILE = os.path.join(BASE_DIR, "fundflow_state.json")
FUNDFLOW_LOCK = threading.Lock()
FUNDFLOW = {}                       # {sym: {date,in,out,main:{in,out},retail:{in,out},minute,amts,seen,seen_order,last_ts,fails}}


def _ff_today():
    return time.strftime("%Y-%m-%d")


def _ff_new():
    return {"date": _ff_today(), "in": 0.0, "out": 0.0,
            "main": {"in": 0.0, "out": 0.0}, "retail": {"in": 0.0, "out": 0.0},
            "minute": {}, "amts": collections.deque(maxlen=2000),
            "seen": {}, "seen_order": collections.deque(maxlen=5000),
            "last_ts": None, "fails": 0}


def _ff_p80(arr):
    """第80分位(最大20%大单的边界), 无数据返回None"""
    if not arr:
        return None
    s = sorted(arr)
    return s[int(len(s) * 0.8) - 1]


def save_fundflow():
    try:
        with FUNDFLOW_LOCK:
            d = {}
            for k, s in FUNDFLOW.items():
                d[k] = {"date": s["date"], "in": s["in"], "out": s["out"],
                        "main": s["main"], "retail": s["retail"],
                        "minute": s["minute"], "last_ts": s["last_ts"],
                        "amts": list(s["amts"])[-800:],
                        "seen": list(s["seen_order"])[-800:]}
            body = json.dumps(d)
        with open(FUNDFLOW_FILE, "w") as fh:
            fh.write(body)
    except Exception:
        pass


def load_fundflow():
    global FUNDFLOW
    try:
        with open(FUNDFLOW_FILE) as fh:
            d = json.loads(fh.read())
        today = _ff_today()
        for k, s in d.items():
            if s.get("date") != today:
                continue
            s.setdefault("main", {"in": 0.0, "out": 0.0})
            s.setdefault("retail", {"in": 0.0, "out": 0.0})
            s.setdefault("minute", {})
            saved_seen = s.get("seen", [])
            saved_seen = list(saved_seen) if isinstance(saved_seen, list) else []
            s["amts"] = collections.deque(s.get("amts", [])[:800], maxlen=2000)
            s["seen"] = {}
            s["seen_order"] = collections.deque(maxlen=5000)
            for key in saved_seen:
                s["seen"][key] = True
                s["seen_order"].append(key)
            FUNDFLOW[k] = s
    except Exception:
        pass


def _mm(t):
    try:
        return int(t[:2]) * 60 + int(t[3:5])
    except Exception:
        return 0


def _back_minutes(prev, cur):
    """cur 相对 prev 回退的分钟数; 前进(同一时段)返回0"""
    a, b = _mm(prev), _mm(cur)
    if b >= a:
        return 0
    return a - b


def fetch_tick_details(c):
    """抓取单商品最新逐笔成交, 返回 [(HH:MM:SS, price, vol, dir)], dir: 1=主动买 2=主动卖"""
    url = ("https://push2.eastmoney.com/api/qt/stock/details/get?cb=jQuery"
           "&secid=%s&fields1=f1,f2,f3,f4,f5&fields2=f51,f52,f53,f54,f55&pos=-100" % c.secid)
    raw = None
    for _ in range(2):
        try:
            raw = http_get(url, headers={"Referer": "https://quote.eastmoney.com/"}, timeout=8)
            break
        except Exception:
            time.sleep(0.8)
    if not raw:
        return None
    m = re.search(r"jQuery\((\{.*\})\)", raw)
    if not m:
        return None
    try:
        d = json.loads(m.group(1)).get("data")
    except Exception:
        return None
    if not d or not d.get("details"):
        return None
    out = []
    for s in d["details"]:
        p = s.split(",")
        if len(p) < 5:
            continue
        try:
            t = p[0].strip()
            price = float(p[1])
            vol = float(p[2])
            side = int(p[4])
        except (ValueError, IndexError):
            continue
        if side not in (1, 2):
            continue
        out.append((t, price, vol, side))
    return out


def _ff_tick(c):
    """单商品资金流增量累计, 返回 True=成功"""
    with FUNDFLOW_LOCK:
        s = FUNDFLOW.get(c.key)
        if s is None or s["date"] != _ff_today():
            s = _ff_new()
            FUNDFLOW[c.key] = s
    rows = fetch_tick_details(c)
    if not rows:
        with FUNDFLOW_LOCK:
            s["fails"] += 1
        return False
    mult = MULT.get(c.key, 1)
    with FUNDFLOW_LOCK:
        s["fails"] = 0
        cur_last = rows[-1][0]
        if s["last_ts"] and _back_minutes(s["last_ts"], cur_last) > 30:
            ns = _ff_new()
            ns["last_ts"] = cur_last
            FUNDFLOW[c.key] = ns
            s = ns
        thr = _ff_p80(s["amts"])
        if thr is None:
            thr = rows[-1][1] * mult * 3 if rows else 1.0
        for (t, price, vol, side) in rows:
            key = "%s|%.6f|%.3f|%d" % (t, price, vol, side)
            if key in s["seen"]:
                continue
            s["seen"][key] = True
            s["seen_order"].append(key)
            amt = price * mult * vol
            s["amts"].append(amt)
            bucket = t[:5]
            if bucket not in s["minute"]:
                s["minute"][bucket] = [0.0, 0.0]
            if side == 1:
                s["in"] += amt
                s["minute"][bucket][0] += amt
                if amt >= thr:
                    s["main"]["in"] += amt
                else:
                    s["retail"]["in"] += amt
            else:
                s["out"] += amt
                s["minute"][bucket][1] += amt
                if amt >= thr:
                    s["main"]["out"] += amt
                else:
                    s["retail"]["out"] += amt
        while len(s["seen"]) > 4000 and s["seen_order"]:
            old = s["seen_order"].popleft()
            s["seen"].pop(old, None)
        s["last_ts"] = cur_last
    save_fundflow()
    return True


def fundflow_loop():
    """资金流后台轮询: 14商品错峰抓取, 自适应降级"""
    while True:
        interval = FUNDFLOW_INTERVAL[0]
        start = time.time()
        fails = 0
        for c in COMMODITIES:
            try:
                if not _ff_tick(c):
                    fails += 1
            except Exception:
                fails += 1
            time.sleep(0.5)
        if fails >= max(2, len(COMMODITIES) * 3 // 5):
            FUNDFLOW_INTERVAL[0] = min(60, FUNDFLOW_INTERVAL[0] * 2)
        elif fails == 0 and FUNDFLOW_INTERVAL[0] > 15:
            FUNDFLOW_INTERVAL[0] = 15
        elapsed = time.time() - start
        if elapsed < interval:
            time.sleep(interval - elapsed)


def fundflow_payload(sym):
    with FUNDFLOW_LOCK:
        s = FUNDFLOW.get(sym)
        if s is None or s["date"] != _ff_today():
            s = None
        else:
            data = (s["in"], s["out"], dict(s["main"]), dict(s["retail"]),
                    dict(s["minute"]))
    empty = {"date": _ff_today(), "in": 0.0, "out": 0.0, "net": 0.0, "ratio": None,
             "main": {"in": 0.0, "out": 0.0, "net": 0.0, "share": 0.0},
             "retail": {"in": 0.0, "out": 0.0, "net": 0.0, "share": 0.0},
             "minute": []}
    if s is None:
        return empty
    s_in, s_out, main, retail, minute = data
    total = s_in + s_out
    main_total = main["in"] + main["out"]
    mn = main["in"] - main["out"]
    rn = retail["in"] - retail["out"]
    return {"date": _ff_today(),
            "in": round(s_in), "out": round(s_out),
            "net": round(s_in - s_out),
            "ratio": round(s_in / s_out, 2) if s_out else None,
            "main": {"in": round(main["in"]), "out": round(main["out"]),
                     "net": round(mn), "share": round(main_total / total * 100, 1) if total else 0.0},
            "retail": {"in": round(retail["in"]), "out": round(retail["out"]),
                       "net": round(rn),
                       "share": round((1 - main_total / total) * 100, 1) if total else 0.0},
            "minute": [[k, round(v[0]), round(v[1])] for k, v in sorted(minute.items())]}

TRADE = {"cash": INITIAL_CASH, "positions": [], "pending": [], "history": [],
         "realized": 0.0, "wins": 0, "losses": 0}


def save_trade():
    try:
        with TRADE_LOCK:
            d = json.dumps(TRADE)
        with open(TRADE_FILE, "w") as fh:
            fh.write(d)
    except Exception:
        pass


def load_trade():
    global TRADE
    try:
        with open(TRADE_FILE) as fh:
            TRADE = json.loads(fh.read())
        TRADE.setdefault("cash", INITIAL_CASH)
        TRADE.setdefault("positions", [])
        TRADE.setdefault("pending", [])
        TRADE.setdefault("history", [])
        TRADE.setdefault("realized", 0.0)
        TRADE.setdefault("wins", 0)
        TRADE.setdefault("losses", 0)
    except Exception:
        pass


def reset_trade():
    with TRADE_LOCK:
        TRADE["cash"] = INITIAL_CASH
        TRADE["positions"] = []
        TRADE["pending"] = []
        TRADE["history"] = []
        TRADE["realized"] = 0.0
        TRADE["wins"] = 0
        TRADE["losses"] = 0
    save_trade()
    return {"ok": True, "msg": "账户已重置"}


def _price_of(sym):
    c = BY_KEY.get(sym)
    return c.last_spot["price"] if c and c.last_spot else None


def _pnl(pos, price):
    diff = (price - pos["entry"]) if pos["side"] == "buy" else (pos["entry"] - price)
    return diff * MULT.get(pos["sym"], 1) * pos["qty"]


def _margin(price, mult, qty, lev):
    return price * mult * qty / max(1, lev)


def place_order(sym, side, qty, lev, otype, limit_price=None, source="manual"):
    qty = int(qty or 0)
    lev = int(lev or 10)
    if qty <= 0:
        return {"ok": False, "msg": "手数必须≥1"}
    if lev < 1 or lev > 50:
        return {"ok": False, "msg": "杠杆范围1~50"}
    if side not in ("buy", "sell"):
        return {"ok": False, "msg": "方向错误"}
    c = BY_KEY.get(sym)
    if not c:
        return {"ok": False, "msg": "未知商品"}
    price = _price_of(sym)
    if not price:
        return {"ok": False, "msg": "暂无该商品行情"}
    mult = MULT.get(sym, 1)
    margin = _margin(price, mult, qty, lev)
    fee = price * mult * qty * COMMISSION_RATE
    result = None
    with TRADE_LOCK:
        if TRADE["cash"] < margin + fee:
            return {"ok": False, "msg": "可用资金不足(需$%.0f)" % (margin + fee)}
        if otype == "limit" and limit_price:
            lp = float(limit_price)
            fill = (side == "buy" and price <= lp) or (side == "sell" and price >= lp)
            if not fill:
                ORDER_CNT[0] += 1
                TRADE["cash"] -= margin + fee
                TRADE["pending"].append({"id": ORDER_CNT[0], "sym": sym, "side": side, "qty": qty,
                                         "lev": lev, "limit_price": lp, "fee": fee,
                                         "created": time.strftime("%m-%d %H:%M")})
                result = {"ok": True, "msg": "限价单已挂出, 到价自动成交"}
                oid = ORDER_CNT[0]
        if result is None:
            ORDER_CNT[0] += 1
            TRADE["cash"] -= margin + fee
            TRADE["positions"].append({"id": ORDER_CNT[0], "sym": sym, "side": side, "qty": qty,
                                       "lev": lev, "entry": price, "fee": fee, "source": source,
                                       "open_time": time.strftime("%m-%d %H:%M")})
            result = {"ok": True, "msg": "%s %s %d手 已成交 @%.2f" % (c.name, "买入" if side == "buy" else "卖出", qty, price)}
    save_trade()
    return result


def close_position(pos_id, manual=False):
    with TRADE_LOCK:
        pos = next((p for p in TRADE["positions"] if p["id"] == int(pos_id)), None)
        if not pos:
            return {"ok": False, "msg": "持仓不存在"}
        price = _price_of(pos["sym"])
        if not price:
            return {"ok": False, "msg": "暂无行情"}
        mult = MULT.get(pos["sym"], 1)
        pnl = _pnl(pos, price)
        fee = price * mult * pos["qty"] * COMMISSION_RATE
        margin = _margin(pos["entry"], mult, pos["qty"], pos["lev"])
        TRADE["cash"] += margin + pnl - fee
        TRADE["realized"] += pnl
        if pnl >= 0:
            TRADE["wins"] += 1
        else:
            TRADE["losses"] += 1
        TRADE["history"].insert(0, {"sym": pos["sym"], "side": pos["side"], "qty": pos["qty"],
                                    "entry": pos["entry"], "exit": price, "pnl": round(pnl, 2),
                                    "source": pos.get("source", "manual"),
                                    "close_time": time.strftime("%m-%d %H:%M")})
        TRADE["history"] = TRADE["history"][:100]
        TRADE["positions"].remove(pos)
        src = pos.get("source", "manual")
        side_txt = "多" if pos["side"] == "buy" else "空"
    save_trade()
    name = BY_KEY.get(pos["sym"]).name if BY_KEY.get(pos["sym"]) else pos["sym"]
    # 手动平仓机器人持仓 → 记入下单记录
    if src == "bot" and manual:
        log_bot_action(pos["sym"], BOT_TF, "手动平%s" % side_txt, price, pnl, qty=pos["qty"])
    return {"ok": True, "msg": "%s 已平仓, 盈亏 $%.2f" % (name, pnl)}


def close_all_bot_positions():
    """一键平掉所有机器人持仓(不影响手动仓)"""
    with TRADE_LOCK:
        ids = [p["id"] for p in TRADE["positions"] if p.get("source") == "bot"]
    if not ids:
        return {"ok": True, "msg": "无机器人持仓"}
    total = 0.0
    msgs = []
    for i in ids:
        r = close_position(i, manual=True)
        msgs.append(r["msg"])
        if "盈亏" in r.get("msg", ""):
            try:
                total += float(r["msg"].split("$")[-1])
            except Exception:
                pass
    return {"ok": True, "msg": "已平仓机器人持仓 %d 笔, 合计盈亏 $%.2f" % (len(msgs), total)}


def close_all_positions():
    with TRADE_LOCK:
        ids = [p["id"] for p in TRADE["positions"]]
    msgs = []
    for i in ids:
        r = close_position(i)
        msgs.append(r["msg"])
    return {"ok": True, "msg": "已全部平仓: " + " | ".join(msgs)}


def cancel_order(oid):
    ok = False
    with TRADE_LOCK:
        for i, p in enumerate(TRADE["pending"]):
            if p["id"] == int(oid):
                price = _price_of(p["sym"])
                mult = MULT.get(p["sym"], 1)
                margin = _margin(price or p["limit_price"], mult, p["qty"], p["lev"])
                TRADE["cash"] += margin + p["fee"]
                del TRADE["pending"][i]
                ok = True
                break
    if ok:
        save_trade()
        return {"ok": True, "msg": "已撤单"}
    return {"ok": False, "msg": "挂单不存在"}


def check_limit_orders():
    """限价单成交检查(由tick循环每3秒调用)"""
    with TRADE_LOCK:
        pending = list(TRADE["pending"])
    filled = []
    for p in pending:
        price = _price_of(p["sym"])
        if not price:
            continue
        if (p["side"] == "buy" and price <= p["limit_price"]) or \
           (p["side"] == "sell" and price >= p["limit_price"]):
            filled.append((p, price))
    if not filled:
        return
    with TRADE_LOCK:
        for p, price in filled:
            if p not in TRADE["pending"]:
                continue
            TRADE["pending"].remove(p)
            p = dict(p)
            p["entry"] = price
            p["open_time"] = time.strftime("%m-%d %H:%M")
            TRADE["positions"].append(p)
    save_trade()


def account_payload():
    with TRADE_LOCK:
        cash = TRADE["cash"]
        positions = list(TRADE["positions"])
        pending = list(TRADE["pending"])
        history = list(TRADE["history"])
        realized = TRADE["realized"]
        wins, losses = TRADE["wins"], TRADE["losses"]
    margin_used = 0.0
    unrealized = 0.0
    pos_out = []
    for p in positions:
        price = _price_of(p["sym"]) or p["entry"]
        mult = MULT.get(p["sym"], 1)
        margin = _margin(p["entry"], mult, p["qty"], p["lev"])
        pnl = _pnl(p, price)
        margin_used += margin
        unrealized += pnl
        pos_out.append({"id": p["id"], "sym": p["sym"],
                        "name": (BY_KEY.get(p["sym"]).name if BY_KEY.get(p["sym"]) else p["sym"]),
                        "side": p["side"], "qty": p["qty"], "lev": p["lev"], "source": p.get("source", "manual"),
                        "entry": round(p["entry"], 2), "current": round(price, 2),
                        "pnl": round(pnl, 2), "margin": round(margin, 2),
                        "open_time": p["open_time"]})
    pend_out = []
    for p in pending:
        price = _price_of(p["sym"])
        pend_out.append({"id": p["id"], "sym": p["sym"],
                         "name": (BY_KEY.get(p["sym"]).name if BY_KEY.get(p["sym"]) else p["sym"]),
                         "side": p["side"], "qty": p["qty"], "lev": p["lev"],
                         "limit_price": p["limit_price"],
                         "current": round(price, 2) if price else None,
                         "created": p["created"]})
    equity = cash + margin_used + unrealized
    total = (wins + losses) if (wins + losses) > 0 else 0
    return {"initial": INITIAL_CASH, "cash": round(cash, 2), "margin_used": round(margin_used, 2),
            "unrealized": round(unrealized, 2), "equity": round(equity, 2),
            "realized": round(realized, 2), "available": round(cash, 2),
            "total_pnl": round(equity - INITIAL_CASH, 2),
            "wins": wins, "losses": losses,
            "win_rate": round(wins * 100.0 / total, 1) if total else 0,
            "positions": pos_out, "pending": pend_out,
            "history": [{"sym": h["sym"], "side": h["side"], "qty": h["qty"],
                         "entry": h["entry"], "exit": h["exit"], "pnl": h["pnl"],
                         "name": (BY_KEY.get(h["sym"]).name if BY_KEY.get(h["sym"]) else h["sym"]),
                         "close_time": h["close_time"]} for h in history[:20]]}


# ---------------------------------------------------------------- 邮箱提醒引擎
ALERT_FILE = os.path.join(BASE_DIR, "alert_config.json")
SENT_FILE = os.path.join(BASE_DIR, "alert_sent.json")
DEFAULT_ALERT = {
    "enabled": True,
    "smtp": {"host": "smtp.qq.com", "port": 465, "user": "", "auth": "", "to": ""},
    "triggers": {"high_conf": True, "volume_news": True, "daily": True,
                 "flip": False, "breakout": False},
    "high_conf_threshold": 80,
    "check_interval_min": 5,
    "daily_time": "20:00",
}
_ALERT_LOCK = threading.Lock()
_daily_sent_today = False
prev_dirs = {}


def load_alert_config():
    cfg = dict(DEFAULT_ALERT)
    cfg["smtp"] = dict(DEFAULT_ALERT["smtp"])
    cfg["triggers"] = dict(DEFAULT_ALERT["triggers"])
    try:
        with open(ALERT_FILE) as fh:
            d = json.load(fh)
        for k in cfg:
            if k in d:
                if isinstance(cfg[k], dict) and isinstance(d[k], dict):
                    cfg[k].update(d[k])
                else:
                    cfg[k] = d[k]
    except Exception:
        pass
    return cfg


def save_alert_config(cfg):
    with _ALERT_LOCK:
        with open(ALERT_FILE, "w") as fh:
            json.dump(cfg, fh, ensure_ascii=False, indent=2)


def load_sent():
    try:
        return set(json.load(open(SENT_FILE)).get("sent", []))
    except Exception:
        return set()


def save_sent(s):
    try:
        with open(SENT_FILE, "w") as fh:
            json.dump({"sent": list(s)[-600:]}, fh)
    except Exception:
        pass


def send_email(subject, html_body):
    cfg = load_alert_config()
    smtp = cfg["smtp"]
    if not smtp.get("auth") or not smtp.get("user"):
        return False, "SMTP未配置(请在提醒设置里填写授权码)"
    try:
        if int(smtp.get("port", 465)) == 465:
            server = smtplib.SMTP_SSL(smtp["host"], int(smtp["port"]), timeout=15)
        else:
            server = smtplib.SMTP(smtp["host"], int(smtp["port"]), timeout=15)
            server.starttls()
        server.login(smtp["user"], smtp["auth"])
        msg = MIMEText(html_body, "html", "utf-8")
        msg["Subject"] = Header(subject, "utf-8")
        msg["From"] = smtp["user"]
        msg["To"] = smtp.get("to") or smtp["user"]
        server.sendmail(smtp["user"], [msg["To"]], msg.as_string())
        server.quit()
        return True, "邮件已发送"
    except Exception as e:
        return False, "发送失败: %s" % e


def _high_conf_signals(cfg):
    items = []
    th = cfg.get("high_conf_threshold", 80)
    for c in COMMODITIES:
        frames = c.state.get("frames") or {}
        for tf in ORDER:
            f = frames.get(tf)
            if f and f.get("conf") is not None and f["conf"] >= th and f.get("direction") in ("多", "空"):
                items.append((c.name, tf, f["direction"], f["conf"], f.get("close")))
    return items


def _volume_news_signals():
    items = []
    for c in COMMODITIES:
        for sp in (c.state.get("spikes") or [])[:3]:
            if sp.get("news"):
                items.append((c.name, sp.get("time"), sp.get("vol_ratio"), sp.get("direction"),
                              sp["news"][:2]))
    return items


def alert_scan():
    global _daily_sent_today
    cfg = load_alert_config()
    if not cfg.get("enabled"):
        return
    if not cfg["smtp"].get("auth"):
        return
    sent = load_sent()
    msgs = []

    if cfg["triggers"].get("high_conf"):
        fresh = []
        for (nm, tf, d, conf, close) in _high_conf_signals(cfg):
            key = "hc:%s:%s:%s" % (nm, tf, d)
            if key not in sent:
                fresh.append((nm, tf, d, conf, close))
                sent.add(key)
        if fresh:
            rows = "".join("<tr><td>%s</td><td>%s</td><td style='color:%s'>%s</td><td><b>%s%%</b></td><td>%s</td></tr>"
                           % (esc_html(nm), tf, "#ff4d4f" if d == "多" else "#3fb950", d, conf,
                              esc_html(str(close)))
                           for nm, tf, d, conf, close in fresh)
            msgs.append(("高置信度信号", "<h3>⚠️ 高置信度信号(≥%s%%)</h3><table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;color:#333'>"
                         "<tr><th>商品</th><th>周期</th><th>方向</th><th>置信度</th><th>现价</th></tr>%s</table>" % (th, rows)))

    if cfg["triggers"].get("volume_news"):
        fresh = []
        for (nm, tm, vr, d, news) in _volume_news_signals():
            key = "vn:%s:%s" % (nm, tm)
            if key not in sent:
                fresh.append((nm, tm, vr, d, news))
                sent.add(key)
        if fresh:
            rows = "".join("<li><b>%s</b> %s · 量能%s倍 · 方向%s<br/><span style='color:#888'>关联新闻: %s</span></li>"
                           % (esc_html(nm), esc_html(tm), vr, esc_html(d), esc_html(" | ".join(news)))
                           for nm, tm, vr, d, news in fresh)
            msgs.append(("放量异动+新闻", "<h3>⚡ 放量异动(关联重要新闻)</h3><ul>%s</ul>" % rows))

    for subject, body in msgs:
        html = "<div style='font-family:sans-serif;max-width:640px;margin:auto'>%s<p style='color:#999;font-size:12px'>大宗商品预测 · %s</p></div>" % (body, time.strftime("%Y-%m-%d %H:%M"))
        ok, r = send_email("【商品提醒】" + subject, html)
        print("%s 提醒邮件 %s: %s" % (time.strftime("%H:%M"), subject, r), flush=True)
        if not ok:
            break
    save_sent(sent)


def alert_daily():
    global _daily_sent_today
    cfg = load_alert_config()
    now = time.strftime("%H:%M")
    if now == cfg.get("daily_time") and cfg.get("triggers", {}).get("daily") and not _daily_sent_today:
        _daily_sent_today = True
        signals = _high_conf_signals(cfg)
        if not signals:
            return
        rows = "".join("<tr><td>%s</td><td>%s</td><td style='color:%s'>%s</td><td><b>%s%%</b></td></tr>"
                       % (esc_html(nm), tf, "#ff4d4f" if d == "多" else "#3fb950", d, conf)
                       for nm, tf, d, conf, _ in signals)
        body = "<h2>📊 每日简报</h2><p>当前高置信度信号汇总(≥%s%%)：</p><table border='1' cellpadding='6' cellspacing='0' style='border-collapse:collapse;color:#333'><tr><th>商品</th><th>周期</th><th>方向</th><th>置信度</th></tr>%s</table>" % (cfg.get("high_conf_threshold", 80), rows)
        send_email("【每日简报】高置信度信号汇总", body)
    elif now != cfg.get("daily_time"):
        _daily_sent_today = False


def esc_html(s):
    return html.escape(str(s))


def alert_loop():
    global _daily_sent_today
    while True:
        try:
            alert_scan()
        except Exception as e:
            print("alert scan error:", e, flush=True)
        try:
            alert_daily()
        except Exception as e:
            print("alert daily error:", e, flush=True)
        time.sleep(30)


def alert_config_payload():
    cfg = load_alert_config()
    return {"enabled": cfg.get("enabled", True),
            "triggers": cfg.get("triggers", {}),
            "high_conf_threshold": cfg.get("high_conf_threshold", 80),
            "check_interval_min": cfg.get("check_interval_min", 5),
            "daily_time": cfg.get("daily_time", "20:00"),
            "smtp_configured": bool(cfg["smtp"].get("auth")),
            "smtp_host": cfg["smtp"].get("host", ""),
            "smtp_user": cfg["smtp"].get("user", ""),
            "smtp_to": cfg["smtp"].get("to", "")}


def alert_config_update(data):
    cfg = load_alert_config()
    if "enabled" in data:
        cfg["enabled"] = bool(data["enabled"])
    if "triggers" in data and isinstance(data["triggers"], dict):
        for k in ("high_conf", "volume_news", "daily", "flip", "breakout"):
            if k in data["triggers"]:
                cfg["triggers"][k] = bool(data["triggers"][k])
    if "high_conf_threshold" in data:
        cfg["high_conf_threshold"] = int(data["high_conf_threshold"])
    if "check_interval_min" in data:
        cfg["check_interval_min"] = int(data["check_interval_min"])
    if "daily_time" in data:
        cfg["daily_time"] = data["daily_time"]
    smtp = data.get("smtp") or {}
    for k in ("host", "port", "user", "auth", "to"):
        if k in smtp:
            cfg["smtp"][k] = smtp[k]
    save_alert_config(cfg)
    return {"ok": True, "msg": "提醒设置已保存"}


# ---------------------------------------------------------------- 自动交易(回测+机器人)
BOT_FILE = os.path.join(BASE_DIR, "bot_config.json")
BOT_TF = "H1"   # 机器人交易周期(跟随页面选中)
BOT_SIG_PROC = {}  # sym -> 已处理的最新神龙信号时间
BOT_LOG = []
BOT_LOG_FILE = os.path.join(BASE_DIR, "bot_log.json")
BOT_EQUITY = []
BOT_EQUITY_FILE = os.path.join(BASE_DIR, "bot_equity.json")


def load_bot_logs():
    global BOT_LOG, BOT_EQUITY
    try:
        with open(BOT_LOG_FILE) as fh:
            BOT_LOG = json.load(fh)
    except Exception:
        BOT_LOG = []
    try:
        with open(BOT_EQUITY_FILE) as fh:
            BOT_EQUITY = json.load(fh)
    except Exception:
        BOT_EQUITY = []
    # 补记当前已开的机器人持仓到下单记录
    existing = set((l.get("sym"), l.get("price")) for l in BOT_LOG if l.get("action", "").startswith("开"))
    with TRADE_LOCK:
        for p in TRADE["positions"]:
            if p.get("source") == "bot" and (p["sym"], p["entry"]) not in existing:
                action = "开多" if p["side"] == "buy" else "开空"
                BOT_LOG.insert(0, {"ts": p.get("open_time") or time.strftime("%m-%d %H:%M"),
                                   "sym": p["sym"],
                                   "name": BY_KEY[p["sym"]].name if p["sym"] in BY_KEY else p["sym"],
                                   "tf": BOT_TF, "action": action,
                                   "price": round(p["entry"], 2), "pnl": None,
                                   "qty": int(p["qty"]) if p.get("qty") is not None else None})
                existing.add((p["sym"], p["entry"]))
    if len(BOT_LOG) > 100:
        del BOT_LOG[100:]
    try:
        with open(BOT_LOG_FILE, "w") as fh:
            json.dump(BOT_LOG[:100], fh)
    except Exception:
        pass


def log_bot_action(sym, tf, action, price, pnl=None, qty=None):
    BOT_LOG.insert(0, {"ts": time.strftime("%m-%d %H:%M"), "sym": sym,
                       "name": BY_KEY[sym].name if sym in BY_KEY else sym,
                       "tf": tf, "action": action, "price": round(price, 2) if price else None,
                       "pnl": round(pnl, 2) if pnl is not None else None,
                       "qty": int(qty) if qty is not None else None})
    if len(BOT_LOG) > 100:
        del BOT_LOG[100:]
    try:
        with open(BOT_LOG_FILE, "w") as fh:
            json.dump(BOT_LOG[:100], fh)
    except Exception:
        pass


def bot_equity_snapshot():
    realized = 0.0
    with TRADE_LOCK:
        for h in TRADE["history"]:
            if h.get("source") == "bot":
                realized += h.get("pnl", 0)
    unrealized = 0.0
    with TRADE_LOCK:
        for p in TRADE["positions"]:
            if p.get("source") == "bot":
                price = _price_of(p["sym"]) or p["entry"]
                mult = MULT.get(p["sym"], 1)
                unrealized += ((price - p["entry"]) if p["side"] == "buy" else (p["entry"] - price)) * mult * p["qty"]
    BOT_EQUITY.append([time.strftime("%m-%d %H:%M"), round(realized + unrealized, 2)])
    if len(BOT_EQUITY) > 500:
        del BOT_EQUITY[:len(BOT_EQUITY) - 500]
    try:
        with open(BOT_EQUITY_FILE, "w") as fh:
            json.dump(BOT_EQUITY[-500:], fh)
    except Exception:
        pass


def bot_stats():
    with TRADE_LOCK:
        history = [h for h in TRADE["history"] if h.get("source") == "bot"]
        open_count = sum(1 for p in TRADE["positions"] if p.get("source") == "bot")
    realized = sum(h.get("pnl", 0) for h in history)
    wins = sum(1 for h in history if h.get("pnl", 0) > 0)
    losses = sum(1 for h in history if h.get("pnl", 0) <= 0)
    unrealized = 0.0
    with TRADE_LOCK:
        for p in TRADE["positions"]:
            if p.get("source") == "bot":
                price = _price_of(p["sym"]) or p["entry"]
                mult = MULT.get(p["sym"], 1)
                unrealized += ((price - p["entry"]) if p["side"] == "buy" else (p["entry"] - price)) * mult * p["qty"]
    gross_p = sum(h["pnl"] for h in history if h["pnl"] > 0)
    gross_l = abs(sum(h["pnl"] for h in history if h["pnl"] <= 0))
    return {"realized": round(realized, 2), "unrealized": round(unrealized, 2),
            "total_pnl": round(realized + unrealized, 2),
            "trades": len(history) + open_count,
            "wins": wins, "losses": losses,
            "win_rate": round(wins * 100.0 / len(history), 1) if history else 0,
            "profit_factor": round(gross_p / gross_l, 2) if gross_l else (None if gross_p else 0)}
BOT_DEFAULT = {
    "enabled": False, "threshold": 60, "ai_threshold": 45, "stop_loss": 3.0, "take_profit": 6.0,
    "max_positions": 3, "qty": 1, "cooldown_min": 60, "timeout_h": 24,
    "interval_min": 5, "lev": 10, "strategy": "ai", "tf": "H1",
    "symbols": [c.key for c in COMMODITIES if c.key not in ("coffee", "cocoa")],
}


def _to_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _to_int(v, default=0):
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def load_bot_config():
    cfg = dict(BOT_DEFAULT)
    cfg["symbols"] = list(BOT_DEFAULT["symbols"])
    try:
        with open(BOT_FILE) as fh:
            d = json.load(fh)
        for k in cfg:
            if k in d:
                cfg[k] = d[k]
    except Exception:
        pass
    # 数字字段类型归一化: 配置文件可能存字符串, 统一转数值避免字符串乘法/比较等隐患
    for k in ("threshold", "ai_threshold", "stop_loss", "take_profit", "timeout_h"):
        cfg[k] = _to_float(cfg.get(k), BOT_DEFAULT.get(k, 0.0))
    for k in ("max_positions", "qty", "cooldown_min", "interval_min", "lev"):
        cfg[k] = _to_int(cfg.get(k), BOT_DEFAULT.get(k, 0))
    return cfg


def save_bot_config(cfg):
    try:
        with open(BOT_FILE, "w") as fh:
            json.dump(cfg, fh, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _backtest_frames(h1_kl, d1_kl):
    """预计算滚动D1/H1帧"""
    d1_frames = []
    for j in range(len(d1_kl)):
        if j + 1 < 20:
            continue
        d1_frames.append((d1_kl[j][0], analyze_bars(d1_kl[:j + 1])))
    h1_frames = [None] * len(h1_kl)
    for i in range(20, len(h1_kl)):
        h1_frames[i] = analyze_bars(h1_kl[:i + 1])
    return d1_frames, h1_frames


def _d1_at(d1_frames, date):
    latest = None
    for dt, fr in d1_frames:
        if dt <= date:
            latest = fr
        else:
            break
    return latest


def run_backtest(sym, params):
    if sym == "all":
        return _backtest_all(params)
    return _backtest_single(sym, params)


def _backtest_all(params):
    results = []
    for ckey in COMMODITIES:
        r = _backtest_single(ckey.key, params)
        if r and r.get("ok"):
            results.append({"name": r["name"], "key": ckey.key, "stats": r["stats"],
                            "trades": r["trades"], "equity": r["equity"]})
    if not results:
        return {"ok": False, "msg": "没有可回测的商品(K线不足)"}
    n = len(results)
    total_pnl = sum(r["stats"]["total_pnl"] for r in results)
    trades = sum(r["stats"]["trades"] for r in results)
    wins = sum(r["stats"]["wins"] for r in results)
    losses = sum(r["stats"]["losses"] for r in results)
    gross_p = sum(r["stats"]["avg_win"] * r["stats"]["wins"] for r in results)
    gross_l = sum(r["stats"]["avg_loss"] * r["stats"]["losses"] for r in results)
    max_dd = max((r["stats"]["max_drawdown"] for r in results), default=0)
    return {"ok": True, "name": "全部商品(%d个)" % n, "all": True, "results": results,
            "stats": {"trades": trades, "wins": wins, "losses": losses,
                      "win_rate": round(wins * 100.0 / trades, 1) if trades else 0,
                      "total_pnl": round(total_pnl, 2),
                      "total_pnl_pct": round(total_pnl / 100000.0 / n * 100, 2),
                      "profit_factor": round(gross_p / abs(gross_l), 2) if gross_l else (None if gross_p else 0),
                      "max_drawdown": round(max_dd, 2),
                      "avg_win": round(gross_p / wins, 2) if wins else 0,
                      "avg_loss": round(gross_l / losses, 2) if losses else 0}}


def _backtest_single(sym, params):
    c = BY_KEY.get(sym)
    if not c:
        return {"ok": False, "msg": "未知商品"}
    kl = c.state.get("klines") or {}
    h1_kl = kl.get("H1") or []
    d1_kl = kl.get("D1") or []
    if len(h1_kl) < 40 or len(d1_kl) < 25:
        return {"ok": False, "msg": "K线历史不足(需H1≥40根,D1≥25根)"}
    threshold = int(params.get("threshold", 60))
    stop_loss = float(params.get("stop_loss", 3.0))
    take_profit = float(params.get("take_profit", 6.0))
    qty = int(params.get("qty", 1))
    mult = MULT.get(sym, 1)

    d1_frames, h1_frames = _backtest_frames(h1_kl, d1_kl)
    positions = []
    realized = 0.0
    equity = []
    trades = []
    initial = 100000.0

    for i in range(20, len(h1_kl)):
        hf = h1_frames[i]
        if hf is None:
            continue
        date = h1_kl[i][0].split(" ")[0]
        df = _d1_at(d1_frames, date)
        price = h1_kl[i][4]

        for pos in list(positions):
            entry = pos["entry"]
            chg = (price - entry) / entry * 100 if pos["side"] == "buy" else (entry - price) / entry * 100
            if chg <= -stop_loss or chg >= take_profit:
                pnl = ((price - entry) if pos["side"] == "buy" else (entry - price)) * mult * qty
                realized += pnl
                trades.append({"time": h1_kl[i][0], "side": pos["side"], "qty": qty,
                               "entry": round(entry, 2), "exit": round(price, 2),
                               "pnl": round(pnl, 2), "reason": "止盈止损"})
                positions.remove(pos)

        for pos in list(positions):
            flip = hf["direction"] == ("空" if pos["side"] == "buy" else "多")
            if flip and hf["direction"] in ("多", "空"):
                pnl = ((price - pos["entry"]) if pos["side"] == "buy" else (pos["entry"] - price)) * mult * qty
                realized += pnl
                trades.append({"time": h1_kl[i][0], "side": pos["side"], "qty": qty,
                               "entry": round(pos["entry"], 2), "exit": round(price, 2),
                               "pnl": round(pnl, 2), "reason": "H1翻转"})
                positions.remove(pos)

        if df and hf["direction"] in ("多", "空") and hf["direction"] == df["direction"] \
                and hf["conf"] >= threshold and not positions:
            positions.append({"side": hf["direction"], "entry": price})

        unrealized = sum((price - p["entry"]) if p["side"] == "buy" else (p["entry"] - price)
                         for p in positions) * mult * qty
        equity.append([h1_kl[i][0], round(initial + realized + unrealized, 2)])

    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    total_pnl = realized
    peak = equity[0][1] if equity else initial
    max_dd = 0.0
    for _, v in equity:
        peak = max(peak, v)
        dd = (peak - v) / peak * 100
        max_dd = max(max_dd, dd)
    profit_factor = (sum(t["pnl"] for t in wins) / abs(sum(t["pnl"] for t in losses))) if losses else float("inf")

    return {"ok": True, "name": c.name, "stats": {
        "trades": len(trades), "wins": len(wins), "losses": len(losses),
        "win_rate": round(len(wins) * 100.0 / len(trades), 1) if trades else 0,
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl / initial * 100, 2),
        "profit_factor": round(profit_factor, 2) if losses else (None if wins else 0),
        "max_drawdown": round(max_dd, 2),
        "avg_win": round(sum(t["pnl"] for t in wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0,
    }, "equity": equity, "trades": trades[-60:]}


# -------- 神龙通道信号引擎 (Python移植) --------
def _calc_xma(arr, length):
    h = (length - 1) // 2
    n = len(arr)
    src = [0.0] * n
    for i in range(n):
        if i >= h:
            s = 0.0
            for j in range(i + h, i - h - 1, -1):
                if j < n:
                    s += arr[j]
            src[i] = s / length
        else:
            s = 0.0
            for j in range(i + h, -1, -1):
                if j < n:
                    s += arr[j]
            src[i] = (s + s * ((h - i) / (h + i + 1))) / length
    dst = [0.0] * n
    for i in range(n):
        if i >= h:
            s = 0.0
            for j in range(i + h, i - h - 1, -1):
                if j < n:
                    s += src[j]
            dst[i] = s / length
        else:
            s = 0.0
            for j in range(i + h, -1, -1):
                if j < n:
                    s += src[j]
            dst[i] = (s + s * ((h - i) / (h + i + 1))) / length
    return dst


def shenlong_signals(klines, xma_len=25, belt_n=90, k=3.9):
    """klines: [[time,o,h,l,c,v]...] 旧→新. 返回通道线+状态配对信号列表"""
    N = len(klines)
    if N < 30:
        return None
    H = [b[2] for b in klines]
    L = [b[3] for b in klines]
    revH = list(reversed(H))
    revL = list(reversed(L))
    wH = [0.0] * N
    wL = [0.0] * N
    for i in range(N - 20):
        sh = sl = 0.0
        for w in range(21):
            sh += (20 - w) * revH[i + w]
            sl += (20 - w) * revL[i + w]
        wH[i] = sh / 210.0
        wL[i] = sl / 210.0
    for i in range(N):
        if not wH[i]:
            wH[i] = revH[i]
        if not wL[i]:
            wL[i] = revL[i]
    belt3 = [0.0] * N
    belt4 = [0.0] * N
    p3 = wH[N - 1]
    p4 = wL[N - 1]
    for i in range(N - 1, -1, -1):
        belt3[i] = (2 * wH[i] + (belt_n - 1) * p3) / (belt_n + 1)
        p3 = belt3[i]
        belt4[i] = (2 * wL[i] + (belt_n - 1) * p4) / (belt_n + 1)
        p4 = belt4[i]
    x1 = _calc_xma(revL, xma_len)
    x2 = _calc_xma(revH, xma_len)
    buf1 = [0.0] * N
    buf2 = [0.0] * N
    buf3 = [0.0] * N
    buf4 = [0.0] * N
    sl1 = [0.0] * N
    sl2 = [0.0] * N
    ch = [2] * N
    for i in range(N):
        a = x1[i]
        b = x2[i]
        buf1[i] = 2 * a - b
        buf2[i] = 2 * b - a
        buf3[i] = k * (b - a) + b
        buf4[i] = a - k * (b - a)
        bs = belt3[i] - belt4[i]
        sl1[i] = belt3[i] + 2 * bs
        sl2[i] = belt4[i] - 2 * bs
        if buf2[i] > sl1[i] and buf1[i] > sl2[i]:
            ch[i] = 0
        elif buf2[i] < sl1[i] and buf1[i] < sl2[i]:
            ch[i] = 1
        else:
            ch[i] = 2
    b1 = buf1[::-1]
    b2 = buf2[::-1]
    sl1r = sl1[::-1]
    sl2r = sl2[::-1]
    chh = ch[::-1]
    sigs = []
    state = "flat"
    for i in range(1, N):
        Hc = klines[i][2]
        Lc = klines[i][3]
        Hp = klines[i - 1][2]
        Lp = klines[i - 1][3]
        b1c, b1p = b1[i], b1[i - 1]
        b2c, b2p = b2[i], b2[i - 1]
        up = chh[i] == 0
        down = chh[i] == 1
        side = chh[i] == 2
        buyCross = b1p < Lp and b1c > Lc
        sellCross = Hp < b2p and Hc > b2c
        buyCond = buyCross and (up or side)
        sellCond = sellCross and (down or side)
        clsBuyCond = sellCross and (up or (not side)) and (not (down or side))
        clsSellCond = buyCross and (down or (not side)) and (not (up or side))
        tstr = klines[i][0]
        if state == "long" and clsBuyCond:
            sigs.append((i, "clsBuy", Hc, tstr))
            state = "flat"
        elif state == "short" and clsSellCond:
            sigs.append((i, "clsSell", Lc, tstr))
            state = "flat"
        if buyCond and state != "long":
            if state == "short":
                sigs.append((i, "clsSell", Lc, tstr))
            sigs.append((i, "buy", Lc, tstr))
            state = "long"
        elif sellCond and state != "short":
            if state == "long":
                sigs.append((i, "clsBuy", Hc, tstr))
            sigs.append((i, "sell", Hc, tstr))
            state = "short"
    return {"buf1": b1, "buf2": b2, "buf3": buf3[::-1], "buf4": buf4[::-1],
            "buf5": belt3[::-1], "buf6": belt4[::-1], "sl1": sl1r, "sl2": sl2r,
            "ch": chh, "sigs": sigs}


def run_shenlong_backtest(sym, params):
    """神龙通道策略回测"""
    c = BY_KEY.get(sym)
    if not c:
        return {"ok": False, "msg": "未知商品"}
    tf = params.get("tf", "H1")
    kl = c.state.get("klines", {}).get(tf, [])
    if len(kl) < 45:
        return {"ok": False, "msg": "%s K线不足(%d根,需≥45)" % (tf, len(kl))}
    r = shenlong_signals(kl, int(params.get("xma", 25)), int(params.get("belt", 90)))
    if not r or not r.get("sigs"):
        return {"ok": False, "msg": "无信号"}
    qty = int(params.get("qty", 1))
    stop_loss = float(params.get("stop_loss", 3.0))
    take_profit = float(params.get("take_profit", 6.0))
    mult = MULT.get(sym, 1)
    N = len(kl)
    sig_by_idx = {}
    for (idx, stype, price, tstr) in r["sigs"]:
        sig_by_idx.setdefault(idx, []).append((stype, price, tstr))
    positions = []
    realized = 0.0
    equity = []
    trades = []
    initial = 100000.0
    for i in range(1, N):
        price = kl[i][4]
        for pos in list(positions):
            entry = pos["entry"]
            chg = (price - entry) / entry * 100 if pos["side"] == "buy" else (entry - price) / entry * 100
            if chg <= -stop_loss or chg >= take_profit:
                pnl = ((price - entry) if pos["side"] == "buy" else (entry - price)) * mult * qty
                realized += pnl
                trades.append({"time": kl[i][0], "side": pos["side"], "qty": qty, "entry": round(entry, 2),
                               "exit": round(price, 2), "pnl": round(pnl, 2), "reason": "止盈止损"})
                positions.remove(pos)
        for (stype, sp, tstr) in sig_by_idx.get(i, []):
            if stype == "buy":
                positions = [p for p in positions if p["side"] != "sell"]
                if not any(p["side"] == "buy" for p in positions):
                    positions.append({"side": "buy", "entry": sp})
            elif stype == "sell":
                positions = [p for p in positions if p["side"] != "buy"]
                if not any(p["side"] == "sell" for p in positions):
                    positions.append({"side": "sell", "entry": sp})
            elif stype == "clsBuy":
                for pos in list(positions):
                    if pos["side"] == "buy":
                        pnl = (price - pos["entry"]) * mult * qty
                        realized += pnl
                        trades.append({"time": kl[i][0], "side": "buy", "qty": qty, "entry": round(pos["entry"], 2),
                                       "exit": round(price, 2), "pnl": round(pnl, 2), "reason": "平多信号"})
                        positions.remove(pos)
            elif stype == "clsSell":
                for pos in list(positions):
                    if pos["side"] == "sell":
                        pnl = (pos["entry"] - price) * mult * qty
                        realized += pnl
                        trades.append({"time": kl[i][0], "side": "sell", "qty": qty, "entry": round(pos["entry"], 2),
                                       "exit": round(price, 2), "pnl": round(pnl, 2), "reason": "平空信号"})
                        positions.remove(pos)
        unrealized = sum((price - p["entry"]) if p["side"] == "buy" else (p["entry"] - price) for p in positions) * mult * qty
        equity.append([kl[i][0], round(initial + realized + unrealized, 2)])
    wins = [t for t in trades if t["pnl"] > 0]
    losses = [t for t in trades if t["pnl"] <= 0]
    peak = equity[0][1] if equity else initial
    max_dd = 0.0
    for _, v in equity:
        peak = max(peak, v)
        dd = (peak - v) / peak * 100
        max_dd = max(max_dd, dd)
    profit_factor = (sum(t["pnl"] for t in wins) / abs(sum(t["pnl"] for t in losses))) if losses else (None if wins else 0)
    return {"ok": True, "name": "%s · %s · 神龙" % (c.name, tf), "shenlong": True,
            "stats": {"trades": len(trades), "wins": len(wins), "losses": len(losses),
                      "win_rate": round(len(wins) * 100.0 / len(trades), 1) if trades else 0,
                      "total_pnl": round(realized, 2),
                      "total_pnl_pct": round(realized / initial * 100, 2),
                      "profit_factor": round(profit_factor, 2) if losses else (None if wins else 0),
                      "max_drawdown": round(max_dd, 2),
                      "avg_win": round(sum(t["pnl"] for t in wins) / len(wins), 2) if wins else 0,
                      "avg_loss": round(sum(t["pnl"] for t in losses) / len(losses), 2) if losses else 0},
            "equity": equity, "trades": trades[-60:]}


# -------- 实盘机器人 --------
BOT_COOLDOWN = {}


def composite_signal(sym, cfg):
    """AI复合评分: D1/H1/M30多周期加权 + RSI/量能/持仓情绪修正, 返回 -100~100"""
    c = BY_KEY.get(sym)
    if not c:
        return None
    frames = c.state.get("frames") or {}
    weights = [("D1", 0.4), ("H1", 0.35), ("M30", 0.25)]
    parts = []
    for tf, w in weights:
        f = frames.get(tf)
        if f and f.get("score") is not None:
            parts.append((f["score"], w))
    if not parts:
        return None
    base = sum(s * w for s, w in parts) / sum(w for _, w in parts)
    adj = 0
    reasons = []
    h1 = frames.get("H1") or {}
    if h1.get("rsi") is not None:
        r = h1["rsi"]
        if r >= 70:
            adj -= 10
            reasons.append("RSI超买")
        elif r <= 30:
            adj += 10
            reasons.append("RSI超卖")
    if h1.get("vol_ratio") and h1["vol_ratio"] > 1.5:
        if h1.get("direction") == "多":
            adj += 8
            reasons.append("放量上行")
        elif h1.get("direction") == "空":
            adj -= 8
            reasons.append("放量下行")
    if h1.get("sentiment_class") == "bull":
        adj += 5
        reasons.append("多方占优")
    elif h1.get("sentiment_class") == "bear":
        adj -= 5
        reasons.append("空方占优")
    score = round(base * 20 + adj, 1)
    th = float(cfg.get("ai_threshold", 45))
    if score >= th:
        d = "多"
    elif score <= -th:
        d = "空"
    else:
        d = "震荡"
    return {"direction": d, "score": score, "reasons": reasons}


def _bot_signal(sym, cfg):
    """机器人信号: 使用AI复合评分引擎"""
    sig = composite_signal(sym, cfg)
    if not sig or sig["direction"] in ("震荡",):
        return None
    return sig["direction"]


def _parse_hm(s):
    try:
        return int(time.mktime(time.strptime(time.strftime("%Y-") + s, "%Y-%m-%d %H:%M")))
    except Exception:
        return None


def _manage_bot_positions(cfg):
    """止损止盈超时管理(所有策略共用)"""
    global BOT_COOLDOWN
    now = time.time()
    stop_loss = float(cfg.get("stop_loss", 3.0))
    take_profit = float(cfg.get("take_profit", 6.0))
    timeout_h = float(cfg.get("timeout_h", 24))
    with TRADE_LOCK:
        bot_pos = [p for p in TRADE["positions"] if p.get("source") == "bot"]
    for pos in bot_pos:
        price = _price_of(pos["sym"])
        if not price:
            continue
        entry = pos["entry"]
        chg = (price - entry) / entry * 100 if pos["side"] == "buy" else (entry - price) / entry * 100
        ts = _parse_hm(pos.get("open_time", ""))
        timed_out = ts and (now - ts) > timeout_h * 3600
        if chg <= -stop_loss or chg >= take_profit or timed_out:
            reason = "止损" if chg <= -stop_loss else "止盈" if chg >= take_profit else "超时"
            pnl = ((price - entry) if pos["side"] == "buy" else (entry - price)) * MULT.get(pos["sym"], 1) * pos["qty"]
            cr = close_position(pos["id"])
            if cr and cr.get("ok"):
                BOT_COOLDOWN[pos["sym"]] = now
                log_bot_action(pos["sym"], BOT_TF, reason, price, pnl, qty=pos["qty"])
            else:
                print("%s %s平仓失败: %s" % (pos["sym"], reason, (cr or {}).get("msg")), flush=True)


def _bot_pos_side(sym):
    """返回该商品机器人持仓方向: buy/sell/None"""
    with TRADE_LOCK:
        for p in TRADE["positions"]:
            if p.get("source") == "bot" and p["sym"] == sym:
                return p["side"]
    return None


def bot_tick_shenlong(cfg):
    """神龙通道策略: 跟随当前周期(BOT_TF)的信号开平仓, 只处理最近K线的新信号"""
    global BOT_SIG_PROC
    tf = BOT_TF
    qty = int(cfg.get("qty", 1))
    lev = int(cfg.get("lev", 10))
    cooldown_s = cfg.get("cooldown_min", 60) * 60
    now = time.time()
    for sym in cfg.get("symbols", []):
        if BOT_COOLDOWN.get(sym) and now - BOT_COOLDOWN[sym] < cooldown_s:
            continue
        c = BY_KEY.get(sym)
        if not c:
            continue
        kl = c.state.get("klines", {}).get(tf, [])
        if len(kl) < 45:
            continue
        r = shenlong_signals(kl, 25, 90)
        if not r or not r.get("sigs"):
            continue
        last = r["sigs"][-1]
        idx, stype, price, tstr = last
        if idx < len(kl) - 2:
            continue
        key = (tstr, stype)
        if BOT_SIG_PROC.get(sym) == key:
            continue
        BOT_SIG_PROC[sym] = key
        side = _bot_pos_side(sym)
        if stype == "buy":
            if side == "sell":
                cr = close_position(_bot_pos_id(sym))
                if cr and cr.get("ok"):
                    log_bot_action(sym, tf, "平空", price, None, qty=qty)
            if not _bot_pos_side(sym):
                or_ = place_order(sym, "buy", qty, lev, "market", source="bot")
                if or_ and or_.get("ok"):
                    log_bot_action(sym, tf, "开多", price, qty=qty)
                else:
                    print("开多失败 %s: %s" % (sym, (or_ or {}).get("msg")), flush=True)
                    BOT_SIG_PROC.pop(sym, None)
        elif stype == "sell":
            if side == "buy":
                cr = close_position(_bot_pos_id(sym))
                if cr and cr.get("ok"):
                    log_bot_action(sym, tf, "平多", price, None, qty=qty)
            if not _bot_pos_side(sym):
                or_ = place_order(sym, "sell", qty, lev, "market", source="bot")
                if or_ and or_.get("ok"):
                    log_bot_action(sym, tf, "开空", price, qty=qty)
                else:
                    print("开空失败 %s: %s" % (sym, (or_ or {}).get("msg")), flush=True)
                    BOT_SIG_PROC.pop(sym, None)
        elif stype == "clsBuy":
            if side == "buy":
                cr = close_position(_bot_pos_id(sym))
                if cr and cr.get("ok"):
                    BOT_COOLDOWN[sym] = now
                    log_bot_action(sym, tf, "平多", price, qty=qty)
        elif stype == "clsSell":
            if side == "sell":
                cr = close_position(_bot_pos_id(sym))
                if cr and cr.get("ok"):
                    BOT_COOLDOWN[sym] = now
                    log_bot_action(sym, tf, "平空", price, qty=qty)


def _bot_pos_id(sym):
    with TRADE_LOCK:
        for p in TRADE["positions"]:
            if p.get("source") == "bot" and p["sym"] == sym:
                return p["id"]
    return None


def bot_tick(cfg):
    _manage_bot_positions(cfg)
    if cfg.get("strategy") == "shenlong":
        bot_tick_shenlong(cfg)
    else:
        bot_tick_ai(cfg)
    bot_equity_snapshot()


def bot_tick_ai(cfg):
    global BOT_COOLDOWN
    now = time.time()
    qty = int(cfg.get("qty", 1))
    lev = int(cfg.get("lev", 10))
    with TRADE_LOCK:
        bot_count = sum(1 for p in TRADE["positions"] if p.get("source") == "bot")
        active_syms = set(p["sym"] for p in TRADE["positions"] if p.get("source") == "bot")

    cooldown_s = cfg.get("cooldown_min", 60) * 60
    for sym in cfg.get("symbols", []):
        if bot_count >= int(cfg.get("max_positions", 3)):
            break
        if sym in active_syms:
            continue
        if BOT_COOLDOWN.get(sym) and now - BOT_COOLDOWN[sym] < cooldown_s:
            continue
        sig = _bot_signal(sym, cfg)
        if sig:
            or_ = place_order(sym, "buy" if sig == "多" else "sell", qty, lev, "market", source="bot")
            if or_ and or_.get("ok"):
                log_bot_action(sym, BOT_TF, "开多" if sig == "多" else "开空", _price_of(sym), qty=qty)
                bot_count += 1
                active_syms.add(sym)
            else:
                print("开仓失败 %s: %s" % (sym, (or_ or {}).get("msg")), flush=True)


def bot_loop():
    while True:
        try:
            cfg = load_bot_config()
            if cfg.get("enabled"):
                bot_tick(cfg)
                print("%s 机器人扫描完成" % time.strftime("%H:%M"), flush=True)
            interval = max(60, int(load_bot_config().get("interval_min", 5)) * 60)
        except Exception as e:
            print("bot error:", repr(e), flush=True)
            interval = 300
        time.sleep(interval)


def bot_payload():
    cfg = load_bot_config()
    with TRADE_LOCK:
        bot_pos = [p for p in TRADE["positions"] if p.get("source") == "bot"]
        history = [h for h in TRADE["history"] if h.get("source") == "bot"]
    pos_out = []
    for p in bot_pos:
        price = _price_of(p["sym"]) or p["entry"]
        mult = MULT.get(p["sym"], 1)
        pnl = ((price - p["entry"]) if p["side"] == "buy" else (p["entry"] - price)) * mult * p["qty"]
        pos_out.append({"sym": p["sym"], "name": BY_KEY[p["sym"]].name, "side": p["side"],
                        "qty": p["qty"], "entry": round(p["entry"], 2), "current": round(price, 2),
                        "pnl": round(pnl, 2), "open_time": p["open_time"]})
    scores = []
    for sym in cfg.get("symbols", []):
        sig = composite_signal(sym, cfg)
        if sig:
            scores.append({"sym": sym, "name": BY_KEY[sym].name, "direction": sig["direction"],
                           "score": sig["score"], "reasons": sig["reasons"]})
    scores.sort(key=lambda x: -abs(x["score"]))
    return {"enabled": cfg.get("enabled"), "threshold": cfg.get("threshold"),
            "ai_threshold": cfg.get("ai_threshold", 45),
            "stop_loss": cfg.get("stop_loss"), "take_profit": cfg.get("take_profit"),
            "max_positions": cfg.get("max_positions"), "qty": cfg.get("qty"),
            "cooldown_min": cfg.get("cooldown_min"), "timeout_h": cfg.get("timeout_h"),
            "interval_min": cfg.get("interval_min"), "strategy": cfg.get("strategy", "ai"),
            "tf": cfg.get("tf") or BOT_TF, "symbols": cfg.get("symbols"),
            "stats": bot_stats(),
            "log": BOT_LOG[:100],
            "equity": BOT_EQUITY[-120:],
            "positions": pos_out, "scores": scores,
            "history": [{"sym": h["sym"], "name": BY_KEY[h["sym"]].name if h["sym"] in BY_KEY else h["sym"],
                         "side": h["side"], "qty": h["qty"], "entry": h["entry"], "exit": h["exit"],
                         "pnl": h["pnl"], "close_time": h["close_time"]} for h in history[:20]],
            "cooldown": {k: round((time.time() - v) / 60) for k, v in BOT_COOLDOWN.items()}}


def bot_activate(sym, tf, strategy):
    """从详情页一键激活: 只交易当前商品+当前周期+所选策略"""
    global BOT_TF
    if sym not in BY_KEY:
        return {"ok": False, "msg": "未知商品"}
    if tf not in ORDER:
        return {"ok": False, "msg": "未知周期"}
    strategy = strategy if strategy in ("ai", "shenlong") else "ai"
    cfg = load_bot_config()
    cfg["strategy"] = strategy
    cfg["symbols"] = [sym]
    cfg["tf"] = tf
    cfg["enabled"] = True
    save_bot_config(cfg)
    BOT_TF = tf
    name = BY_KEY[sym].name
    sname = "🐉神龙" if strategy == "shenlong" else "AI复合"
    return {"ok": True, "msg": "机器人已启动: %s · %s · %s" % (sname, name, tf)}


def bot_config_update(data):
    cfg = load_bot_config()
    for k in ("enabled", "threshold", "ai_threshold", "stop_loss", "take_profit", "max_positions",
              "qty", "cooldown_min", "timeout_h", "interval_min", "lev", "strategy"):
        if k in data:
            cfg[k] = data[k]
    if "symbols" in data and isinstance(data["symbols"], list):
        cfg["symbols"] = data["symbols"]
    save_bot_config(cfg)
    return {"ok": True, "msg": "机器人设置已保存"}


# ---------------------------------------------------------------- HTTP 服务
class ThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


class Handler(http.server.BaseHTTPRequestHandler):
    def _send(self, code, ctype, body, cache=False):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        if not cache:
            self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]
        q = {}
        if "?" in self.path:
            for kv in self.path.split("?", 1)[1].split("&"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    q[k] = v
        if path.startswith("/api/home"):
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(home_payload(), ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/alertconfig"):
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(alert_config_payload(), ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/bot"):
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(bot_payload(), ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/config"):
            self._send(200, "application/json; charset=utf-8",
                       json.dumps({"twelve_configured": bool(TD_KEY)}, ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/account"):
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(account_payload(), ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/trade") or path.startswith("/api/close") or path.startswith("/api/reset"):
            self._send(400, "application/json; charset=utf-8",
                       json.dumps({"ok": False, "msg": "需POST请求"}).encode("utf-8"))
        elif path.startswith("/api/state"):
            sym = q.get("sym", "gold")
            c = BY_KEY.get(sym) or COMMODITIES[0]
            with ACTIVE_LOCK:
                global ACTIVE
                ACTIVE = c.key
            if not c.building and (time.time() - c.last_update > 12):
                c.building = True
                threading.Thread(target=lambda cc=c: _build_async(cc), daemon=True).start()
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(detail_payload(c), ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/tick"):
            sym = q.get("sym", "gold")
            c = BY_KEY.get(sym) or COMMODITIES[0]
            spot = c.last_spot
            if spot:
                tick = {"price": spot["price"], "high": spot["high"], "low": spot["low"],
                        "chg": round((spot["price"] / spot["prev"] - 1) * 100, 2) if spot["prev"] else 0.0,
                        "ts": spot["ts"]}
            else:
                tick = {"price": None, "chg": None}
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(tick, ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/fundflow"):
            sym = q.get("sym", "gold")
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(fundflow_payload(sym), ensure_ascii=False).encode("utf-8"))
        elif path.startswith("/api/accuracy"):
            try:
                mc = int(q.get("min_conf", ACC_MIN_CONF))
            except Exception:
                mc = ACC_MIN_CONF
            days = q.get("days") or None
            if days is not None:
                try:
                    days = int(days)
                except Exception:
                    days = None
            start = q.get("start") or None
            end = q.get("end") or None
            self._send(200, "application/json; charset=utf-8",
                       json.dumps(accuracy_payload(mc, days, start, end), ensure_ascii=False).encode("utf-8"))
        elif path == "/echarts.min.js":
            try:
                with open(os.path.join(BASE_DIR, "echarts.min.js"), "rb") as f:
                    self._send(200, "application/javascript; charset=utf-8", f.read(), cache=True)
            except Exception:
                self._send(404, "text/plain", b"not found")
        else:
            self._send(200, "text/html; charset=utf-8", PAGE.encode("utf-8"))

    def do_POST(self):
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length).decode("utf-8") if length else ""
        try:
            data = json.loads(body) if body else {}
        except Exception:
            data = {}
        path = self.path.split("?")[0]
        if path.startswith("/api/config"):
            save_api_config({"twelve_key": (data.get("twelve_key") or "").strip()})
            r = {"ok": True, "msg": "配置已保存"}
        elif path.startswith("/api/backtest"):
            if data.get("strategy") == "shenlong":
                r = run_shenlong_backtest(data.get("sym", "gold"), data)
            else:
                r = run_backtest(data.get("sym", "gold"), data)
        elif path.startswith("/api/trade"):
            r = place_order(data.get("sym"), data.get("side"), data.get("qty"),
                            data.get("lev", 10), data.get("type", "market"),
                            data.get("price"))
        elif path.startswith("/api/bot/activate"):
            r = bot_activate(data.get("sym"), data.get("tf", "H1"), data.get("strategy", "shenlong"))
        elif path.startswith("/api/bot"):
            r = bot_config_update(data)
        elif path.startswith("/api/alertconfig"):
            r = alert_config_update(data)
        elif path.startswith("/api/alerts/test"):
            ok, msg = send_email("【测试】提醒通道正常", "<h3>✅ 邮箱提醒测试成功</h3><p>大宗商品预测系统提醒通道已打通，后续高置信度信号/放量新闻/每日简报将通过此邮箱发送。</p>")
            r = {"ok": ok, "msg": msg}
        elif path.startswith("/api/closeall"):
            r = close_all_positions()
        elif path.startswith("/api/closebot"):
            r = close_all_bot_positions()
        elif path.startswith("/api/close"):
            r = close_position(data.get("id"), manual=True)
        elif path.startswith("/api/cancel"):
            r = cancel_order(data.get("id"))
        elif path.startswith("/api/reset"):
            r = reset_trade()
        elif path.startswith("/api/accuracy/reset"):
            global ACC_RECORDS, ACC_SEQ, ACC_SEEN
            ACC_RECORDS = []
            ACC_SEQ[0] = 0
            ACC_SEEN.clear()
            ACC_DIRTY[0] = True
            save_accuracy(force=True)
            r = {"ok": True, "msg": "统计已重置, 将从下一次更新重新记录"}            
        else:
            r = {"ok": False, "msg": "未知接口"}
        self._send(200, "application/json; charset=utf-8",
                   json.dumps(r, ensure_ascii=False).encode("utf-8"))

    def log_message(self, fmt, *args):
        pass


# ---------------------------------------------------------------- HTML 页面(SPA)
PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>大宗商品多周期预测</title>
<script src="/echarts.min.js"></script>
<style>
  :root {
    --bg: linear-gradient(160deg,#070b13 0%,#101827 55%,#0a0e17 100%);
    --bg-solid:#0a0e17;
    --panel: rgba(22,27,34,.72);
    --panel-solid:#161b22;
    --inner:#1c2431;
    --inner2:#101827;
    --border: rgba(255,255,255,.08);
    --border2:#30363d;
    --text:#e6edf3;
    --text2:#c9d1d9;
    --sub:#8b949e;
    --dim:#484f58;
    --accent:#58a6ff;
    --accent2:#8b6bff;
    --up:#ff4d4f;
    --down:#3fb950;
    --warn:#d29922;
    --up-soft:rgba(255,77,79,.16);
    --down-soft:rgba(63,185,80,.16);
    --warn-soft:rgba(210,153,34,.16);
  }
  [data-theme="light"] {
    --bg: linear-gradient(160deg,#f0f4fa 0%,#ffffff 55%,#f5f7fb 100%);
    --bg-solid:#f5f7fb;
    --panel: rgba(255,255,255,.85);
    --panel-solid:#ffffff;
    --inner:#e8edf5;
    --inner2:#f0f3f9;
    --border: rgba(15,30,60,.12);
    --border2:#d3dbe6;
    --text:#1c2733;
    --text2:#2c3a4b;
    --sub:#5b6b7d;
    --dim:#93a1b3;
    --accent:#2563eb;
    --accent2:#7c3aed;
    --up:#dc2626;
    --down:#059669;
    --warn:#b45309;
    --up-soft:rgba(220,38,38,.12);
    --down-soft:rgba(5,150,105,.12);
    --warn-soft:rgba(180,83,9,.14);
  }
  * { margin:0; padding:0; box-sizing:border-box; }
  body { font-family:-apple-system,"PingFang SC","Microsoft YaHei",sans-serif;
         background:var(--bg); color:var(--text); min-height:100vh; }
  .wrap { max-width:1280px; margin:0 auto; padding:18px; }
  .head { display:flex; flex-wrap:wrap; gap:14px; align-items:center; justify-content:space-between; margin-bottom:14px; }
  .head .tt { font-size:22px; font-weight:800; background:linear-gradient(90deg,var(--accent),var(--accent2));
              -webkit-background-clip:text; -webkit-text-fill-color:transparent; cursor:pointer; }
  .head .sub { color:var(--sub); font-size:12px; margin-top:2px; }
  .panel { background:var(--panel); border:1px solid var(--border); border-radius:16px;
           padding:16px 20px; backdrop-filter:blur(12px); box-shadow:0 8px 30px rgba(0,0,0,.35); margin-bottom:14px; }
  .pricebox { text-align:right; }
  .pricebox .lb { font-size:12px; color:var(--sub); }
  .price { font-family:"Consolas",monospace; font-size:38px; font-weight:800; line-height:1.1; }
  .up { color:var(--up); } .down { color:var(--down); }
  .pchg { font-size:14px; margin-left:6px; font-weight:600; }
  .meta { display:flex; gap:18px; align-items:center; margin-top:4px; font-size:13px; color:var(--sub); flex-wrap:wrap; }
  .meta b { color:var(--text); }
  .flash-up { animation:fl .8s ease; } .flash-down { animation:fl .8s ease; }
  @keyframes fl { 0%{transform:scale(1.12);} 100%{transform:scale(1);} }
  .chip { display:inline-block; padding:2px 8px; border-radius:10px; font-size:11px; font-weight:700; margin:1px 1px; }
  .chip.more { background:var(--up-soft); color:var(--up); }
  .chip.empty { background:var(--down-soft); color:var(--down); }
  .chip.neutral { background:var(--warn-soft); color:var(--warn); }
  .sech { font-size:16px; font-weight:800; margin:4px 0 12px; display:flex; align-items:center; gap:8px; }
  .sech .ico { width:8px; height:20px; border-radius:4px; display:inline-block; }
  .comgrid { display:grid; grid-template-columns:repeat(auto-fill,minmax(240px,1fr)); gap:12px; }
  .comcard { background:var(--panel); border:1px solid var(--border); border-radius:14px;
             padding:14px; cursor:pointer; transition:transform .15s, box-shadow .15s; position:relative; overflow:hidden; }
  .comcard:hover { transform:translateY(-3px); box-shadow:0 10px 30px rgba(0,0,0,.4); }
  .comcard::before { content:""; position:absolute; top:0; left:0; right:0; height:3px; }
  .comcard.gold::before { background:linear-gradient(90deg,#ffd166,#ff9f43); }
  .comcard.precious::before { background:linear-gradient(90deg,#ffd166,#ffb86b); }
  .comcard.energy::before { background:linear-gradient(90deg,#58a6ff,#8b6bff); }
  .comcard.metal::before { background:linear-gradient(90deg,#ff9f43,#ff6b6d); }
  .comcard.agri::before { background:linear-gradient(90deg,#3fb950,#8bff7a); }
  .comcard .cn { font-size:15px; font-weight:800; color:var(--accent); }
  .comcard .cp { font-family:"Consolas",monospace; font-size:20px; font-weight:800; margin:4px 0; }
  .comcard .dirs { margin-top:6px; }
  .toolbar { display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin:12px 0 10px; }
  .tfbar { display:flex; gap:6px; flex-wrap:wrap; }
  .tfbar button { background:var(--inner); border:1px solid var(--border2); color:var(--sub); padding:5px 13px;
                  border-radius:8px; cursor:pointer; font-size:13px; font-weight:600; }
  .tfbar button.on { background:color-mix(in srgb, var(--accent) 15%, transparent); border-color:var(--accent); color:var(--accent); }
  .indbar { display:flex; gap:6px; flex-wrap:wrap; margin-left:auto; }
  .indbar button { background:var(--inner); border:1px solid var(--border2); color:var(--sub); padding:5px 11px;
                   border-radius:8px; cursor:pointer; font-size:12px; font-weight:600; }
  .indbar button.on { border-color:var(--accent2); color:var(--accent2); }
  #mainChart { width:100%; height:400px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:14px; }
  .card { background:var(--panel); border:1px solid var(--border); border-radius:14px;
          padding:14px 16px; position:relative; overflow:hidden; }
  .card::before { content:""; position:absolute; top:0; left:0; right:0; height:3px; }
  .card.more::before { background:linear-gradient(90deg,var(--up),#ffb86b); }
  .card.empty::before { background:linear-gradient(90deg,var(--down),#8bff7a); }
  .card.neutral::before { background:linear-gradient(90deg,var(--warn),#ffd166); }
  .card .top { display:flex; justify-content:space-between; align-items:center; margin-bottom:6px; }
  .card .tf { font-size:14px; font-weight:800; color:var(--accent); }
  .badge { font-size:22px; font-weight:800; padding:1px 14px; border-radius:10px; }
  .badge.more { background:var(--up-soft); color:var(--up); }
  .badge.empty { background:var(--down-soft); color:var(--down); }
  .badge.neutral { background:var(--warn-soft); color:var(--warn); }
  .badge.flip { animation:pop .6s ease; }
  @keyframes pop { 0%{transform:scale(1);} 35%{transform:scale(1.35) rotate(-4deg);} 70%{transform:scale(.9);} 100%{transform:scale(1);} }
  .cprice { font-family:"Consolas",monospace; font-size:18px; font-weight:700; }
  .crow { display:flex; align-items:center; gap:12px; margin:4px 0 8px; }
  .ring { width:50px; height:50px; border-radius:50%; flex-shrink:0;
          background:conic-gradient(var(--rc) calc(var(--v)*1%), var(--inner) 0);
          display:flex; align-items:center; justify-content:center; }
  .ring.more { --rc:var(--up); } .ring.empty { --rc:var(--down); } .ring.neutral { --rc:var(--warn); }
  .ring .rin { width:38px; height:38px; border-radius:50%; background:var(--panel-solid);
               display:flex; align-items:center; justify-content:center; font-size:11px; font-weight:800; }
  .miniChart { width:100%; height:70px; }
  .summary { font-size:12px; color:var(--text2); line-height:1.55; margin-top:6px; }
  .sentiment { font-size:12px; font-weight:700; margin-top:6px; }
  .sentiment.up { color:var(--up); } .sentiment.down { color:var(--down); }
  .kv { font-size:12px; color:var(--sub); margin-top:8px; }
  .kv b { color:var(--text); }
  .toast { position:fixed; top:16px; left:50%; transform:translateX(-50%); z-index:99;
           background:var(--panel-solid); border:1px solid var(--warn); color:var(--warn); padding:8px 18px;
           border-radius:10px; font-size:13px; font-weight:600; box-shadow:0 6px 24px rgba(0,0,0,.5);
           opacity:0; transition:opacity .3s; pointer-events:none; max-width:70%; }
  .toast.show { opacity:1; }
  .spike { font-size:12px; color:var(--text2); padding:6px 10px; background:var(--inner2); border-radius:8px;
           margin-bottom:6px; border-left:3px solid var(--warn); }
  .spike-news { font-size:11px; color:var(--sub); margin-top:4px; line-height:1.5; }
  .news li { font-size:12px; color:var(--sub); list-style:none; padding:2px 0; overflow:hidden;
             text-overflow:ellipsis; white-space:nowrap; }
  .news h3 { font-size:14px; color:var(--accent); margin-bottom:6px; }
  .news li.imp { color:var(--warn); }
  .imp-tag { display:inline-block; font-size:10px; padding:1px 6px; border-radius:8px;
             background:var(--warn-soft); color:var(--warn); font-weight:700; }
  .stale-tag { font-size:12px; padding:2px 8px; border-radius:10px; background:#6e40c9; color:#fff; margin-left:6px; }
  .foot { text-align:center; color:var(--dim); font-size:12px; margin-top:18px; }
  .back { cursor:pointer; color:var(--accent); font-size:14px; font-weight:600; margin-bottom:10px; display:inline-block; }
  .navbtn { background:linear-gradient(90deg,#ffd166,#ff9f43); border:none; color:#111; padding:8px 18px;
            border-radius:10px; font-size:14px; font-weight:800; cursor:pointer; }
  .navbtn.alt { background:linear-gradient(90deg,var(--accent),var(--accent2)); color:#fff; margin-left:8px; }
  .navbtn.bot { background:linear-gradient(90deg,#3fb950,#8bff7a); color:#111; margin-left:8px; }
  .navbtn.theme { background:var(--inner); color:var(--text); border:1px solid var(--border2); margin-left:8px; font-weight:700; }
  .navbtn:hover { filter:brightness(1.1); }
  .tsec { font-size:15px; font-weight:800; margin:14px 0 10px; color:var(--accent); }
  .table { width:100%; border-collapse:collapse; font-size:13px; }
  .table th { color:var(--sub); text-align:left; padding:6px 8px; border-bottom:1px solid var(--border2); font-weight:600; }
  .table td { padding:6px 8px; border-bottom:1px solid var(--border); color:var(--text2); }
  .btn { padding:5px 14px; border-radius:8px; border:1px solid var(--border2); background:var(--inner); color:var(--text2);
         cursor:pointer; font-size:13px; }
  .btn:hover { border-color:var(--accent); color:var(--accent); }
  .btn.red { border-color:var(--up); color:var(--up); }
  .btn.green { border-color:var(--down); color:var(--down); }
  .btn.warn { border-color:var(--warn); color:var(--warn); }
  .acc { display:grid; grid-template-columns:repeat(auto-fill,minmax(150px,1fr)); gap:12px; }
  .acc .ac { background:var(--inner2); border:1px solid var(--border2); border-radius:12px; padding:12px 14px; }
  .acc .ac .lb { font-size:11px; color:var(--sub); }
  .acc .ac .vl { font-size:20px; font-weight:800; margin-top:4px; }
  .orderform { display:flex; flex-wrap:wrap; gap:12px; align-items:end; }
  .orderform label { font-size:12px; color:var(--sub); display:block; margin-bottom:4px; }
  .orderform select,.orderform input { background:var(--inner2); border:1px solid var(--border2); color:var(--text);
    border-radius:8px; padding:8px 10px; font-size:14px; min-width:110px; }
  .statbox { display:flex; gap:8px; margin:10px 0; flex-wrap:wrap; }
  .indpanel { display:none; border:1px dashed var(--border2); border-radius:10px; padding:10px 14px; margin-top:10px; }
  .indpanel.show { display:block; }
  .indpanel .orderform input { min-width:70px; padding:5px 8px; font-size:13px; }
  .bm-item { padding:9px 14px; cursor:pointer; font-size:13px; color:var(--text); border-bottom:1px solid var(--border); }
  .bm-item:hover { background:var(--inner); color:var(--accent); }
  .bm-item:last-child { border-bottom:none; }
  .ffrow { display:flex; align-items:center; gap:10px; margin-top:6px; }
  .ffl { width:56px; font-size:12px; color:var(--text2); white-space:nowrap; }
  .ffbar { flex:1; height:10px; background:var(--inner2); border:1px solid var(--border2); border-radius:5px; overflow:hidden; }
  .ffbar i { display:block; height:100%; border-radius:5px; transition:width .6s ease; }
  .ffr { width:52px; text-align:right; font-size:12px; color:var(--text2); }
  @media (max-width:640px){
    .price { font-size:28px; }
    .grid { grid-template-columns:1fr; }
    .comgrid { grid-template-columns:1fr; }
    .acc { grid-template-columns:1fr 1fr; }
    #mainChart { height:320px; }
    .panel { padding:12px 14px; }
    .head { flex-direction:column; align-items:flex-start; gap:8px; }
    .pricebox { text-align:left; }
    .wrap { padding:10px; }
    .indbar { margin-left:0; }
  }
</style>
</head>
<body>
<div class="toast" id="toast"></div>
<div class="wrap">
  <div class="head" id="head">
    <div>
      <div class="tt" data-go="">大宗商品多周期预测</div>
      <div class="sub" id="headSub">黄金 · 均线/MACD/RSI/布林带/支撑阻力</div>
    </div>
    <div class="pricebox" id="priceBox"></div>
    <div class="meta" id="metaLine"></div>
    <div><button class="navbtn" data-go="trade">💰 模拟交易</button>
      <button class="navbtn alt" data-go="alerts">🔔 提醒设置</button>
      <button class="navbtn" data-go="accuracy">📊 准确率</button>
      <div style="position:relative;display:inline-block">
        <button class="navbtn bot" id="botBtn">🤖 自动交易</button>
        <div id="botStatus" style="font-size:10px;color:var(--sub);margin-top:2px;text-align:right"></div>
        <div id="botMenu" style="display:none;position:absolute;right:0;top:100%;z-index:300;background:var(--panel-solid);border:1px solid var(--border2);border-radius:10px;min-width:200px;margin-top:6px;box-shadow:0 10px 34px rgba(0,0,0,.45)">
          <div style="padding:8px 12px;font-size:11px;color:var(--sub);border-bottom:1px solid var(--border)">启动机器人 · 当前商品+周期</div>
          <div class="bm-item" data-botstrat="ai">🧠 AI复合交易</div>
          <div class="bm-item" data-botstrat="shenlong">🐉 神龙指标交易</div>
          <div class="bm-item" data-botstop="1">⏹ 停止机器人</div>
          <div class="bm-item" data-go="bot">📋 机器人设置页</div>
        </div>
      </div>
      <button class="navbtn theme" id="themeBtn" data-theme="">🌓</button></div>
  </div>
  <div id="view"></div>
  <div class="foot">价格每3秒实时 · 分析每45秒更新 · 仅供技术研究，不构成投资建议</div>
</div>
<script>
var ORDER=["M1","M5","M15","M30","H1","H4","D1"];
var KIND={"多":["多","more"],"空":["空","empty"],"震荡":["震荡","neutral"]};
var curSym="gold", curDec=2, curSign="$", curUnit="";
var lastState=null, mainChart=null, mainTF=(function(){ try{ return localStorage.getItem("mainTF")||"H1"; }catch(e){ return "H1"; } })(), mainInd="macd", charts={}, lastPrice=null, prevDir={};
var homeData=null, botData=null, chartKL=null, lastTickPrice=null, forecastEnabled=true, forecastN=8;
var indParams={ma:[5,20], bb:[20,2], rsi:14, kdj:[9,3,3]};

function esc(s){ return String(s==null?"-":s); }
function sgn(c){ return (c==null)?false:(c>=0); }
function fmt(n,d){ d=d==null?2:d; return (n==null)?"--":Number(n).toLocaleString("zh-CN",{minimumFractionDigits:d,maximumFractionDigits:d}); }
function maArr(arr,n){ return arr.map(function(_,i){ if(i+1<n)return "-"; var s=0; for(var j=i-n+1;j<=i;j++)s+=arr[j]; return +((s/n).toFixed(2)); }); }
function boll2(arr,n,k){ var up=[],lo=[],mid=maArr(arr,n); for(var i=0;i<arr.length;i++){ if(mid[i]==="-"){up.push("-");lo.push("-");} else { var s=0; for(var j=i-n+1;j<=i;j++)s+=(arr[j]-mid[i])*(arr[j]-mid[i]); var sd=Math.sqrt(s/n); up.push(+(mid[i]+k*sd).toFixed(2)); lo.push(+(mid[i]-k*sd).toFixed(2)); } } return {up:up,lo:lo}; }
function emaArr(arr,n){ var k=2/(n+1),out=[],p=arr[0]; for(var i=0;i<arr.length;i++){ p=i? arr[i]*k+p*(1-k) : arr[i]; out.push(p); } return out; }
function macdArr(closes){ var e12=emaArr(closes,12),e26=emaArr(closes,26); var dif=e12.map(function(v,i){return v-e26[i];}); var dea=emaArr(dif,9); var hist=dif.map(function(v,i){return 2*(v-dea[i]);}); return {dif:dif,dea:dea,hist:hist}; }
function rsiArr(closes,n){ var out=[],g=0,l=0; for(var i=0;i<closes.length;i++){ if(i===0){out.push(50);continue;} var d=closes[i]-closes[i-1]; var gu=d>0?d:0, lu=d<0?-d:0; if(i<=n){ g+=gu; l+=lu; if(i===n){ g/=n; l/=n; } } else { g=(g*(n-1)+gu)/n; l=(l*(n-1)+lu)/n; } if(l===0){ out.push(100); } else { out.push(+(100-100/(1+g/l)).toFixed(2)); } } return out; }
function kdjArr(kl,n,k){ var K=[],D=[],pk=50,pd=50,rsv=0; for(var i=0;i<kl.length;i++){ if(i>=n-1){ var ll=Infinity,hh=-Infinity; for(var j=i-n+1;j<=i;j++){ ll=Math.min(ll,kl[j][3]); hh=Math.max(hh,kl[j][2]); } rsv=hh===ll?50:((kl[i][4]-ll)/(hh-ll)*100); } pk=2/3*pk+1/3*rsv; pd=2/3*pd+1/3*pk; K.push(pk); D.push(pd); } return {k:K,d:D,j:K.map(function(v,i){return 3*v-2*D[i];})}; }

// ===== 神龙通道V3 (移植自 神龙通道免费源码.mq4) =====
function calcXma(prices,len){
  var h=Math.floor((len-1)/2),N=prices.length,src=[],dst=[],i,j,sum;
  for(i=0;i<N;i++){
    if(i>=h){ sum=0; for(j=i+h;j>=i-h;j--){ if(j<N) sum+=prices[j]; } src[i]=sum/len; }
    else { sum=0; for(j=i+h;j>=0;j--){ if(j<N) sum+=prices[j]; } src[i]=(sum+sum*((h-i)/(h+i+1)))/len; }
  }
  for(i=0;i<N;i++){
    if(i>=h){ sum=0; for(j=i+h;j>=i-h;j--){ if(j<src.length) sum+=src[j]; } dst[i]=sum/len; }
    else { sum=0; for(j=i+h;j>=0;j--){ if(j<src.length) sum+=src[j]; } dst[i]=(sum+sum*((h-i)/(h+i+1)))/len; }
  }
  return dst;
}
function shenLong(kl, params){
  var N=kl.length, xmaLen=params&&params.xma||25, beltN=params&&params.belt||90, k=3.9;
  if(N<30) return null;
  var revH=[],revL=[],i;
  for(i=N-1;i>=0;i--){ revH.push(kl[i][2]); revL.push(kl[i][3]); } // 0=newest
  // 加权高低点 21根
  var wH=[],wL=[];
  for(i=0;i<N-20;i++){ var sh=0,sl=0; for(var w=0;w<21;w++){ sh+=(20-w)*revH[i+w]; sl+=(20-w)*revL[i+w]; } wH[i]=sh/210; wL[i]=sl/210; }
  // 最旧20根加权缺失→用原始高低价兜底
  for(i=0;i<N;i++){ if(!wH[i]) wH[i]=revH[i]; if(!wL[i]) wL[i]=revL[i]; }
  // 带平滑(period90, 种子=最旧bar加权值确保收敛)
  var belt3=[],belt4=[],p3=wH[N-1],p4=wL[N-1];
  for(i=N-1;i>=0;i--){ belt3[i]=(2*wH[i]+(beltN-1)*p3)/(beltN+1); p3=belt3[i]; belt4[i]=(2*wL[i]+(beltN-1)*p4)/(beltN+1); p4=belt4[i]; }
  // XMA²
  var x1=calcXma(revL,xmaLen), x2=calcXma(revH,xmaLen);
  var buf1=[],buf2=[],buf3=[],buf4=[],buf5=[],buf6=[],sl1=[],sl2=[],ch=[];
  for(i=0;i<N;i++){
    var a=x1[i],b=x2[i];
    buf1[i]=2*a-b; buf2[i]=2*b-a; buf3[i]=k*(b-a)+b; buf4[i]=a-k*(b-a);
    buf5[i]=belt3[i]; buf6[i]=belt4[i];
    sl1[i]=belt3[i]+2*(belt3[i]-belt4[i]); sl2[i]=belt4[i]-2*(belt3[i]-belt4[i]);
    ch[i]= (buf2[i]>sl1[i]&&buf1[i]>sl2[i])?0 : (buf2[i]<sl1[i]&&buf1[i]<sl2[i])?1 : 2; // 0上升 1下降 2横盘
  }
  // 反转回图表顺序(旧→新)
  function rev(a){ return a.slice().reverse(); }
  var out={buf1:rev(buf1),buf2:rev(buf2),buf3:rev(buf3),buf4:rev(buf4),buf5:rev(buf5),buf6:rev(buf6),sl1:rev(sl1),sl2:rev(sl2),ch:rev(ch),sig:[]};
  // 信号(图表顺序, 源码触发 + 显示配对)
  var state='flat';
  for(i=1;i<N;i++){
    var L=kl[i][3],H=kl[i][2],Lp=kl[i-1][3],Hp=kl[i-1][2];
    var b1=out.buf1[i],b1p=out.buf1[i-1],b2=out.buf2[i],b2p=out.buf2[i-1];
    var up=(out.ch[i]===0),down=(out.ch[i]===1),side=(out.ch[i]===2);
    var buyCross = b1p<Lp && b1>L;
    var sellCross = Hp<b2p && H>b2;
    var buyCond  = buyCross  && (up||side);
    var sellCond = sellCross && (down||side);
    var clsBuyCond  = sellCross && (up||!side) && !(down||side);
    var clsSellCond = buyCross  && (down||!side) && !(up||side);
    if(state==='long' && clsBuyCond){ out.sig.push({i:i,t:'clsBuy',price:H}); state='flat'; }
    else if(state==='short' && clsSellCond){ out.sig.push({i:i,t:'clsSell',price:L}); state='flat'; }
    if(buyCond && state!=='long'){
      if(state==='short'){ out.sig.push({i:i,t:'clsSell',price:L}); }
      out.sig.push({i:i,t:'buy',price:L}); state='long';
    } else if(sellCond && state!=='short'){
      if(state==='long'){ out.sig.push({i:i,t:'clsBuy',price:H}); }
      out.sig.push({i:i,t:'sell',price:H}); state='short';
    }
  }
  return out;
}
// ===== 预测未来N根 (线性回归+ATR) =====
function linSlope(arr){
  var n=arr.length, sx=0,sy=0,sxy=0,sxx=0;
  for(var i=0;i<n;i++){ sx+=i; sy+=arr[i]; sxy+=i*arr[i]; sxx+=i*i; }
  var d=n*sxx-sx*sx; if(d===0) return 0;
  return (n*sxy-sx*sy)/d;
}
function atrVal(kl,n){
  var s=0,c=0;
  for(var i=kl.length-n;i<kl.length;i++){ if(i<1) continue; var h=kl[i][2],l=kl[i][3],pc=kl[i-1][4]; s+=Math.max(h-l,Math.abs(h-pc),Math.abs(l-pc)); c++; }
  return c?s/c:0;
}
// ===== y轴自适应: 只按可见蜡烛定价, 叠加线超出裁切 =====
function visibleCandleRange(kl, nTotal, startPct, endPct){
  var lo=Infinity, hi=-Infinity, i;
  var s0=Math.max(0,Math.floor(nTotal*startPct/100)), e0=Math.min(kl.length-1,Math.ceil(nTotal*endPct/100)-1);
  for(i=s0;i<=e0;i++){ if(kl[i]){ lo=Math.min(lo,kl[i][3]); hi=Math.max(hi,kl[i][2]); } }
  if(!isFinite(lo)||!isFinite(hi)){ for(i=0;i<kl.length;i++){ lo=Math.min(lo,kl[i][3]); hi=Math.max(hi,kl[i][2]); } }
  var span=hi-lo; if(!span||span<0) span=hi*0.002;
  var pad=Math.max(span*0.015, hi*0.0008);
  return {min:lo-pad, max:hi+pad};
}
function applyYRange(){
  if(!mainChart||!chartKL||chartKL.length<2) return;
  var dz=((mainChart.getOption&&mainChart.getOption())||{}).dataZoom||[];
  var start=dz[0]&&dz[0].start!=null?dz[0].start:0, end=dz[0]&&dz[0].end!=null?dz[0].end:100;
  var nTotal=chartKL.length+(forecastEnabled?forecastN:0);
  var yr=visibleCandleRange(chartKL,nTotal,start,end);
  try{ mainChart.setOption({yAxis:[{min:yr.min,max:yr.max}]}); }catch(e){}
}
function calcForecast(kl,frames){
  var N=kl.length; if(N<30) return null;
  var closes=kl.map(function(b){return b[4];});
  var slope=linSlope(closes.slice(-20));
  var lastC=closes[closes.length-1];
  var maxSlope=lastC*0.005;                     // 斜率上限(单根≤0.5%)
  if(Math.abs(slope)>maxSlope) slope=Math.sign(slope)*maxSlope;
  var atr=Math.min(atrVal(kl,14), lastC*0.06);  // ATR上限
  var proj=[], i;
  for(i=1;i<=forecastN;i++) proj.push(lastC+slope*i);
  var tUp=proj[proj.length-1]+atr*0.8, tLo=proj[proj.length-1]-atr*0.8;
  var parts=[],ws=0;
  [["D1",0.4],["H1",0.35],["M30",0.25]].forEach(function(p){ var f=frames&&frames[p[0]]; if(f&&f.score!=null){ parts.push(f.score*p[1]); ws+=p[1]; } });
  var score = parts.length? (parts.reduce(function(a,b){return a+b;})/ws)*20 : 0;
  var dir = score>=45?"多":score<=-45?"空":"震荡";
  return {proj:proj, tUp:tUp, tLo:tLo, slope:slope, dir:dir, score:Math.round(score)};
}

function loadPrefs(){
  try{
    var t=localStorage.getItem("goldTheme"); if(t) document.body.setAttribute("data-theme",t);
    var ip=localStorage.getItem("goldIndParams"); if(ip) indParams=JSON.parse(ip);
    var mi=localStorage.getItem("goldMainInd"); if(mi) mainInd=mi;
    if(localStorage.getItem("goldFc")!=null) forecastEnabled=localStorage.getItem("goldFc")==="1";
  }catch(e){}
}
function savePrefs(){ try{
  localStorage.setItem("goldTheme",document.body.getAttribute("data-theme")||"dark");
  localStorage.setItem("goldIndParams",JSON.stringify(indParams));
  localStorage.setItem("goldMainInd",mainInd);
  localStorage.setItem("goldFc",forecastEnabled?"1":"0");
}catch(e){} }
function toggleTheme(){ var t=document.body.getAttribute("data-theme")==="light"?"dark":"light"; document.body.setAttribute("data-theme",t); savePrefs(); }
function catCls(cat){ return cat==="贵金属"?"precious":cat==="能源"?"energy":cat==="金属"?"metal":"agri"; }

function route(){
  var h=location.hash.replace(/^#\//,"");
  if(!h){ showHome(); } else if(h==="trade"){ showTrade(); } else if(h==="alerts"){ showAlertsPage(); } else if(h==="bot"){ showBotPage(); } else if(h==="accuracy"){ showAccuracy(); } else { showDetail(h); }
}
window.addEventListener("hashchange",route);
document.addEventListener("click",function(e){
  var el=e.target.closest("[data-go]");
  if(el){ location.hash="#/"+(el.getAttribute("data-go")||""); return; }
  var qs=e.target.closest("[data-side]");
  if(qs){ qtTrade(qs.getAttribute("data-side")); return; }
  var act=e.target.closest("[data-act]");
  if(act){ postAction(act.getAttribute("data-act"),{}); return; }
  var cl=e.target.closest("[data-close]");
  if(cl){ postAction("/api/close",{id:cl.getAttribute("data-close")}); return; }
  var ca=e.target.closest("[data-cancel]");
  if(ca){ postAction("/api/cancel",{id:ca.getAttribute("data-cancel")}); return; }
  var bt=e.target.closest("[data-bot]");
  if(bt){ toggleBot(); return; }
  var ind=e.target.closest("[data-ind]");
  if(ind){ mainInd=ind.getAttribute("data-ind"); document.querySelectorAll("#indbar button").forEach(function(x){x.classList.toggle("on",x===ind);}); savePrefs(); if(lastState) drawMain(); return; }
  var pt=e.target.closest("[data-tf]");
  if(pt){ mainTF=pt.getAttribute("data-tf"); try{ localStorage.setItem("mainTF",mainTF); }catch(e){} document.querySelectorAll("#tfbar button").forEach(function(x){x.classList.remove("on");}); pt.classList.add("on"); if(lastState) drawMain(); return; }
  if(e.target.hasAttribute&&e.target.hasAttribute("data-theme")){ toggleTheme(); return; }
  if(e.target.closest("[data-param]")){ var p=document.getElementById("indPanel"); if(p) p.classList.toggle("show"); return; }
  if(e.target.closest("[data-apply-param]")){ applyIndParams(); return; }
  var fc=e.target.closest("[data-fc]");
  if(fc){ forecastEnabled=!forecastEnabled; fc.classList.toggle("on",forecastEnabled); savePrefs(); if(lastState) drawMain(); return; }
  if(e.target.closest("#botBtn")){ toggleBotMenu(); return; }
  var bs2=e.target.closest("[data-bscope]");
  if(bs2){ setBotScope(bs2.getAttribute("data-bscope")); return; }
  if(e.target.closest("[data-closebot]")){ postAction("/api/closebot",{}); return; }
  var bs=e.target.closest("[data-botstrat]");
  if(bs){ activateBot(bs.getAttribute("data-botstrat")); return; }
  if(e.target.closest("[data-botstop]")){ closeBotMenu(); postAction("/api/bot",{enabled:false}); return; }
  if(e.target.closest("#botMenu")==null){ closeBotMenu(); }
});
function toggleBotMenu(){ var m=document.getElementById("botMenu"); if(m) m.style.display=m.style.display==="none"?"block":"none"; }
function closeBotMenu(){ var m=document.getElementById("botMenu"); if(m) m.style.display="none"; }
function activateBot(strategy){
  closeBotMenu();
  if(!curSym){ var t=document.getElementById("toast"); t.textContent="⚠️ 请先在商品页面选择"; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},2500); return; }
  fetch("/api/bot/activate",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({sym:curSym,tf:mainTF,strategy:strategy})})
    .then(function(r){return r.json();}).then(function(res){
      var t=document.getElementById("toast"); t.textContent=(res.ok?"✅ ":"⚠️ ")+res.msg; t.classList.add("show");
      setTimeout(function(){t.classList.remove("show");},3500);
      updateBotStatus();
    }).catch(function(){});
}
function updateBotStatus(){
  fetch("/api/bot?t="+Date.now()).then(function(r){return r.json();}).then(function(b){
    var el=document.getElementById("botStatus"); if(!el) return;
    if(b.enabled){
      var sname=b.strategy==="shenlong"?"🐉神龙":"🧠AI";
      var symKey=(b.symbols&&b.symbols[0])||"";
      var nm=symKey;
      if(homeData&&homeData.commodities){ var f=homeData.commodities.find(function(c){return c.key===symKey;}); if(f) nm=f.name; }
      else if(lastState&&lastState.meta&&curSym===symKey){ nm=lastState.meta.name; }
      el.innerHTML="<span style='color:#3fb950'>●</span> "+sname+" · "+esc(nm)+" "+esc(b.tf)+" 运行中";
    } else { el.innerHTML=""; }
  }).catch(function(){});
}

// ---------- 首页 ----------
function showHome(){
  curSym=null;
  document.getElementById("headSub").textContent="14个国际大宗商品 · 价格实时 · 多周期自动判断";
  var v=document.getElementById("view");
  v.innerHTML="<div class='panel'><div class='sech'><span class='ico' style='background:linear-gradient(#58a6ff,#8b6bff)'></span>大宗商品</div><div class='comgrid' id='comGrid'></div></div>"+
    "<div class='panel'><div class='sech'><span class='ico' style='background:linear-gradient(#ffd166,#ff9f43)'></span>金属</div><div class='comgrid' id='metalGrid'></div></div>"+
    "<div class='panel'><div class='sech'><span class='ico' style='background:linear-gradient(#ffd166,#ff4d4f)'></span>高准确度推断</div><div id='hiConf'></div></div>"+
    "<div class='panel news'><h3>财经新闻 <span style='font-size:11px;color:#8b949e'>重要=金色</span></h3><ul id='homeNews'></ul></div>";
  homeRefresh();
  if(!window._htimer) window._htimer=setInterval(homeRefresh,15000);
}
function homeRefresh(){
  fetch("/api/home?t="+Date.now()).then(function(r){return r.json();}).then(function(h){
    homeData=h;
    renderHomeHead(h);
    renderComGrid("comGrid",h.commodities||[]);
    renderComGrid("metalGrid",(h.commodities||[]).filter(function(c){return c.cat==="贵金属"||c.cat==="金属";}));
    renderHighConf(h.high_conf||[]);
    var nu=document.getElementById("homeNews");
    if(nu){ nu.innerHTML=(h.news||[]).map(function(n){var t=typeof n==="string"?n:n.title; var imp=(typeof n==="object"&&n.important); return "<li class='"+(imp?"imp":"")+"'>"+(imp?"<span class='imp-tag'>重要</span> ":"")+"• "+esc(t)+"</li>";}).join(""); }
    showAlerts(h.alerts||[]);
  }).catch(function(e){ console.error(e); });
}
function renderHomeHead(h){
  var dxy=h.dxy||{};
  var gold=(h.commodities||[]).find(function(c){return c.key==="gold";});
  var ph="",pc="";
  if(gold&&gold.price!=null){ var up=sgn(gold.chg); ph="<div class='lb'>黄金现货价</div><div><span class='price "+(up?"up":"down")+"'>"+fmt(gold.price,2)+"</span><span class='pchg "+(up?"up":"down")+"'>"+(up?"+":"")+gold.chg+"%</span></div>"; }
  else { ph="<div class='lb'>黄金现货价</div><div><span class='price'>--</span></div>"; }
  document.getElementById("priceBox").innerHTML=ph;
  document.getElementById("metaLine").innerHTML=
    "<span>美元指数 <b>"+(dxy.value!=null?dxy.value.toFixed(2):"--")+"</b> "+((dxy.dir==="up")?"↑":(dxy.dir==="down")?"↓":"→")+"</span>"+
    "<span id='homeCons'></span><span>"+esc(h.ts)+"</span>";
  var chips=(h.commodities||[]).map(function(c){ var d=c.dirs&&c.dirs.D1; var cls=d==="多"?"more":d==="空"?"empty":"neutral"; return "<span class='chip "+cls+"'>"+c.name+(d?("·"+d.charAt(0)):"·?")+"</span>"; }).join("");
  var el=document.getElementById("homeCons"); if(el) el.innerHTML=chips;
}
function renderComGrid(id,list){
  var el=document.getElementById(id); if(!el) return;
  el.innerHTML=list.map(function(c){
    var up=sgn(c.chg);
    var conf=(c.confs&&c.confs.D1)!=null?c.confs.D1:"-";
    var dir=c.dirs&&c.dirs.D1?c.dirs.D1:"-";
    var cls=dir==="多"?"more":dir==="空"?"empty":"neutral";
    var chips=ORDER.map(function(tf){ var d=c.dirs&&c.dirs[tf]; var cc=d==="多"?"more":d==="空"?"empty":"neutral"; return "<span class='chip "+cc+"'>"+tf+"·"+(d?d.charAt(0):"?")+"</span>"; }).join("");
    return "<div class='comcard "+catCls(c.cat)+"' data-go='"+c.key+"'>"+
      "<div class='cn'>"+esc(c.name)+" <span style='font-size:10px;color:#8b949e'>"+esc(c.unit)+"</span></div>"+
      "<div class='cp "+(up?"up":"down")+"'>"+fmt(c.price,c.decimals)+" <span class='pchg'>"+(c.chg!=null?((up?"+":"")+c.chg+"%"):"-")+"</span></div>"+
      "<div><span class='badge "+cls+"' style='font-size:14px'>D1·"+(dir==="-"?"?":dir)+"</span> <span style='font-size:11px;color:#8b949e'>置信 "+conf+"%</span></div>"+
      "<div class='dirs'>"+chips+"</div></div>";
  }).join("");
}
function renderHighConf(list){
  var el=document.getElementById("hiConf"); if(!el) return;
  if(!list.length){ el.innerHTML="<div style='color:#8b949e;font-size:13px'>暂无高置信度信号(置信度≥70%)</div>"; return; }
  el.innerHTML=list.map(function(x){
    var cls=x.dir==="多"?"more":"empty";
    return "<div class='hicard' data-go='"+x.key+"'><b class='"+cls+"'>"+x.dir+"</b> "+esc(x.name)+" · "+x.tf+" · 置信"+x.conf+"%</div>";
  }).join("");
}

// ---------- 详情页 ----------
function showDetail(sym){
  if(mainChart){ try{ mainChart.dispose(); }catch(e){} mainChart=null; }
  Object.keys(charts).forEach(function(k){ try{charts[k].dispose();}catch(e){} }); charts={};
  if(ffChart){ try{ ffChart.dispose(); }catch(e){} ffChart=null; }
  if(botEqRef){ try{ botEqRef.dispose(); }catch(e){} botEqRef=null; }
  botFp=""; spikeFp=""; detailNewsFp="";
  chartKL=null; lastPrice=null; prevDir={};
  curSym=sym;
  curDec=(homeData&&homeData.commodities.find(function(c){return c.key===sym;}))?homeData.commodities.find(function(c){return c.key===sym;}).decimals:2;
  var v=document.getElementById("view");
  v.innerHTML="<a class='back' data-go=''>← 返回首页</a>"+
    "<div class='panel'><div class='toolbar'>"+
    "<div class='tfbar' id='tfbar'></div>"+
    "<div class='indbar' id='indbar'><button data-ind='none' class='"+(mainInd==='none'?"on":"")+"'>无</button><button data-ind='macd' class='"+(mainInd==='macd'?"on":"")+"'>MACD</button><button data-ind='rsi' class='"+(mainInd==='rsi'?"on":"")+"'>RSI</button><button data-ind='kdj' class='"+(mainInd==='kdj'?"on":"")+"'>KDJ</button><button data-ind='shenlong' class='"+(mainInd==='shenlong'?"on":"")+"'>🐉神龙</button>"+
    "<button data-param=''>⚙ 参数</button><button data-fc='' class='"+(forecastEnabled?'on':'')+"'>📈预测</button></div></div>"+
    "<div id='mainChartLabel' style='font-size:12px;color:#8b949e'>主图 · H1</div><div id='mainChart'></div>"+
    "<div id='shenLegend' style='display:none;font-size:11px;color:#8b949e;margin-top:4px'>🐉 神龙信号: <span style='color:#ff4d4f'>买=触下轨</span> · <span style='color:#3fb950'>卖=触上轨</span> · <span style='color:#d29922'>平多=上轨离场 / 平空=下轨离场</span></div>"+
    "<div class='indpanel' id='indPanel'><div class='orderform'>"+
    "<div><label>均线周期(逗号分隔)</label><input id='pMA' value='5,20'></div>"+
    "<div><label>布林周期</label><input id='pBB' value='20'></div>"+
    "<div><label>布林倍数</label><input id='pBBK' value='2'></div>"+
    "<div><label>RSI周期</label><input id='pRSI' value='14'></div>"+
    "<div><label>KDJ周期</label><input id='pKDJ' value='9,3,3'></div>"+
    "<div><button class='btn green' data-apply-param=''>应用</button></div></div>"+
    "<div style='font-size:11px;color:#8b949e;margin-top:6px'>参数只影响图表显示，卡片评分/机器人信号仍用标准参数</div></div></div>"+
    "<div class='grid' id='grid'></div>"+
    "<div class='panel' id='quickTrade'></div>"+
    "<div class='panel' id='fundflow'></div>"+
    "<div class='panel' id='botMonitor'></div>"+
    "<div class='panel' id='spikes'></div>"+
    "<div class='panel news'><h3>财经新闻 <span style='font-size:11px;color:#8b949e'>重要=金色</span></h3><ul id='detailNews'></ul></div>";
  initTFbar();
  buildGridSkeleton();
  renderQuickTrade();
  detailRefresh();
  renderBotMonitor();
  renderFundflow();
  if(!window._dtimer){ window._dtimer=setInterval(detailRefresh,15000); window._ttimer=setInterval(function(){livePrice();renderBotMonitor();renderFundflow();},5000); }
}
function initTFbar(){
  var bar=document.getElementById("tfbar"); if(!bar) return;
  bar.innerHTML=ORDER.map(function(t){ return "<button data-tf='"+t+"' class='"+(t===mainTF?"on":"")+"'>"+t+"</button>"; }).join("");
}
function buildGridSkeleton(){
  var g=document.getElementById("grid"); if(!g) return;
  var html="";
  ORDER.forEach(function(tf){
    html+="<div class='card' id='card-"+tf+"'>"+
      "<div class='top'><span class='tf'>"+tf+"</span><span class='badge' id='bd-"+tf+"'>-</span></div>"+
      "<div class='crow'><span class='cprice' id='cp-"+tf+"'>--</span><span class='pchg' id='cg-"+tf+"'></span>"+
      "<div class='ring' id='rg-"+tf+"' style='--v:0'><div class='rin' id='rc-"+tf+"'>0%</div></div></div>"+
      "<div class='miniChart' id='mini-"+tf+"'></div>"+
      "<div class='summary' id='sm-"+tf+"'></div>"+
      "<div class='sentiment' id='st-"+tf+"'></div>"+
      "<div class='kv' id='kv-"+tf+"'></div></div>";
  });
  g.innerHTML=html;
}
function refreshCards(s){
  var frames=s.frames||{};
  ORDER.forEach(function(tf){
    var f=frames[tf]||{};
    var k=KIND[f.direction]||KIND["震荡"];
    var bd=document.getElementById("bd-"+tf);
    if(bd){ bd.textContent=k[0]; bd.className="badge "+k[1]+(f.flip?" flip":""); }
    var cp=document.getElementById("cp-"+tf);
    if(cp) cp.textContent=f.close!=null?fmt(f.close,curDec):"--";
    var cg=document.getElementById("cg-"+tf);
    if(cg){ cg.textContent=(f.chg!=null?((sgn(f.chg)?"+":"")+f.chg+"%"):""); cg.className="pchg "+(sgn(f.chg)?"up":"down"); }
    var conf=(f.conf==null)?0:Math.min(100,f.conf);
    var rg=document.getElementById("rg-"+tf);
    if(rg){ rg.className="ring "+k[1]; rg.style.setProperty("--v",conf); }
    var rc=document.getElementById("rc-"+tf); if(rc) rc.textContent=conf+"%";
    var sm=document.getElementById("sm-"+tf);
    if(sm) sm.innerHTML=f.error?'<span style="color:var(--warn)">'+esc(f.error)+'</span>':esc(f.summary||"");
    var st=document.getElementById("st-"+tf);
    if(st) st.innerHTML=f.sentiment?'<span class="'+(f.sentiment_class==="bull"?"up":f.sentiment_class==="bear"?"down":"")+'">多空：'+esc(f.sentiment)+' <span style="color:#8b949e;font-weight:400">[推断]</span></span>':"";
    var kv=document.getElementById("kv-"+tf);
    if(kv) kv.innerHTML=(!f.accumulate&&f.vol_ratio!=null)?("量 "+esc(f.vol)+" · 均量比 "+esc(f.vol_ratio)+(f.spike?" · <b style='color:var(--warn)'>放量!</b>":"")+" · 置信"+esc(f.conf)+"% · 评分 "+esc(f.score)+" · RSI "+esc(f.rsi)+" · 布林 "+esc(f.boll_pos)+" · 阻力<b class='up'>"+fmt(f.resistance,curDec)+"</b>/支撑<b class='down'>"+fmt(f.support,curDec)+"</b>"):"";
    var el=document.getElementById("mini-"+tf);
    if(el && s.klines && s.klines[tf] && s.klines[tf].length>2){
      if(!charts[tf]) charts[tf]=echarts.init(el);
      drawMini(charts[tf],tf,s);
    }
  });
}
function drawMini(chart,tf,s){
  var kl=s.klines[tf]; var closes=kl.map(function(b){return b[4];});
  var f=s.frames[tf]||{}; var c=(f.direction==="空")?"#3fb950":"#ff4d4f";
  chart.setOption({backgroundColor:"transparent",animation:false,grid:{left:2,right:2,top:4,bottom:2},
    xAxis:{type:"category",data:kl.map(function(b){return b[0];}),show:false},yAxis:{type:"value",show:false,scale:true},
    series:[{type:"line",data:closes,symbol:"none",smooth:true,lineStyle:{width:1.6,color:c},
      areaStyle:{color:new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:(f.direction==="空")?"rgba(63,185,80,.35)":"rgba(255,77,79,.35)"},{offset:1,color:"rgba(0,0,0,0)"}])}}]},{lazyUpdate:true});
}
function applyIndParams(){
  try{
    var ma=(document.getElementById("pMA").value||"5,20").split(",").map(function(x){return parseInt(x);}).filter(function(x){return x>0&&x<250;});
    if(ma.length) indParams.ma=ma;
    indParams.bb=[parseInt(document.getElementById("pBB").value)||20, parseFloat(document.getElementById("pBBK").value)||2];
    indParams.rsi=parseInt(document.getElementById("pRSI").value)||14;
    var kd=(document.getElementById("pKDJ").value||"9,3,3").split(",").map(function(x){return parseInt(x);});
    if(kd.length===3) indParams.kdj=kd;
    savePrefs();
    var t=document.getElementById("toast"); t.textContent="✅ 指标参数已应用"; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},2000);
    if(lastState) drawMain();
  }catch(e){}
}
function detailRefresh(){
  if(!curSym) return;
  fetch("/api/state?sym="+curSym+"&tf="+mainTF+"&t="+Date.now()).then(function(r){return r.json();}).then(function(s){
    lastState=s;
    curDec=(s.meta&&s.meta.decimals)!=null?s.meta.decimals:2;
    curSign=(s.meta&&s.meta.sign)||"$"; curUnit=(s.meta&&s.meta.unit)||"";
    document.getElementById("headSub").textContent=s.meta.name+" · "+s.meta.unit+" · 均线/MACD/RSI/布林带/支撑阻力";
    renderDetailHead(s);
    refreshCards(s);
    drawMain();
    renderSpikes(s);
    var nu=document.getElementById("detailNews");
    if(nu){ var nh=(s.news||[]).map(function(n){var t=typeof n==="string"?n:n.title; var imp=(typeof n==="object"&&n.important); return "<li class='"+(imp?"imp":"")+"'>"+(imp?"<span class='imp-tag'>重要</span> ":"")+"• "+esc(t)+"</li>";}).join(""); if(nh!==detailNewsFp){ nu.innerHTML=nh; detailNewsFp=nh; } }
    showFlip(s);
    showAlerts(s.alerts||[]);
  }).catch(function(e){ console.error(e); });
}
function renderDetailHead(s){
  var spot=s.spot||{};
  var hb=document.getElementById("priceBox");
  if(spot.price!=null){
    var up=sgn(spot.chg);
    hb.innerHTML="<div class='lb'>"+esc(s.meta.name)+"现价</div><div><span class='price "+(up?"up":"down")+"'>"+fmt(spot.price,curDec)+"</span><span class='pchg "+(up?"up":"down")+"'>"+(up?"+":"")+spot.chg+"%</span></div>";
  }
  var fq=s.futures||{};
  var fh="";
  if(fq.vol!=null) fh+="今日量 <b>"+esc(fq.vol)+"</b>";
  if(fq.oi!=null) fh+=" · 持仓 <b>"+esc(fq.oi)+"</b>"+(fq.oi_chg!=null?(" <span class='"+(fq.oi_chg>0?"up":"down")+"'>("+(fq.oi_chg>0?"+":"")+fq.oi_chg+")</span>"):"");
  document.getElementById("metaLine").innerHTML=
    "<span>美元指数 <b>"+((s.dxy&&s.dxy.value!=null)?s.dxy.value.toFixed(2):"--")+"</b></span>"+
    (fh?"<span>"+fh+"</span>":"")+
    "<span>"+esc(s.ts)+"</span>";
}
function livePrice(){
  if(!curSym) return;
  fetch("/api/tick?sym="+curSym+"&t="+Date.now()).then(function(r){return r.json();}).then(function(t){
    if(t.price!=null){
      lastTickPrice=t.price;
      var el=document.querySelector("#priceBox .price"); if(!el) return;
      el.textContent=fmt(t.price,curDec);
      el.className="price "+(sgn(t.chg)?"up":"down");
      lastPrice=t.price;
      updateMainLive();
    }
  }).catch(function(){});
}
function updateMainLive(){
  if(!mainChart||!chartKL||chartKL.length===0||lastTickPrice==null) return;
  var last=chartKL[chartKL.length-1];
  last[2]=Math.max(last[2],lastTickPrice);
  last[3]=Math.min(last[3],lastTickPrice);
  last[4]=lastTickPrice;
  try{
    mainChart.setOption({
      series:[{name:"K线",data:chartKL.map(function(b){return [b[1],b[4],b[2],b[3]];})}],
      yAxis:[{markLine:{silent:true,symbol:"none",animation:false,
        label:{show:true,fontSize:10,color:"#ffd166"},
        data:[{yAxis:lastTickPrice,lineStyle:{color:"#8b949e",type:"solid"},label:{formatter:"最新 "+fmt(lastTickPrice,curDec)}}]}}]
    },{lazyUpdate:true});
  }catch(e){}
}
function splitSegs(ch,vals,cats){
  var segs=[],start=0;
  for(var i=1;i<=ch.length;i++){
    if(i===ch.length||ch[i]!==ch[start]){
      var data=[];
      for(var j=0;j<cats.length;j++) data.push((j>=start&&j<i)?vals[j]:null);
      segs.push({cls:ch[start],data:data}); start=i;
    }
  }
  return segs;
}
function drawMain(){
  var s=lastState; if(!s||!s.klines) return;
  var tf=s.klines[mainTF]?mainTF:null;
  if(!tf){ for(var i=0;i<ORDER.length;i++){ if(s.klines[ORDER[i]]){ tf=ORDER[i]; break; } } }
  if(!tf||!s.klines[tf]) return;
  var el=document.getElementById("mainChart"); if(!el) return;
  var lbl=document.getElementById("mainChartLabel"); if(lbl) lbl.textContent="主图 · "+tf+(tf!==mainTF?"(暂用)":"");
  if(!mainChart){ try{ mainChart=echarts.init(el); }catch(e){ mainChart=null; } } if(!mainChart) return;
  var kl=s.klines[tf];
  if(!kl || kl.length<2){
    if(mainChart) mainChart.clear();
    if(lbl) lbl.textContent="主图 · "+tf+" (数据积累中, 已收"+(kl?kl.length:0)+"根)";
    return;
  }
  kl = kl.filter(function(b){ return b&&isFinite(b[1])&&isFinite(b[2])&&isFinite(b[3])&&isFinite(b[4]); });
  if(kl.length<2){ if(mainChart) mainChart.clear(); return; }
  chartKL=kl;
  if(!mainChart._dz){ mainChart._dz=true; mainChart.on("datazoom",applyYRange); }
  var isShen = mainInd==="shenlong";
  var slg=document.getElementById("shenLegend"); if(slg) slg.style.display = isShen ? "block" : "none";
  var hasInd = mainInd!=="none" && !isShen;
  var cats=kl.map(function(b){return b[0];});
  var ohlc=kl.map(function(b){return [b[1],b[4],b[2],b[3]];});
  var closes=kl.map(function(b){return b[4];});
  var vols=kl.map(function(b){ return {value:b[5],itemStyle:{color:(b[4]>=b[1]?"rgba(255,77,79,.7)":"rgba(63,185,80,.7)")}}; });
  var rawVols=kl.map(function(b){return b[5]||0;});
  var f=s.frames[tf]||{};
  var markData=[];
  for(var vi=20;vi<kl.length;vi++){ var ss=0; for(var vj=vi-20;vj<vi;vj++) ss+=rawVols[vj]; var va=ss/20;
    if(va>0&&rawVols[vi]>va*2) markData.push({coord:[cats[vi],kl[vi][2]],value:"放量"}); }
  if(markData.length>5) markData=markData.slice(-5);

  var fc = forecastEnabled ? calcForecast(kl, s.frames) : null;
  var catsExt = fc ? cats.concat(Array.apply(null,new Array(fc.proj.length)).map(function(){return "";})) : cats;
  var n=kl.length;
  var dz0=((mainChart&&mainChart.getOption&&mainChart.getOption())||{}).dataZoom||[];
  var startPct=dz0[0]&&dz0[0].start!=null?dz0[0].start:0, endPct=dz0[0]&&dz0[0].end!=null?dz0[0].end:100;
  var nTotal=n+(fc?forecastN:0);
  var yr=visibleCandleRange(kl,nTotal,startPct,endPct);

  var maColors=["#58a6ff","#d29922","#3fb950","#ff4d4f","#8b6bff","#0fb6b6"];
  var maSeries=indParams.ma.map(function(p,idx){ return {name:"MA"+p,type:"line",data:maArr(closes,p),smooth:true,symbol:"none",lineStyle:{width:1.1,color:maColors[idx%maColors.length]}}; });
  var bb=boll2(closes,indParams.bb[0],indParams.bb[1]);

  var grids=[],xArr=[],yArr=[];
  if(hasInd){ grids=[{left:8,right:14,top:26,height:"44%",containLabel:true},{left:8,right:14,top:"46%",height:"13%",containLabel:false},{left:8,right:14,top:"63%",height:"18%",containLabel:false}]; }
  else { grids=[{left:8,right:14,top:26,height:"58%",containLabel:true},{left:8,right:14,top:"72%",height:"16%",containLabel:false}]; }
  xArr.push({type:"category",data:catsExt,axisLine:{lineStyle:{color:"#333"}},axisLabel:{color:"#666",fontSize:10}});
  xArr.push({type:"category",data:catsExt,gridIndex:1,axisLabel:{show:false},axisLine:{lineStyle:{color:"#333"}},splitLine:{show:false}});
  yArr.push({type:"value",scale:false,position:"right",min:yr.min,max:yr.max,axisLabel:{color:"#666",fontSize:10},splitLine:{lineStyle:{color:"#1c2431"}}});
  yArr.push({type:"value",gridIndex:1,position:"right",axisLabel:{color:"#666",fontSize:9},splitLine:{show:false}});
  var indSeries=[],indLegend=[],xIdx=[0,1];
  if(hasInd){
    xArr.push({type:"category",data:catsExt,gridIndex:2,axisLabel:{color:"#666",fontSize:9},axisLine:{lineStyle:{color:"#333"}},splitLine:{show:false}});
    yArr.push({type:"value",gridIndex:2,position:"right",axisLabel:{color:"#666",fontSize:9},splitLine:{show:false}});
    xIdx=[0,1,2];
    if(mainInd==="macd"){
      var md=macdArr(closes); indLegend=["DIF","DEA","MACD"];
      indSeries=[{name:"MACD",type:"bar",xAxisIndex:2,yAxisIndex:2,data:md.hist,barWidth:"55%",itemStyle:{color:function(p){return md.hist[p.dataIndex]>=0?"rgba(255,77,79,.7)":"rgba(63,185,80,.7)";}}},
        {name:"DIF",type:"line",xAxisIndex:2,yAxisIndex:2,data:md.dif,symbol:"none",lineStyle:{width:1,color:"#d29922"}},
        {name:"DEA",type:"line",xAxisIndex:2,yAxisIndex:2,data:md.dea,symbol:"none",lineStyle:{width:1,color:"#58a6ff"}}];
    } else if(mainInd==="rsi"){
      var rs=rsiArr(closes,indParams.rsi); indLegend=["RSI"];
      indSeries=[{name:"RSI",type:"line",xAxisIndex:2,yAxisIndex:2,data:rs,symbol:"none",lineStyle:{width:1.3,color:"#8b6bff"},
        markLine:{silent:true,symbol:"none",data:[{yAxis:70,lineStyle:{color:"#ff4d4f",type:"dashed"},label:{formatter:"70"}},{yAxis:30,lineStyle:{color:"#3fb950",type:"dashed"},label:{formatter:"30"}}]}}];
    } else if(mainInd==="kdj"){
      var kd=kdjArr(kl,indParams.kdj[0],indParams.kdj[1],indParams.kdj[2]); indLegend=["K","D","J"];
      indSeries=[{name:"K",type:"line",xAxisIndex:2,yAxisIndex:2,data:kd.k,symbol:"none",lineStyle:{width:1,color:"#58a6ff"}},
        {name:"D",type:"line",xAxisIndex:2,yAxisIndex:2,data:kd.d,symbol:"none",lineStyle:{width:1,color:"#d29922"}},
        {name:"J",type:"line",xAxisIndex:2,yAxisIndex:2,data:kd.j,symbol:"none",lineStyle:{width:1,color:"#ff4d4f"}}];
    }
  }

  var series=[];
  series.push({name:"K线",type:"candlestick",data:ohlc,itemStyle:{color:"#ff4d4f",color0:"#3fb950",borderColor:"#ff4d4f",borderColor0:"#3fb950"},
    markLine:{silent:true,symbol:"none",label:{show:true,fontSize:10,color:"#ffd166"},data:[{yAxis:f.resistance,lineStyle:{color:"#ff4d4f",type:"dashed"},label:{formatter:"阻力 "+fmt(f.resistance,curDec)}},{yAxis:f.support,lineStyle:{color:"#3fb950",type:"dashed"},label:{formatter:"支撑 "+fmt(f.support,curDec)}}]},
    markPoint:{silent:true,symbol:"pin",symbolSize:38,label:{show:true,fontSize:9,color:"#fff",formatter:"放量"},data:markData,itemStyle:{color:"#d29922"}}});
  maSeries.forEach(function(m){ series.push(m); });
  series.push({name:"布林",type:"line",data:bb.up,symbol:"none",lineStyle:{width:1,color:"#8b949e",type:"dashed"}});
  series.push({name:"布林",type:"line",data:bb.lo,symbol:"none",lineStyle:{width:1,color:"#8b949e",type:"dashed"}});
  series.push({name:"成交量",type:"bar",xAxisIndex:1,yAxisIndex:1,data:vols,barWidth:"60%"});
  indSeries.forEach(function(x){ series.push(x); });

  var graphicArr=[];
  if(fc){
    var projData=fc.proj.map(function(v,i){return {value:[catsExt[n+i],v]};});
    var arrowCol=fc.dir==="多"?"#ff4d4f":fc.dir==="空"?"#3fb950":"#8b949e";
    var arrowRot=fc.dir==="多"?0:fc.dir==="空"?180:90;
    var fcMarks=[{coord:[catsExt[n+fc.proj.length-1],fc.proj[fc.proj.length-1]],symbol:"triangle",symbolSize:18,symbolRotate:arrowRot,itemStyle:{color:arrowCol},
      label:{show:true,fontSize:10,color:arrowCol,position:"right",formatter:(fc.dir==="多"?"看多↑ ":fc.dir==="空"?"看空↓ ":"震荡→ ")}},
      {coord:[catsExt[n+fc.proj.length-1],fc.tUp],symbol:"circle",symbolSize:6,itemStyle:{color:"#d29922"},
       label:{show:true,fontSize:11,color:"#d29922",position:"top",formatter:"目标 "+fmt(fc.tLo,curDec)+"~"+fmt(fc.tUp,curDec)+" · 评分"+fc.score}}];
    series.push({name:"预测",type:"line",data:projData,symbol:"none",lineStyle:{width:1.6,type:"dashed",color:fc.dir==="空"?"#3fb950":"#ff4d4f"},
      markPoint:{silent:true,data:fcMarks},
      markLine:{silent:true,symbol:"none",data:[{xAxis:cats[n-1],lineStyle:{color:"#8b949e",type:"dashed"},label:{show:true,position:"insideEndTop",formatter:"未来 →",color:"#8b949e",fontSize:10}}]},
      markArea:{silent:true,itemStyle:{color:fc.dir==="空"?"rgba(63,185,80,.12)":"rgba(255,77,79,.12)"},
        data:[[{xAxis:catsExt[n],yAxis:fc.tLo},{xAxis:catsExt[n+forecastN-1],yAxis:fc.tUp}]]}});
  }

  if(isShen){
    var shen=shenLong(kl,indParams);
    if(shen){
      var chCols=["#ff4d4f","#3fb950","#8b949e"];
      var bandRect=function(params,api){ var x=api.coord([api.value(0),api.value(1)]); var y0=api.coord([api.value(0),api.value(2)]);
        var w=(api.size([1,0])||[6])[0]*0.55; var y=Math.min(x[1],y0[1]); var h=Math.abs(x[1]-y0[1]);
        return {type:"rect",shape:{x:x[0]-w/2,y:y,width:w,height:Math.max(h,1)},style:api.style()}; };
      // 通道彩色带(下轨buf2→上轨buf1, 红/绿/灰)
      series.push({type:"custom",z:1,silent:true,renderItem:bandRect,
        data:shen.ch.map(function(c,i){return {value:[cats[i],shen.buf2[i],shen.buf1[i]],itemStyle:{color:chCols[c],opacity:0.5}};})});
      // 回归带色带(buf6→buf5, 灰)
      series.push({type:"custom",z:1,silent:true,renderItem:bandRect,
        data:shen.ch.map(function(c,i){return {value:[cats[i],shen.buf6[i],shen.buf5[i]],itemStyle:{color:"#8b949e",opacity:0.22}};})});
      series.push({name:"通道上",type:"line",data:shen.buf1,symbol:"none",lineStyle:{width:1.2,color:"#ff8a8b"}});
      series.push({name:"通道下",type:"line",data:shen.buf2,symbol:"none",lineStyle:{width:1.2,color:"#5fd68a"}});
      series.push({name:"投影上",type:"line",data:shen.buf3,symbol:"none",lineStyle:{width:1,color:"#ff4d4f",type:"dashed"}});
      series.push({name:"投影下",type:"line",data:shen.buf4,symbol:"none",lineStyle:{width:1,color:"#3fb950",type:"dashed"}});
      series.push({name:"止损带",type:"line",data:shen.sl1,symbol:"none",lineStyle:{width:0.8,color:"#ff6b6b",type:"dotted"}});
      series.push({name:"止损带",type:"line",data:shen.sl2,symbol:"none",lineStyle:{width:0.8,color:"#3fb950",type:"dotted"}});
      var sigData=shen.sig.map(function(sg){
        var lab=sg.t==="buy"?"买":sg.t==="sell"?"卖":(sg.t==="clsBuy"?"平多":"平空");
        var col=sg.t==="buy"?"#ff4d4f":sg.t==="sell"?"#3fb950":"#d29922";
        var pos=sg.t==="buy"?"bottom":sg.t==="sell"?"top":"middle";
        return {coord:[cats[sg.i],sg.price],itemStyle:{color:col},symbol:"circle",symbolSize:6,label:{show:true,fontSize:14,fontWeight:"bold",color:col,position:pos,formatter:lab}};
      });
      series.push({name:"信号",type:"line",data:[],symbol:"none",markPoint:{silent:true,data:sigData}});
    }
  }

  var legend=["K线"].concat(indParams.ma.map(function(p){return "MA"+p;})).concat(["布林","成交量"]).concat(indLegend);
  if(isShen) legend=legend.concat(["通道上","通道下","投影上","投影下","回归带","止损带","信号","预测"]);
  else if(fc) legend=legend.concat(["预测"]);

  var opt={backgroundColor:"transparent",animation:false,
    tooltip:{trigger:"axis",axisPointer:{type:"cross"},formatter:function(ps){ var p=ps[0]; var b=kl[p.dataIndex];
      return esc(cats[p.dataIndex])+"<br/>开"+fmt(b[1],curDec)+" 高"+fmt(b[2],curDec)+" 低"+fmt(b[3],curDec)+" 收"+fmt(b[4],curDec)+"<br/>量 "+((b[5]||0)/10000).toFixed(2)+"万手"; }},
    legend:{data:legend,textStyle:{color:"#8b949e",fontSize:11},top:0,right:10},
    grid:grids,
    xAxis:xArr, yAxis:yArr,
    dataZoom:[{type:"inside",xAxisIndex:xIdx},{type:"slider",xAxisIndex:xIdx,height:14,bottom:0,borderColor:"#30363d",backgroundColor:"#161b22",dataBackground:{lineStyle:{color:"#444"},areaStyle:{color:"#222"}}}],
    series:series};
  try{ mainChart.setOption(opt,true); mainChart.resize(); }
  catch(e){ try{ mainChart.dispose(); }catch(x){} try{ mainChart=echarts.init(el); mainChart.setOption(opt,true); mainChart.resize(); }catch(x){ console.error(x); } }
}
var spikeFp="";
var detailNewsFp="";
function renderSpikes(s){
  var el=document.getElementById("spikes"); if(!el) return;
  var sp=(s.spikes||[]).slice(0,8);
  var fp=JSON.stringify(sp);
  if(fp===spikeFp) return;
  spikeFp=fp;
  if(!sp.length){ el.innerHTML=""; return; }
  el.innerHTML="<h3 style='font-size:14px;color:#58a6ff;margin-bottom:8px'>⚡ 放量异动溯源</h3>"+
    sp.map(function(it){ var up=sgn(it.chg);
      return "<div class='spike'>[<b>"+esc(it.tf)+"</b>] "+esc(it.time)+" · 量能"+esc(it.vol_ratio)+"倍"+(it.chg!=null?" · 价格 <span class='"+(up?"up":"down")+"'>"+(up?"+":"")+it.chg+"%</span>":"")+" · 方向 "+esc(it.direction||"-")+(it.news&&it.news.length?("<div class='spike-news'>关联新闻: "+it.news.map(function(x){return "「"+esc(x)+"」";}).join(" ")+"</div>"):"")+"</div>"; }).join("");
}
function showFlip(s){
  ORDER.forEach(function(tf){ var f=s.frames&&s.frames[tf];
    if(f&&f.flip&&prevDir[tf]){ var t=document.getElementById("toast"); t.textContent=tf+" 方向变化: "+prevDir[tf]+" → "+(f.direction||""); t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},2600); }
    if(f) prevDir[tf]=f.direction; });
}
function showAlerts(alerts){
  var t=document.getElementById("toast"); if(!alerts||!alerts.length) return;
  alerts.forEach(function(a){ t.textContent="⚠ 重要新闻: "+a; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},4000); });
}

// ---------- 交易 ----------
function postTrade(data, cb){
  fetch("/api/trade",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)}).then(function(r){return r.json();}).then(function(res){
    if(cb) cb(res);
    var t=document.getElementById("toast"); t.textContent=(res.ok?"✅ ":"⚠️ ")+res.msg; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},3000);
    accountRefresh();
  }).catch(function(){});
}
function postAction(url,data){
  fetch(url,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data||{})}).then(function(r){return r.json();}).then(function(res){
    var t=document.getElementById("toast"); t.textContent=(res.ok?"✅ ":"⚠️ ")+res.msg; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},3000);
    accountRefresh();
    renderBotMonitor();
  }).catch(function(){});
}
function renderQuickTrade(){
  var el=document.getElementById("quickTrade"); if(!el||!curSym) return;
  var nm=(homeData&&homeData.commodities.find(function(c){return c.key===curSym;}))||{name:curSym};
  el.innerHTML="<div class='tsec'>快速下单 · "+esc(nm.name)+"</div>"+
    "<div class='orderform'>"+
    "<div><label>方向</label><select id='qtSide'><option value='buy'>买入(多)</option><option value='sell'>卖出(空)</option></select></div>"+
    "<div><label>手数</label><input id='qtQty' type='number' value='1' min='1'></div>"+
    "<div><label>杠杆</label><input id='qtLev' type='number' value='10' min='1' max='50'></div>"+
    "<div><button class='btn green' data-side='buy'>买入</button> <button class='btn red' data-side='sell'>卖出</button></div>"+
    "</div>";
}
function qtTrade(side){
  var qty=document.getElementById("qtQty")?document.getElementById("qtQty").value:1;
  var lev=document.getElementById("qtLev")?document.getElementById("qtLev").value:10;
  var sel=document.getElementById("qtSide");
  var s=side||(sel?sel.value:"buy");
  postTrade({sym:curSym,side:s,qty:qty,lev:lev,type:"market"});
}
var ffChart=null;
function fmtAmt(n){
  if(n==null||isNaN(n)) return "--";
  var abs=Math.abs(n);
  if(abs>=1e9) return (n/1e9).toFixed(2)+"B";
  if(abs>=1e6) return (n/1e6).toFixed(2)+"M";
  if(abs>=1e3) return (n/1e3).toFixed(1)+"K";
  return n.toFixed(0);
}
function renderFundflow(){
  var el=document.getElementById("fundflow"); if(!el||!curSym) return;
  if(!el.dataset.ffinit){
    el.innerHTML="<div id='ffData'></div><div id='ffChart' style='height:150px;margin-top:6px'></div>";
    el.dataset.ffinit="1";
  }
  var dataBox=document.getElementById("ffData"), chartBox=document.getElementById("ffChart");
  if(!dataBox||!chartBox) return;
  fetch("/api/fundflow?sym="+curSym+"&t="+Date.now()).then(function(r){return r.json();}).then(function(f){
    if(!f.in && !f.out){
      dataBox.innerHTML="<div class='tsec'>💰 资金流向 · "+esc(f.date)+"</div><div style='color:#8b949e;font-size:13px'>暂无资金流数据 — 数据源未覆盖该商品，或当日无逐笔成交</div>";
      chartBox.style.display="none";
      return;
    }
    chartBox.style.display="block";
    var iC=f.in||0, oC=f.out||0, net=iC-oC;
    var ratio=(f.ratio==null)?"-":Number(f.ratio).toFixed(2);
    var main=f.main||{in:0,out:0,net:0,share:0}, ret=f.retail||{in:0,out:0,net:0,share:0};
    dataBox.innerHTML="<div class='tsec'>💰 资金流向 · "+esc(f.date)+" <span style='font-size:11px;color:#8b949e'>主力=当日单笔前20%大单 · 主动买卖</span></div>"+
      "<div class='acc'>"+
      "<div class='ac'><div class='lb'>流入(主动买)</div><div class='vl up'>$"+fmtAmt(iC)+"</div></div>"+
      "<div class='ac'><div class='lb'>流出(主动卖)</div><div class='vl down'>$"+fmtAmt(oC)+"</div></div>"+
      "<div class='ac'><div class='lb'>净流入</div><div class='vl "+(net>=0?"up":"down")+"'>"+(net>=0?"+":"-")+"$"+fmtAmt(Math.abs(net))+"</div></div>"+
      "<div class='ac'><div class='lb'>比值(入:出)</div><div class='vl'>"+ratio+" : 1</div></div>"+
      "</div>"+
      "<div style='margin-top:10px'>"+
      "<div class='ffrow'><div class='ffl'>主力(大单)</div><div class='ffbar'><i style='width:"+main.share+"%;background:linear-gradient(90deg,#ff4d4f,#d29922)'></i></div><div class='ffr'><b>"+Number(main.share||0).toFixed(1)+"%</b></div></div>"+
      "<div style='font-size:11px;color:#8b949e;margin:2px 0 4px 0'>入 $"+fmtAmt(main.in)+" · 出 $"+fmtAmt(main.out)+" · 净 "+(main.net>=0?"+":"-")+"$"+fmtAmt(Math.abs(main.net))+"</div>"+
      "<div class='ffrow'><div class='ffl'>散户(小单)</div><div class='ffbar'><i style='width:"+ret.share+"%;background:linear-gradient(90deg,#3fb950,#58a6ff)'></i></div><div class='ffr'><b>"+Number(ret.share||0).toFixed(1)+"%</b></div></div>"+
      "<div style='font-size:11px;color:#8b949e;margin-top:2px'>入 $"+fmtAmt(ret.in)+" · 出 $"+fmtAmt(ret.out)+" · 净 "+(ret.net>=0?"+":"-")+"$"+fmtAmt(Math.abs(ret.net))+"</div>"+
      "</div>"+
      "<div class='tsec' style='margin-top:10px'>当日分时资金流 <span style='font-size:11px;color:#8b949e'>柱=净(红正/绿负)</span></div>";
    drawFFChart(f.minute||[]);
  }).catch(function(){});
}
function drawFFChart(minute){
  var box=document.getElementById("ffChart"); if(!box) return;
  if(!ffChart){ try{ ffChart=echarts.init(box); }catch(e){ return; } }
  var cats=minute.map(function(m){return m[0];});
  var nets=minute.map(function(m){return (m[1]||0)-(m[2]||0);});
  ffChart.setOption({
    animation:false,
    grid:{left:6,right:8,top:8,bottom:4,containLabel:true},
    tooltip:{trigger:"axis",axisPointer:{type:"shadow"},
      formatter:function(ps){ var m=ps[0]; var v=m.value; var row=minute[m.dataIndex]||[0,0,0];
        return m.axisValue+"<br/>净 "+(v>=0?"+":"-")+"$"+fmtAmt(Math.abs(v))+
        "<br/><span style='color:#ff4d4f'>流入 $"+fmtAmt(row[1])+"</span> · <span style='color:#3fb950'>流出 $"+fmtAmt(row[2])+"</span>"; }},
    xAxis:{type:"category",data:cats,axisLine:{lineStyle:{color:"#2d333b"}},
      axisLabel:{color:"#8b949e",fontSize:10,interval:Math.max(0,Math.ceil(cats.length/7)-1)}},
    yAxis:{type:"value",splitLine:{lineStyle:{color:"rgba(139,148,158,0.12)"}},
      axisLabel:{color:"#8b949e",fontSize:10,formatter:function(v){return fmtAmt(v);}}},
    series:[{type:"bar",data:nets,barMaxWidth:9,
      itemStyle:{color:function(p){return p.value>=0?"#ff4d4f":"#3fb950";},borderRadius:[2,2,0,0]}}]
  }, {notMerge:true});
}
function botName(key){ var f=homeData&&homeData.commodities&&homeData.commodities.find(function(c){return c.key===key;}); return f?f.name:key; }
var botPrevPos={};
var botScope="all";
var botEqRef=null;
var botFp="";
function setBotScope(s){ botScope=s; botFp=""; renderBotMonitor(); }
function botScopeFilter(it){ return botScope==="all" || it.sym===curSym; }
function renderBotMonitor(){
  var el=document.getElementById("botMonitor"); if(!el||!curSym) return;
  if(!el.dataset.botinit){
    el.innerHTML="<div id='botHead'></div><div id='botBody'></div><div class='tsec' style='margin-top:10px'>机器人权益曲线</div><div id='botEqChart' style='width:100%;height:120px;margin-top:6px'></div>";
    el.dataset.botinit="1";
  }
  var headBox=document.getElementById("botHead"), bodyBox=document.getElementById("botBody");
  if(!headBox||!bodyBox) return;
  fetch("/api/bot?t="+Date.now()).then(function(r){return r.json();}).then(function(b){
    if(!b.enabled){
      if(botFp!=="off"){
        headBox.innerHTML="<div class='tsec'>🤖 机器人监控</div>";
        bodyBox.innerHTML="<div style='color:#8b949e;font-size:13px'>机器人未运行 — 点右上角「🤖 自动交易」选策略启动</div>";
        var eqBox=document.getElementById("botEqChart"); if(eqBox) eqBox.innerHTML="";
        botFp="off";
      }
      botPrevPos[curSym]="";
      return;
    }
    var st=b.stats||{};
    var sname=b.strategy==="shenlong"?"🐉神龙":"🧠AI";
    var scope = (b.symbols&&b.symbols.length===1&&b.symbols[0]===curSym) ? (botName(curSym)+" "+esc(b.tf)) : "多商品·"+esc(b.tf);
    var head="<div class='tsec'>🤖 机器人监控 <span style='font-size:11px;color:#8b949e'>"+sname+" · "+scope+" · <b style='color:#3fb950'>运行中</b></span>"+
      "<span style='float:right'><button class='btn' data-bscope='all' style='padding:3px 10px;font-size:12px'>全部</button> <button class='btn' data-bscope='sym' style='padding:3px 10px;font-size:12px'>本商品</button> <button class='btn red' data-closebot='1' style='padding:3px 10px;font-size:12px'>平仓全部</button></span></div>"+
      "<div class='acc'>"+
      "<div class='ac'><div class='lb'>总盈亏</div><div class='vl "+(sgn(st.total_pnl)?"up":"down")+"'>"+(sgn(st.total_pnl)?"+":"")+fmt(st.total_pnl,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>浮动盈亏</div><div class='vl "+(sgn(st.unrealized)?"up":"down")+"'>"+(sgn(st.unrealized)?"+":"")+fmt(st.unrealized,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>已实现</div><div class='vl "+(sgn(st.realized)?"up":"down")+"'>"+(sgn(st.realized)?"+":"")+fmt(st.realized,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>胜率</div><div class='vl'>"+esc(st.win_rate)+"%</div></div>"+
      "<div class='ac'><div class='lb'>交易笔数</div><div class='vl'>"+esc(st.trades)+"</div></div>"+
      "<div class='ac'><div class='lb'>盈亏比</div><div class='vl'>"+(st.profit_factor==null?"-":st.profit_factor)+"</div></div>"+
      "</div>";
    var rows="";
    (b.positions||[]).filter(botScopeFilter).forEach(function(p){
      rows+="<tr><td>"+esc(p.name)+"</td><td class='"+(p.side==="buy"?"up":"down")+"'>"+(p.side==="buy"?"多":"空")+"</td><td>"+p.qty+"</td>"+
        "<td>"+esc(p.open_time)+"</td><td>"+fmt(p.entry,2)+"</td><td>"+fmt(p.current,2)+"</td>"+
        "<td class='"+(sgn(p.pnl)?"up":"down")+"'>"+(sgn(p.pnl)?"+":"")+fmt(p.pnl,2)+"</td><td style='color:#3fb950'>持仓中</td>"+
        "<td><button class='btn red' data-close='"+p.id+"' style='padding:2px 10px;font-size:12px'>平仓</button></td></tr>";
    });
    (b.history||[]).filter(botScopeFilter).slice(0,12).forEach(function(h){
      rows+="<tr><td>"+esc(h.name)+"</td><td class='"+(h.side==="buy"?"up":"down")+"'>"+(h.side==="buy"?"多":"空")+"</td><td>"+h.qty+"</td>"+
        "<td>-</td><td>"+fmt(h.entry,2)+"</td><td>"+fmt(h.exit,2)+"</td>"+
        "<td class='"+(sgn(h.pnl)?"up":"down")+"'>"+(sgn(h.pnl)?"+":"")+fmt(h.pnl,2)+"</td><td>已平 · "+esc(h.close_time)+"</td></tr>";
    });
    var table=rows?("<table class='table' style='margin-top:8px'><thead><tr><th>商品</th><th>方向</th><th>手数</th><th>开仓时间</th><th>开仓价</th><th>现/平价</th><th>盈亏</th><th>状态</th><th></th></tr></thead><tbody>"+rows+"</tbody></table>"):"<div style='color:#8b949e;font-size:12px;margin-top:8px'>"+(botScope==="all"?"暂无机器人交易":"本商品暂无机器人交易")+"</div>";
    var logsArr=(b.log||[]).filter(botScopeFilter);
    var logRows=logsArr.map(function(l,idx){
      var col=(l.action.indexOf("开多")>=0||l.action.indexOf("平多")>=0||l.action==="止盈")?"up":"down";
      var pnlTd=(l.pnl!=null)?("<td class='"+(sgn(l.pnl)?"up":"down")+"'>"+(sgn(l.pnl)?"+":"")+fmt(l.pnl,2)+"</td>"):"<td>-</td>";
      return "<tr><td style='color:#8b949e'>"+(idx+1)+"</td><td>"+esc(l.ts)+"</td>"+
        "<td class='"+col+"'><b>"+esc(l.action)+"</b></td>"+
        (botScope==="all"?"<td>"+esc(l.name)+"</td>":"")+
        "<td>"+esc(l.tf||"-")+"</td><td>"+(l.qty!=null?l.qty:"-")+"</td><td>"+fmt(l.price,2)+"</td>"+pnlTd+"</tr>";
    }).join("");
    var logs=logRows?("<div style='max-height:240px;overflow-y:auto;margin-top:6px'><table class='table'><thead><tr><th>#</th><th>时间</th><th>动作</th>"+(botScope==="all"?"<th>商品</th>":"")+"<th>周期</th><th>手数</th><th>价格</th><th>盈亏</th></tr></thead><tbody>"+logRows+"</tbody></table></div>"):"<div style='color:#8b949e'>暂无记录</div>";
    var fp=JSON.stringify([st.total_pnl,st.unrealized,st.realized,st.win_rate,st.trades,st.profit_factor,botScope,rows,logs,(b.equity||[]).length]);
    if(fp===botFp) return;
    botFp=fp;
    headBox.innerHTML=head;
    bodyBox.innerHTML="<div class='tsec' style='margin-top:10px'>交易记录</div>"+table+
      "<div class='tsec' style='margin-top:10px'>下单记录 <span style='font-size:11px;color:#8b949e'>(共"+(logsArr.length)+"条)</span></div>"+logs;
    drawBotEq(b);
    var nowKey=(b.positions||[]).filter(function(p){return p.sym===curSym;}).map(function(p){return p.side+p.qty+":"+p.entry;}).join("|");
    if(botPrevPos[curSym]&&botPrevPos[curSym]!==nowKey){
      var nl=b.log&&b.log[0];
      if(nl&&nl.sym===curSym){ var t=document.getElementById("toast"); t.textContent="🤖 "+esc(nl.action)+" · "+esc(nl.name)+" "+esc(nl.tf)+" @"+fmt(nl.price,2); t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},3000); }
    }
    botPrevPos[curSym]=nowKey;
  }).catch(function(){});
}
function drawBotEq(b){
  var eqBox=document.getElementById("botEqChart"); if(!eqBox) return;
  var eq=b.equity||[];
  if(eq.length>1){
    if(!botEqRef){
      eqBox.innerHTML="";
      try{ botEqRef=echarts.init(eqBox); }catch(e){ return; }
    }
    botEqRef.setOption({animation:false,backgroundColor:"transparent",grid:{left:32,right:10,top:8,bottom:18},
      xAxis:{type:"category",data:eq.map(function(e){return e[0];}),axisLabel:{color:"#666",fontSize:9,interval:Math.max(1,Math.floor(eq.length/4))}},
      yAxis:{type:"value",scale:true,axisLabel:{color:"#666",fontSize:9},splitLine:{lineStyle:{color:"#1c2431"}}},
      series:[{type:"line",data:eq.map(function(e){return e[1];}),symbol:"none",smooth:true,lineStyle:{width:2,color:"#58a6ff"},areaStyle:{color:new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:"rgba(88,166,255,.3)"},{offset:1,color:"rgba(0,0,0,0)"}])}}]},{notMerge:true});
  } else {
    if(botEqRef){ try{ botEqRef.dispose(); }catch(e){} botEqRef=null; }
    eqBox.innerHTML="<div style='color:#8b949e;font-size:12px'>权益数据积累中...</div>";
  }
}
function showTrade(){
  document.getElementById("headSub").textContent="模拟交易 · 虚拟资金 $100,000 · 杠杆1-50倍 · 手续费0.03%";
  var v=document.getElementById("view");
  v.innerHTML="<a class='back' data-go=''>← 返回首页</a>"+
    "<div class='panel'><div class='tsec'>账户总览</div><div class='acc' id='accBox'></div></div>"+
    "<div class='panel'><div class='tsec'>下单</div><div class='orderform' id='tradeForm'></div>"+
    "<div class='statbox'><button class='btn warn' data-act='/api/reset'>重置账户</button>"+
    "<button class='btn red' data-act='/api/closeall'>全部平仓</button></div></div>"+
    "<div class='panel'><div class='tsec'>持仓</div><table class='table' id='posTable'><thead><tr><th>商品</th><th>方向</th><th>手数</th><th>杠杆</th><th>开仓价</th><th>现价</th><th>浮盈</th><th>保证金</th><th>来源</th><th>开仓时间</th><th></th></tr></thead><tbody></tbody></table></div>"+
    "<div class='panel'><div class='tsec'>挂单</div><table class='table' id='pendTable'><thead><tr><th>商品</th><th>方向</th><th>手数</th><th>限价</th><th>现价</th><th>时间</th><th></th></tr></thead><tbody></tbody></table></div>"+
    "<div class='panel'><div class='tsec'>已平仓历史</div><table class='table' id='hisTable'><thead><tr><th>商品</th><th>方向</th><th>手数</th><th>开仓价</th><th>平仓价</th><th>盈亏</th><th>时间</th></tr></thead><tbody></tbody></table></div>";
  if(!homeData){
    fetch("/api/home?t="+Date.now()).then(function(r){return r.json();}).then(function(h){ homeData=h; renderTradeForm(); });
  }
  renderTradeForm();
  accountRefresh();
  if(!window._atimer) window._atimer=setInterval(accountRefresh,5000);
}
function renderTradeForm(){
  var el=document.getElementById("tradeForm"); if(!el) return;
  var opts=(homeData&&homeData.commodities)||[];
  var sel=opts.map(function(c){return "<option value='"+c.key+"'>"+esc(c.name)+"</option>";}).join("");
  el.innerHTML=
    "<div><label>商品</label><select id='tfSym'>"+sel+"</select></div>"+
    "<div><label>方向</label><select id='tfSide'><option value='buy'>买入(多)</option><option value='sell'>卖出(空)</option></select></div>"+
    "<div><label>手数</label><input id='tfQty' type='number' value='1' min='1'></div>"+
    "<div><label>杠杆</label><input id='tfLev' type='number' value='10' min='1' max='50'></div>"+
    "<div><label>类型</label><select id='tfType'><option value='market'>市价</option><option value='limit'>限价</option></select></div>"+
    "<div id='priceWrap' style='display:none'><label>限价</label><input id='tfPrice' type='number' step='0.01'></div>"+
    "<div><button class='btn green' onclick='submitTrade()'>提交订单</button></div>";
  var tsel=document.getElementById("tfType");
  if(tsel) tsel.onchange=function(){ document.getElementById("priceWrap").style.display=this.value==="limit"?"":"none"; };
}
function submitTrade(){
  var price=document.getElementById("tfPrice")?document.getElementById("tfPrice").value:null;
  postTrade({sym:document.getElementById("tfSym").value,
    side:document.getElementById("tfSide").value,
    qty:document.getElementById("tfQty").value,
    lev:document.getElementById("tfLev").value,
    type:document.getElementById("tfType").value,
    price:price});
}
function accountRefresh(){
  if(location.hash!=="#/trade") return;
  fetch("/api/account?t="+Date.now()).then(function(r){return r.json();}).then(function(a){
    if(!document.getElementById("accBox")) return;
    var up=sgn(a.unrealized);
    document.getElementById("accBox").innerHTML=
      "<div class='ac'><div class='lb'>总权益</div><div class='vl'>$"+fmt(a.equity,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>现金</div><div class='vl'>$"+fmt(a.cash,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>占用保证金</div><div class='vl'>$"+fmt(a.margin_used,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>浮动盈亏</div><div class='vl "+(up?"up":"down")+"'>"+(up?"+":"")+fmt(a.unrealized,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>总盈亏</div><div class='vl "+(sgn(a.total_pnl)?"up":"down")+"'>"+(sgn(a.total_pnl)?"+":"")+fmt(a.total_pnl,2)+"</div></div>"+
      "<div class='ac'><div class='lb'>胜率</div><div class='vl'>"+esc(a.win_rate)+"%</div></div>"+
      "<div class='ac'><div class='lb'>胜/负</div><div class='vl'>"+esc(a.wins)+"/"+esc(a.losses)+"</div></div>";
    var pos=a.positions||[];
    document.querySelector("#posTable tbody").innerHTML=pos.map(function(p){
      var srcTag=p.source==="bot"?"<span style='color:#8b6bff;font-weight:700'>🤖机器人</span>":"<span style='color:#8b949e'>手动</span>";
      return "<tr><td>"+esc(p.name)+"</td><td class='"+(p.side==="buy"?"up":"down")+"'>"+(p.side==="buy"?"多":"空")+"</td>"+
        "<td>"+p.qty+"</td><td>"+p.lev+"x</td><td>"+fmt(p.entry,2)+"</td><td>"+fmt(p.current,2)+"</td>"+
        "<td class='"+(sgn(p.pnl)?"up":"down")+"'>"+(sgn(p.pnl)?"+":"")+fmt(p.pnl,2)+"</td><td>"+fmt(p.margin,2)+"</td>"+
        "<td>"+srcTag+"</td><td>"+esc(p.open_time)+"</td><td><button class='btn red' data-close='"+p.id+"'>平仓</button></td></tr>";
    }).join("")||"<tr><td colspan='11' style='color:#8b949e'>暂无持仓</td></tr>";
    var pend=a.pending||[];
    document.querySelector("#pendTable tbody").innerHTML=pend.map(function(p){
      return "<tr><td>"+esc(p.name)+"</td><td class='"+(p.side==="buy"?"up":"down")+"'>"+(p.side==="buy"?"多":"空")+"</td>"+
        "<td>"+p.qty+"</td><td>"+fmt(p.limit_price,2)+"</td><td>"+(p.current!=null?fmt(p.current,2):"-")+"</td>"+
        "<td>"+esc(p.created)+"</td><td><button class='btn warn' data-cancel='"+p.id+"'>撤单</button></td></tr>";
    }).join("")||"<tr><td colspan='7' style='color:#8b949e'>暂无挂单</td></tr>";
    var his=a.history||[];
    document.querySelector("#hisTable tbody").innerHTML=his.map(function(h){
      return "<tr><td>"+esc(h.name)+"</td><td class='"+(h.side==="buy"?"up":"down")+"'>"+(h.side==="buy"?"多":"空")+"</td>"+
        "<td>"+h.qty+"</td><td>"+fmt(h.entry,2)+"</td><td>"+fmt(h.exit,2)+"</td>"+
        "<td class='"+(sgn(h.pnl)?"up":"down")+"'>"+(sgn(h.pnl)?"+":"")+fmt(h.pnl,2)+"</td><td>"+esc(h.close_time)+"</td></tr>";
    }).join("")||"<tr><td colspan='7' style='color:#8b949e'>暂无历史</td></tr>";
  }).catch(function(){});
}

// ---------- 提醒 ----------
function showAlertsPage(){
  document.getElementById("headSub").textContent="提醒设置 · 高置信度/放量新闻/每日简报 · 邮件发送";
  var v=document.getElementById("view");
  v.innerHTML="<a class='back' data-go=''>← 返回首页</a>"+
    "<div class='panel'><div class='tsec'>触发条件</div><div id='trigBox'></div></div>"+
    "<div class='panel'><div class='tsec'>参数</div><div class='orderform' id='paramBox'></div></div>"+
    "<div class='panel'><div class='tsec'>邮箱配置(SMTP)</div><div class='orderform' id='smtpBox'></div>"+
    "<div class='statbox'><button class='btn green' onclick='saveAlerts()'>保存设置</button>"+
    "<button class='btn warn' onclick='testAlert()'>发送测试邮件</button><span id='alertMsg'></span></div></div>"+
    "<div class='panel'><div class='tsec'>备用数据源 (Twelve Data)</div>"+
    "<div class='orderform'><div><label>Twelve Data API Key</label><input id='tdKey' type='password' placeholder='(可留空)' style='min-width:260px'></div>"+
    "<div><button class='btn green' onclick='saveTDKey()'>保存Key</button></div><div id='tdStatus'></div></div>"+
    "<div style='font-size:11px;color:#8b949e;margin-top:6px'>东财限流时自动用Twelve Data兜底K线。免费注册: twelvedata.com (8次/分)</div></div>"+
    "<div class='panel'><div class='tsec'>说明</div>"+
    "<div style='font-size:13px;color:#8b949e;line-height:1.8'>"+
    "· 提醒线程每5分钟扫描一次信号<br/>"+
    "· <b>授权码</b>不是邮箱登录密码：登录邮箱网页版 → 设置 → 账户 → 开启SMTP服务后生成<br/>"+
    "· QQ邮箱: smtp.qq.com:465 / 163邮箱: smtp.163.com:465<br/>"+
    "· 保存后点「发送测试邮件」验证是否通畅</div></div>";
  alertRefresh();
  tdStatus();
}
function alertRefresh(){
  fetch("/api/alertconfig?t="+Date.now()).then(function(r){return r.json();}).then(function(c){
    if(!document.getElementById("trigBox")) return;
    var defs=[["high_conf","高置信度信号(置信度≥阈值)"],["volume_news","放量异动+重要新闻"],
      ["daily","每日简报"],["flip","方向翻转"],["breakout","突破关键位"]];
    var tr=c.triggers||{};
    document.getElementById("trigBox").innerHTML=defs.map(function(d){
      return "<label style='display:inline-flex;align-items:center;gap:6px;margin:0 14px 8px 0;color:#c9d1d9;font-size:13px'>"+
        "<input type='checkbox' id='tg-"+d[0]+"' "+(tr[d[0]]?"checked":"")+"> "+d[1]+"</label>";
    }).join("");
    document.getElementById("paramBox").innerHTML=
      "<div><label>高置信度阈值(%)</label><input id='pTh' type='number' value='"+esc(c.high_conf_threshold)+"'></div>"+
      "<div><label>检查间隔(分钟)</label><input id='pIv' type='number' value='"+esc(c.check_interval_min)+"'></div>"+
      "<div><label>每日简报时间</label><input id='pDt' type='time' value='"+esc(c.daily_time)+"'></div>";
    document.getElementById("smtpBox").innerHTML=
      "<div><label>SMTP主机</label><input id='sHost' value='"+esc(c.smtp_host||"smtp.qq.com")+"'></div>"+
      "<div><label>端口</label><input id='sPort' value='465' style='min-width:70px'></div>"+
      "<div><label>邮箱账号</label><input id='sUser' value='"+esc(c.smtp_user)+"' placeholder='xxx@qq.com'></div>"+
      "<div><label>SMTP授权码</label><input id='sAuth' type='password' placeholder='(已配置则留空)'></div>"+
      "<div><label>收件邮箱</label><input id='sTo' value='"+esc(c.smtp_to)+"' placeholder='(默认同发件)'></div>"+
      "<div><label>状态</label><div style='padding-top:6px'>"+(c.smtp_configured?"<b style='color:#3fb950'>已配置 ✅</b>":"<b style='color:#d29922'>未配置授权码</b>")+"</div></div>";
  }).catch(function(e){ console.error(e); });
}
function saveAlerts(){
  var data={triggers:{}, high_conf_threshold:document.getElementById("pTh").value,
    check_interval_min:document.getElementById("pIv").value, daily_time:document.getElementById("pDt").value};
  ["high_conf","volume_news","daily","flip","breakout"].forEach(function(t){
    var el=document.getElementById("tg-"+t); data.triggers[t]=el?el.checked:false; });
  var auth=document.getElementById("sAuth").value;
  if(auth) data.smtp={host:document.getElementById("sHost").value,port:document.getElementById("sPort").value,
    user:document.getElementById("sUser").value,auth:auth,to:document.getElementById("sTo").value};
  else data.smtp={host:document.getElementById("sHost").value,port:document.getElementById("sPort").value,
    user:document.getElementById("sUser").value,to:document.getElementById("sTo").value};
  fetch("/api/alertconfig",{method:"POST",headers:{"Content-Type":"application/json"},
    body:JSON.stringify(data)}).then(function(r){return r.json();}).then(function(res){
    var m=document.getElementById("alertMsg"); m.textContent=res.msg||"已保存";
    alertRefresh();
    var t=document.getElementById("toast"); t.textContent="✅ "+res.msg; t.classList.add("show");
    setTimeout(function(){t.classList.remove("show");},3000);
  }).catch(function(){});
}
function testAlert(){
  var m=document.getElementById("alertMsg"); if(m) m.textContent="发送中...";
  fetch("/api/alerts/test",{method:"POST"}).then(function(r){return r.json();}).then(function(res){
    if(m) m.textContent=res.msg; else alert(res.msg);
  }).catch(function(){});
}
function saveTDKey(){
  var key=document.getElementById("tdKey").value;
  fetch("/api/config",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({twelve_key:key})})
    .then(function(r){return r.json();}).then(function(res){
      var t=document.getElementById("toast"); t.textContent="✅ "+res.msg; t.classList.add("show");
      setTimeout(function(){t.classList.remove("show");},2500);
      var st=document.getElementById("tdStatus"); if(st) st.textContent=key?"已保存":"已清空";
    }).catch(function(){});
}
function tdStatus(){
  fetch("/api/config?t="+Date.now()).then(function(r){return r.json();}).then(function(c){
    var st=document.getElementById("tdStatus");
    if(st) st.innerHTML=c.twelve_configured?"<b style='color:#3fb950'>备用源已配置 ✅</b>":"<b style='color:#8b949e'>未配置</b>";
  }).catch(function(){});
}
// ---------- 自动交易 ----------
function showBotPage(){
  document.getElementById("headSub").textContent="自动交易 · AI复合评分 · 回测+实盘模拟";
  var v=document.getElementById("view");
  v.innerHTML="<a class='back' data-go=''>← 返回首页</a>"+
    "<div class='panel'><div class='tsec'>🤖 实盘模拟机器人</div>"+
    "<div class='orderform' id='botForm'></div>"+
    "<div class='statbox'><button class='btn green' onclick='saveBot()'>保存并应用</button>"+
    "<button class='btn red' data-bot='toggle'>"+(botData&&botData.enabled?"关闭机器人":"开启机器人")+"</button>"+
    "<span id='botMsg'></span></div>"+
    "<div class='tsec' style='margin-top:14px'>机器人持仓</div><table class='table' id='botPos'><thead><tr><th>商品</th><th>方向</th><th>手数</th><th>开仓价</th><th>现价</th><th>浮盈</th><th>时间</th></tr></thead><tbody></tbody></table></div>"+
    "<div class='panel'><div class='tsec'>🧠 AI评分排行 (多周期复合评分)</div><table class='table' id='aiRank'><thead><tr><th>商品</th><th>方向</th><th>AI评分</th><th>依据</th></tr></thead><tbody></tbody></table></div>"+
    "<div class='panel'><div class='tsec'>机器人历史交易</div><table class='table' id='botHist'><thead><tr><th>商品</th><th>方向</th><th>手数</th><th>开仓</th><th>平仓</th><th>盈亏</th><th>时间</th></tr></thead><tbody></tbody></table></div>"+
    "<div class='panel'><div class='tsec'>📊 历史回测</div>"+
    "<div class='orderform' id='btForm'></div>"+
    "<div class='statbox'><button class='btn green' onclick='runBacktest()'>跑回测</button></div>"+
    "<div class='acc' id='btStats' style='margin-top:10px'></div>"+
    "<div id='btChart' style='width:100%;height:280px;margin-top:12px'></div>"+
    "<table class='table' id='btTrades' style='margin-top:10px'><thead><tr><th>时间</th><th>方向</th><th>开仓</th><th>平仓</th><th>盈亏</th><th>原因</th></tr></thead><tbody></tbody></table></div>";
  if(!homeData){
    fetch("/api/home?t="+Date.now()).then(function(r){return r.json();}).then(function(h){ homeData=h; renderBotForms(); });
  }
  renderBotForms();
  botRefresh();
  if(!window._btimer) window._btimer=setInterval(botRefresh,10000);
}
function renderBotForms(){
  var opts=(homeData&&homeData.commodities)?homeData.commodities.map(function(c){return "<option value='"+c.key+"'>"+esc(c.name)+"</option>";}).join(""):"";
  var el=document.getElementById("btForm");
  if(el) el.innerHTML=
    "<div><label>策略</label><select id='btStrategy'><option value='ai'>AI复合评分</option><option value='shenlong'>🐉神龙通道</option></select></div>"+
    "<div><label>商品</label><select id='btSym'><option value='all'>全部(汇总)</option>"+opts+"</select></div>"+
    "<div id='btTfWrap' style='display:none'><label>周期</label><select id='btTf'><option>M1</option><option>M5</option><option>M15</option><option selected>M30</option><option>H1</option><option>H4</option><option>D1</option></select></div>"+
    "<div><label>置信度阈值(%)</label><input id='btTh' type='number' value='60'></div>"+
    "<div><label>止损(%)</label><input id='btSl' type='number' value='3' step='0.5'></div>"+
    "<div><label>止盈(%)</label><input id='btTp' type='number' value='6' step='0.5'></div>"+
    "<div><label>手数</label><input id='btQty' type='number' value='1' min='1'></div>";
  var bts=document.getElementById("btStrategy");
  if(bts) bts.onchange=function(){ document.getElementById("btTfWrap").style.display=this.value==="shenlong"?"":"none"; };
  var bf=document.getElementById("botForm");
  if(bf) bf.innerHTML=
    "<div><label>策略</label><select id='bStrategy'><option value='ai'"+(botData&&botData.strategy==="ai"?" selected":"")+">AI复合评分</option><option value='shenlong'"+(botData&&botData.strategy==="shenlong"?" selected":"")+">🐉神龙通道</option></select></div>"+
    "<div><label>交易周期</label><div style='padding-top:7px'><b id='bTf'>"+(botData?botData.tf:"H1")+"</b><span style='color:#8b949e;font-size:11px'>(跟随页面选中)</span></div></div>"+
    "<div><label>AI入场阈值</label><input id='bAi' type='number' value='45'></div>"+
    "<div><label>最大持仓</label><input id='bMax' type='number' value='3' min='1'></div>"+
    "<div><label>每笔手数</label><input id='bQty' type='number' value='1' min='1'></div>"+
    "<div><label>止损(%)</label><input id='bSl' type='number' value='3' step='0.5'></div>"+
    "<div><label>止盈(%)</label><input id='bTp' type='number' value='6' step='0.5'></div>"+
    "<div><label>冷却(分钟)</label><input id='bCd' type='number' value='60'></div>"+
    "<div><label>超时(小时)</label><input id='bTt' type='number' value='24'></div>"+
    "<div><label>检查间隔(分钟)</label><input id='bIv' type='number' value='5'></div>"+
    "<div style='grid-column:1/-1'><label>交易商品(勾选,咖啡/可可K线暂不可用)</label><div id='symSel'>"+
    ((homeData&&homeData.commodities||[]).map(function(c){
      var disabled=(c.key==="coffee"||c.key==="cocoa")?" disabled":"";
      var checked=(botData&&botData.symbols&&botData.symbols.indexOf(c.key)>=0)?" checked":"";
      return "<label style='display:inline-flex;align-items:center;gap:4px;margin:3px 12px 3px 0;font-size:12px;color:#c9d1d9'>"+
        "<input type='checkbox' value='"+c.key+"'"+checked+disabled+"> "+esc(c.name)+(disabled?"(无K线)":"")+"</label>";
    }).join(""))+"</div></div>";
  if(botData){
    document.getElementById("bAi").value=botData.ai_threshold;
    document.getElementById("bSl").value=botData.stop_loss;
    document.getElementById("bTp").value=botData.take_profit;
    document.getElementById("bMax").value=botData.max_positions;
    document.getElementById("bQty").value=botData.qty;
    document.getElementById("bCd").value=botData.cooldown_min;
    document.getElementById("bTt").value=botData.timeout_h;
    document.getElementById("bIv").value=botData.interval_min;
  }
}
function saveBot(){
  var data={strategy:document.getElementById("bStrategy").value,
    ai_threshold:document.getElementById("bAi").value,
    stop_loss:document.getElementById("bSl").value,
    take_profit:document.getElementById("bTp").value, max_positions:document.getElementById("bMax").value,
    qty:document.getElementById("bQty").value, cooldown_min:document.getElementById("bCd").value,
    timeout_h:document.getElementById("bTt").value, interval_min:document.getElementById("bIv").value,
    symbols:Array.from(document.querySelectorAll("#symSel input:checked")).map(function(cb){return cb.value;})};
  fetch("/api/bot",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(r){return r.json();}).then(function(res){
      var m=document.getElementById("botMsg"); if(m) m.textContent=(res.msg||"已保存")+" · 勾选商品"+data.symbols.length+"个";
    }).catch(function(){});
}
function botRefresh(){
  if(location.hash!=="#/bot") return;
  fetch("/api/bot?t="+Date.now()).then(function(r){return r.json();}).then(function(b){
    botData=b;
    var btn=document.querySelector("[data-bot='toggle']");
    if(btn) btn.textContent=b.enabled?"关闭机器人":"开启机器人";
    var tfd=document.getElementById("bTf"); if(tfd) tfd.textContent=b.tf||"H1";
    var strSel=document.getElementById("bStrategy");
    if(strSel) strSel.value=b.strategy||"ai";
    var pos=b.positions||[];
    document.querySelector("#botPos tbody").innerHTML=pos.map(function(p){
      return "<tr><td>"+esc(p.name)+"</td><td class='"+(p.side==="buy"?"up":"down")+"'>"+(p.side==="buy"?"多":"空")+"</td>"+
        "<td>"+p.qty+"</td><td>"+fmt(p.entry,2)+"</td><td>"+fmt(p.current,2)+"</td>"+
        "<td class='"+(sgn(p.pnl)?"up":"down")+"'>"+(sgn(p.pnl)?"+":"")+fmt(p.pnl,2)+"</td><td>"+esc(p.open_time)+"</td></tr>";
    }).join("")||"<tr><td colspan='7' style='color:#8b949e'>机器人无持仓</td></tr>";
    var rank=document.getElementById("aiRank");
    if(rank){
      document.querySelector("#aiRank tbody").innerHTML=(b.scores||[]).map(function(x){
        var cls=x.direction==="多"?"up":x.direction==="空"?"down":"";
        return "<tr><td>"+esc(x.name)+"</td><td class='"+cls+"'><b>"+esc(x.direction)+"</b></td>"+
          "<td><b class='"+(x.score>=0?"up":"down")+"'>"+(x.score>=0?"+":"")+x.score+"</b></td><td style='font-size:11px;color:#8b949e'>"+esc((x.reasons||[]).join("、")||"-")+"</td></tr>";
      }).join("")||"<tr><td colspan='4' style='color:#8b949e'>暂无评分数据</td></tr>";
    }
    var bh=document.getElementById("botHist");
    if(bh){
      document.querySelector("#botHist tbody").innerHTML=(b.history||[]).map(function(h){
        return "<tr><td>"+esc(h.name)+"</td><td class='"+(h.side==="buy"?"up":"down")+"'>"+(h.side==="buy"?"多":"空")+"</td>"+
          "<td>"+h.qty+"</td><td>"+h.entry+"</td><td>"+h.exit+"</td>"+
          "<td class='"+(sgn(h.pnl)?"up":"down")+"'>"+(sgn(h.pnl)?"+":"")+h.pnl+"</td><td>"+esc(h.close_time)+"</td></tr>";
      }).join("")||"<tr><td colspan='7' style='color:#8b949e'>机器人暂无已平仓记录</td></tr>";
    }
    if(document.getElementById("bAi")&&!document.getElementById("bAi").value){ renderBotForms(); }
    if(botData&&document.querySelectorAll("#symSel input").length){
      document.querySelectorAll("#symSel input").forEach(function(cb){
        cb.checked = botData.symbols && botData.symbols.indexOf(cb.value)>=0;
      });
    }
  }).catch(function(){});
}
function toggleBot(){
  var data={enabled: !(botData&&botData.enabled)};
  fetch("/api/bot",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(r){return r.json();}).then(function(res){ botRefresh(); });
}
function runBacktest(){
  var strategy=document.getElementById("btStrategy").value;
  var sym=document.getElementById("btSym").value;
  var data={strategy:strategy, sym: sym==="all"?"gold":sym, threshold:document.getElementById("btTh").value,
    stop_loss:document.getElementById("btSl").value, take_profit:document.getElementById("btTp").value,
    qty:document.getElementById("btQty").value};
  if(strategy==="shenlong") data.tf=document.getElementById("btTf").value;
  fetch("/api/backtest",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(data)})
    .then(function(r){return r.json();}).then(function(res){
      if(!res.ok){ var t=document.getElementById("toast"); t.textContent="⚠️ "+res.msg; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},3000); return; }
      renderBacktest(res, sym==="all");
    }).catch(function(e){ console.error(e); });
}
function renderBacktest(r, isAll){
  var s=r.stats;
  document.getElementById("btStats").innerHTML=
    "<div class='ac'><div class='lb'>交易次数</div><div class='vl'>"+esc(s.trades)+"</div></div>"+
    "<div class='ac'><div class='lb'>胜率</div><div class='vl'>"+esc(s.win_rate)+"%</div></div>"+
    "<div class='ac'><div class='lb'>总盈亏</div><div class='vl "+(sgn(s.total_pnl)?"up":"down")+"'>"+(sgn(s.total_pnl)?"+":"")+fmt(s.total_pnl,2)+"</div></div>"+
    "<div class='ac'><div class='lb'>盈亏比</div><div class='vl'>"+(s.profit_factor==null?"-":s.profit_factor)+"</div></div>"+
    "<div class='ac'><div class='lb'>最大回撤</div><div class='vl down'>"+esc(s.max_drawdown)+"%</div></div>"+
    "<div class='ac'><div class='lb'>平均盈/亏</div><div class='vl'>+"+s.avg_win+" / "+s.avg_loss+"</div></div>";
  if(r.all){
    var tb=document.querySelector("#btTrades tbody");
    tb.innerHTML=r.results.map(function(x){
      return "<tr><td><b>"+esc(x.name)+"</b></td><td>"+x.stats.trades+"笔</td><td>"+x.stats.win_rate+"%</td>"+
        "<td class='"+(sgn(x.stats.total_pnl)?"up":"down")+"'>"+(sgn(x.stats.total_pnl)?"+":"")+fmt(x.stats.total_pnl,2)+"</td>"+
        "<td>"+esc(x.stats.max_drawdown)+"%</td><td>-</td></tr>";
    }).join("");
    var th=document.querySelector("#btTrades thead tr");
    th.innerHTML="<th>商品</th><th>交易</th><th>胜率</th><th>盈亏$</th><th>最大回撤</th><th>原因</th>";
    return;
  }
  var el=document.getElementById("btChart");
  if(window._btChart){ window._btChart.dispose(); }
  window._btChart=echarts.init(el);
  window._btChart.setOption({backgroundColor:"transparent",
    tooltip:{trigger:"axis"},
    grid:{left:50,right:16,top:16,bottom:24},
    xAxis:{type:"category",data:r.equity.map(function(x){return x[0];}),axisLabel:{color:"#666",fontSize:10}},
    yAxis:{type:"value",scale:true,axisLabel:{color:"#666",fontSize:10},splitLine:{lineStyle:{color:"#1c2431"}}},
    series:[{name:"权益",type:"line",data:r.equity.map(function(x){return x[1];}),smooth:true,symbol:"none",
      lineStyle:{width:2,color:"#58a6ff"},areaStyle:{color:new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:"rgba(88,166,255,.35)"},{offset:1,color:"rgba(0,0,0,0)"}])}}]});
  var th=document.querySelector("#btTrades thead tr");
  th.innerHTML="<th>时间</th><th>方向</th><th>开仓</th><th>平仓</th><th>盈亏</th><th>原因</th>";
  document.querySelector("#btTrades tbody").innerHTML=(r.trades||[]).map(function(t){
    return "<tr><td>"+esc(t.time)+"</td><td class='"+(t.side==="buy"?"up":"down")+"'>"+(t.side==="buy"?"多":"空")+"</td>"+
      "<td>"+t.entry+"</td><td>"+t.exit+"</td>"+
      "<td class='"+(sgn(t.pnl)?"up":"down")+"'>"+(sgn(t.pnl)?"+":"")+t.pnl+"</td><td>"+esc(t.reason)+"</td></tr>";
  }).join("")||"<tr><td colspan='6' style='color:#8b949e'>无交易</td></tr>";
  var t=document.getElementById("toast"); t.textContent="✅ "+r.name+" 回测完成: "+s.trades+"笔 总盈亏$"+s.total_pnl; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},3500);
}
// ---------- 准确率统计 ----------
function showAccuracy(){
  document.getElementById("headSub").textContent="预测准确率统计 · 只记录置信度≥70%的多/空预测 · 下一根K线收盘判定";
  var v=document.getElementById("view");
  v.innerHTML="<a class='back' data-go=''>← 返回首页</a>"+
    "<div class='panel'><div class='tsec'>📊 预测准确率统计</div>"+
    "<div style='font-size:12px;color:#8b949e;line-height:1.8'>判定规则: 预测「多/空」后, 到该周期下一根K线收盘时, 价格是否朝预测方向走(变动需超0.02%); 方向为「震荡」不记录; 最新一根未收盘的记为未决; 覆盖M1到D1全部周期。<br/>「有效样本」=命中+失误(参与准确率计算), 平盘/未决/过期不计入准确率。</div>"+
    "<div class='statbox'><span style='font-size:13px;color:#c9d1d9'>最低置信度</span>"+
    "<select id='accMin' style='background:#1c2431;color:#e6edf3;border:1px solid #30363d;border-radius:8px;padding:5px 10px'>"+
    "<option value='70'>≥70%</option><option value='75'>≥75%</option><option value='80'>≥80%</option><option value='85'>≥85%</option><option value='90'>≥90%</option></select>"+
    "<span style='font-size:13px;color:#c9d1d9'>日期范围</span>"+
    "<select id='accDays' style='background:#1c2431;color:#e6edf3;border:1px solid #30363d;border-radius:8px;padding:5px 10px'>"+
    "<option value=''>全部</option><option value='1'>最近1天</option><option value='3'>最近3天</option><option value='7'>最近7天</option><option value='30'>最近30天</option></select>"+
    "<input id='accStart' type='date' title='开始日期' style='background:#1c2431;color:#e6edf3;border:1px solid #30363d;border-radius:8px;padding:4px 8px'>"+
    "<span style='color:#8b949e'>~</span>"+
    "<input id='accEnd' type='date' title='结束日期' style='background:#1c2431;color:#e6edf3;border:1px solid #30363d;border-radius:8px;padding:4px 8px'>"+
    "<button class='btn green' onclick='accRefresh()'>刷新</button>"+
    "<button class='btn red' onclick='accReset()'>重置统计</button>"+
    "<span id='accMsg' style='font-size:12px;color:#8b949e'></span></div></div>"+
    "<div class='panel'><div class='tsec'>总览</div><div class='acc' id='accOverview'></div></div>"+
    "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px'>"+
    "<div class='panel' style='margin-bottom:0'><div class='tsec'>按周期</div><table class='table' id='accTf'><thead><tr><th>周期</th><th>有效样本</th><th>命中</th><th>失误</th><th>平盘</th><th>未决</th><th>准确率</th></tr></thead><tbody></tbody></table></div>"+
    "<div class='panel' style='margin-bottom:0'><div class='tsec'>按置信度分档</div><table class='table' id='accConf'><thead><tr><th>置信度</th><th>有效样本</th><th>命中</th><th>失误</th><th>平盘</th><th>未决</th><th>准确率</th></tr></thead><tbody></tbody></table></div></div>"+
    "<div class='panel'><div class='tsec'>多空方向对比</div><div class='acc' id='accDir'></div></div>"+
    "<div class='panel'><div class='tsec'>按商品</div><table class='table' id='accSym'><thead><tr><th>商品</th><th>有效样本</th><th>命中</th><th>失误</th><th>平盘</th><th>未决</th><th>准确率</th></tr></thead><tbody></tbody></table></div>"+
    "<div class='panel'><div class='tsec'>最近记录 <span style='font-size:11px;color:#8b949e'>(最新40条)</span></div>"+
    "<div style='max-height:320px;overflow-y:auto'><table class='table' id='accRecent'><thead><tr><th>商品</th><th>周期</th><th>方向</th><th>置信</th><th>预测价</th><th>下一根收盘</th><th>结果</th><th>K线时间</th></tr></thead><tbody></tbody></table></div></div>";
  var sel=document.getElementById("accMin");
  if(sel) sel.onchange=accRefresh;
  var ds=document.getElementById("accDays");
  if(ds) ds.onchange=accRefresh;
  var st=document.getElementById("accStart");
  if(st) st.onchange=accRefresh;
  var en=document.getElementById("accEnd");
  if(en) en.onchange=accRefresh;
  accRefresh();
  if(!window._accTimer) window._accTimer=setInterval(accRefresh,15000);
}
function accStatRow(st,label){
  return "<td>"+label+"</td><td>"+st.resolved+"</td><td class='up'>"+st.hits+"</td><td class='down'>"+st.misses+"</td>"+
    "<td>"+st.flat+"</td><td>"+st.pending+"</td><td class='"+(st.accuracy!=null&&st.accuracy>=60?"up":"down")+"'><b>"+(st.accuracy==null?"-":st.accuracy+"%")+"</b></td>";
}
function accRefresh(){
  var mc=document.getElementById("accMin")?document.getElementById("accMin").value:70;
  var ds=document.getElementById("accDays")?document.getElementById("accDays").value:"";
  var st=document.getElementById("accStart")?document.getElementById("accStart").value:"";
  var en=document.getElementById("accEnd")?document.getElementById("accEnd").value:"";
  var url="/api/accuracy?min_conf="+mc+"&t="+Date.now();
  if(ds) url+="&days="+ds;
  if(st) url+="&start="+st;
  if(en) url+="&end="+en;
  fetch(url).then(function(r){return r.json();}).then(function(a){
    renderAcc(a);
    var m=document.getElementById("accMsg");
    if(m){
      var range=a.start?"范围 "+a.start+" ~ "+(a.end||"今天"):(a.days?"最近 "+a.days+" 天":"全部时间");
      m.textContent="更新于 "+esc(a.ts)+" · "+range;
    }
  }).catch(function(){});
}
function renderAcc(a){
  var o=a.overall;
  document.getElementById("accOverview").innerHTML=
    "<div class='ac'><div class='lb'>已判定样本</div><div class='vl'>"+o.resolved+"</div></div>"+
    "<div class='ac'><div class='lb'>命中</div><div class='vl up'>"+o.hits+"</div></div>"+
    "<div class='ac'><div class='lb'>失误</div><div class='vl down'>"+o.misses+"</div></div>"+
    "<div class='ac'><div class='lb'>准确率</div><div class='vl "+(o.accuracy!=null&&o.accuracy>=60?"up":"down")+"'>"+(o.accuracy==null?"-":o.accuracy+"%")+"</div></div>"+
    "<div class='ac'><div class='lb'>平盘</div><div class='vl'>"+o.flat+"</div></div>"+
    "<div class='ac'><div class='lb'>未决</div><div class='vl'>"+o.pending+"</div></div>"+
    "<div class='ac'><div class='lb'>记录总量</div><div class='vl'>"+o.samples+"</div></div>";
  document.querySelector("#accTf tbody").innerHTML=ORDER.map(function(tf){
    return "<tr>"+accStatRow(a.by_tf[tf]||{},tf)+"</tr>";
  }).join("");
  document.querySelector("#accConf tbody").innerHTML=
    "<tr>"+accStatRow(a.by_conf["70"],"70-79%")+"</tr>"+
    "<tr>"+accStatRow(a.by_conf["80"],"80-89%")+"</tr>"+
    "<tr>"+accStatRow(a.by_conf["90"],"90-100%")+"</tr>";
  document.getElementById("accDir").innerHTML=
    "<div class='ac'><div class='lb'>看多 有效样本/准确率</div><div class='vl "+(a.by_dir["多"].accuracy!=null&&a.by_dir["多"].accuracy>=60?"up":"down")+"'>"+a.by_dir["多"].resolved+"笔 / "+(a.by_dir["多"].accuracy==null?"-":a.by_dir["多"].accuracy+"%")+"</div></div>"+
    "<div class='ac'><div class='lb'>看空 有效样本/准确率</div><div class='vl "+(a.by_dir["空"].accuracy!=null&&a.by_dir["空"].accuracy>=60?"up":"down")+"'>"+a.by_dir["空"].resolved+"笔 / "+(a.by_dir["空"].accuracy==null?"-":a.by_dir["空"].accuracy+"%")+"</div></div>";
  var syms=Object.keys(a.by_sym||{});
  document.querySelector("#accSym tbody").innerHTML=syms.map(function(sym){
    var st=a.by_sym[sym];
    return "<tr>"+accStatRow(st,esc((a.names&&a.names[sym])||sym))+"</tr>";
  }).join("")||"<tr><td colspan='7' style='color:#8b949e'>暂无数据</td></tr>";
  document.querySelector("#accRecent tbody").innerHTML=(a.recent||[]).map(function(r){
    var lbl=r.state==="hit"?"<span class='up'><b>命中</b></span>":r.state==="miss"?"<span class='down'><b>失误</b></span>":r.state==="flat"?"平盘":r.state==="pending"?"<span style='color:#8b949e'>未决</span>":"过期";
    return "<tr><td>"+esc(r.name)+"</td><td>"+esc(r.tf)+"</td><td class='"+(r.direction==="多"?"up":"down")+"'>"+(r.direction==="多"?"多":"空")+"</td><td>"+r.conf+"%</td><td>"+fmt(r.close,2)+"</td><td>"+(r.next_close!=null?fmt(r.next_close,2):"-")+"</td><td>"+lbl+"</td><td>"+esc(r.time)+"</td></tr>";
  }).join("")||"<tr><td colspan='8' style='color:#8b949e'>暂无记录(需要置信度≥"+(a.min_conf||70)+"%且方向为多/空的预测)</td></tr>";
}
function accReset(){
  if(!confirm("确定清空全部准确率统计并重新开始?")) return;
  fetch("/api/accuracy/reset",{method:"POST",headers:{"Content-Type":"application/json"},body:"{}"}).then(function(r){return r.json();}).then(function(res){
    var t=document.getElementById("toast"); t.textContent="✅ "+res.msg; t.classList.add("show"); setTimeout(function(){t.classList.remove("show");},3000);
    accRefresh();
  }).catch(function(){});
}
window.addEventListener("resize",function(){ if(mainChart) mainChart.resize(); Object.keys(charts).forEach(function(k){ try{charts[k].resize();}catch(e){} }); if(window._btChart) window._btChart.resize(); });
loadPrefs();
route();
setInterval(updateBotStatus,8000);
</script>
</body>
</html>
"""


# ---------------------------------------------------------------- 主入口
def main():
    global INTERVAL
    parser = argparse.ArgumentParser(description="大宗商品多周期预测看板")
    parser.add_argument("--port", type=int, default=8099, help="监听端口 (默认8099)")
    parser.add_argument("--interval", type=int, default=45, help="分析刷新间隔秒 (默认45)")
    args = parser.parse_args()
    INTERVAL = args.interval

    for c in COMMODITIES:
        fr, kl, sp = load_state(c)
        c.state = {"frames": fr, "klines": kl, "spot": sp}

    load_accuracy()
    for c in COMMODITIES:
        try:
            replay_accuracy(c)
        except Exception:
            pass
    save_accuracy(force=True)
    print("准确率统计已加载: %d 条记录" % len(ACC_RECORDS), flush=True)

    load_trade()
    load_api_config()
    load_bot_logs()
    load_fundflow()
    _cfg0 = load_bot_config()
    if _cfg0.get("tf") in ORDER:
        global BOT_TF
        BOT_TF = _cfg0["tf"]
    threading.Thread(target=tick_loop, daemon=True).start()
    threading.Thread(target=alert_loop, daemon=True).start()
    threading.Thread(target=bot_loop, daemon=True).start()
    threading.Thread(target=seeder_loop, daemon=True).start()
    threading.Thread(target=updater_loop, daemon=True).start()
    threading.Thread(target=fundflow_loop, daemon=True).start()
    threading.Thread(target=accuracy_loop, daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", args.port), Handler)
    print("=" * 52)
    print("  大宗商品多周期预测看板已启动")
    print("  本机访问  : http://localhost:%d" % args.port)
    try:
        import socket
        ip = socket.gethostbyname(socket.gethostname())
        if ip.startswith("127."):
            ip = "127.0.0.1"
        print("  局域网访问: http://%s:%d" % (ip, args.port))
    except Exception:
        pass
    print("  Ctrl+C 停止")
    print("=" * 52)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n已停止")


if __name__ == "__main__":
    main()
