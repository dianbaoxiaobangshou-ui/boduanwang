import json
import logging
import os
import sys
import time
import typing
import ssl
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime


OKX_BASE_URL = "https://www.okx.com"
TELEGRAM_API_BASE = "https://api.telegram.org"


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def setup_logger(log_path: str) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def normalize_bar(bar: str) -> str:
    b = bar.strip().lower()
    mapping = {
        "1m": "1m",
        "1min": "1m",
        "1分钟": "1m",
        "3m": "3m",
        "3min": "3m",
        "3分钟": "3m",
        "5m": "5m",
        "5min": "5m",
        "5分钟": "5m",
        "15m": "15m",
        "15min": "15m",
        "15分钟": "15m",
        "30m": "30m",
        "30min": "30m",
        "30分钟": "30m",
        "1h": "1H",
        "1小时": "1H",
        "2h": "2H",
        "2小时": "2H",
        "4h": "4H",
        "4小时": "4H",
        "6h": "6H",
        "6小时": "6H",
        "12h": "12H",
        "12小时": "12H",
        "1d": "1D",
        "日线": "1D",
        "1天": "1D",
        "1w": "1W",
        "周线": "1W",
        "1mth": "1M",
        "1月": "1M",
        "1q": "3M",
        "季度": "3M",
    }
    return mapping.get(b, bar)


def normalize_contract_type(t: str) -> str:
    s = t.strip().upper()
    if s in ("SWAP", "永续"):
        return "SWAP"
    return s


def pair_to_inst_id(pair: str, contract_type: str) -> str:
    pair = pair.upper().strip()
    contract_type = normalize_contract_type(contract_type)
    base = ""
    quote = ""
    if "-" in pair:
        parts = [p for p in pair.split("-") if p]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
    elif "/" in pair:
        parts = [p for p in pair.split("/") if p]
        if len(parts) >= 2:
            base, quote = parts[0], parts[1]
    else:
        if pair.endswith("USDT"):
            base = pair[:-4]
            quote = "USDT"
        elif pair.endswith("USD"):
            base = pair[:-3]
            quote = "USD"
    if not base or not quote:
        raise ValueError(f"不支持的交易对格式: {pair}")
    base = base.replace("-", "").replace("/", "")
    inst_id = f"{base}-{quote}-{contract_type}"
    return inst_id


def http_get(url: str, params: dict, timeout: float = 10.0, ssl_verify: bool = True) -> dict:
    qs = urlencode(params)
    req = Request(f"{url}?{qs}", headers={"User-Agent": "okx-monitor/1.0"})
    try:
        if url.lower().startswith("https") and not ssl_verify:
            ctx = ssl._create_unverified_context()
            with urlopen(req, timeout=timeout, context=ctx) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw)
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except HTTPError as e:
        logging.error(f"HTTP错误 {e.code} {e.reason} url={url} params={params}")
        raise
    except URLError as e:
        logging.error(f"网络错误 {e.reason} url={url} params={params}")
        raise
    except Exception as e:
        logging.error(f"解析错误 {e} url={url} params={params}")
        raise


def fetch_closed_candles(inst_id: str, bar: str, limit: int = 10, ssl_verify: bool = True) -> typing.List[dict]:
    url = f"{OKX_BASE_URL}/api/v5/market/candles"
    params = {
        "instId": inst_id,
        "bar": bar,
        "limit": str(limit),
    }
    data = http_get(url, params, ssl_verify=ssl_verify)
    if str(data.get("code")) != "0" or "data" not in data:
        logging.error(f"OKX返回异常: {data}")
        raise RuntimeError("OKX API 返回异常")
    rows = data["data"]
    candles = []
    for r in rows:
        if len(r) < 5:
            logging.warning(f"K线数据维度不足: {r}")
            continue
        ts = int(r[0])
        o = float(r[1])
        h = float(r[2])
        l = float(r[3])
        c = float(r[4])
        confirm = int(r[8]) if len(r) > 8 and str(r[8]).isdigit() else 1
        candles.append(
            {
                "ts": ts,
                "o": o,
                "h": h,
                "l": l,
                "c": c,
                "confirm": confirm,
            }
        )
    closed = [x for x in candles if int(x.get("confirm", 1)) == 1]
    return closed


def classify_candle(c: dict) -> str:
    return "阳" if c["c"] > c["o"] else "阴" if c["c"] < c["o"] else "十"


def pattern_signal(last3: typing.List[dict]) -> typing.Optional[str]:
    if len(last3) < 3:
        return None
    p = [classify_candle(last3[0]), classify_candle(last3[1]), classify_candle(last3[2])]
    if p == ["阴", "阴", "阳"]:
        return "可能走强，择机做多！！！"
    if p == ["阳", "阳", "阴"]:
        return "可能走弱，择机做空！！！"
    return None


def send_telegram(token: str, chat_id: str, text: str, max_retries: int = 3, ssl_verify: bool = True) -> bool:
    url = f"{TELEGRAM_API_BASE}/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    for attempt in range(1, max_retries + 1):
        try:
            resp = http_get(url, payload, timeout=10.0, ssl_verify=ssl_verify)
            ok = bool(resp.get("ok", False))
            if ok:
                return True
            logging.warning(f"Telegram发送失败，响应: {resp}")
        except Exception as e:
            logging.warning(f"Telegram发送异常: {e} 尝试{attempt}/{max_retries}")
        time.sleep(2 * attempt)
    return False


def ts_to_str(ts_ms: int) -> str:
    try:
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts_ms / 1000))
    except Exception:
        return str(ts_ms)


def _time_to_minutes(s: str) -> int:
    parts = s.strip().split(":")
    if len(parts) != 2:
        return -1
    h = int(parts[0])
    m = int(parts[1])
    return h * 60 + m


def _dow_to_name(i: int) -> str:
    names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    return names[i]


def now_in_windows(windows: typing.List[dict]) -> bool:
    if not windows:
        return True
    now = datetime.now()
    dow = _dow_to_name(now.weekday())
    minutes = now.hour * 60 + now.minute
    for w in windows:
        days = w.get("days", "*")
        start = _time_to_minutes(str(w.get("start", "00:00")))
        end = _time_to_minutes(str(w.get("end", "23:59")))
        if days != "*" and isinstance(days, list) and dow not in days:
            continue
        if start <= end:
            if start <= minutes <= end:
                return True
        else:
            if minutes >= start or minutes <= end:
                return True
    return False

def run_monitor(config_path: str) -> None:
    cfg = load_config(config_path)
    setup_logger(cfg.get("log_path", "okx_monitor.log"))
    token = cfg.get("telegram_token", "")
    chat_id = str(cfg.get("telegram_chat_id", ""))
    ssl_verify = bool(cfg.get("ssl_verify", True))
    if not token or not chat_id:
        logging.error("缺少Telegram配置")
        return

    poll_interval = int(cfg.get("poll_interval_seconds", 30))
    watchers = cfg.get("watchers", [])
    state_last_closed_ts = {}
    global_windows = cfg.get("active_windows") or []
    last_mtime = os.path.getmtime(config_path)

    logging.info("启动监控")

    while True:
        try:
            mtime = os.path.getmtime(config_path)
            if mtime != last_mtime:
                last_mtime = mtime
                cfg = load_config(config_path)
                token = cfg.get("telegram_token", "")
                chat_id = str(cfg.get("telegram_chat_id", ""))
                ssl_verify = bool(cfg.get("ssl_verify", True))
                poll_interval = int(cfg.get("poll_interval_seconds", 30))
                watchers = cfg.get("watchers", [])
                global_windows = cfg.get("active_windows") or []
                logging.info("配置已重新加载")
        except Exception as _e:
            pass
        if not now_in_windows(global_windows):
            time.sleep(poll_interval)
            continue
        for w in watchers:
            try:
                pair = w.get("pair", "")
                ctype = w.get("contract_type", "SWAP")
                bar = normalize_bar(w.get("bar", "12H"))
                w_windows = w.get("active_windows")
                if w_windows and not now_in_windows(w_windows):
                    continue
                inst_id = pair_to_inst_id(pair, ctype)
                closed = fetch_closed_candles(inst_id, bar, limit=10, ssl_verify=ssl_verify)
                if len(closed) < 3:
                    logging.info(f"{inst_id} {bar} 已收盘K线不足，跳过")
                    continue
                latest_closed = closed[0]
                key = (inst_id, bar)
                last_ts = state_last_closed_ts.get(key)
                if last_ts is not None and latest_closed["ts"] == last_ts:
                    continue
                state_last_closed_ts[key] = latest_closed["ts"]

                last3 = [closed[2], closed[1], closed[0]]
                signal = pattern_signal(last3)
                if signal:
                    tstr = ts_to_str(latest_closed["ts"])
                    msg = (
                        f"币种: {inst_id}\n"
                        f"周期: {bar}\n"
                        f"信号: {signal}\n"
                        f"时间: {tstr}\n"
                    )
                    ok = send_telegram(token, chat_id, msg, ssl_verify=ssl_verify)
                    if ok:
                        logging.info(f"信号已推送: {inst_id} {bar} {signal} ts={latest_closed['ts']}")
                    else:
                        logging.error(f"消息推送失败: {inst_id} {bar} {signal}")
                else:
                    logging.info(f"{inst_id} {bar} 无信号")

            except Exception as e:
                logging.error(f"处理 {w} 发生错误: {e}")
                continue

        time.sleep(poll_interval)


if __name__ == "__main__":
    config_file = os.path.join(os.path.dirname(__file__), "config.json")
    if not os.path.exists(config_file):
        print("缺少配置文件 config.json")
        sys.exit(1)
    run_monitor(config_file)

