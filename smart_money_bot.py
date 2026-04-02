"""
╔══════════════════════════════════════════════════════════════════════╗
║          SMART MONEY CONCEPT (SMC) BINANCE FUTURES BOT v11           ║
║  Зміни vs v10.1:                                                     ║
║  WS-1: BinanceWsTrading — Trading WebSocket API                      ║
║         wss://ws-fapi.binance.com/ws-fapi/v1                        ║
║         Всі ордери (place/cancel/query) через WS замість REST        ║
║  WS-2: BinanceMarketStream — Market Data WebSocket                   ║
║         wss://fstream.binance.com/stream                            ║
║         Klines (1d/4h/15m) стримуються в реальному часі             ║
║         REST get_klines використовується тільки для ініціалізації   ║
║  WS-3: User Data Stream WebSocket                                    ║
║         ORDER_TRADE_UPDATE → реактивне закриття позицій             ║
║         ACCOUNT_UPDATE → реактивний баланс                          ║
║         Більше не потрібний polling get_position_risk               ║
║  WS-4: Авто-реконект з exponential backoff для всіх WS              ║
║  WS-5: listenKey keepalive кожні 30 хвилин (REST)                   ║
╚══════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import logging
import time
import hmac
import hashlib
import html as html_lib
import os
import math
import json
import uuid
from decimal import Decimal, ROUND_DOWN
from datetime import datetime, timezone
from urllib.parse import urlencode
from collections import defaultdict, deque

import aiohttp
import pandas as pd
import numpy as np

try:
  from dotenv import load_dotenv
  load_dotenv()
except ModuleNotFoundError:
  pass

from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from db import (
  init_db,
  db_save_position, db_delete_position, db_load_positions,
  db_save_pending,  db_delete_pending,  db_load_pending,
  db_save_trade,    db_load_history,
  db_save_daily_state, db_load_daily_state,
)

# ─────────────────────────────────────────────
#  ПЕРЕВІРКА ЗМІННИХ СЕРЕДОВИЩА
# ─────────────────────────────────────────────
_REQUIRED = ["TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "BINANCE_API_KEY", "BINANCE_API_SECRET"]
_missing  = [k for k in _REQUIRED if not os.getenv(k)]
if _missing:
  raise ValueError(f"Відсутні змінні у .env: {_missing}")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")
BINANCE_API_KEY    = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

# ─────────────────────────────────────────────
#  НАЛАШТУВАННЯ ТОРГІВЛІ
# ─────────────────────────────────────────────
IS_DEMO    = True   # True = demo-fapi, False = реальний fapi
REST_BASE  = "https://demo-fapi.binance.com"   if IS_DEMO else "https://fapi.binance.com"
WS_STREAM  = "wss://fstream.binance.com"       # Market streams (немає demo варіанту)
WS_API     = "wss://ws-fapi.binance.com/ws-fapi/v1"  # Trading WS API

SYMBOLS = [
  "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "ADAUSDT", "AVAXUSDT",
  "LINKUSDT", "DOTUSDT", "LTCUSDT", "ATOMUSDT", "NEARUSDT", "APTUSDT",
  "ARBUSDT", "OPUSDT", "INJUSDT", "SUIUSDT", "WIFUSDT", "UNIUSDT",
  "AAVEUSDT", "TRXUSDT", "ICPUSDT", "ALGOUSDT",
]

TREND_TF          = "4h"   # Напрямок
HIGHER_TF         = "1h"   # Структура (BOS, POI)
LOWER_TF          = "15m"  # Точка входу

# Скільки свічок тримати в кеші для кожного символу і TF
KLINE_CACHE_SIZE  = 250

SCAN_INTERVAL_SEC = 300
LEVERAGE          = 15     # Збільшене плече (оскільки стопи будуть дуже короткими)
RISK_PCT          = 1.0    # Залишаємо 1% ризику на угоду (це важливо!)
MAX_SL_PCT        = 0.05   # Дозволяємо трохи більший відступ для волатильної крипти

MARGIN_PCT = 3.0
MIN_OPTIONAL_SCORE = 6
STRICT_MIN_SCORE   = 9

ATR_PERIOD        = 14
ATR_SL_MULT       = 1.5

MAX_POSITIONS     = 5
DAILY_LOSS_LIMIT  = -5.0

ICT_KILLZONES: list[tuple[int, int]] = [
  (8,  10),
  (12, 15),
  (18, 20),
]
LOCAL_UTC_OFFSET  = 3
SESSION_START_UTC = 7
SESSION_END_UTC   = 20

MIN_ATR_PCT = 0.003
MAX_ATR_PCT = 0.08

OB_MITIGATION_THRESHOLD = 0.50
OTE_LOW  = 0.618
OTE_HIGH = 0.786

TP_RR_RATIOS = [2.0, 3.0, 5.0]
TP_VOLUMES   = [0.40, 0.40, 0.20]

_TP_THRESHOLD_MARGIN = 0.02
TP1_THRESHOLD = round(1.0 - TP_VOLUMES[0] + _TP_THRESHOLD_MARGIN, 4)
TP2_THRESHOLD = round(1.0 - TP_VOLUMES[0] - TP_VOLUMES[1] + _TP_THRESHOLD_MARGIN, 4)

SWING_LOOKBACK   = 5
SWING_CONFIRM    = 3
MIN_VOLUME_RATIO = 1.2
EMA_FAST  = 21
EMA_SLOW  = 50
EMA_TREND = 200

MIN_ADX    = 20
ADX_PERIOD = 14

MAX_CORR_SAME_DIR = 2
CORRELATION_GROUPS: list[set] = [
  {"ETHUSDT", "SOLUSDT", "AVAXUSDT", "NEARUSDT", "APTUSDT", "SUIUSDT"},
  {"ARBUSDT", "OPUSDT"},
  {"UNIUSDT", "AAVEUSDT", "LINKUSDT"},
  {"XRPUSDT", "ADAUSDT", "DOTUSDT", "LTCUSDT", "TRXUSDT", "ALGOUSDT", "ICPUSDT"},
  {"INJUSDT", "ATOMUSDT"},
]

# WS реконект налаштування
WS_MAX_RECONNECT_DELAY = 60   # секунди
WS_PING_INTERVAL       = 180  # 3 хвилини
LISTEN_KEY_REFRESH_SEC = 1800 # 30 хвилин

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────
logging.basicConfig(
  level=logging.INFO,
  format="%(asctime)s | %(levelname)s | %(message)s",
  datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  СТАН БОТА
# ─────────────────────────────────────────────
bot_state: dict = {
  "running":        False,
  "open_positions": {},
  "pending_limits": {},
  "trade_history":  [],
  "total_pnl":      0.0,
  "scan_count":     0,
  "exchange_info":  {},
  "daily_pnl":      0.0,
  "daily_date":     None,
  # WS стан
  "ws_trading_ready":  False,   # Trading WS API підключений
  "ws_market_ready":   False,   # Market stream підключений
  "ws_userdata_ready": False,   # User data stream підключений
  "listen_key":        None,    # для User Data Stream
  "account_balance":   0.0,     # з ACCOUNT_UPDATE
  # Кеш свічок: {symbol: {tf: deque([row, ...])}}
  "kline_cache":       defaultdict(lambda: defaultdict(lambda: deque(maxlen=KLINE_CACHE_SIZE))),
  # Активні позиції на біржі (з ORDER_TRADE_UPDATE / ACCOUNT_UPDATE)
  "active_positions":  {},      # {symbol: abs_qty}
}

_position_lock    = asyncio.Lock()
_ws_api_lock      = asyncio.Lock()
_kline_cache_lock = asyncio.Lock()

# Pending WS API запити: {req_id: asyncio.Future}
_ws_pending: dict[str, asyncio.Future] = {}

# Глобальний WS API websocket (для надсилання команд)
_ws_api_ws = None


# ─────────────────────────────────────────────
#  ICT KILLZONES
# ─────────────────────────────────────────────
def is_in_killzone() -> bool:
  hour = datetime.now(timezone.utc).hour
  return any(start <= hour < end for start, end in ICT_KILLZONES)


def is_active_session() -> bool:
  hour = datetime.now(timezone.utc).hour
  return SESSION_START_UTC <= hour < SESSION_END_UTC


def session_status() -> str:
  now_utc    = datetime.now(timezone.utc)
  hour_utc   = now_utc.hour
  hour_local = (hour_utc + LOCAL_UTC_OFFSET) % 24

  for start, end in ICT_KILLZONES:
    if start <= hour_utc < end:
      names = {
        (8,  10): "🇬🇧 London Open KZ",
        (12, 15): "🔥 London/NY Overlap KZ",
        (18, 20): "🇺🇸 NY Afternoon KZ",
      }
      name = names.get((start, end), f"KZ {start}:00–{end}:00")
      local_start = (start + LOCAL_UTC_OFFSET) % 24
      local_end   = (end   + LOCAL_UTC_OFFSET) % 24
      return (
        f"{name} | UTC {hour_utc:02d}:xx / "
        f"🇺🇦 {hour_local:02d}:xx | вікно {local_start:02d}:00–{local_end:02d}:00"
      )

  if SESSION_START_UTC <= hour_utc < SESSION_END_UTC:
    next_kz = next(((s, e) for s, e in ICT_KILLZONES if s > hour_utc), None)
    if next_kz:
      local_next = (next_kz[0] + LOCAL_UTC_OFFSET) % 24
      return (
        f"⏳ Між KZ | UTC {hour_utc:02d}:xx / 🇺🇦 {hour_local:02d}:xx "
        f"— очікуємо {next_kz[0]:02d}:00 UTC ({local_next:02d}:00 за вашим часом) "
        f"[поріг: {STRICT_MIN_SCORE}/13]"
      )
    return (
      f"😴 Після останнього KZ | UTC {hour_utc:02d}:xx / 🇺🇦 {hour_local:02d}:xx "
      f"[поріг: {STRICT_MIN_SCORE}/13]"
    )

  if hour_utc < SESSION_START_UTC:
    local_first = (ICT_KILLZONES[0][0] + LOCAL_UTC_OFFSET) % 24
    return (
      f"🌙 Азіатська сесія | UTC {hour_utc:02d}:xx / 🇺🇦 {hour_local:02d}:xx "
      f"— очікуємо {ICT_KILLZONES[0][0]:02d}:00 UTC ({local_first:02d}:00)"
    )

  local_first = (ICT_KILLZONES[0][0] + LOCAL_UTC_OFFSET) % 24
  return (
    f"😴 Після-NY | UTC {hour_utc:02d}:xx / 🇺🇦 {hour_local:02d}:xx "
    f"— очікуємо 08:00 UTC ({local_first:02d}:00) завтра"
  )


# ─────────────────────────────────────────────
#  TELEGRAM SAFE SEND
# ─────────────────────────────────────────────
async def safe_send(bot: Bot, text: str, **kwargs) -> None:
  for attempt in range(3):
    try:
      await bot.send_message(
        chat_id=TELEGRAM_CHAT_ID, text=text,
        parse_mode="HTML", **kwargs
      )
      return
    except Exception as e:
      log.warning(f"safe_send attempt {attempt+1}/3: {e}")
      if attempt < 2:
        await asyncio.sleep(2)
  log.error("safe_send: повідомлення не надіслано після 3 спроб.")


# ─────────────────────────────────────────────
#  ФОРМАТУВАННЯ ЦІН ТА ОБ'ЄМІВ
# ─────────────────────────────────────────────
def _get_info(symbol: str) -> dict:
  return bot_state["exchange_info"].get(
    symbol, {"tick": 0.0001, "step": 0.001, "min_notional": 5.0}
  )


def fmt_price(symbol: str, price: float) -> str:
  info = _get_info(symbol)
  tick = Decimal(str(info["tick"]))
  p    = Decimal(str(price))
  rounded  = (p / tick).quantize(Decimal("1"), rounding=ROUND_DOWN) * tick
  tick_str = f"{tick:.10f}".rstrip("0")
  prec     = len(tick_str.split(".")[1]) if "." in tick_str else 0
  return f"{rounded:.{prec}f}"


def fmt_qty(symbol: str, qty: float) -> str:
  info = _get_info(symbol)
  step = Decimal(str(info["step"]))
  q    = Decimal(str(qty))
  rounded  = (q / step).quantize(Decimal("1"), rounding=ROUND_DOWN) * step
  step_str = f"{step:.10f}".rstrip("0")
  prec     = len(step_str.split(".")[1]) if "." in step_str else 0
  return f"{rounded:.{prec}f}"


def qty_float(symbol: str, qty: float) -> float:
  return float(fmt_qty(symbol, qty))


def price_float(symbol: str, price: float) -> float:
  return float(fmt_price(symbol, price))


# ─────────────────────────────────────────────
#  WS-1: TRADING WebSocket API
#  wss://ws-fapi.binance.com/ws-fapi/v1
#  Замінює всі торгові REST-виклики:
#    place_market_order, place_limit_order,
#    place_sl_market_order, cancel_order,
#    get_account, get_order
# ─────────────────────────────────────────────
class BinanceWsTrading:
  """
  Persistent WebSocket до Binance Trading API.
  Кожен запит отримує унікальний UUID.
  Відповідь matchується через _ws_pending dict.
  Автоматично реконектиться при розриві.
  """

  WS_URL_LIVE = "wss://ws-fapi.binance.com/ws-fapi/v1"
  WS_URL_DEMO = "wss://ws-fapi.binance.com/ws-fapi/v1"  # демо використовує той самий WS API

  def __init__(self, api_key: str, api_secret: str, is_demo: bool = True):
    self.api_key    = api_key
    self.api_secret = api_secret
    self.is_demo    = is_demo
    self._ws        = None
    self._connected = asyncio.Event()
    self._pending: dict[str, asyncio.Future] = {}
    self._recv_task = None
    self._ping_task = None
    self._reconnect_delay = 1

  def _sign(self, params: dict) -> str:
    qs  = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
    sig = hmac.new(
      self.api_secret.encode("utf-8"),
      qs.encode("utf-8"),
      hashlib.sha256
    ).hexdigest()
    return sig

  async def connect(self) -> None:
    """Запускає фоновий цикл реконекту."""
    asyncio.ensure_future(self._connect_loop())

  async def _connect_loop(self) -> None:
    while True:
      try:
        log.info("🔌 Trading WS API: підключення...")
        timeout   = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(limit=5)
        headers   = {"X-MBX-APIKEY": self.api_key}
        if self.is_demo:
          headers["X-MBX-DEMO-TRADING"] = "1"
        async with aiohttp.ClientSession(
          connector=connector, timeout=timeout
        ) as session:
          async with session.ws_connect(
            self.WS_URL_LIVE,
            headers=headers,
            heartbeat=30
          ) as ws:
            self._ws = ws
            self._connected.set()
            bot_state["ws_trading_ready"] = True
            self._reconnect_delay = 1
            log.info("✅ Trading WS API підключено")
            self._ping_task = asyncio.ensure_future(self._ping_loop())
            await self._recv_loop(ws)
      except Exception as e:
        log.warning(f"Trading WS API розрив: {e}")
      finally:
        self._connected.clear()
        bot_state["ws_trading_ready"] = False
        if self._ping_task:
          self._ping_task.cancel()
        # Відхиляємо всі pending futures
        for fut in self._pending.values():
          if not fut.done():
            fut.set_exception(ConnectionError("WS disconnected"))
        self._pending.clear()
        delay = min(self._reconnect_delay, WS_MAX_RECONNECT_DELAY)
        log.info(f"Trading WS: реконект через {delay}s")
        await asyncio.sleep(delay)
        self._reconnect_delay = min(self._reconnect_delay * 2, WS_MAX_RECONNECT_DELAY)

  async def _recv_loop(self, ws) -> None:
    async for msg in ws:
      if msg.type == aiohttp.WSMsgType.TEXT:
        try:
          data = json.loads(msg.data)
          req_id = str(data.get("id", ""))
          if req_id in self._pending:
            fut = self._pending.pop(req_id)
            if not fut.done():
              fut.set_result(data)
        except Exception as e:
          log.warning(f"Trading WS recv parse error: {e}")
      elif msg.type == aiohttp.WSMsgType.PING:
        await ws.pong(msg.data)
      elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
        break

  async def _ping_loop(self) -> None:
    while True:
      await asyncio.sleep(WS_PING_INTERVAL)
      if self._ws and not self._ws.closed:
        try:
          await self._ws.ping()
        except Exception:
          pass

  async def _request(self, method: str, params: dict, timeout: float = 10.0) -> dict:
    """Надсилає запит через WS API і чекає відповідь."""
    await asyncio.wait_for(self._connected.wait(), timeout=15.0)
    req_id = str(uuid.uuid4())
    ts     = int(time.time() * 1000)
    p      = {**params, "apiKey": self.api_key, "timestamp": ts, "recvWindow": 5000}
    p["signature"] = self._sign(p)
    msg = {"id": req_id, "method": method, "params": p}
    loop = asyncio.get_event_loop()
    fut  = loop.create_future()
    self._pending[req_id] = fut
    try:
      await self._ws.send_str(json.dumps(msg))
      resp = await asyncio.wait_for(fut, timeout=timeout)
      return resp
    except asyncio.TimeoutError:
      self._pending.pop(req_id, None)
      return {"status": 504, "error": {"code": -1, "msg": "WS timeout"}}
    except Exception as e:
      self._pending.pop(req_id, None)
      return {"status": 500, "error": {"code": -1, "msg": str(e)}}

  def _parse(self, resp: dict) -> dict:
    """Нормалізує відповідь WS API до формату схожого на REST."""
    if resp.get("status") == 200:
      return resp.get("result", {})
    # Повертаємо REST-подібну помилку
    err = resp.get("error", {})
    return {"code": err.get("code", -1), "msg": err.get("msg", "unknown WS error")}

  # ── Публічні методи (аналоги REST) ──────────────────────────────

  async def set_leverage(self, symbol: str, leverage: int) -> dict:
    resp = await self._request("v1/leverage", {"symbol": symbol, "leverage": leverage})
    return self._parse(resp)

  async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> dict:
    try:
      resp = await self._request("v1/marginType", {"symbol": symbol, "marginType": margin_type})
      return self._parse(resp)
    except Exception:
      return {}

  async def place_market_order(self, symbol: str, side: str, quantity: float,
                               reduce_only: bool = False) -> dict:
    params = {
      "symbol":     symbol,
      "side":       side,
      "type":       "MARKET",
      "quantity":   fmt_qty(symbol, quantity),
      "reduceOnly": "true" if reduce_only else "false",
    }
    resp = await self._request("order.place", params)
    return self._parse(resp)

  async def place_limit_order(self, symbol: str, side: str, quantity: float,
                              price: float, reduce_only: bool = False) -> dict:
    params = {
      "symbol":      symbol,
      "side":        side,
      "type":        "LIMIT",
      "quantity":    fmt_qty(symbol, quantity),
      "price":       fmt_price(symbol, price),
      "timeInForce": "GTC",
      "reduceOnly":  "true" if reduce_only else "false",
    }
    resp = await self._request("order.place", params)
    return self._parse(resp)

  async def place_sl_market_order(self, symbol: str, side: str,
                                  quantity: float, stop_price: float) -> dict:
    sp = float(stop_price)
    params = {
      "symbol":      symbol, "side": side,
      "type":        "STOP_MARKET",
      "quantity":    fmt_qty(symbol, quantity),
      "stopPrice":   fmt_price(symbol, sp),
      "reduceOnly":  "true",
      "workingType": "MARK_PRICE",
      "timeInForce": "GTC",
    }
    resp = await self._request("order.place", params)
    r = self._parse(resp)
    if _is_ok(r):
      return r
    # Fallback STOP limit
    if isinstance(r, dict) and r.get("code") in (-4120, -4003):
      buf     = sp * 0.003
      limit_p = sp - buf if side == "SELL" else sp + buf
      params2 = {
        "symbol":      symbol, "side": side,
        "type":        "STOP",
        "quantity":    fmt_qty(symbol, quantity),
        "price":       fmt_price(symbol, limit_p),
        "stopPrice":   fmt_price(symbol, sp),
        "reduceOnly":  "true",
        "workingType": "MARK_PRICE",
        "timeInForce": "GTC",
      }
      resp2 = await self._request("order.place", params2)
      r2 = self._parse(resp2)
      if _is_ok(r2):
        log.info(f"SL fallback→STOP limit OK {symbol}")
        return r2
    return r

  async def cancel_order(self, symbol: str, order_id: str) -> dict:
    resp = await self._request("order.cancel",
                               {"symbol": symbol, "orderId": int(order_id)})
    return self._parse(resp)

  async def cancel_all_orders(self, symbol: str) -> dict:
    resp = await self._request("allOpenOrders.delete", {"symbol": symbol})
    return self._parse(resp)

  async def get_order(self, symbol: str, order_id: str) -> dict:
    resp = await self._request("order.status",
                               {"symbol": symbol, "orderId": int(order_id)})
    return self._parse(resp)

  async def get_open_orders(self, symbol: str) -> list:
    resp = await self._request("openOrders.status", {"symbol": symbol})
    r = self._parse(resp)
    return r if isinstance(r, list) else []

  async def get_account(self) -> dict:
    resp = await self._request("v2/account", {})
    return self._parse(resp)

  async def get_position_risk(self) -> list:
    resp = await self._request("v2/positionRisk", {})
    r = self._parse(resp)
    return r if isinstance(r, list) else []

  async def get_user_trades(self, symbol: str, limit: int = 30) -> list:
    resp = await self._request("userTrades", {"symbol": symbol, "limit": limit})
    r = self._parse(resp)
    return r if isinstance(r, list) else []

  async def close(self) -> None:
    if self._ws and not self._ws.closed:
      await self._ws.close()


# ─────────────────────────────────────────────
#  WS-2: MARKET DATA STREAM
#  wss://fstream.binance.com/stream
#  Підписка на kline streams для всіх символів
#  і трьох таймфреймів (1d, 4h, 15m)
#  Кеш свічок заповнюється з REST при старті,
#  потім оновлюється через stream
# ─────────────────────────────────────────────
class BinanceMarketStream:
  """
  Multi-stream WebSocket для ринкових даних.
  Підтримує до 1024 стримів на одне з'єднання.
  При отриманні закритої свічки (x=true) — оновлює кеш.
  """

  STREAM_URL = "wss://fstream.binance.com/stream"

  def __init__(self, symbols: list[str], timeframes: list[str]):
    self.symbols    = symbols
    self.timeframes = timeframes
    self._task      = None
    self._reconnect_delay = 1
    # Кеш поточних (незакритих) свічок: {symbol: {tf: dict}}
    self._current_candle: dict = defaultdict(lambda: defaultdict(dict))

  def _build_streams(self) -> list[str]:
    streams = []
    for sym in self.symbols:
      for tf in self.timeframes:
        streams.append(f"{sym.lower()}@kline_{tf}")
    return streams

  async def start(self) -> None:
    self._task = asyncio.ensure_future(self._connect_loop())

  async def _connect_loop(self) -> None:
    streams = self._build_streams()
    # Combined stream URL: /stream?streams=stream1/stream2/...
    streams_path = "/".join(streams)
    url = f"{self.STREAM_URL}?streams={streams_path}"

    while True:
      try:
        log.info(f"📡 Market Stream WS: підключення ({len(streams)} стримів)...")
        async with aiohttp.ClientSession() as session:
          async with session.ws_connect(url, heartbeat=30) as ws:
            self._reconnect_delay = 1
            bot_state["ws_market_ready"] = True
            log.info("✅ Market Stream WS підключено")
            await self._recv_loop(ws)
      except Exception as e:
        log.warning(f"Market Stream WS розрив: {e}")
      finally:
        bot_state["ws_market_ready"] = False
        delay = min(self._reconnect_delay, WS_MAX_RECONNECT_DELAY)
        log.info(f"Market Stream WS: реконект через {delay}s")
        await asyncio.sleep(delay)
        self._reconnect_delay = min(self._reconnect_delay * 2, WS_MAX_RECONNECT_DELAY)

  async def _recv_loop(self, ws) -> None:
    async for msg in ws:
      if msg.type == aiohttp.WSMsgType.TEXT:
        try:
          outer = json.loads(msg.data)
          # Combined stream формат: {"stream": "...", "data": {...}}
          data  = outer.get("data", outer)
          if data.get("e") == "kline":
            await self._handle_kline(data)
        except Exception as e:
          log.debug(f"Market stream parse error: {e}")
      elif msg.type == aiohttp.WSMsgType.PING:
        await ws.pong(msg.data)
      elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
        break

  async def _handle_kline(self, data: dict) -> None:
    symbol = data["s"]
    k      = data["k"]
    tf     = k["i"]
    is_closed = k["x"]  # True = свічка закрита

    candle_row = [
      int(k["t"]),      # open time (ms)
      k["o"],           # open
      k["h"],           # high
      k["l"],           # low
      k["c"],           # close
      k["v"],           # volume
      int(k["T"]),      # close time
      k["q"],           # quote volume
      int(k["n"]),      # trades
      k["V"],           # taker buy base vol
      k["Q"],           # taker buy quote vol
      "0",              # ignore
    ]

    # Завжди зберігаємо поточну (можливо незакриту) свічку
    self._current_candle[symbol][tf] = candle_row

    if is_closed and symbol in bot_state["kline_cache"]:
      async with _kline_cache_lock:
        cache = bot_state["kline_cache"][symbol][tf]
        # Уникаємо дублікатів за open time
        if cache and cache[-1][0] == candle_row[0]:
          cache[-1] = candle_row
        else:
          cache.append(candle_row)

  def get_current_candle(self, symbol: str, tf: str) -> list | None:
    """Повертає поточну (можливо незакриту) свічку."""
    return self._current_candle.get(symbol, {}).get(tf)


# ─────────────────────────────────────────────
#  WS-3: USER DATA STREAM
#  Реактивна обробка ORDER_TRADE_UPDATE і ACCOUNT_UPDATE
#  Замінює polling check_open_positions() для виявлення SL
# ─────────────────────────────────────────────
class BinanceUserDataStream:
  """
  User Data WebSocket Stream.
  listenKey отримується через REST, потім підтримується keepalive кожні 30 хв.
  """

  STREAM_BASE = "wss://fstream.binance.com/ws"
  DEMO_STREAM = "wss://dstream.binancefuture.com/ws"   # демо не має user stream

  def __init__(self, rest_client, is_demo: bool = True, bot: Bot = None):
    self.rest    = rest_client
    self.is_demo = is_demo
    self.bot     = bot
    self._task   = None
    self._keepalive_task = None
    self._reconnect_delay = 1

  async def start(self) -> None:
    self._task = asyncio.ensure_future(self._connect_loop())

  async def _get_listen_key(self) -> str | None:
    try:
      r = await self.rest._post("/fapi/v1/listenKey", {})
      return r.get("listenKey")
    except Exception as e:
      log.warning(f"get_listen_key: {e}")
      return None

  async def _keepalive_listen_key(self, key: str) -> None:
    try:
      await self.rest._put(f"/fapi/v1/listenKey", {"listenKey": key})
    except Exception as e:
      log.warning(f"keepalive listenKey: {e}")

  async def _connect_loop(self) -> None:
    while True:
      listen_key = await self._get_listen_key()
      if not listen_key:
        log.warning("User Data Stream: не отримали listenKey, чекаємо 30s")
        await asyncio.sleep(30)
        continue

      bot_state["listen_key"] = listen_key
      url = f"{self.STREAM_BASE}/{listen_key}"

      # Keepalive task
      if self._keepalive_task:
        self._keepalive_task.cancel()
      self._keepalive_task = asyncio.ensure_future(
        self._keepalive_loop(listen_key)
      )

      try:
        log.info("📡 User Data Stream WS: підключення...")
        async with aiohttp.ClientSession() as session:
          async with session.ws_connect(url, heartbeat=30) as ws:
            self._reconnect_delay = 1
            bot_state["ws_userdata_ready"] = True
            log.info("✅ User Data Stream WS підключено")
            await self._recv_loop(ws)
      except Exception as e:
        log.warning(f"User Data Stream розрив: {e}")
      finally:
        bot_state["ws_userdata_ready"] = False
        delay = min(self._reconnect_delay, WS_MAX_RECONNECT_DELAY)
        log.info(f"User Data Stream: реконект через {delay}s")
        await asyncio.sleep(delay)
        self._reconnect_delay = min(self._reconnect_delay * 2, WS_MAX_RECONNECT_DELAY)

  async def _keepalive_loop(self, key: str) -> None:
    while True:
      await asyncio.sleep(LISTEN_KEY_REFRESH_SEC)
      await self._keepalive_listen_key(key)
      log.info("🔄 listenKey keepalive надіслано")

  async def _recv_loop(self, ws) -> None:
    async for msg in ws:
      if msg.type == aiohttp.WSMsgType.TEXT:
        try:
          data = json.loads(msg.data)
          event = data.get("e")
          if event == "ORDER_TRADE_UPDATE":
            await self._handle_order_update(data)
          elif event == "ACCOUNT_UPDATE":
            await self._handle_account_update(data)
        except Exception as e:
          log.debug(f"User Data Stream parse error: {e}")
      elif msg.type == aiohttp.WSMsgType.PING:
        await ws.pong(msg.data)
      elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
        break

  async def _handle_order_update(self, data: dict) -> None:
    """
    Реактивна обробка виконання ордерів.
    Якщо SL ордер спрацював (FILLED + reduceOnly) — закриваємо позицію.
    Якщо Limit ордер заповнений — конвертуємо pending → open.
    """
    o      = data.get("o", {})
    symbol = o.get("s")
    status = o.get("X")      # NEW, PARTIALLY_FILLED, FILLED, CANCELED, ...
    otype  = o.get("o")      # STOP_MARKET, LIMIT, MARKET, ...
    ro     = o.get("R", False)  # reduceOnly
    side   = o.get("S")      # BUY/SELL
    qty    = float(o.get("q", 0))
    price  = float(o.get("ap", 0) or o.get("p", 0))  # avgPrice or price
    oid    = str(o.get("i", ""))
    rPnL   = float(o.get("rp", 0))  # realizedProfit

    if not symbol:
      return

    # ── SL спрацював (STOP_MARKET/STOP filled + reduceOnly) ────
    if (status == "FILLED" and ro and
      otype in ("STOP_MARKET", "STOP", "TAKE_PROFIT_MARKET")):
      async with _position_lock:
        if symbol in bot_state["open_positions"]:
          pos = bot_state["open_positions"].pop(symbol)
        else:
          return
      await db_delete_position(symbol)
      reason   = "SL ❌" if rPnL < 0 else "TP exchange ✅"
      exit_p   = price if price > 0 else pos["entry_price"]
      pnl      = rPnL if rPnL != 0 else (
        (exit_p - pos["entry_price"]) * pos["qty"]
        if pos["signal"] == "BUY"
        else (pos["entry_price"] - exit_p) * pos["qty"]
      )
      bot_state["total_pnl"] += pnl
      bot_state["daily_pnl"] += pnl
      await db_save_daily_state(bot_state["daily_date"], bot_state["daily_pnl"])
      await db_save_trade({
        "symbol": symbol, "signal": pos["signal"],
        "entry": pos["entry_price"], "exit": exit_p,
        "pnl": pnl, "reason": reason,
        "time": datetime.now(timezone.utc)
      })
      emoji    = "✅" if pnl >= 0 else "❌"
      pnl_sign = "+" if pnl >= 0 else ""
      if self.bot:
        await safe_send(self.bot,
                        f"{emoji} <b>ЗАКРИТО (WS) — {symbol}</b>\n"
                        f"PnL: <b>{pnl_sign}{pnl:.4f} USDT</b> | {reason}\n"
                        f"Вхід: {pos['entry_price']} → Вихід: {exit_p:.4f}"
                        )

    # ── Limit ордер filled — переводимо pending → open ──────────
    elif status == "FILLED" and not ro and otype == "LIMIT":
      if symbol in bot_state["pending_limits"]:
        p = bot_state["pending_limits"].get(symbol)
        if p and str(p.get("order_id")) == oid:
          # Обробляємо через check_pending_limits пізніше
          # але реактивно помічаємо як filled
          p["_ws_filled"] = True
          p["_ws_price"]  = price
          p["_ws_qty"]    = qty

  async def _handle_account_update(self, data: dict) -> None:
    """Оновлюємо кеш балансу та активних позицій."""
    a = data.get("a", {})
    # Баланс
    for asset in a.get("B", []):
      if asset.get("a") == "USDT":
        bot_state["account_balance"] = float(asset.get("wb", 0))
    # Позиції
    for pos in a.get("P", []):
      sym = pos.get("s")
      amt = abs(float(pos.get("pa", 0)))
      if amt > 0:
        bot_state["active_positions"][sym] = amt
      elif sym in bot_state["active_positions"]:
        del bot_state["active_positions"][sym]


# ─────────────────────────────────────────────
#  REST CLIENT (залишається для init)
#  Використовується тільки для:
#    - get_exchange_info
#    - get_klines (ініціалізація кешу)
#    - listenKey POST/PUT
#    - get_price (fallback)
#    - get_available_balance (fallback якщо WS не готовий)
#    - get_user_trades (для розрахунку PnL)
# ─────────────────────────────────────────────
class BinanceRestClient:
  def __init__(self, api_key: str, api_secret: str, base_url: str):
    self.api_key    = api_key
    self.api_secret = api_secret
    self.base_url   = base_url
    self._session: aiohttp.ClientSession | None = None

  async def _get_session(self) -> aiohttp.ClientSession:
    if self._session is None or self._session.closed:
      timeout   = aiohttp.ClientTimeout(total=30, connect=10)
      connector = aiohttp.TCPConnector(limit=10, enable_cleanup_closed=True)
      headers   = {"X-MBX-APIKEY": self.api_key}
      if IS_DEMO:
        headers["X-MBX-DEMO-TRADING"] = "1"
      self._session = aiohttp.ClientSession(
        headers=headers, timeout=timeout, connector=connector
      )
    return self._session

  def _sign(self, params: dict) -> str:
    qs  = urlencode(params)
    sig = hmac.new(
      self.api_secret.encode("utf-8"),
      qs.encode("utf-8"),
      hashlib.sha256
    ).hexdigest()
    return qs + "&signature=" + sig

  async def _request(self, method: str, path: str,
                     params: dict | None = None, signed: bool = False) -> dict:
    session = await self._get_session()
    p = dict(params or {})
    if signed:
      p["timestamp"]  = int(time.time() * 1000)
      p["recvWindow"] = 5000
      query = self._sign(p)
      url   = f"{self.base_url}{path}?{query}"
    else:
      url = f"{self.base_url}{path}" + (f"?{urlencode(p)}" if p else "")

    for attempt in range(3):
      try:
        async with getattr(session, method)(url) as resp:
          data = await resp.json()
          if resp.status == 429:
            wait = int(resp.headers.get("Retry-After", 5))
            await asyncio.sleep(wait)
            continue
          if resp.status >= 400 and "code" not in data:
            data = {"code": resp.status, "msg": f"HTTP {resp.status}"}
          return data
      except Exception as e:
        log.warning(f"REST {method.upper()} {path} attempt {attempt+1}/3: {e}")
        if attempt < 2:
          await asyncio.sleep(1 * (attempt + 1))
    return {"code": -1, "msg": "Max retries exceeded"}

  async def _get(self, path, params=None, signed=False):
    return await self._request("get", path, params, signed)

  async def _post(self, path, params=None, signed=True):
    return await self._request("post", path, params, signed)

  async def _put(self, path, params=None, signed=True):
    return await self._request("put", path, params, signed)

  async def get_exchange_info(self):
    return await self._get("/fapi/v1/exchangeInfo")

  async def get_klines(self, symbol: str, interval: str, limit: int = 250):
    return await self._get("/fapi/v1/klines",
                           {"symbol": symbol, "interval": interval, "limit": limit})

  async def get_price(self, symbol: str) -> float:
    r = await self._get("/fapi/v1/ticker/price", {"symbol": symbol})
    return float(r.get("price", 0))

  async def get_account(self):
    return await self._get("/fapi/v2/account", signed=True)

  async def get_user_trades(self, symbol: str, limit: int = 30) -> list:
    r = await self._get("/fapi/v1/userTrades",
                        {"symbol": symbol, "limit": limit}, signed=True)
    return r if isinstance(r, list) else []

  async def get_position_risk(self) -> list:
    r = await self._get("/fapi/v2/positionRisk", signed=True)
    return r if isinstance(r, list) else []

  async def get_open_orders(self, symbol: str) -> list:
    r = await self._get("/fapi/v1/openOrders", {"symbol": symbol}, signed=True)
    return r if isinstance(r, list) else []

  async def get_order(self, symbol: str, order_id: str) -> dict:
    return await self._get("/fapi/v1/order",
                           {"symbol": symbol, "orderId": int(order_id)}, signed=True)

  async def _delete(self, path: str, params: dict | None = None) -> dict:
    return await self._request("delete", path, params, signed=True)

  async def set_leverage(self, symbol: str, leverage: int) -> dict:
    return await self._post("/fapi/v1/leverage",
                            {"symbol": symbol, "leverage": leverage})

  async def set_margin_type(self, symbol: str, margin_type: str = "ISOLATED") -> dict:
    try:
      return await self._post("/fapi/v1/marginType",
                              {"symbol": symbol, "marginType": margin_type})
    except Exception:
      return {}

  async def place_market_order(self, symbol: str, side: str, quantity: float,
                               reduce_only: bool = False) -> dict:
    params = {
      "symbol":     symbol,
      "side":       side,
      "type":       "MARKET",
      "quantity":   fmt_qty(symbol, quantity),
      "reduceOnly": "true" if reduce_only else "false",
    }
    return await self._post("/fapi/v1/order", params)

  async def place_limit_order(self, symbol: str, side: str, quantity: float,
                              price: float, reduce_only: bool = False) -> dict:
    params = {
      "symbol":      symbol,
      "side":        side,
      "type":        "LIMIT",
      "quantity":    fmt_qty(symbol, quantity),
      "price":       fmt_price(symbol, price),
      "timeInForce": "GTC",
      "reduceOnly":  "true" if reduce_only else "false",
    }
    return await self._post("/fapi/v1/order", params)

  async def place_sl_market_order(self, symbol: str, side: str,
                                  quantity: float, stop_price: float) -> dict:
    sp     = float(stop_price)
    params = {
      "symbol":      symbol,
      "side":        side,
      "type":        "STOP_MARKET",
      "quantity":    fmt_qty(symbol, quantity),
      "stopPrice":   fmt_price(symbol, sp),
      "reduceOnly":  "true",
      "workingType": "MARK_PRICE",
      "timeInForce": "GTC",
    }
    r = await self._post("/fapi/v1/order", params)
    if _is_ok(r):
      return r
    # Fallback → STOP limit
    if isinstance(r, dict) and r.get("code") in (-4120, -4003):
      buf     = sp * 0.003
      limit_p = sp - buf if side == "SELL" else sp + buf
      params2 = {
        "symbol":      symbol,
        "side":        side,
        "type":        "STOP",
        "quantity":    fmt_qty(symbol, quantity),
        "price":       fmt_price(symbol, limit_p),
        "stopPrice":   fmt_price(symbol, sp),
        "reduceOnly":  "true",
        "workingType": "MARK_PRICE",
        "timeInForce": "GTC",
      }
      r2 = await self._post("/fapi/v1/order", params2)
      if _is_ok(r2):
        log.info(f"SL fallback→STOP limit OK {symbol}")
        return r2
    return r

  async def cancel_order(self, symbol: str, order_id: str) -> dict:
    return await self._delete("/fapi/v1/order",
                              {"symbol": symbol, "orderId": int(order_id)})

  async def cancel_all_orders(self, symbol: str) -> dict:
    return await self._delete("/fapi/v1/allOpenOrders", {"symbol": symbol})

  async def connect(self) -> None:
    """REST не потребує WS-з'єднання — одразу позначаємо як готовий."""
    bot_state["ws_trading_ready"] = True
    log.info("✅ REST Trading API готовий")

  async def close(self) -> None:
    if self._session and not self._session.closed:
      await self._session.close()


# ─────────────────────────────────────────────
#  ДОПОМІЖНІ ФУНКЦІЇ
# ─────────────────────────────────────────────
def _is_ok(resp: dict) -> bool:
  if not isinstance(resp, dict):
    return False
  code = resp.get("code")
  if code is not None and isinstance(code, int) and code < 0:
    return False
  return "orderId" in resp


def _api_err(resp: dict) -> str:
  return f"code={resp.get('code')} msg={resp.get('msg','?')}"


async def get_available_balance(rest: BinanceRestClient) -> float:
  """
  Спочатку перевіряємо кеш з ACCOUNT_UPDATE (WS).
  Якщо 0 або WS не готовий — fallback до REST.
  """
  cached = bot_state.get("account_balance", 0.0)
  if cached > 0 and bot_state.get("ws_userdata_ready"):
    return cached
  try:
    acc    = await rest.get_account()
    assets = acc.get("assets", [])
    for a in assets:
      if a.get("asset") == "USDT":
        v = float(a.get("availableBalance", 0))
        if v > 0:
          return v
    v = float(acc.get("availableBalance", 0))
    return v if v > 0 else 0.0
  except Exception as e:
    log.warning(f"get_available_balance: {e}")
    return 0.0


def check_correlation(symbol: str, signal: str) -> bool:
  for group in CORRELATION_GROUPS:
    if symbol not in group:
      continue
    same_dir = sum(
      1 for sym, pos in {
        **bot_state["open_positions"],
        **{s: {"signal": p["signal"]} for s, p in bot_state["pending_limits"].items()}
      }.items()
      if sym in group and pos["signal"] == signal
    )
    if same_dir >= MAX_CORR_SAME_DIR:
      log.info(
        f"⚠️ Correlation guard: {symbol} {signal} blocked — "
        f"{same_dir}/{MAX_CORR_SAME_DIR} same-dir already in cluster"
      )
      return False
  return True


async def get_real_pnl(rest: BinanceRestClient, symbol: str,
                       pos: dict, fallback_price: float) -> tuple[float, float, str]:
  try:
    trades = await rest.get_user_trades(symbol, limit=50)
    if isinstance(trades, list) and trades:
      open_ts  = pos["open_time"].timestamp() * 1000 \
        if isinstance(pos["open_time"], datetime) else 0
      realized   = 0.0
      exit_price = fallback_price
      for t in trades:
        if open_ts and float(t.get("time", 0)) < open_ts - 60_000:
          continue
        rpnl = float(t.get("realizedPnl", 0))
        if abs(rpnl) > 0:
          realized  += rpnl
          exit_price = float(t.get("price", fallback_price))
      if realized != 0.0:
        reason = "TP ✅" if realized > 0 else "SL ❌"
        return exit_price, realized, reason
  except Exception as e:
    log.warning(f"get_real_pnl {symbol}: {e}")

  entry = pos["entry_price"]
  qty   = pos["original_qty"]
  if pos["signal"] == "BUY":
    pnl = (fallback_price - entry) * qty
  else:
    pnl = (entry - fallback_price) * qty
  reason = "TP ✅" if pnl >= 0 else "SL ❌"
  return fallback_price, pnl, reason


# ─────────────────────────────────────────────
#  KLINE CACHE — отримати DataFrame з кешу
# ─────────────────────────────────────────────
def cache_to_df(symbol: str, tf: str,
                market_stream: BinanceMarketStream | None = None) -> pd.DataFrame:
  """
  Конвертує кеш свічок для символу/TF у DataFrame.
  Додає поточну незакриту свічку якщо є.
  """
  rows = list(bot_state["kline_cache"][symbol][tf])
  if not rows:
    return pd.DataFrame(columns=["open", "high", "low", "close", "vol"])

  # Додаємо поточну незакриту свічку (якщо вона новіша ніж остання закрита)
  if market_stream:
    cur = market_stream.get_current_candle(symbol, tf)
    if cur and (not rows or cur[0] > rows[-1][0]):
      rows = rows + [cur]

  cols = ["ts","open","high","low","close","vol",
          "close_ts","qvol","trades","taker_buy","taker_buy_q","ignore"]
  df = pd.DataFrame(rows, columns=cols)
  for c in ["open","high","low","close","vol"]:
    df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
  df["ts"] = pd.to_datetime(df["ts"].astype(int), unit="ms")
  df = df.set_index("ts").dropna()
  # Відкидаємо поточну незакриту свічку для аналізу (як і раніше iloc[:-1])
  return df.iloc[:-1] if len(df) > 1 else df


# ─────────────────────────────────────────────
#  SMC ІНДИКАТОРИ (без змін)
# ─────────────────────────────────────────────
def klines_to_df(raw) -> pd.DataFrame:
  if not raw or not isinstance(raw, list):
    return pd.DataFrame(columns=["open", "high", "low", "close", "vol"])
  cols = ["ts","open","high","low","close","vol",
          "close_ts","qvol","trades","taker_buy","taker_buy_q","ignore"]
  df = pd.DataFrame(raw, columns=cols)
  for c in ["open","high","low","close","vol"]:
    df[c] = df[c].astype(float)
  df["ts"] = pd.to_datetime(df["ts"], unit="ms")
  df = df.set_index("ts")
  return df.iloc[:-1]


def calc_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
  h, l, c = df["high"], df["low"], df["close"]
  tr = pd.concat([
    (h - l),
    (h - c.shift()).abs(),
    (l - c.shift()).abs()
  ], axis=1).max(axis=1)
  return tr.rolling(period).mean()


def calc_ema(df: pd.DataFrame, period: int) -> pd.Series:
  return df["close"].ewm(span=period, adjust=False).mean()


def calc_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
  delta = df["close"].diff()
  gain  = delta.clip(lower=0).rolling(period).mean()
  loss  = (-delta.clip(upper=0)).rolling(period).mean()
  rs    = gain / (loss + 1e-10)
  return 100 - (100 / (1 + rs))


def calc_adx(df: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
  high  = df["high"]; low = df["low"]; close = df["close"]
  tr = pd.concat([
    (high - low),
    (high - close.shift()).abs(),
    (low  - close.shift()).abs(),
  ], axis=1).max(axis=1)
  up_move   = high.diff()
  down_move = -low.diff()
  plus_dm  = pd.Series(
    np.where((up_move > down_move) & (up_move > 0), up_move, 0.0), index=df.index
  )
  minus_dm = pd.Series(
    np.where((down_move > up_move) & (down_move > 0), down_move, 0.0), index=df.index
  )
  atr_s    = tr.rolling(period).mean()
  plus_di  = 100 * plus_dm.rolling(period).mean() / (atr_s + 1e-10)
  minus_di = 100 * minus_dm.rolling(period).mean() / (atr_s + 1e-10)
  dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di + 1e-10)
  return dx.rolling(period).mean()


def volume_ok(df: pd.DataFrame) -> bool:
  avg = df["vol"].rolling(20).mean().iloc[-1]
  return df["vol"].iloc[-1] >= avg * MIN_VOLUME_RATIO


def find_swing_points(df: pd.DataFrame, lookback: int = SWING_LOOKBACK,
                      confirm: int = SWING_CONFIRM) -> tuple[list, list]:
  highs, lows = [], []
  for i in range(lookback, len(df) - confirm):
    window = df["high"].iloc[i - lookback: i + 1]
    if df["high"].iloc[i] == window.max():
      futures = df["high"].iloc[i + 1: i + confirm + 1]
      if (futures < df["high"].iloc[i]).all():
        highs.append((df.index[i], df["high"].iloc[i]))
    window_l = df["low"].iloc[i - lookback: i + 1]
    if df["low"].iloc[i] == window_l.min():
      futures_l = df["low"].iloc[i + 1: i + confirm + 1]
      if (futures_l > df["low"].iloc[i]).all():
        lows.append((df.index[i], df["low"].iloc[i]))
  return highs, lows


def get_daily_trend(df: pd.DataFrame) -> str:
  e200  = calc_ema(df, EMA_TREND).iloc[-1]
  e50   = calc_ema(df, EMA_SLOW).iloc[-1]
  price = df["close"].iloc[-1]
  if price > e200 and e50 > e200:
    return "bull"
  if price < e200 and e50 < e200:
    return "bear"
  return "sideways"


def get_htf_bias(df: pd.DataFrame) -> str:
  e21   = calc_ema(df, EMA_FAST).iloc[-1]
  e50   = calc_ema(df, EMA_SLOW).iloc[-1]
  price = df["close"].iloc[-1]
  if price > e21 and e21 > e50:
    return "bullish"
  if price < e21 and e21 < e50:
    return "bearish"
  return "neutral"


def detect_bos(df: pd.DataFrame) -> tuple[str, int, str]:
  highs, lows = find_swing_points(df)
  if len(highs) < 2 or len(lows) < 2:
    return "neutral", 0, "Мало свінгів"
  price = df["close"].iloc[-1]
  rh    = [h[1] for h in highs[-4:]]
  rl    = [l[1] for l in lows[-4:]]
  prev_high = max(rh[:-1]) if len(rh) > 1 else rh[0]
  prev_low  = min(rl[:-1]) if len(rl) > 1 else rl[0]
  if price > prev_high:
    return "bullish", 3, f"BOS вгору &gt; {prev_high:.4f}"
  if price < prev_low:
    return "bearish", 3, f"BOS вниз &lt; {prev_low:.4f}"
  if len(rh) >= 2 and rh[-1] > rh[-2] and len(rl) >= 2 and rl[-1] > rl[-2]:
    return "bullish", 2, "HH+HL структура"
  if len(rh) >= 2 and rh[-1] < rh[-2] and len(rl) >= 2 and rl[-1] < rl[-2]:
    return "bearish", 2, "LH+LL структура"
  return "neutral", 0, "Нейтральна структура"


def detect_pd_zone(df: pd.DataFrame) -> tuple[str, float]:
  highs, lows = find_swing_points(df)
  if not highs or not lows:
    return "equilibrium", 0.5
  hv = max(h[1] for h in highs[-5:]) if len(highs) >= 5 else max(h[1] for h in highs)
  lv = min(l[1] for l in lows[-5:])  if len(lows)  >= 5 else min(l[1] for l in lows)
  if hv <= lv:
    return "equilibrium", 0.5
  r = (df["close"].iloc[-1] - lv) / (hv - lv)
  if r < 0.35:
    return "discount", r
  if r > 0.65:
    return "premium", r
  return "equilibrium", r


def is_ob_mitigated(df: pd.DataFrame, ob_top: float, ob_bottom: float,
                    direction: str, ob_index: int) -> bool:
  ob_height = ob_top - ob_bottom
  if ob_height <= 0:
    return True
  midpoint    = ob_bottom + ob_height * OB_MITIGATION_THRESHOLD
  start_check = ob_index + 4
  if start_check >= len(df):
    return False
  future_candles = df.iloc[start_check:]
  if direction == "bullish":
    if future_candles["low"].min() < midpoint:
      return True
  else:
    if future_candles["high"].max() > midpoint:
      return True
  return False


def find_ob_with_fvg(df: pd.DataFrame, direction: str) -> list[dict]:
  results = []
  for i in range(2, len(df) - 4):
    ob = df.iloc[i]; n1 = df.iloc[i + 1]; n2 = df.iloc[i + 2]
    body = abs(ob["close"] - ob["open"])
    if body < ob["close"] * 0.001:
      continue
    ok = False
    if (direction == "bullish"
      and ob["close"] < ob["open"]
      and n1["close"] > n1["open"]
      and n2["close"] > n2["open"]
      and n2["close"] > ob["high"]):
      ok = True
    elif (direction == "bearish"
          and ob["close"] > ob["open"]
          and n1["close"] < n1["open"]
          and n2["close"] < n2["open"]
          and n2["close"] < ob["low"]):
      ok = True
    if not ok or i + 3 >= len(df):
      continue
    n3  = df.iloc[i + 3]
    fvg = False
    if direction == "bullish" and n3["low"] > ob["high"]:
      fvg = True
    elif direction == "bearish" and n3["high"] < ob["low"]:
      fvg = True
    if not fvg:
      continue
    if is_ob_mitigated(df, ob["high"], ob["low"], direction, i):
      continue
    results.append({
      "ob_top": ob["high"], "ob_bottom": ob["low"],
      "entry": (ob["high"] + ob["low"]) / 2, "ob_index": i,
    })
  return results[-3:] if results else []


def find_liquidity(df: pd.DataFrame, direction: str) -> tuple[float | None, bool]:
  highs, lows = find_swing_points(df)
  price = df["close"].iloc[-1]
  tol   = 0.002
  if direction == "bullish":
    for i in range(len(highs) - 1, 0, -1):
      for j in range(i - 1, max(i - 5, 0) - 1, -1):
        if abs(highs[i][1] - highs[j][1]) / (highs[i][1] + 1e-10) < tol:
          liq = (highs[i][1] + highs[j][1]) / 2
          if liq > price * 1.005:
            return liq, True
  else:
    for i in range(len(lows) - 1, 0, -1):
      for j in range(i - 1, max(i - 5, 0) - 1, -1):
        if abs(lows[i][1] - lows[j][1]) / (lows[i][1] + 1e-10) < tol:
          liq = (lows[i][1] + lows[j][1]) / 2
          if liq < price * 0.995:
            return liq, True
  return None, False


def check_candle_confirmation(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
  if len(df) < 4:
    return False, "Мало даних"
  last = df.iloc[-1]; prev = df.iloc[-2]
  body = abs(last["close"] - last["open"])
  rng  = last["high"] - last["low"]
  if rng == 0:
    return False, "Нульовий діапазон"
  br = body / rng
  if direction == "bullish":
    if (last["close"] > last["open"]
      and last["close"] > prev["high"]
      and last["open"] < prev["low"]):
      return True, "Bullish Engulfing"
    lw = (last["open"] - last["low"]) if last["close"] > last["open"] \
      else (last["close"] - last["low"])
    if lw > body * 2 and br > 0.2:
      return True, "Pin Bar (Молот)"
  else:
    if (last["close"] < last["open"]
      and last["close"] < prev["low"]
      and last["open"] > prev["high"]):
      return True, "Bearish Engulfing"
    uw = (last["high"] - last["open"]) if last["close"] < last["open"] \
      else (last["high"] - last["close"])
    if uw > body * 2 and br > 0.2:
      return True, "Pin Bar (Зірка)"
  return False, "Немає підтвердження"


def detect_sfp(df: pd.DataFrame, direction: str) -> tuple[bool, str]:
  if len(df) < 15:
    return False, "Мало даних для SFP"
  highs, lows = find_swing_points(df, lookback=4, confirm=2)
  last = df.iloc[-1]
  body       = abs(last["close"] - last["open"])
  candle_rng = last["high"] - last["low"]
  if candle_rng < 1e-10:
    return False, "Нульовий діапазон SFP"
  MIN_WICK_RATIO = 0.45
  if direction == "bullish":
    if not lows:
      return False, "Немає swing lows для SFP"
    recent_lows = [l[1] for l in lows[-4:] if l[0] < df.index[-1]]
    if not recent_lows:
      return False, "Немає попередніх swing lows"
    for swing_low in sorted(recent_lows, reverse=True):
      if last["low"] < swing_low and last["close"] > swing_low:
        lower_wick = last["close"] - last["low"] if last["close"] > last["open"] \
          else last["open"] - last["low"]
        if lower_wick / candle_rng >= MIN_WICK_RATIO:
          pct_below = (swing_low - last["low"]) / swing_low * 100
          return True, f"✅ Bullish SFP: пробив low {swing_low:.4f} на {pct_below:.2f}%, закрився вище"
    return False, "SFP не підтверджено"
  else:
    if not highs:
      return False, "Немає swing highs для SFP"
    recent_highs = [h[1] for h in highs[-4:] if h[0] < df.index[-1]]
    if not recent_highs:
      return False, "Немає попередніх swing highs"
    for swing_high in sorted(recent_highs):
      if last["high"] > swing_high and last["close"] < swing_high:
        upper_wick = last["high"] - last["close"] if last["close"] < last["open"] \
          else last["high"] - last["open"]
        if upper_wick / candle_rng >= MIN_WICK_RATIO:
          pct_above = (last["high"] - swing_high) / swing_high * 100
          return True, f"✅ Bearish SFP: пробив high {swing_high:.4f} на {pct_above:.2f}%, закрився нижче"
    return False, "SFP не підтверджено"


def check_ote_zone(df: pd.DataFrame, direction: str,
                   ob_top: float, ob_bottom: float) -> tuple[bool, str, float | None]:
  if len(df) < 20:
    return False, "Мало даних для OTE", None
  highs, lows = find_swing_points(df, lookback=5, confirm=3)
  price = df["close"].iloc[-1]
  if direction == "bullish":
    if len(highs) < 1 or len(lows) < 1:
      return False, "Мало свінгів для OTE", None
    last_high_ts, last_high_val = highs[-1]
    prior_lows = [l for l in lows if l[0] < last_high_ts]
    if not prior_lows:
      return False, "Немає попереднього low для OTE", None
    swing_low_val  = prior_lows[-1][1]
    swing_high_val = last_high_val
    move_size      = swing_high_val - swing_low_val
    if move_size <= 0:
      return False, "Некоректний рух для OTE", None
    ote_top    = swing_high_val - move_size * OTE_LOW
    ote_bottom = swing_high_val - move_size * OTE_HIGH
    golden_pocket = swing_high_val - move_size * 0.705
    ob_mid = (ob_top + ob_bottom) / 2
    if ote_bottom <= price <= ote_top or ote_bottom <= ob_mid <= ote_top:
      fib_level = (swing_high_val - price) / move_size * 100
      return True, f"✅ OTE Bullish: Fib {fib_level:.1f}% | GP @ {golden_pocket:.4f}", golden_pocket
    fib_current = (swing_high_val - price) / move_size * 100
    return False, f"⚪ OTE поза зоною (Fib {fib_current:.1f}%, треба 61.8–78.6%)", None
  else:
    if len(highs) < 1 or len(lows) < 1:
      return False, "Мало свінгів для OTE", None
    last_low_ts, last_low_val = lows[-1]
    prior_highs = [h for h in highs if h[0] < last_low_ts]
    if not prior_highs:
      return False, "Немає попереднього high для OTE", None
    swing_high_val = prior_highs[-1][1]
    swing_low_val  = last_low_val
    move_size      = swing_high_val - swing_low_val
    if move_size <= 0:
      return False, "Некоректний рух для OTE", None
    ote_bottom    = swing_low_val + move_size * OTE_LOW
    ote_top       = swing_low_val + move_size * OTE_HIGH
    golden_pocket = swing_low_val + move_size * 0.705
    ob_mid = (ob_top + ob_bottom) / 2
    if ote_bottom <= price <= ote_top or ote_bottom <= ob_mid <= ote_top:
      fib_level = (price - swing_low_val) / move_size * 100
      return True, f"✅ OTE Bearish: Fib {fib_level:.1f}% | GP @ {golden_pocket:.4f}", golden_pocket
    fib_current = (price - swing_low_val) / move_size * 100
    return False, f"⚪ OTE поза зоною (Fib {fib_current:.1f}%, треба 61.8–78.6%)", None


# ─────────────────────────────────────────────
#  SMC АНАЛІЗ (без змін у логіці)
# ─────────────────────────────────────────────
def smc_analysis(df_daily: pd.DataFrame,
                 df_4h: pd.DataFrame,
                 df_15m: pd.DataFrame) -> tuple[str | None, int, list[str], float | None]:
  score  = 0
  detail = []
  limit_entry = None

  in_kz = is_in_killzone()
  effective_min = MIN_OPTIONAL_SCORE if in_kz else STRICT_MIN_SCORE
  kz_label = (
    f"✅ ICT Killzone активна (+2) [поріг: {effective_min}/13]"
    if in_kz else
    f"⏳ Поза KZ (+0) [суворий поріг: {effective_min}/13]"
  )

  trend = get_daily_trend(df_daily)
  if trend == "bull":
    direction = "bullish"; score += 3
    detail.append("📈 Денний тренд: BULLISH (+3) [gate]")
  elif trend == "bear":
    direction = "bearish"; score += 3
    detail.append("📉 Денний тренд: BEARISH (+3) [gate]")
  else:
    return None, 0, ["⛔ Бічний тренд — gate не пройдено"], None

  htf_bias = get_htf_bias(df_4h)
  if htf_bias == direction:
    score += 2
    detail.append("✅ 4H bias збігається (+2) [gate]")
  else:
    detail.append(f"❌ 4H bias: {htf_bias} проти {direction} — gate не пройдено")
    return None, 0, detail, None

  adx_val = calc_adx(df_4h).iloc[-1]
  if np.isnan(adx_val) or adx_val < MIN_ADX:
    return None, 0, [f"⛔ ADX 4H = {adx_val:.1f} < {MIN_ADX} — ranging market"], None
  detail.append(f"✅ ADX 4H = {adx_val:.1f} ≥ {MIN_ADX} [gate]")

  optional_score = 0
  if in_kz:
    optional_score += 2
  detail.append(kz_label)

  struct, ss, sd = detect_bos(df_4h)
  if struct == direction:
    optional_score += ss
    detail.append(f"✅ 4H {sd} (+{ss})")
  else:
    detail.append("⚪ 4H структура нейтральна (0)")

  pd_zone, pd_ratio = detect_pd_zone(df_4h)
  if direction == "bullish" and pd_zone == "discount":
    optional_score += 2; detail.append(f"✅ Discount Zone {pd_ratio:.0%} (+2)")
  elif direction == "bearish" and pd_zone == "premium":
    optional_score += 2; detail.append(f"✅ Premium Zone {pd_ratio:.0%} (+2)")
  elif pd_zone == "equilibrium":
    optional_score += 1; detail.append(f"➡️ Equilibrium {pd_ratio:.0%} (+1)")
  else:
    optional_score -= 1; detail.append("❌ Зона проти тренду (-1)")

  price    = df_15m["close"].iloc[-1]
  ob_zones = find_ob_with_fvg(df_15m, direction)
  ob_found = False
  ob_top_found = ob_bottom_found = 0.0

  for zone in reversed(ob_zones):
    dist = abs(price - zone["entry"]) / (price + 1e-10)
    if dist > 0.03:
      continue
    if zone["ob_bottom"] * 0.998 <= price <= zone["ob_top"] * 1.002:
      optional_score += 4  # Було 3. Робимо це головним критерієм
      detail.append("✅ OB+FVG: Ціна всередині свіжої зони (+4)")
      limit_entry = zone["entry"]
      ob_top_found, ob_bottom_found = zone["ob_top"], zone["ob_bottom"]
      ob_found = True; break
    elif direction == "bullish" and price > zone["ob_top"]:
      optional_score += 1
      detail.append("⏳ OB+FVG нижче поточної ціни (+1)")
      limit_entry = zone["entry"]
      ob_top_found, ob_bottom_found = zone["ob_top"], zone["ob_bottom"]
      ob_found = True; break
    elif direction == "bearish" and price < zone["ob_bottom"]:
      optional_score += 1
      detail.append("⏳ OB+FVG вище поточної ціни (+1)")
      limit_entry = zone["entry"]
      ob_top_found, ob_bottom_found = zone["ob_top"], zone["ob_bottom"]
      ob_found = True; break

  if not ob_found:
    detail.append("⚪ OB+FVG не знайдено (0)")
    atr_buf = calc_atr(df_15m, ATR_PERIOD).iloc[-1]
    ob_top_found    = price + atr_buf
    ob_bottom_found = price - atr_buf

  sfp_found, sfp_desc = detect_sfp(df_15m, direction)
  if sfp_found:
    optional_score += 4; detail.append(f"{sfp_desc} (+2)")
  else:
    detail.append(f"⚪ SFP: {sfp_desc} (0)")

  in_ote, ote_desc, ote_entry = check_ote_zone(
    df_15m, direction, ob_top_found, ob_bottom_found
  )
  if in_ote:
    optional_score += 3; detail.append(f"{ote_desc} (+2)")
    if ote_entry is not None:
      limit_entry = ote_entry
  else:
    detail.append(f"{ote_desc} (0)")

  liq, liq_found = find_liquidity(df_4h, direction)
  if liq_found:
    optional_score += 1; detail.append(f"✅ Ліквідність @ {liq:.4f} (+1)")

  rsi = calc_rsi(df_15m).iloc[-1]
  if direction == "bullish" and 30 <= rsi <= 60:
    optional_score += 1; detail.append(f"✅ RSI {rsi:.1f} (+1)")
  elif direction == "bearish" and 40 <= rsi <= 70:
    optional_score += 1; detail.append(f"✅ RSI {rsi:.1f} (+1)")
  else:
    detail.append(f"⚪ RSI {rsi:.1f} (0)")

  if volume_ok(df_15m):
    optional_score += 1; detail.append("✅ Підвищений об'єм (+1)")
  else:
    detail.append("⚪ Звичайний об'єм (0)")

  candle_ok, candle_name = check_candle_confirmation(df_15m, direction)
  if candle_ok:
    optional_score += 1; detail.append(f"✅ {candle_name} (+1)")
  else:
    detail.append(f"⚪ {candle_name} (0)")

  score += optional_score
  min_total = 5 + effective_min
  detail.append(
    f"\n📊 Бал: {score} | gate=5 + optional={optional_score}/13 | "
    f"поріг={min_total} ({'KZ' if in_kz else 'non-KZ'})"
  )

  if optional_score >= effective_min:
    signal = "BUY" if direction == "bullish" else "SELL"
  else:
    signal = None
    detail.append(f"⛔ Бал {optional_score} < {effective_min} — сигнал відхилено")

  return signal, score, detail, limit_entry


# ─────────────────────────────────────────────
#  SL / TP РОЗРАХУНОК
# ─────────────────────────────────────────────
def calc_sl_from_structure(df_15m: pd.DataFrame, atr_4h: float,
                           direction: str, entry_price: float) -> float:
  # У SMC стоп ставиться строго за структурний екстремум з мінімальним буфером
  highs, lows = find_swing_points(df_15m, lookback=5, confirm=2)

  # Використовуємо буфер 0.15% (від хибних проколів спредом), замість ATR
  buffer_pct = 0.0015

  if direction == "bullish":
    relevant_lows = [l[1] for l in lows if l[1] < entry_price]
    if relevant_lows:
      structure_sl = max(relevant_lows) # Найближчий Swing Low нижче входу
    else:
      structure_sl = df_15m['low'].iloc[-10:].min() # Локальний мінімум

    return structure_sl * (1 - buffer_pct)
  else:
    relevant_highs = [h[1] for h in highs if h[1] > entry_price]
    if relevant_highs:
      structure_sl = min(relevant_highs) # Найближчий Swing High вище входу
    else:
      structure_sl = df_15m['high'].iloc[-10:].max()

    return structure_sl * (1 + buffer_pct)


def calc_position(symbol: str, balance: float, price: float,
                  atr_4h: float, atr_15m: float, side: str,
                  df_15m: pd.DataFrame) -> tuple[float, float, list[float]]:
  info    = _get_info(symbol)
  min_not = info.get("min_notional", 5.0)

  direction = "bullish" if side == "BUY" else "bearish"
  raw_sl    = calc_sl_from_structure(df_15m, atr_4h, direction, price)
  sl_pct    = abs(price - raw_sl) / price
  if sl_pct > MAX_SL_PCT:
    return 0.0, 0.0, []
  sl_dist = abs(price - raw_sl)
  if sl_dist <= 0:
    sl_dist = atr_4h * ATR_SL_MULT

  risk_usdt = balance * (RISK_PCT / 100.0)
  raw_qty   = risk_usdt / sl_dist

  max_qty_by_margin = (balance * (MARGIN_PCT / 100.0) * LEVERAGE) / price
  if raw_qty > max_qty_by_margin:
    raw_qty   = max_qty_by_margin
    risk_usdt = raw_qty * sl_dist

  max_qty_by_margin = (balance * 0.90 * LEVERAGE) / price
  if raw_qty > max_qty_by_margin:
    raw_qty = max_qty_by_margin

  max_qty_exchange = info.get("max_qty", 1e9)
  if raw_qty > max_qty_exchange:
    raw_qty = max_qty_exchange

  min_qty_by_notional = (min_not * 1.1) / price
  if raw_qty < min_qty_by_notional:
    raw_qty     = min_qty_by_notional
    actual_risk = raw_qty * sl_dist
    if actual_risk > balance * 0.10:
      return 0.0, 0.0, []

  qty = qty_float(symbol, raw_qty)
  if qty <= 0:
    return 0.0, 0.0, []

  needed_margin = qty * price / LEVERAGE
  if needed_margin > balance * 0.95:
    return 0.0, 0.0, []

  sl = price_float(symbol, raw_sl)
  tps = [
    price_float(symbol, price + sl_dist * rr if side == "BUY" else price - sl_dist * rr)
    for rr in TP_RR_RATIOS
  ]
  return qty, sl, tps


def atr_from_tps(tps: list[float], entry: float) -> float:
  if tps and TP_RR_RATIOS[0] != 0:
    return abs(tps[0] - entry) / TP_RR_RATIOS[0]
  return entry * 0.01


# ─────────────────────────────────────────────
#  ВИСТАВЛЕННЯ SL ЧЕРЕЗ WS API
# ─────────────────────────────────────────────
async def attach_sl_only(ws: BinanceWsTrading,
                         symbol: str, signal: str,
                         qty: float, sl: float) -> tuple[str, bool]:
  close_side = "SELL" if signal == "BUY" else "BUY"
  for attempt in range(3):
    sl_ord = await ws.place_sl_market_order(symbol, close_side, qty, sl)
    if _is_ok(sl_ord):
      sl_id = str(sl_ord["orderId"])
      log.info(f"✅ SL {symbol}: orderId={sl_id} stopPrice={sl}")
      return sl_id, True
    log.warning(f"⚠️ SL attempt {attempt+1}/3 {symbol}: {_api_err(sl_ord)}")
    await asyncio.sleep(0.5)
  return "software", False


# ─────────────────────────────────────────────
#  ФОРМАТУВАННЯ ПОВІДОМЛЕНЬ
# ─────────────────────────────────────────────
def fmt_signal_msg(symbol: str, signal: str, score: int,
                   detail: list[str], price: float, sl: float,
                   tps: list[float], qty: float, balance: float,
                   is_limit: bool, atr_4h: float) -> str:
  emoji    = "🟢" if signal == "BUY" else "🔴"
  risk     = balance * (RISK_PCT / 100)
  sl_dist  = abs(price - sl)
  rr_str   = " | ".join([f"1:{rr:.0f}" for rr in TP_RR_RATIOS])
  safe_det = [html_lib.escape(d) for d in detail]
  dstr     = "\n".join(f"  • {d}" for d in safe_det)
  tps_str  = " | ".join([fmt_price(symbol, t) for t in tps])
  mode     = "LIMIT" if is_limit else "MARKET"
  kz_mark  = "🔥 KZ" if is_in_killzone() else f"⏳ non-KZ (поріг {STRICT_MIN_SCORE})"
  ws_tag   = "🔌 WS API"
  return (
    f"{emoji} <b>SMC СИГНАЛ v11 — {symbol}</b>\n{'─'*34}\n"
    f"<b>Напрям:</b> {'LONG 📈' if signal=='BUY' else 'SHORT 📉'} | {score} балів | {mode} | {kz_mark} | {ws_tag}\n"
    f"<b>Вхід:</b> <code>{fmt_price(symbol, price)}</code>\n"
    f"<b>SL:</b> <code>{fmt_price(symbol, sl)}</code> "
    f"(dist={sl_dist:.4f} | ATR4H={atr_4h:.4f})\n"
    f"<b>TP (1/2/3):</b> <code>{tps_str}</code> [{rr_str}] [software]\n"
    f"<b>Кількість:</b> {fmt_qty(symbol, qty)}\n"
    f"<b>Ризик:</b> ~{risk:.2f} USDT ({RISK_PCT}%)\n"
    f"<b>Сесія:</b> {session_status()}\n"
    f"{'─'*34}\n<b>Аналіз:</b>\n{dstr}\n"
  )


def fmt_open_msg(symbol: str, signal: str, price: float, sl: float,
                 tps: list[float], qty: float, order_id: str,
                 sl_id: str, otype: str = "MARKET") -> str:
  emoji   = "🟢" if signal == "BUY" else "🔴"
  tps_str = " | ".join([fmt_price(symbol, t) for t in tps])
  sl_ok   = ("✅" if sl_id and sl_id != "software"
             else ("🛡 software" if sl_id == "software" else "❌"))
  vols_str = " | ".join([f"{int(v*100)}%" for v in TP_VOLUMES])
  return (
    f"{emoji} <b>ПОЗИЦІЯ ВІДКРИТА — {symbol}</b>\n{'─'*34}\n"
    f"<b>Тип:</b> {'LONG 📈' if signal=='BUY' else 'SHORT 📉'} [{otype}] 🔌 WS API\n"
    f"<b>Вхід:</b> <code>{fmt_price(symbol, price)}</code>\n"
    f"<b>SL:</b> {sl_ok} <code>{fmt_price(symbol, sl)}</code>\n"
    f"<b>TP (1/2/3):</b> <code>{tps_str}</code> [{vols_str}] [software TP]\n"
    f"<b>Кількість:</b> {fmt_qty(symbol, qty)}\n"
    f"<b>OrderID:</b> <code>{order_id}</code>\n"
  )


# ─────────────────────────────────────────────
#  ВІДКРИТТЯ ПОЗИЦІЇ (через WS API)
# ─────────────────────────────────────────────
async def open_position(ws: BinanceWsTrading, rest: BinanceRestClient,
                        bot: Bot, symbol: str, signal: str,
                        entry_price: float, sl: float,
                        tps: list[float], qty: float,
                        use_limit: bool = False) -> None:
  async with _position_lock:
    if symbol in bot_state["open_positions"] or symbol in bot_state["pending_limits"]:
      return
    total_open = len(bot_state["open_positions"]) + len(bot_state["pending_limits"])
    if total_open >= MAX_POSITIONS:
      return

    try:
      await ws.set_leverage(symbol, LEVERAGE)
      await ws.set_margin_type(symbol, "ISOLATED")

      side = "BUY" if signal == "BUY" else "SELL"

      if use_limit:
        order = await ws.place_limit_order(symbol, side, qty, entry_price)
        if not _is_ok(order):
          raise Exception(f"Limit order rejected: {_api_err(order)}")
        order_id = str(order["orderId"])
        pending  = {
          "symbol": symbol, "signal": signal, "order_id": order_id,
          "entry_price": entry_price, "sl": sl, "tps": tps, "qty": qty,
          "created_at": datetime.now(timezone.utc).isoformat()
        }
        bot_state["pending_limits"][symbol] = pending
        await db_save_pending(pending)
        await safe_send(bot,
                        f"⏳ <b>LIMIT {signal} {symbol}</b> @ "
                        f"<code>{fmt_price(symbol, entry_price)}</code> виставлено (WS API)"
                        )
        return

      order = await ws.place_market_order(symbol, side, qty)
      if not _is_ok(order):
        raise Exception(f"Market order rejected: {_api_err(order)}")

      order_id   = str(order["orderId"])
      real_entry = float(order.get("avgPrice") or 0)
      if real_entry == 0:
        await asyncio.sleep(0.8)
        # Fallback — REST user trades
        trades = await rest.get_user_trades(symbol, limit=5)
        if trades:
          for t in reversed(trades):
            if str(t.get("orderId")) == order_id:
              real_entry = float(t["price"])
              break
      if real_entry == 0:
        real_entry = await rest.get_price(symbol)
      if real_entry == 0:
        real_entry = entry_price

      exec_qty = float(order.get("executedQty") or qty)
      if exec_qty == 0:
        exec_qty = qty

      sl_dist  = abs(real_entry - sl) if sl != 0 else atr_from_tps(tps, entry_price)
      real_sl  = price_float(symbol,
                             real_entry - sl_dist if signal == "BUY" else real_entry + sl_dist
                             )
      real_tps = [
        price_float(symbol,
                    real_entry + sl_dist * rr if signal == "BUY"
                    else real_entry - sl_dist * rr
                    )
        for rr in TP_RR_RATIOS
      ]

      sl_id, sl_placed = await attach_sl_only(ws, symbol, signal, exec_qty, real_sl)

      if not sl_placed and sl_id != "software":
        log.error(f"❌ SL не виставлено для {symbol}. Закриваємо.")
        await ws.place_market_order(symbol,
                                    "SELL" if signal == "BUY" else "BUY",
                                    exec_qty, reduce_only=True
                                    )
        await safe_send(bot, f"🚨 <b>{symbol}</b> — SL не виставлено! Позицію закрито.")
        return

      pos = {
        "symbol":       symbol,
        "signal":       signal,
        "entry_price":  real_entry,
        "sl":           real_sl,
        "tps":          real_tps,
        "qty":          exec_qty,
        "original_qty": exec_qty,
        "order_id":     order_id,
        "sl_order_id":  sl_id,
        "tp_order_ids": [],
        "open_time":    datetime.now(timezone.utc),
        "sl_state":     0,
        "sw_tp_hit":    0,
        "sw_closed_qty": 0.0,
      }
      bot_state["open_positions"][symbol] = pos
      await db_save_position(pos)

      await safe_send(bot, fmt_open_msg(
        symbol, signal, real_entry, real_sl, real_tps,
        exec_qty, order_id, sl_id
      ))

    except Exception as e:
      log.error(f"open_position {symbol}: {e}")
      await safe_send(bot, f"⚠️ Помилка відкриття <b>{symbol}</b>:\n<code>{e}</code>")


# ─────────────────────────────────────────────
#  ПЕРЕВІРКА PENDING LIMIT ОРДЕРІВ (через WS API)
# ─────────────────────────────────────────────
async def check_pending_limits(ws: BinanceWsTrading, rest: BinanceRestClient,
                               bot: Bot) -> None:
  for symbol in list(bot_state["pending_limits"].keys()):
    p = bot_state["pending_limits"][symbol]

    # Реактивний заповнення через User Data Stream
    if p.get("_ws_filled"):
      real_entry = p.get("_ws_price") or p["entry_price"]
      exec_qty   = p.get("_ws_qty")   or p["qty"]
      status = "FILLED"
    else:
      age_h = (datetime.now(timezone.utc) -
               datetime.fromisoformat(p["created_at"])).total_seconds() / 3600
      if age_h > 4:
        await ws.cancel_all_orders(symbol)
        async with _position_lock:
          bot_state["pending_limits"].pop(symbol, None)
        await db_delete_pending(symbol)
        await safe_send(bot, f"⏰ Ліміт <b>{symbol}</b> скасовано (таймаут 4 год)")
        continue

      try:
        order_info = await ws.get_order(symbol, p["order_id"])
      except Exception:
        continue
      status = order_info.get("status", "")
      if status not in ("FILLED", "PARTIALLY_FILLED"):
        if status in ("CANCELED", "EXPIRED", "REJECTED"):
          async with _position_lock:
            bot_state["pending_limits"].pop(symbol, None)
          await db_delete_pending(symbol)
        continue
      real_entry = float(order_info.get("avgPrice") or 0) or p["entry_price"]
      exec_qty   = float(order_info.get("executedQty") or p["qty"]) or p["qty"]

    if exec_qty == 0:
      continue

    sl_dist  = abs(p["entry_price"] - p["sl"]) if p.get("sl") else atr_from_tps(p["tps"], p["entry_price"])
    real_sl  = price_float(symbol,
                           real_entry - sl_dist if p["signal"] == "BUY" else real_entry + sl_dist
                           )
    real_tps = [
      price_float(symbol,
                  real_entry + sl_dist * rr if p["signal"] == "BUY"
                  else real_entry - sl_dist * rr
                  )
      for rr in TP_RR_RATIOS
    ]

    sl_id, sl_placed = await attach_sl_only(ws, symbol, p["signal"], exec_qty, real_sl)

    if not sl_placed and sl_id != "software":
      log.error(f"❌ SL не виставлено для {symbol} після fill. Закриваємо.")
      await ws.place_market_order(symbol,
                                  "SELL" if p["signal"] == "BUY" else "BUY",
                                  exec_qty, reduce_only=True
                                  )
      async with _position_lock:
        bot_state["pending_limits"].pop(symbol, None)
      await db_delete_pending(symbol)
      await safe_send(bot, f"🚨 <b>{symbol}</b> LIMIT заповнений, але SL не виставлено. Закрито.")
      continue

    pos = {
      "symbol": symbol, "signal": p["signal"],
      "entry_price": real_entry, "sl": real_sl,
      "tps": real_tps, "qty": exec_qty, "original_qty": exec_qty,
      "order_id": p["order_id"], "sl_order_id": sl_id,
      "tp_order_ids": [],
      "open_time": datetime.now(timezone.utc), "sl_state": 0,
      "sw_tp_hit": 0, "sw_closed_qty": 0.0,
    }
    async with _position_lock:
      bot_state["open_positions"][symbol] = pos
      bot_state["pending_limits"].pop(symbol, None)
    await db_save_position(pos)
    await db_delete_pending(symbol)
    await safe_send(bot, fmt_open_msg(
      symbol, p["signal"], real_entry, real_sl, real_tps,
      exec_qty, p["order_id"], sl_id, "LIMIT"
    ))


# ─────────────────────────────────────────────
#  МОНІТОРИНГ ВІДКРИТИХ ПОЗИЦІЙ (Software TP)
#  WS-3 обробляє SL реактивно.
#  Цей цикл тільки для Software TP management.
# ─────────────────────────────────────────────
async def check_open_positions(ws: BinanceWsTrading, rest: BinanceRestClient,
                               bot: Bot) -> None:
  if not bot_state["open_positions"]:
    return

  # Отримуємо active_positions з WS кешу (ACCOUNT_UPDATE)
  # Fallback до REST якщо WS не готовий
  if bot_state.get("ws_userdata_ready") and bot_state["active_positions"]:
    active = dict(bot_state["active_positions"])
  else:
    try:
      positions = await ws.get_position_risk()
      active = {
        p["symbol"]: abs(float(p["positionAmt"]))
        for p in positions
        if isinstance(p, dict) and abs(float(p.get("positionAmt", 0))) > 0
      }
    except Exception as e:
      log.warning(f"get_position_risk: {e}")
      return

  for symbol in list(bot_state["open_positions"].keys()):
    pos = bot_state["open_positions"].get(symbol)
    if not pos:
      continue

    # ── Позиція повністю закрита (SL вже оброблено User Data Stream) ──
    # Але перевіряємо ще раз для надійності
    if symbol not in active:
      # Якщо UserData WS готовий і нема позиції — вже оброблено в _handle_order_update
      if bot_state.get("ws_userdata_ready"):
        continue
      # Інакше — обробляємо тут (fallback)
      try:
        fallback = await rest.get_price(symbol)
      except Exception:
        fallback = pos["entry_price"]
      exit_p, pnl, reason = await get_real_pnl(rest, symbol, pos, fallback)
      await ws.cancel_all_orders(symbol)
      async with _position_lock:
        bot_state["open_positions"].pop(symbol, None)
      await db_delete_position(symbol)
      bot_state["total_pnl"] += pnl
      bot_state["daily_pnl"] += pnl
      await db_save_daily_state(bot_state["daily_date"], bot_state["daily_pnl"])
      await db_save_trade({
        "symbol": symbol, "signal": pos["signal"],
        "entry": pos["entry_price"], "exit": exit_p,
        "pnl": pnl, "reason": reason,
        "time": datetime.now(timezone.utc)
      })
      emoji    = "✅" if pnl >= 0 else "❌"
      pnl_sign = "+" if pnl >= 0 else ""
      await safe_send(bot,
                      f"{emoji} <b>ЗАКРИТО — {symbol}</b>\n"
                      f"PnL: <b>{pnl_sign}{pnl:.4f} USDT</b> | {reason}\n"
                      f"Вхід: {pos['entry_price']} → Вихід: {exit_p:.4f}"
                      )
      continue

    # ── Software TP перевірка ───────────────────────────────────
    try:
      mark_price   = await rest.get_price(symbol)
      signal_sw    = pos["signal"]
      tps          = pos.get("tps", [])
      original_qty = pos.get("original_qty", pos["qty"])
      sw_tp_hit    = pos.get("sw_tp_hit", 0)
      active_qty   = active[symbol]
      close_side   = "SELL" if signal_sw == "BUY" else "BUY"

      if sw_tp_hit < len(tps):
        tp_price = tps[sw_tp_hit]
        tp_hit = (
          (signal_sw == "BUY"  and mark_price >= tp_price) or
          (signal_sw == "SELL" and mark_price <= tp_price)
        )

        if tp_hit:
          tp_idx  = sw_tp_hit
          if tp_idx < len(tps) - 1:
            tp_qty_raw = original_qty * TP_VOLUMES[tp_idx]
            tp_qty     = min(qty_float(symbol, tp_qty_raw), active_qty)
          else:
            tp_qty = active_qty

          if tp_qty > 0:
            close_ord = await ws.place_market_order(
              symbol, close_side, tp_qty, reduce_only=True
            )
            if _is_ok(close_ord):
              tp_pnl = (
                         (mark_price - pos["entry_price"]) if signal_sw == "BUY"
                         else (pos["entry_price"] - mark_price)
                       ) * tp_qty

              pos["sw_tp_hit"]     = tp_idx + 1
              pos["sw_closed_qty"] = pos.get("sw_closed_qty", 0.0) + tp_qty
              pos["qty"]           = active_qty - tp_qty

              new_sl_price = None
              if tp_idx == 0:
                new_sl_price = pos["entry_price"]
                sl_msg = f"SL → беззбиток: <code>{new_sl_price}</code>"
              elif tp_idx == 1:
                new_sl_price = tps[0]
                sl_msg = f"SL → TP1: <code>{new_sl_price}</code>"

              if new_sl_price is not None:
                remaining_qty = active_qty - tp_qty
                if remaining_qty > 0:
                  old_sl_id = pos.get("sl_order_id", "")
                  if old_sl_id and old_sl_id != "software":
                    try:
                      await ws.cancel_order(symbol, old_sl_id)
                    except Exception:
                      pass
                  else:
                    try:
                      open_orders = await ws.get_open_orders(symbol)
                      for o in open_orders:
                        if o.get("type") in ("STOP_MARKET", "STOP") and o.get("reduceOnly"):
                          await ws.cancel_order(symbol, str(o["orderId"]))
                    except Exception:
                      pass
                  new_sl_ord = await ws.place_sl_market_order(
                    symbol, close_side, remaining_qty,
                    price_float(symbol, new_sl_price)
                  )
                  if _is_ok(new_sl_ord):
                    pos["sl"]          = price_float(symbol, new_sl_price)
                    pos["sl_order_id"] = str(new_sl_ord["orderId"])

              await db_save_position(pos)

              if tp_idx == len(tps) - 1:
                await ws.cancel_all_orders(symbol)
                total_pnl = (
                              (mark_price - pos["entry_price"]) if signal_sw == "BUY"
                              else (pos["entry_price"] - mark_price)
                            ) * original_qty
                async with _position_lock:
                  bot_state["open_positions"].pop(symbol, None)
                await db_delete_position(symbol)
                bot_state["total_pnl"] += total_pnl
                bot_state["daily_pnl"] += total_pnl
                await db_save_daily_state(bot_state["daily_date"], bot_state["daily_pnl"])
                await db_save_trade({
                  "symbol": symbol, "signal": signal_sw,
                  "entry": pos["entry_price"], "exit": mark_price,
                  "pnl": total_pnl, "reason": "TP3 ✅ (software)",
                  "time": datetime.now(timezone.utc)
                })
                pnl_sign = "+" if total_pnl >= 0 else ""
                await safe_send(bot,
                                f"🎯 <b>TP3 ПОВНІСТЮ — {symbol}</b>\n"
                                f"PnL: <b>{pnl_sign}{total_pnl:.4f} USDT</b> ✅\n"
                                f"Вхід: {pos['entry_price']} → {mark_price:.4f}"
                                )
                continue

              await safe_send(bot,
                              f"💰 <b>TP{tp_idx+1} — {symbol}</b>\n"
                              f"Закрито {TP_VOLUMES[tp_idx]:.0%} @ <code>{mark_price:.4f}</code>\n"
                              f"PnL частини: <b>+{tp_pnl:.4f} USDT</b>\n"
                              f"{'🔒 ' + sl_msg if new_sl_price else ''}"
                              )
    except Exception as e:
      log.warning(f"Software TP check {symbol}: {e}")

    if symbol not in bot_state["open_positions"]:
      continue

    pos        = bot_state["open_positions"][symbol]
    active_qty = active.get(symbol, 0)

    # ── SL відновлення ──────────────────────────────────────────
    if active_qty > 0:
      sl_id = pos.get("sl_order_id", "")
      if sl_id and sl_id != "software":
        try:
          open_orders = await ws.get_open_orders(symbol)
          sl_exists   = any(
            str(o.get("orderId")) == sl_id for o in open_orders
          )
          if not sl_exists:
            close_side = "SELL" if pos["signal"] == "BUY" else "BUY"
            new_sl_ord = await ws.place_sl_market_order(
              symbol, close_side, active_qty, pos["sl"]
            )
            if _is_ok(new_sl_ord):
              pos["sl_order_id"] = str(new_sl_ord["orderId"])
              pos["qty"]         = active_qty
              await db_save_position(pos)
              log.info(f"🔄 SL відновлено {symbol} @ {pos['sl']}")
              await safe_send(bot,
                              f"🔄 <b>{symbol}</b>: SL відновлено @ "
                              f"<code>{pos['sl']}</code>"
                              )
        except Exception as e:
          log.warning(f"SL check {symbol}: {e}")


# ─────────────────────────────────────────────
#  ІНІЦІАЛІЗАЦІЯ КЕШУ KLINES (REST → Cache)
# ─────────────────────────────────────────────
async def init_kline_cache(rest: BinanceRestClient) -> None:
  """
  Завантажує початкові свічки для всіх символів і TF через REST.
  Після цього оновлення приходять через Market Stream WS.
  """
  log.info(f"📥 Завантаження kline кешу для {len(SYMBOLS)} символів × 3 TF...")
  tf_list = [TREND_TF, HIGHER_TF, LOWER_TF]

  for symbol in SYMBOLS:
    for tf in tf_list:
      try:
        raw = await rest.get_klines(symbol, tf, limit=KLINE_CACHE_SIZE)
        if raw and isinstance(raw, list):
          async with _kline_cache_lock:
            cache = bot_state["kline_cache"][symbol][tf]
            for row in raw:
              cache.append([
                int(row[0]), row[1], row[2], row[3], row[4], row[5],
                int(row[6]), row[7], int(row[8]), row[9], row[10], row[11]
              ])
        await asyncio.sleep(0.05)  # Throttle REST
      except Exception as e:
        log.warning(f"init_kline_cache {symbol} {tf}: {e}")

  log.info("✅ Kline кеш ініціалізовано")


# ─────────────────────────────────────────────
#  ЗАВАНТАЖЕННЯ ПРАВИЛ БІРЖІ
# ─────────────────────────────────────────────
async def load_exchange_rules(rest: BinanceRestClient) -> None:
  try:
    info = await rest.get_exchange_info()
    for s in info.get("symbols", []):
      if s["symbol"] not in SYMBOLS:
        continue
      tick = float(next(
        f["tickSize"] for f in s["filters"] if f["filterType"] == "PRICE_FILTER"
      ))
      lot_filter = next(
        (f for f in s["filters"] if f["filterType"] == "LOT_SIZE"), {}
      )
      step    = float(lot_filter.get("stepSize", 0.001))
      max_qty = float(lot_filter.get("maxQty", 1e9))
      min_not = float(next(
        (f.get("notional", 5.0) for f in s["filters"]
         if f["filterType"] == "MIN_NOTIONAL"), 5.0
      ))
      bot_state["exchange_info"][s["symbol"]] = {
        "tick": tick, "step": step, "min_notional": min_not, "max_qty": max_qty
      }
    log.info(f"✅ Правила завантажено для {len(bot_state['exchange_info'])} пар")
  except Exception as e:
    log.error(f"❌ exchangeInfo помилка: {e}")


# ─────────────────────────────────────────────
#  СКАНУВАННЯ РИНКІВ (використовує WS кеш)
# ─────────────────────────────────────────────
async def scan_markets(ws: BinanceWsTrading, rest: BinanceRestClient,
                       bot: Bot,
                       market_stream: BinanceMarketStream) -> None:
  bot_state["scan_count"] += 1

  if not is_active_session():
    log.info("💤 Поза сесією (7–20 UTC) — сканування пропущено")
    return

  today = datetime.now(timezone.utc).date()
  if bot_state["daily_date"] != today:
    bot_state["daily_date"] = today
    bot_state["daily_pnl"]  = 0.0
    await db_save_daily_state(today, 0.0)

  balance = await get_available_balance(rest)
  if balance <= 0:
    log.warning("Нульовий баланс — скіп скану")
    return

  daily_pct = (bot_state["daily_pnl"] / balance) * 100
  if daily_pct <= DAILY_LOSS_LIMIT:
    log.warning(f"⚠️ Денний ліміт збитків: {daily_pct:.2f}% — торгівля зупинена")
    return

  total_open = len(bot_state["open_positions"]) + len(bot_state["pending_limits"])
  if total_open >= MAX_POSITIONS:
    return

  in_kz = is_in_killzone()
  ws_status = "✅" if bot_state["ws_trading_ready"] else "❌"
  ms_status = "✅" if bot_state["ws_market_ready"] else "❌"
  log.info(
    f"🔍 Скан #{bot_state['scan_count']} | "
    f"{'KZ 🔥' if in_kz else f'non-KZ (поріг {STRICT_MIN_SCORE})'} | "
    f"Trading WS: {ws_status} | Market WS: {ms_status}"
  )

  for symbol in SYMBOLS:
    if symbol in bot_state["open_positions"] or symbol in bot_state["pending_limits"]:
      continue
    total_open = len(bot_state["open_positions"]) + len(bot_state["pending_limits"])
    if total_open >= MAX_POSITIONS:
      break

    try:
      # ── Використовуємо WS кеш замість REST ──────────────────
      async with _kline_cache_lock:
        df_d  = cache_to_df(symbol, TREND_TF,  market_stream)
        df_4h = cache_to_df(symbol, HIGHER_TF, market_stream)
        df_15 = cache_to_df(symbol, LOWER_TF,  market_stream)

      # Fallback до REST якщо кеш порожній
      if len(df_d) < 50 or len(df_4h) < 50 or len(df_15) < 50:
        log.debug(f"{symbol}: кеш неповний, завантажуємо REST")
        raw_d, raw_4h, raw_15 = await asyncio.gather(
          rest.get_klines(symbol, TREND_TF,  limit=210),
          rest.get_klines(symbol, HIGHER_TF, limit=210),
          rest.get_klines(symbol, LOWER_TF,  limit=210),
        )
        df_d  = klines_to_df(raw_d)
        df_4h = klines_to_df(raw_4h)
        df_15 = klines_to_df(raw_15)
        if len(df_d) < 50 or len(df_4h) < 50 or len(df_15) < 50:
          continue

      price   = df_15["close"].iloc[-1]
      atr_15m = calc_atr(df_15, ATR_PERIOD).iloc[-1]
      atr_4h  = calc_atr(df_4h, ATR_PERIOD).iloc[-1]

      if atr_15m <= 0 or np.isnan(atr_15m) or atr_4h <= 0 or np.isnan(atr_4h):
        continue

      atr_15m_pct = atr_15m / price
      atr_4h_pct  = atr_4h  / price
      if atr_15m_pct < MIN_ATR_PCT or atr_4h_pct > MAX_ATR_PCT:
        continue

      signal, score, detail, limit_entry = smc_analysis(df_d, df_4h, df_15)
      if not signal:
        continue

      entry_calc = limit_entry if limit_entry else price

      balance = await get_available_balance(rest)
      if balance <= 0:
        break

      qty, sl, tps = calc_position(
        symbol, balance, entry_calc, atr_4h, atr_15m, signal, df_15
      )
      if qty <= 0:
        continue

      if not check_correlation(symbol, signal):
        continue

      await safe_send(bot, fmt_signal_msg(
        symbol, signal, score, detail, entry_calc,
        sl, tps, qty, balance,
        limit_entry is not None, atr_4h
      ))

      await open_position(
        ws, rest, bot, symbol, signal,
        entry_calc, sl, tps, qty,
        use_limit=(limit_entry is not None)
      )
      await asyncio.sleep(1.5)

    except Exception as e:
      log.error(f"{symbol} scan error: {e}")


# ─────────────────────────────────────────────
#  ГОЛОВНИЙ ЦИКЛ
# ─────────────────────────────────────────────
async def scanner_loop(ws: BinanceWsTrading, rest: BinanceRestClient,
                       bot: Bot, market_stream: BinanceMarketStream) -> None:
  await load_exchange_rules(rest)
  await init_kline_cache(rest)

  bot_state["open_positions"] = await db_load_positions()
  bot_state["pending_limits"] = await db_load_pending()
  bot_state["daily_date"], bot_state["daily_pnl"] = await db_load_daily_state()

  open_c    = len(bot_state["open_positions"])
  pending_c = len(bot_state["pending_limits"])

  kz_str = " | ".join(f"{s:02d}:00–{e:02d}:00" for s, e in ICT_KILLZONES)
  await safe_send(bot,
                  f"🚀 <b>SMC Bot v11 (WebSocket) запущено!</b>\n"
                  f"Ризик: {RISK_PCT}% | Leverage: {LEVERAGE}x | TP: {TP_RR_RATIOS} R\n"
                  f"🔌 Trading: WS API | 📡 Market: WS Stream | 🔔 User Data: WS Stream\n"
                  f"🕐 ICT Killzones: {kz_str} UTC (бонус +2 бали)\n"
                  f"📊 Поріг у KZ: {MIN_OPTIONAL_SCORE}/13 | Поза KZ: {STRICT_MIN_SCORE}/13\n"
                  f"OB mitigation: {OB_MITIGATION_THRESHOLD:.0%}\n"
                  f"Відновлено: {open_c} позицій, {pending_c} pending"
                  )

  last_scan = 0.0
  while True:
    if bot_state["running"]:
      try:
        await check_pending_limits(ws, rest, bot)
        await check_open_positions(ws, rest, bot)
        now = time.monotonic()
        if now - last_scan >= SCAN_INTERVAL_SEC:
          await scan_markets(ws, rest, bot, market_stream)
          last_scan = time.monotonic()
      except Exception as e:
        log.error(f"Loop error: {e}")
    await asyncio.sleep(10)


# ─────────────────────────────────────────────
#  TELEGRAM КОМАНДИ
# ─────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
  keyboard = [
    [InlineKeyboardButton("▶️ Запустити", callback_data="start_bot"),
     InlineKeyboardButton("⏹ Зупинити",  callback_data="stop_bot")],
    [InlineKeyboardButton("📊 Статус",    callback_data="status"),
     InlineKeyboardButton("💰 Баланс",    callback_data="balance")],
    [InlineKeyboardButton("📜 Позиції",   callback_data="positions"),
     InlineKeyboardButton("📈 Історія",   callback_data="history")],
    [InlineKeyboardButton("🔌 WS Статус", callback_data="ws_status")],
  ]
  kz_str = " | ".join(f"{s:02d}:00–{e:02d}:00" for s, e in ICT_KILLZONES)
  await update.message.reply_text(
    f"🤖 <b>SMC Bot v11 (WebSocket Edition)</b>\n\n"
    f"⏱ TF: {TREND_TF} / {HIGHER_TF} / {LOWER_TF}\n"
    f"💹 Leverage: {LEVERAGE}x | Ризик: {RISK_PCT}% за угоду\n"
    f"🎯 Поріг у KZ: {MIN_OPTIONAL_SCORE}/13 | Поза KZ: {STRICT_MIN_SCORE}/13\n"
    f"📐 TP R: {TP_RR_RATIOS} | Об'єми: {[f'{v:.0%}' for v in TP_VOLUMES]}\n"
    f"📏 SL: ATR 4H × {ATR_SL_MULT} | TP: software\n"
    f"🕐 ICT Killzones (+2 бали): {kz_str} UTC\n"
    f"🔌 Trading: WS API | 📡 Market: WS Stream\n"
    f"🛡 Денний ліміт: {DAILY_LOSS_LIMIT}%\n"
    f"📡 Зараз: {session_status()}",
    parse_mode="HTML",
    reply_markup=InlineKeyboardMarkup(keyboard)
  )


async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
  q    = update.callback_query
  data = q.data
  ws: BinanceWsTrading | None      = ctx.bot_data.get("ws")
  rest: BinanceRestClient | None   = ctx.bot_data.get("rest")
  await q.answer()

  if data == "start_bot":
    bot_state["running"] = True
    await q.message.reply_text("✅ Бот запущено!")

  elif data == "stop_bot":
    bot_state["running"] = False
    await q.message.reply_text("⏹ Бот зупинено.")

  elif data == "ws_status":
    t  = "✅" if bot_state.get("ws_trading_ready")  else "❌"
    m  = "✅" if bot_state.get("ws_market_ready")   else "❌"
    ud = "✅" if bot_state.get("ws_userdata_ready") else "❌"
    lk = bot_state.get("listen_key", "—")
    lk_short = lk[:12] + "..." if lk and len(lk) > 12 else (lk or "—")
    cached_syms = sum(
      1 for sym in SYMBOLS
      if len(bot_state["kline_cache"][sym][LOWER_TF]) > 10
    )
    await q.message.reply_text(
      f"🔌 <b>WebSocket Статус</b>\n"
      f"Trading WS API: {t}\n"
      f"Market Stream:  {m}\n"
      f"User Data Stream: {ud}\n"
      f"listenKey: <code>{lk_short}</code>\n"
      f"Kline кеш: {cached_syms}/{len(SYMBOLS)} символів\n"
      f"Balance (WS cache): {bot_state.get('account_balance', 0):.2f} USDT",
      parse_mode="HTML"
    )

  elif data == "status":
    hist  = await db_load_history(1000)
    wins  = sum(1 for t in hist if t["pnl"] > 0)
    total = len(hist)
    wr    = (wins / total * 100) if total else 0
    pnl   = sum(t["pnl"] for t in hist)
    avg   = pnl / total if total else 0
    best  = max((t["pnl"] for t in hist), default=0)
    worst = min((t["pnl"] for t in hist), default=0)
    status_str = "🟢 Активний" if bot_state["running"] else "🔴 Зупинений"
    await q.message.reply_text(
      f"<b>Статус:</b> {status_str}\n"
      f"<b>Відкрито:</b> {len(bot_state['open_positions'])} | "
      f"<b>Pending:</b> {len(bot_state['pending_limits'])}\n"
      f"<b>Сканів:</b> {bot_state['scan_count']}\n"
      f"<b>Сесія зараз:</b> {session_status()}\n"
      f"{'─'*28}\n"
      f"<b>Угод:</b> {total} | <b>Winrate:</b> {wr:.1f}%\n"
      f"<b>Загальний PnL:</b> {'+' if pnl >= 0 else ''}{pnl:.4f} USDT\n"
      f"<b>Середній PnL:</b> {'+' if avg >= 0 else ''}{avg:.4f} USDT\n"
      f"<b>Кращий:</b> +{best:.4f} | <b>Гірший:</b> {worst:.4f}\n"
      f"<b>Денний PnL:</b> {'+' if bot_state['daily_pnl'] >= 0 else ''}"
      f"{bot_state['daily_pnl']:.4f} USDT",
      parse_mode="HTML"
    )

  elif data == "balance":
    if not rest:
      await q.message.reply_text("❌ Клієнт не підключений.")
      return
    try:
      avail = await get_available_balance(rest)
      ws_cached = bot_state.get("account_balance", 0)
      acc   = await rest.get_account()
      total = float(acc.get("totalWalletBalance", 0))
      upnl  = float(acc.get("totalUnrealizedProfit", 0))
      await q.message.reply_text(
        f"💰 <b>Баланс</b>\n"
        f"Загальний: <code>{total:.2f}</code> USDT\n"
        f"Доступний: <code>{avail:.2f}</code> USDT\n"
        f"WS кеш (ACCOUNT_UPDATE): <code>{ws_cached:.2f}</code> USDT\n"
        f"Нереалізований PnL: <code>{upnl:+.4f}</code> USDT\n"
        f"Ризик за угоду: <code>{avail * RISK_PCT / 100:.2f}</code> USDT",
        parse_mode="HTML"
      )
    except Exception as e:
      await q.message.reply_text(f"❌ {e}")

  elif data == "positions":
    pos  = bot_state["open_positions"]
    pend = bot_state["pending_limits"]
    if not pos and not pend:
      await q.message.reply_text("📭 Немає відкритих позицій.")
      return
    lines = []
    for sym, p in pos.items():
      e   = "🟢" if p["signal"] == "BUY" else "🔴"
      dur = int((datetime.now(timezone.utc) - p["open_time"]).total_seconds() / 60)
      tps_str  = " | ".join(str(t) for t in p.get("tps", []))
      tp_hit   = p.get("sw_tp_hit", 0)
      tp_icons = ["⚪","💰","💰💰","🎯"][min(tp_hit, 3)]
      lines.append(
        f"{e} <b>{sym}</b> {'LONG' if p['signal']=='BUY' else 'SHORT'} "
        f"| {dur}хв | TP: {tp_icons} ({tp_hit}/{len(p.get('tps',[]))})\n"
        f"   Вхід: <code>{p['entry_price']}</code> "
        f"SL: <code>{p['sl']}</code>\n"
        f"   TP: <code>{tps_str}</code>"
      )
    for sym, p in pend.items():
      lines.append(f"⏳ <b>{sym}</b> LIMIT @ {p['entry_price']}")
    await q.message.reply_text(
      "<b>📊 Позиції:</b>\n\n" + "\n\n".join(lines),
      parse_mode="HTML"
    )

  elif data == "history":
    hist     = await db_load_history(10)
    all_hist = await db_load_history(1000)
    if not hist:
      await q.message.reply_text("📭 Історія порожня.")
      return
    wins  = sum(1 for t in all_hist if t["pnl"] > 0)
    pnl   = sum(t["pnl"] for t in all_hist)
    wr    = (wins / len(all_hist) * 100) if all_hist else 0
    lines = [
      ("✅" if t["pnl"] > 0 else "❌") +
      f" {t['symbol']} {'+' if t['pnl']>=0 else ''}{t['pnl']:.4f} USDT | {t.get('reason','?')}"
      for t in hist
    ]
    await q.message.reply_text(
      f"<b>📈 Останні {len(hist)} угод:</b>\n\n" + "\n".join(lines) +
      f"\n{'─'*28}\n"
      f"<b>PnL:</b> {'+' if pnl>=0 else ''}{pnl:.4f} USDT\n"
      f"<b>Winrate:</b> {wr:.1f}% ({wins}/{len(all_hist)})",
      parse_mode="HTML"
    )


# ─────────────────────────────────────────────
#  ТОЧКА ВХОДУ
# ─────────────────────────────────────────────
async def main() -> None:
  await init_db()  # MongoDB

  rest = BinanceRestClient(BINANCE_API_KEY, BINANCE_API_SECRET, REST_BASE)
  ws   = rest  # REST API для всіх торгових операцій (demo підтримує лише REST)

  # Market Stream для klines
  market_stream = BinanceMarketStream(
    symbols=SYMBOLS,
    timeframes=[TREND_TF, HIGHER_TF, LOWER_TF]
  )

  try:
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.bot_data["ws"]   = rest
    app.bot_data["rest"] = rest
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(button_handler))

    async with app:
      await app.start()
      await app.updater.start_polling(drop_pending_updates=True)

      # ── REST API ────────────────────────────────────────────────
      import uvicorn
      from api import app as api_app, set_bot_refs

      set_bot_refs(
        state              = bot_state,
        ws                 = ws,
        rest               = rest,
        tg_bot             = app.bot,
        open_position_fn   = open_position,
        symbols            = SYMBOLS,
        lower_tf           = LOWER_TF,
        risk_pct           = RISK_PCT,
        leverage           = LEVERAGE,
        max_positions      = MAX_POSITIONS,
        daily_loss_limit   = DAILY_LOSS_LIMIT,
        ict_killzones      = ICT_KILLZONES,
        session_start_utc  = SESSION_START_UTC,
        session_end_utc    = SESSION_END_UTC,
        local_utc_offset   = LOCAL_UTC_OFFSET,
      )

      api_host = os.getenv("API_HOST", "0.0.0.0")
      api_port = int(os.getenv("API_PORT", "8000"))
      uvicorn_config = uvicorn.Config(
        api_app,
        host      = api_host,
        port      = api_port,
        log_level = "info",
      )
      api_server = uvicorn.Server(uvicorn_config)

      async def _run_api():
        try:
          await api_server.serve()
        except (OSError, SystemExit) as exc:
          log.error(f"❌ REST API не зміг запуститись на {api_host}:{api_port} — {exc}")

      asyncio.ensure_future(_run_api())
      log.info(f"🌐 REST API запускається на {api_host}:{api_port}")

      # ── З'єднання ──────────────────────────────────────────────
      await ws.connect()          # REST trading — встановлює ws_trading_ready=True
      await market_stream.start() # Market Streams (WS klines)

      # User Data Stream (потребує listenKey через REST)
      user_data_stream = BinanceUserDataStream(rest, is_demo=IS_DEMO, bot=app.bot)
      await user_data_stream.start()

      bot_state["running"] = True

      try:
        await scanner_loop(ws, rest, app.bot, market_stream)
      except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Зупинка бота...")
      finally:
        bot_state["running"] = False
        await app.updater.stop()
        await app.stop()
  finally:
    await rest.close()
    log.info("✅ Бот зупинений, всі з'єднання закриті.")


if __name__ == "__main__":
  try:
    asyncio.run(main())
  except KeyboardInterrupt:
    log.info("Зупинено користувачем.")
