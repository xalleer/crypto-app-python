"""
REST API for SMC Bot dashboard.
Endpoints: auth, balance, positions, history, statistics, bot control, user management.
"""

import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, status, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt
from pydantic import BaseModel

from db import (
    get_user, verify_password, create_user, get_all_users,
    update_user, delete_user, super_admin_exists,
    db_load_history,
)

log = logging.getLogger(__name__)

SECRET_KEY             = os.getenv("API_SECRET_KEY", "change-this-secret-in-production")
ALGORITHM              = "HS256"
ACCESS_TOKEN_EXPIRE_M  = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))

app = FastAPI(
    title="SMC Bot API",
    version="1.0.0",
    description="REST API для управління та моніторингу SMC торгового бота.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

# ── Injected bot references (set by main()) ───────────────────────────────────
_bot_state:         dict  = {}
_ws_client                = None
_rest_client              = None
_tg_bot                   = None
_open_position_fn         = None
_symbols:           list  = []
_lower_tf:          str   = "15m"
_risk_pct:          float = 1.0
_leverage:          int   = 5
_max_positions:     int   = 5
_daily_loss_limit:  float = -5.0
_ict_killzones:     list  = [(8, 10), (12, 15), (18, 20)]
_session_start_utc: int   = 7
_session_end_utc:   int   = 20
_local_utc_offset:  int   = 3


def set_bot_refs(
    state:              dict,
    ws,
    rest,
    tg_bot,
    open_position_fn    = None,
    symbols:      list  = None,
    lower_tf:     str   = "15m",
    risk_pct:     float = 1.0,
    leverage:     int   = 5,
    max_positions: int  = 5,
    daily_loss_limit: float = -5.0,
    ict_killzones:    list  = None,
    session_start_utc: int  = 7,
    session_end_utc:   int  = 20,
    local_utc_offset:  int  = 3,
) -> None:
    global _bot_state, _ws_client, _rest_client, _tg_bot
    global _open_position_fn, _symbols, _lower_tf
    global _risk_pct, _leverage, _max_positions, _daily_loss_limit
    global _ict_killzones, _session_start_utc, _session_end_utc, _local_utc_offset
    _bot_state          = state
    _ws_client          = ws
    _rest_client        = rest
    _tg_bot             = tg_bot
    _open_position_fn   = open_position_fn
    _symbols            = symbols or []
    _lower_tf           = lower_tf
    _risk_pct           = risk_pct
    _leverage           = leverage
    _max_positions      = max_positions
    _daily_loss_limit   = daily_loss_limit
    _ict_killzones      = ict_killzones or [(8, 10), (12, 15), (18, 20)]
    _session_start_utc  = session_start_utc
    _session_end_utc    = session_end_utc
    _local_utc_offset   = local_utc_offset


# ─────────────────────────────────────────────
#  PYDANTIC МОДЕЛІ
# ─────────────────────────────────────────────
class Token(BaseModel):
    access_token: str
    token_type:   str


class TokenData(BaseModel):
    username: Optional[str] = None
    role:     Optional[str] = None


class LoginRequest(BaseModel):
    username: str
    password: str


class UserCreate(BaseModel):
    username: str
    password: str
    role:     str  # admin | viewer


class UserUpdate(BaseModel):
    password:  Optional[str]  = None
    role:      Optional[str]  = None
    is_active: Optional[bool] = None


class OpenPositionRequest(BaseModel):
    symbol:      str
    signal:      str            # BUY | SELL
    entry_price: float
    sl:          float
    tps:         list[float]
    qty:         float
    use_limit:   bool = False


class ClosePositionRequest(BaseModel):
    symbol: str


# ─────────────────────────────────────────────
#  JWT HELPERS
# ─────────────────────────────────────────────
def _create_access_token(data: dict, expires_delta: timedelta | None = None) -> str:
    to_encode = data.copy()
    expire    = datetime.now(timezone.utc) + (
        expires_delta or timedelta(minutes=ACCESS_TOKEN_EXPIRE_M)
    )
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    exc = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload  = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub", "")
        if not username:
            raise exc
    except JWTError:
        raise exc
    user = await get_user(username)
    if not user or not user.get("is_active", True):
        raise exc
    return user


def require_role(*roles: str):
    """Dependency factory: allows only users with one of the given roles."""
    async def checker(current_user: dict = Depends(get_current_user)) -> dict:
        if current_user.get("role") not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Потрібна роль: {' або '.join(roles)}",
            )
        return current_user
    return checker


# ─────────────────────────────────────────────
#  AUTH
# ─────────────────────────────────────────────
@app.post("/auth/bootstrap", tags=["auth"], summary="Створити першого super_admin (одноразово)")
async def bootstrap_super_admin(user_data: UserCreate) -> dict:
    """
    Доступний лише якщо super_admin ще не існує.
    Після створення першого super_admin цей ендпоінт блокується.
    """
    if await super_admin_exists():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Super admin вже існує. Використовуйте /auth/login.",
        )
    user = await create_user(user_data.username, user_data.password, "super_admin")
    return {"message": "Super admin створено", "user": user}


@app.post("/auth/register", tags=["auth"], summary="Зареєструвати нового користувача")
async def register(
    user_data:    UserCreate,
    current_user: dict = Depends(get_current_user),
) -> dict:
    """
    super_admin — може створювати admin і viewer.
    admin — може створювати тільки viewer.
    """
    if user_data.role == "super_admin":
        raise HTTPException(status_code=403, detail="Не можна створити super_admin через API")
    if user_data.role == "admin" and current_user["role"] != "super_admin":
        raise HTTPException(status_code=403, detail="Тільки super_admin може створювати admin")
    if user_data.role not in ("admin", "viewer"):
        raise HTTPException(status_code=400, detail="Невалідна роль. Допустимі: admin, viewer")
    if await get_user(user_data.username):
        raise HTTPException(status_code=400, detail="Ім'я користувача вже зайнято")
    user = await create_user(user_data.username, user_data.password, user_data.role)
    return {"message": "Користувача створено", "user": user}


@app.post("/auth/login", response_model=Token, tags=["auth"], summary="Авторизація")
async def login(form_data: LoginRequest) -> dict:
    user = await get_user(form_data.username)
    if not user or not verify_password(form_data.password, user.get("hashed_password", "")):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невірний логін або пароль",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if not user.get("is_active", True):
        raise HTTPException(status_code=400, detail="Обліковий запис неактивний")
    token = _create_access_token(
        data={"sub": user["username"], "role": user["role"]},
        expires_delta=timedelta(minutes=ACCESS_TOKEN_EXPIRE_M),
    )
    return {"access_token": token, "token_type": "bearer"}


@app.get("/auth/me", tags=["auth"], summary="Поточний користувач")
async def get_me(current_user: dict = Depends(get_current_user)) -> dict:
    return {k: v for k, v in current_user.items() if k != "hashed_password"}


# ─────────────────────────────────────────────
#  БАЛАНС
# ─────────────────────────────────────────────
async def _available_balance() -> float:
    cached = _bot_state.get("account_balance", 0.0)
    if cached > 0 and _bot_state.get("ws_userdata_ready"):
        return cached
    if not _rest_client:
        return 0.0
    try:
        acc    = await _rest_client.get_account()
        assets = acc.get("assets", [])
        for a in assets:
            if a.get("asset") == "USDT":
                v = float(a.get("availableBalance", 0))
                if v > 0:
                    return v
        v = float(acc.get("availableBalance", 0))
        return v if v > 0 else 0.0
    except Exception:
        return 0.0


@app.get("/balance", tags=["trading"], summary="Інформація про баланс")
async def get_balance(current_user: dict = Depends(get_current_user)) -> dict:
    if not _rest_client:
        raise HTTPException(status_code=503, detail="Бот не ініціалізований")
    try:
        avail     = await _available_balance()
        acc       = await _rest_client.get_account()
        total     = float(acc.get("totalWalletBalance", 0))
        upnl      = float(acc.get("totalUnrealizedProfit", 0))
        ws_cached = _bot_state.get("account_balance", 0.0)
        return {
            "available_balance":  round(avail,     4),
            "total_balance":      round(total,     4),
            "unrealized_pnl":     round(upnl,      4),
            "ws_cached_balance":  round(ws_cached, 4),
            "daily_pnl":          round(_bot_state.get("daily_pnl",  0.0), 4),
            "total_pnl":          round(_bot_state.get("total_pnl",  0.0), 4),
            "risk_per_trade_usdt": round(avail * _risk_pct / 100, 4),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
#  ПОЗИЦІЇ
# ─────────────────────────────────────────────
@app.get("/positions", tags=["trading"], summary="Відкриті позиції та pending ордери")
async def get_open_positions(current_user: dict = Depends(get_current_user)) -> dict:
    positions = []
    for sym, pos in _bot_state.get("open_positions", {}).items():
        p = {k: v for k, v in pos.items() if k != "_id"}
        if isinstance(p.get("open_time"), datetime):
            p["open_time"] = p["open_time"].isoformat()
        positions.append(p)

    pending = [
        {k: v for k, v in p.items() if k not in ("_id", "_ws_filled", "_ws_price", "_ws_qty")}
        for p in _bot_state.get("pending_limits", {}).values()
    ]
    return {
        "positions": positions,
        "pending":   pending,
        "count":     len(positions),
        "pending_count": len(pending),
    }


@app.get("/positions/history", tags=["trading"], summary="Історія торгівлі")
async def get_trade_history(
    limit:        int  = Query(default=50, ge=1, le=1000),
    current_user: dict = Depends(get_current_user),
) -> dict:
    history = await db_load_history(limit)
    return {"history": history, "count": len(history)}


@app.post("/positions/open", tags=["trading"], summary="Відкрити нову позицію")
async def request_open_position(
    req:          OpenPositionRequest,
    current_user: dict = Depends(require_role("admin", "super_admin")),
) -> dict:
    if not _ws_client or not _rest_client or not _tg_bot:
        raise HTTPException(status_code=503, detail="Бот не ініціалізований")
    if not _bot_state.get("running"):
        raise HTTPException(status_code=400, detail="Бот не запущений")

    symbol = req.symbol.upper()
    if _symbols and symbol not in _symbols:
        raise HTTPException(status_code=400, detail=f"Символ {symbol} відсутній у watchlist")

    if symbol in _bot_state.get("open_positions", {}):
        raise HTTPException(status_code=409, detail=f"Позиція для {symbol} вже відкрита")
    if symbol in _bot_state.get("pending_limits", {}):
        raise HTTPException(status_code=409, detail=f"Pending ордер для {symbol} вже існує")

    total_open = (
        len(_bot_state.get("open_positions", {})) +
        len(_bot_state.get("pending_limits",  {}))
    )
    if total_open >= _max_positions:
        raise HTTPException(status_code=429, detail="Досягнуто ліміт відкритих позицій")

    if req.signal not in ("BUY", "SELL"):
        raise HTTPException(status_code=400, detail="signal має бути BUY або SELL")
    if len(req.tps) == 0:
        raise HTTPException(status_code=400, detail="Потрібно вказати хоча б один TP")

    if not _open_position_fn:
        raise HTTPException(status_code=503, detail="open_position функція не зареєстрована")

    try:
        import asyncio
        asyncio.ensure_future(
            _open_position_fn(
                _ws_client, _rest_client, _tg_bot,
                symbol, req.signal,
                req.entry_price, req.sl, req.tps, req.qty,
                use_limit=req.use_limit,
            )
        )
        return {
            "message":     "Запит на відкриття позиції прийнято",
            "symbol":      symbol,
            "signal":      req.signal,
            "entry_price": req.entry_price,
            "sl":          req.sl,
            "tps":         req.tps,
            "qty":         req.qty,
            "use_limit":   req.use_limit,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/positions/{symbol}/close", tags=["trading"], summary="Закрити позицію")
async def close_position(
    symbol:       str,
    current_user: dict = Depends(require_role("admin", "super_admin")),
) -> dict:
    symbol = symbol.upper()
    if not _ws_client:
        raise HTTPException(status_code=503, detail="Бот не ініціалізований")

    pos = _bot_state.get("open_positions", {}).get(symbol)
    if not pos:
        raise HTTPException(status_code=404, detail=f"Немає відкритої позиції для {symbol}")

    try:
        await _ws_client.cancel_all_orders(symbol)
        close_side = "SELL" if pos["signal"] == "BUY" else "BUY"
        result = await _ws_client.place_market_order(
            symbol, close_side, pos["qty"], reduce_only=True
        )
        return {
            "message": "Запит на закриття позиції надіслано",
            "symbol":  symbol,
            "side":    close_side,
            "qty":     pos["qty"],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─────────────────────────────────────────────
#  СТАТИСТИКА
# ─────────────────────────────────────────────
@app.get("/statistics", tags=["trading"], summary="Торгова статистика")
async def get_statistics(current_user: dict = Depends(get_current_user)) -> dict:
    history = await db_load_history(limit=10_000)
    total   = len(history)

    if total == 0:
        return {
            "total_trades": 0, "wins": 0, "losses": 0,
            "win_rate": 0.0,   "total_pnl": 0.0,
            "avg_pnl": 0.0,    "avg_win": 0.0, "avg_loss": 0.0,
            "best_trade": 0.0, "worst_trade": 0.0,
            "daily_pnl": _bot_state.get("daily_pnl", 0.0),
            "by_symbol": {},   "by_signal": {},
        }

    wins       = [t for t in history if (t.get("pnl") or 0) > 0]
    losses     = [t for t in history if (t.get("pnl") or 0) <= 0]
    total_pnl  = sum(t.get("pnl") or 0 for t in history)
    win_pnl    = sum(t.get("pnl") or 0 for t in wins)
    loss_pnl   = sum(t.get("pnl") or 0 for t in losses)

    by_symbol: dict = {}
    by_signal: dict = {"BUY": {"trades": 0, "wins": 0, "pnl": 0.0},
                       "SELL": {"trades": 0, "wins": 0, "pnl": 0.0}}
    for t in history:
        sym = t.get("symbol", "?")
        if sym not in by_symbol:
            by_symbol[sym] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0.0}
        pnl_val = t.get("pnl") or 0
        by_symbol[sym]["trades"] += 1
        by_symbol[sym]["pnl"]    += pnl_val
        if pnl_val > 0:
            by_symbol[sym]["wins"]   += 1
        else:
            by_symbol[sym]["losses"] += 1

        sig = t.get("signal", "")
        if sig in by_signal:
            by_signal[sig]["trades"] += 1
            by_signal[sig]["pnl"]    += pnl_val
            if pnl_val > 0:
                by_signal[sig]["wins"] += 1

    for sym, s in by_symbol.items():
        s["win_rate"] = round(s["wins"] / max(s["trades"], 1) * 100, 2)
        s["pnl"]      = round(s["pnl"], 4)
    for sig, s in by_signal.items():
        s["win_rate"] = round(s["wins"] / max(s["trades"], 1) * 100, 2)
        s["pnl"]      = round(s["pnl"], 4)

    # Profit factor
    profit_factor = abs(win_pnl / loss_pnl) if loss_pnl != 0 else float("inf")

    return {
        "total_trades":   total,
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       round(len(wins) / total * 100, 2),
        "total_pnl":      round(total_pnl, 4),
        "avg_pnl":        round(total_pnl / total, 4),
        "avg_win":        round(win_pnl  / max(len(wins),   1), 4),
        "avg_loss":       round(loss_pnl / max(len(losses), 1), 4),
        "best_trade":     round(max((t.get("pnl") or 0 for t in history), default=0), 4),
        "worst_trade":    round(min((t.get("pnl") or 0 for t in history), default=0), 4),
        "profit_factor":  round(profit_factor, 4),
        "daily_pnl":      round(_bot_state.get("daily_pnl", 0.0), 4),
        "by_symbol":      by_symbol,
        "by_signal":      by_signal,
    }


# ─────────────────────────────────────────────
#  БОТ — СТАН ТА КЕРУВАННЯ
# ─────────────────────────────────────────────
@app.get("/bot/status", tags=["bot"], summary="Стан бота та WebSocket з'єднань")
async def bot_status(current_user: dict = Depends(get_current_user)) -> dict:
    kline_cache = _bot_state.get("kline_cache", {})
    cached_syms = sum(
        1 for sym in _symbols
        if len(kline_cache.get(sym, {}).get(_lower_tf, [])) > 10
    )
    return {
        "running":            _bot_state.get("running",           False),
        "ws_trading_ready":   _bot_state.get("ws_trading_ready",  False),
        "ws_market_ready":    _bot_state.get("ws_market_ready",   False),
        "ws_userdata_ready":  _bot_state.get("ws_userdata_ready", False),
        "scan_count":         _bot_state.get("scan_count",        0),
        "open_positions":     len(_bot_state.get("open_positions", {})),
        "pending_limits":     len(_bot_state.get("pending_limits", {})),
        "kline_cache_symbols": cached_syms,
        "total_symbols":      len(_symbols),
        "account_balance":    round(_bot_state.get("account_balance", 0.0), 4),
        "daily_pnl":          round(_bot_state.get("daily_pnl", 0.0),       4),
        "total_pnl":          round(_bot_state.get("total_pnl", 0.0),       4),
        "daily_date":         str(_bot_state.get("daily_date", "")),
    }


@app.post("/bot/start", tags=["bot"], summary="Запустити бота")
async def start_bot(current_user: dict = Depends(require_role("admin", "super_admin"))) -> dict:
    _bot_state["running"] = True
    return {"message": "Бот запущено", "running": True}


@app.post("/bot/stop", tags=["bot"], summary="Зупинити бота")
async def stop_bot(current_user: dict = Depends(require_role("admin", "super_admin"))) -> dict:
    _bot_state["running"] = False
    return {"message": "Бота зупинено", "running": False}


@app.get("/bot/config", tags=["bot"], summary="Конфігурація бота")
async def get_config(current_user: dict = Depends(get_current_user)) -> dict:
    return {
        "leverage":          _leverage,
        "risk_pct":          _risk_pct,
        "max_positions":     _max_positions,
        "daily_loss_limit":  _daily_loss_limit,
        "symbols":           _symbols,
        "lower_tf":          _lower_tf,
    }


@app.get("/bot/session", tags=["bot"], summary="Поточна торгова сесія та ICT KZ статус")
async def get_session(current_user: dict = Depends(get_current_user)) -> dict:
    now_utc   = datetime.now(timezone.utc)
    hour_utc  = now_utc.hour
    hour_local = (hour_utc + _local_utc_offset) % 24

    in_kz = any(s <= hour_utc < e for s, e in _ict_killzones)
    active = _session_start_utc <= hour_utc < _session_end_utc

    active_kz = None
    for s, e in _ict_killzones:
        if s <= hour_utc < e:
            active_kz = {
                "start_utc":   s,
                "end_utc":     e,
                "start_local": (s + _local_utc_offset) % 24,
                "end_local":   (e + _local_utc_offset) % 24,
            }
            break

    next_kz = next(
        ({"start_utc": s, "end_utc": e,
          "start_local": (s + _local_utc_offset) % 24}
         for s, e in _ict_killzones if s > hour_utc),
        None,
    )

    return {
        "utc_time":          now_utc.isoformat(),
        "utc_hour":          hour_utc,
        "local_hour":        hour_local,
        "local_utc_offset":  _local_utc_offset,
        "is_active_session": active,
        "is_in_killzone":    in_kz,
        "active_killzone":   active_kz,
        "next_killzone":     next_kz,
        "killzones_utc":     [{"start": s, "end": e} for s, e in _ict_killzones],
        "session_window_utc": {
            "start": _session_start_utc,
            "end":   _session_end_utc,
        },
    }


# ─────────────────────────────────────────────
#  УПРАВЛІННЯ КОРИСТУВАЧАМИ
# ─────────────────────────────────────────────
@app.get("/users", tags=["users"], summary="Список всіх користувачів")
async def list_users(
    current_user: dict = Depends(require_role("super_admin")),
) -> dict:
    users = await get_all_users()
    return {"users": users, "count": len(users)}


@app.get("/users/{user_id}", tags=["users"], summary="Отримати користувача за ID")
async def get_user_by_id_endpoint(
    user_id:      str,
    current_user: dict = Depends(require_role("super_admin")),
) -> dict:
    from bson import ObjectId
    try:
        from db import get_db
        doc = await get_db().users.find_one(
            {"_id": ObjectId(user_id)}, {"hashed_password": 0}
        )
    except Exception:
        raise HTTPException(status_code=400, detail="Невалідний user_id")
    if not doc:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    doc["id"] = str(doc.pop("_id"))
    return doc


@app.put("/users/{user_id}", tags=["users"], summary="Оновити користувача")
async def update_user_endpoint(
    user_id:      str,
    update_data:  UserUpdate,
    current_user: dict = Depends(require_role("super_admin")),
) -> dict:
    data = update_data.model_dump(exclude_none=True)
    if not data:
        raise HTTPException(status_code=400, detail="Немає полів для оновлення")
    if data.get("role") == "super_admin":
        raise HTTPException(status_code=403, detail="Не можна призначити роль super_admin")
    ok = await update_user(user_id, data)
    if not ok:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    return {"message": "Користувача оновлено"}


@app.delete("/users/{user_id}", tags=["users"], summary="Видалити користувача")
async def delete_user_endpoint(
    user_id:      str,
    current_user: dict = Depends(require_role("super_admin")),
) -> dict:
    ok = await delete_user(user_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Користувача не знайдено")
    return {"message": "Користувача видалено"}


# ─────────────────────────────────────────────
#  РИНКОВІ ДАНІ
# ─────────────────────────────────────────────
@app.get("/market/symbols", tags=["market"], summary="Список символів у watchlist")
async def get_symbols(current_user: dict = Depends(get_current_user)) -> dict:
    exchange_info = _bot_state.get("exchange_info", {})
    result = []
    for sym in _symbols:
        info = exchange_info.get(sym, {})
        result.append({
            "symbol":       sym,
            "tick_size":    info.get("tick",          None),
            "step_size":    info.get("step",          None),
            "min_notional": info.get("min_notional",  None),
            "has_position": sym in _bot_state.get("open_positions", {}),
            "has_pending":  sym in _bot_state.get("pending_limits",  {}),
        })
    return {"symbols": result, "count": len(result)}


@app.get("/market/price/{symbol}", tags=["market"], summary="Поточна ціна символу")
async def get_price(
    symbol:       str,
    current_user: dict = Depends(get_current_user),
) -> dict:
    if not _rest_client:
        raise HTTPException(status_code=503, detail="Бот не ініціалізований")
    try:
        price = await _rest_client.get_price(symbol.upper())
        return {"symbol": symbol.upper(), "price": price}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
