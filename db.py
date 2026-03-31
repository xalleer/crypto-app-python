"""
MongoDB database layer (Motor async driver).
Provides the same interface as the previous aiosqlite functions,
plus user management for the REST API.
"""

import os
import logging
from datetime import datetime, timezone, date as _date

from motor.motor_asyncio import AsyncIOMotorClient
import bcrypt as _bcrypt

log = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB  = os.getenv("MONGODB_DB",  "smc_bot")

_client: AsyncIOMotorClient | None = None


def get_db():
    return _client[MONGODB_DB]


# ─────────────────────────────────────────────
#  ІНІЦІАЛІЗАЦІЯ
# ─────────────────────────────────────────────
async def init_db() -> None:
    global _client
    _client = AsyncIOMotorClient(MONGODB_URI)
    db = _client[MONGODB_DB]

    await db.positions.create_index("symbol",   unique=True)
    await db.pending_limits.create_index("symbol", unique=True)
    await db.trade_history.create_index("close_time")
    await db.daily_state.create_index("key",    unique=True)
    await db.users.create_index("username",     unique=True)

    log.info("✅ MongoDB підключено та індекси створено (%s)", MONGODB_DB)


# ─────────────────────────────────────────────
#  ПОЗИЦІЇ
# ─────────────────────────────────────────────
async def db_save_position(pos: dict) -> None:
    db  = get_db()
    doc = dict(pos)
    if isinstance(doc.get("open_time"), datetime):
        doc["open_time"] = doc["open_time"].isoformat()
    await db.positions.replace_one(
        {"symbol": pos["symbol"]}, doc, upsert=True
    )


async def db_delete_position(symbol: str) -> None:
    await get_db().positions.delete_one({"symbol": symbol})


async def db_load_positions() -> dict:
    result: dict = {}
    async for doc in get_db().positions.find({}, {"_id": 0}):
        symbol = doc["symbol"]
        if isinstance(doc.get("open_time"), str):
            doc["open_time"] = datetime.fromisoformat(doc["open_time"])
        doc.setdefault("sw_tp_hit",    0)
        doc.setdefault("sw_closed_qty", 0.0)
        doc.setdefault("original_qty", doc.get("qty", 0.0))
        result[symbol] = doc
    return result


# ─────────────────────────────────────────────
#  PENDING LIMIT ОРДЕРИ
# ─────────────────────────────────────────────
async def db_save_pending(p: dict) -> None:
    db  = get_db()
    doc = dict(p)
    await db.pending_limits.replace_one(
        {"symbol": p["symbol"]}, doc, upsert=True
    )


async def db_delete_pending(symbol: str) -> None:
    await get_db().pending_limits.delete_one({"symbol": symbol})


async def db_load_pending() -> dict:
    result: dict = {}
    async for doc in get_db().pending_limits.find({}, {"_id": 0}):
        result[doc["symbol"]] = doc
    return result


# ─────────────────────────────────────────────
#  ТОРГОВА ІСТОРІЯ
# ─────────────────────────────────────────────
async def db_save_trade(trade: dict) -> None:
    db         = get_db()
    close_time = trade.get("time")
    if isinstance(close_time, datetime):
        close_time = close_time.isoformat()
    doc = {
        "symbol":     trade["symbol"],
        "signal":     trade["signal"],
        "entry":      trade["entry"],
        "exit_price": trade["exit"],
        "pnl":        trade["pnl"],
        "reason":     trade["reason"],
        "close_time": close_time or "",
    }
    await db.trade_history.insert_one(doc)


async def db_load_history(limit: int = 50) -> list:
    cursor = (
        get_db().trade_history
        .find({}, {"_id": 0})
        .sort("close_time", -1)
        .limit(limit)
    )
    result = []
    async for doc in cursor:
        result.append({
            "symbol": doc.get("symbol"),
            "signal": doc.get("signal"),
            "entry":  doc.get("entry"),
            "exit":   doc.get("exit_price"),
            "pnl":    doc.get("pnl"),
            "reason": doc.get("reason"),
            "time":   doc.get("close_time"),
        })
    return result


# ─────────────────────────────────────────────
#  ДЕННИЙ СТАН
# ─────────────────────────────────────────────
async def db_save_daily_state(daily_date, daily_pnl: float) -> None:
    db = get_db()
    await db.daily_state.replace_one(
        {"key": "daily_date"},
        {"key": "daily_date", "value": str(daily_date)},
        upsert=True,
    )
    await db.daily_state.replace_one(
        {"key": "daily_pnl"},
        {"key": "daily_pnl", "value": str(daily_pnl)},
        upsert=True,
    )


async def db_load_daily_state() -> tuple:
    try:
        state: dict = {}
        async for doc in get_db().daily_state.find({}, {"_id": 0}):
            state[doc["key"]] = doc["value"]
        today  = datetime.now(timezone.utc).date()
        d_str  = state.get("daily_date")
        parsed = _date.fromisoformat(d_str) if d_str else None
        if parsed == today:
            return parsed, float(state.get("daily_pnl", 0.0))
        return today, 0.0
    except Exception:
        return datetime.now(timezone.utc).date(), 0.0


# ─────────────────────────────────────────────
#  УПРАВЛІННЯ КОРИСТУВАЧАМИ
# ─────────────────────────────────────────────
def hash_password(plain: str) -> str:
    return _bcrypt.hashpw(plain.encode(), _bcrypt.gensalt()).decode()


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return _bcrypt.checkpw(plain.encode(), hashed.encode())
    except Exception:
        return False


async def create_user(username: str, password: str, role: str) -> dict:
    db  = get_db()
    doc = {
        "username":        username,
        "hashed_password": hash_password(password),
        "role":            role,
        "is_active":       True,
        "created_at":      datetime.now(timezone.utc).isoformat(),
    }
    await db.users.insert_one(doc)
    return _sanitize_user(doc)


async def get_user(username: str) -> dict | None:
    return await get_db().users.find_one({"username": username}, {"_id": 0})


async def get_all_users() -> list:
    result = []
    async for doc in get_db().users.find({}, {"hashed_password": 0}):
        doc["id"] = str(doc.pop("_id"))
        result.append(doc)
    return result


async def update_user(user_id: str, update_data: dict) -> bool:
    from bson import ObjectId
    if "password" in update_data:
        update_data["hashed_password"] = hash_password(update_data.pop("password"))
    result = await get_db().users.update_one(
        {"_id": ObjectId(user_id)}, {"$set": update_data}
    )
    return result.modified_count > 0


async def delete_user(user_id: str) -> bool:
    from bson import ObjectId
    result = await get_db().users.delete_one({"_id": ObjectId(user_id)})
    return result.deleted_count > 0


async def super_admin_exists() -> bool:
    doc = await get_db().users.find_one({"role": "super_admin"})
    return doc is not None


def _sanitize_user(doc: dict) -> dict:
    return {k: v for k, v in doc.items()
            if k not in ("hashed_password", "_id")}
