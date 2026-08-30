import re
import asyncio
import time
import base64
import struct
import sqlite3
import aiosqlite
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from keep import keep_alive
keep_alive()

import aiohttp
from telethon import TelegramClient, events, Button
from telethon.sessions import MemorySession
# ── credentials ──────────────────────────────────────────────────────────────
API_ID    = 30219110
API_HASH  = "06ddc0cbe1980d5cee7ae5274933a5e2"
BOT_TOKEN = "8746237346:AAGW9O-Eoq2lFPnwl71vubIiEAsdPzEQS1s"
BOT_USERNAME = "BLCXCBOT"  # Set this to your actual bot username (without @)

# ── Admin Configuration ───────────────────────────────────────────────────────
ADMIN_IDS = [5048281046]  # Replace with your actual admin user ID(s)
ADMIN_STATES = {}  # Stores active admin interactive prompt state

# ── RPC config (15 EVM Chains) ───────────────────────────────────────────────
RPC_ENDPOINTS = {
    "eth": [
        "https://1rpc.io/eth",
        "https://eth.drpc.org",
        "https://rpc.mevblocker.io",
        "https://ethereum.publicnode.com",
    ],
    "bsc": [
        "https://bsc-dataseed.binance.org",
        "https://bsc-dataseed1.defibit.io",
        "https://binance.llamarpc.com",
        "https://1rpc.io/bnb",
    ],
    "polygon": [
        "https://polygon-bor-rpc.publicnode.com",
        "https://polygon.drpc.org",
        "https://1rpc.io/matic",
        "https://polygon.llamarpc.com",
    ],
    "arbitrum": [
        "https://arb1.arbitrum.io/rpc",
        "https://1rpc.io/arb",
        "https://arbitrum.llamarpc.com",
    ],
    "optimism": [
        "https://mainnet.optimism.io",
        "https://1rpc.io/op",
        "https://optimism.llamarpc.com",
    ],
    "base": [
        "https://mainnet.base.org",
        "https://1rpc.io/base",
        "https://base.llamarpc.com",
    ],
    "avalanche": [
        "https://api.avax.network/ext/bc/C/rpc",
        "https://1rpc.io/avax/c",
        "https://avalanche.llamarpc.com",
    ],
    "fantom": [
        "https://rpc.ftm.tools",
        "https://1rpc.io/ftm",
        "https://fantom.drpc.org",
    ],
    "cronos": [
        "https://evm.cronos.org",
        "https://1rpc.io/cro",
    ],
    "zksync": [
        "https://mainnet.era.zksync.io",
        "https://1rpc.io/zksync20",
    ],
    "linea": [
        "https://rpc.linea.build",
        "https://1rpc.io/linea",
    ],
    "blast": [
        "https://rpc.blast.io",
        "https://blast.drpc.org",
    ],
    "mantle": [
        "https://rpc.mantle.xyz",
        "https://1rpc.io/mantle",
    ],
    "celo": [
        "https://forno.celo.org",
        "https://1rpc.io/celo",
    ],
    "moonbeam": [
        "https://rpc.api.moonbeam.network",
        "https://1rpc.io/glmr",
    ],
}

CHAIN_INFO = {
    "eth":       {"name": "Ethereum",          "native": "ETH",       "block_time": 12},
    "bsc":       {"name": "BNB Smart Chain",   "native": "BNB",       "block_time": 1.5},
    "polygon":   {"name": "Polygon",           "native": "MATIC/POL", "block_time": 2},
    "arbitrum":  {"name": "Arbitrum One",      "native": "ETH",       "block_time": 0.25},
    "optimism":  {"name": "OP Mainnet",        "native": "ETH",       "block_time": 2},
    "base":      {"name": "Base",              "native": "ETH",       "block_time": 2},
    "avalanche": {"name": "Avalanche C-Chain", "native": "AVAX",      "block_time": 2},
    "fantom":    {"name": "Fantom Opera",      "native": "FTM",       "block_time": 1},
    "cronos":    {"name": "Cronos EVM",        "native": "CRO",       "block_time": 5},
    "zksync":    {"name": "zkSync Era",        "native": "ETH",       "block_time": 1},
    "linea":     {"name": "Linea",             "native": "ETH",       "block_time": 3},
    "blast":     {"name": "Blast",             "native": "ETH",       "block_time": 2},
    "mantle":    {"name": "Mantle",            "native": "MNT",       "block_time": 2},
    "celo":      {"name": "Celo",              "native": "CELO",      "block_time": 5},
    "moonbeam":  {"name": "Moonbeam",          "native": "GLMR",      "block_time": 12},
}

EVM_TOKENS = {
    "eth": [
        {"symbol": "USDT", "contract": "0xdac17f958d2ee523a2206206994597c13d831ec7", "decimals": 6, "price_key": "USDT"},
        {"symbol": "USDC", "contract": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "decimals": 6, "price_key": "USDC"},
        {"symbol": "DAI",  "contract": "0x6b175474e89094c44da98b954eedeac495271d0f", "decimals": 18, "price_key": "DAI"},
        {"symbol": "WBTC", "contract": "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", "decimals": 8, "price_key": "BTC"},
        {"symbol": "WETH", "contract": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", "decimals": 18, "price_key": "ETH"},
    ],
    "bsc": [
        {"symbol": "USDT", "contract": "0x55d398326f99059ff775485246999027b3197955", "decimals": 18, "price_key": "USDT"},
        {"symbol": "USDC", "contract": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d", "decimals": 18, "price_key": "USDC"},
        {"symbol": "DAI",  "contract": "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3", "decimals": 18, "price_key": "DAI"},
        {"symbol": "BTCB", "contract": "0x7130d2a12b9bcbfae4f2634d864a1ee1ce3ead9c", "decimals": 18, "price_key": "BTC"},
        {"symbol": "WBNB", "contract": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c", "decimals": 18, "price_key": "BNB"},
        {"symbol": "ETH",  "contract": "0x2170ed0880ac9a755fd29b2688956bd959f933f8", "decimals": 18, "price_key": "ETH"},
    ],
    "polygon": [
        {"symbol": "USDT",   "contract": "0xc2132d05d31c914a87c6611c10748aeb04b58e8f", "decimals": 6, "price_key": "USDT"},
        {"symbol": "USDC.e", "contract": "0x2791bca1f2de4661ed88a30c99a7a9449aa84174", "decimals": 6, "price_key": "USDC"},
        {"symbol": "USDC",   "contract": "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359", "decimals": 6, "price_key": "USDC"},
        {"symbol": "DAI",    "contract": "0x8f3cf7ad23cd3cadbd9735aff958023239c6a063", "decimals": 18, "price_key": "DAI"},
        {"symbol": "WBTC",   "contract": "0x1bfd67037b42c0d4067b8955404b1e40f3db87b1", "decimals": 8, "price_key": "BTC"},
        {"symbol": "WETH",   "contract": "0x7ceb23fd6bc0add59e62ac25578270cff1b9f619", "decimals": 18, "price_key": "ETH"},
    ],
    "arbitrum": [
        {"symbol": "ARB",  "contract": "0x912ce59144191c1204e64559fe8253a0e49e6548", "decimals": 18, "price_key": "ARB"},
        {"symbol": "USDT", "contract": "0xfd086bc7cd5c481dcc9c85ebe478a1c0b69fcbb9", "decimals": 6, "price_key": "USDT"},
        {"symbol": "USDC", "contract": "0xaf88d065e77c8cc2239327c5edb3a432268e5831", "decimals": 6, "price_key": "USDC"},
        {"symbol": "WETH", "contract": "0x82af49447d8a07e3bd95bd0d56f35241523fbab1", "decimals": 18, "price_key": "ETH"},
    ],
    "optimism": [
        {"symbol": "OP",   "contract": "0x4200000000000000000000000000000000000042", "decimals": 18, "price_key": "OP"},
        {"symbol": "USDT", "contract": "0x94b008aa00579c1307b0ef2c499ad98a8ce58e58", "decimals": 6, "price_key": "USDT"},
        {"symbol": "USDC", "contract": "0x0b2c639c533813f4aa9d7837caf62653d097ff85", "decimals": 6, "price_key": "USDC"},
        {"symbol": "WETH", "contract": "0x4200000000000000000000000000000000000006", "decimals": 18, "price_key": "ETH"},
    ],
    "base": [
        {"symbol": "USDC", "contract": "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913", "decimals": 6, "price_key": "USDC"},
        {"symbol": "WETH", "contract": "0x4200000000000000000000000000000000000006", "decimals": 18, "price_key": "ETH"},
        {"symbol": "AERO", "contract": "0x940181a94a35a4569e4529a3cdfb74e38fd98631", "decimals": 18, "price_key": "AERO"},
    ],
    "avalanche": [
        {"symbol": "USDT.e", "contract": "0xc7198437980c041c805a1edcba50c145db200280", "decimals": 6, "price_key": "USDT"},
        {"symbol": "USDC.e", "contract": "0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664", "decimals": 6, "price_key": "USDC"},
        {"symbol": "WAVAX",  "contract": "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7", "decimals": 18, "price_key": "AVAX"},
    ],
    "fantom": [
        {"symbol": "USDT", "contract": "0x049d68029688eabf473097a2fc38ef61633a3c7a", "decimals": 6, "price_key": "USDT"},
        {"symbol": "USDC", "contract": "0x28a92ed536a048880c454688b6651474149fa894", "decimals": 6, "price_key": "USDC"},
    ],
    "cronos": [
        {"symbol": "USDC", "contract": "0xc21223249ca139d8b605df26537c62b66236b225", "decimals": 6, "price_key": "USDC"},
    ],
    "zksync": [
        {"symbol": "ZK",   "contract": "0x5a7d6b2f92c77fad684d2a1613d1578f9f8702c1", "decimals": 18, "price_key": "ZK"},
        {"symbol": "USDC", "contract": "0x1d17cbcf0d6d143135ae902365d2e5e2a16538d4", "decimals": 6, "price_key": "USDC"},
    ],
    "linea": [
        {"symbol": "USDC", "contract": "0x176211869ca2b568f2a7d4ee941e073a821ee1ff", "decimals": 6, "price_key": "USDC"},
    ],
    "blast": [
        {"symbol": "USDB",  "contract": "0x4300000000000000000000000000000000000003", "decimals": 18, "price_key": "USDT"},
        {"symbol": "BLAST", "contract": "0xb1a5700fa2358173fe465e6ea4f2897fa1418774", "decimals": 18, "price_key": "BLAST"},
    ],
    "mantle": [
        {"symbol": "USDT", "contract": "0x201ebd9539c0d5e11b4f25539ab426177b9015c7", "decimals": 6, "price_key": "USDT"},
        {"symbol": "USDC", "contract": "0x09bc4e0d864854c6afb64906588019d53647c037", "decimals": 6, "price_key": "USDC"},
    ],
    "celo": [
        {"symbol": "cUSD", "contract": "0x765de816845861e75a25fca122bb6898b8b1282a", "decimals": 18, "price_key": "USDT"},
    ],
    "moonbeam": [
        {"symbol": "USDC", "contract": "0x931715fee2d06333043d11f65018464f294be780", "decimals": 6, "price_key": "USDC"},
    ],
}

TRANSFER_TOPIC    = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
TRX_USDT_CONTRACT = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
TRX_USDC_CONTRACT = "TE7oViNDFDADuLVH57eRX8Vus976oK2R45"

MAX_ADDRESSES_PER_MESSAGE = 3
PRICE_REFRESH_SECONDS     = 600
STARTING_CREDITS          = 30
REFERRAL_REWARD_CREDITS   = 20
DB_PATH                   = "wallet_bot.db"

# Solana RPC nodes
SOL_RPC_ENDPOINTS = [
    "https://solana-rpc.publicnode.com",
    "https://api.mainnet-beta.solana.com",
    "https://rpc.ankr.com/solana",
]

# ── global price cache ────────────────────────────────────────────────────────
PRICES: dict[str, float] = {}
_prices_fetched_at: float = 0.0


class ApiError(Exception):
    pass


# ═══════════════════════════════════════════════════════════════════════════════
#  DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA busy_timeout=5000;")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                credits INTEGER DEFAULT 30,
                referred_by INTEGER,
                referral_rewarded INTEGER DEFAULT 0,
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER,
                referred_user INTEGER,
                reward INTEGER DEFAULT 20,
                created_at TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.commit()


async def get_setting(key: str, default: str = "") -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key = ?", (key,)) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else default


async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
            (key, value, value)
        )
        await db.commit()


async def is_user_in_channel(user_id: int) -> bool:
    if user_id in ADMIN_IDS:
        return True
    enabled = await get_setting("force_join_enabled", "0")
    if enabled != "1":
        return True
    channel = await get_setting("force_join_channel", "")
    if not channel:
        return True
    ch_clean = channel.replace("https://t.me/", "").replace("http://t.me/", "").strip()
    if not ch_clean.startswith("@") and not ch_clean.startswith("joinchat/"):
        ch_clean = "@" + ch_clean
    try:
        permissions = await client.get_permissions(ch_clean, user_id)
        if permissions and not permissions.is_left:
            return True
        return False
    except Exception:
        try:
            p = await client.get_permissions(channel, user_id)
            return bool(p and not p.is_left)
        except Exception:
            return False


def build_force_join_prompt(channel: str):
    ch_clean = channel.replace("https://t.me/", "").replace("http://t.me/", "").strip()
    if ch_clean.startswith("@"):
        ch_clean = ch_clean[1:]
    link = f"https://t.me/{ch_clean}" if not channel.startswith("http") else channel
    text = (
        "Access Restricted\n\n"
        "You must join our official channel to use this bot.\n\n"
        "1. Click the button below to join the channel.\n"
        "2. After joining, click 'I Have Joined' to continue."
    )
    buttons = [
        [Button.url("Join Channel", link)],
        [Button.inline("I Have Joined", data=b"verify_join")]
    ]
    return text, buttons


async def get_or_create_user(user_id: int, referred_by: int = None) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()

        if row is None:
            now = datetime.now(timezone.utc).isoformat()
            await db.execute(
                "INSERT INTO users (user_id, credits, referred_by, referral_rewarded, created_at) VALUES (?, ?, ?, 0, ?)",
                (user_id, STARTING_CREDITS, referred_by, now)
            )
            await db.commit()
            async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
                row = await cursor.fetchone()

        return dict(row)


async def get_user(user_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        return dict(row) if row else None


async def get_credits(user_id: int) -> int:
    user = await get_user(user_id)
    return user["credits"] if user else 0


async def deduct_credit(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT credits FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        if not row or row[0] <= 0:
            return False
        new_credits = max(0, row[0] - 1)
        await db.execute("UPDATE users SET credits = ? WHERE user_id = ?", (new_credits, user_id))
        await db.commit()
        return True


async def add_credits(user_id: int, amount: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT credits FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return 0
        new_credits = row[0] + amount
        await db.execute("UPDATE users SET credits = ? WHERE user_id = ?", (new_credits, user_id))
        await db.commit()
        return new_credits


async def remove_credits(user_id: int, amount: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT credits FROM users WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()
        if not row:
            return 0
        new_credits = max(0, row[0] - amount)
        await db.execute("UPDATE users SET credits = ? WHERE user_id = ?", (new_credits, user_id))
        await db.commit()
        return new_credits


async def process_referral(referrer_id: int, referred_user_id: int) -> bool:
    if referrer_id == referred_user_id:
        return False

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users WHERE user_id = ?", (referrer_id,)) as cursor:
            if not await cursor.fetchone():
                return False

        async with db.execute(
            "SELECT referral_rewarded FROM users WHERE user_id = ?", (referred_user_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row or row[0] != 0:
                return False

        async with db.execute(
            "SELECT id FROM referrals WHERE referred_user = ?", (referred_user_id,)
        ) as cursor:
            if await cursor.fetchone():
                return False

        await db.execute(
            "UPDATE users SET credits = credits + ? WHERE user_id = ?",
            (REFERRAL_REWARD_CREDITS, referrer_id)
        )
        await db.execute(
            "UPDATE users SET referral_rewarded = 1 WHERE user_id = ?",
            (referred_user_id,)
        )
        now = datetime.now(timezone.utc).isoformat()
        await db.execute(
            "INSERT INTO referrals (referrer_id, referred_user, reward, created_at) VALUES (?, ?, ?, ?)",
            (referrer_id, referred_user_id, REFERRAL_REWARD_CREDITS, now)
        )
        await db.commit()
        return True


async def get_referral_count(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)
        ) as cursor:
            row = await cursor.fetchone()
        return row[0] if row else 0


# ═══════════════════════════════════════════════════════════════════════════════
#  PRICES
# ═══════════════════════════════════════════════════════════════════════════════

ALL_PRICE_KEYS = (
    "ETH", "BNB", "POL", "MATIC", "BTC", "LTC", "TRX", "TON", "USDT", "USDC", "DAI",
    "SOL", "DOGE", "ARB", "OP", "AVAX", "XRP", "ADA", "BCH", "SUI", "KAS", "AERO",
    "FTM", "CRO", "ZK", "MNT", "CELO", "GLMR", "APT", "ATOM", "NEAR", "XLM", "ALGO", "BLAST"
)


async def _prices_coingecko(session: aiohttp.ClientSession) -> dict[str, float]:
    ids = (
        "ethereum,binancecoin,polygon-ecosystem-token,bitcoin,litecoin,tron,"
        "the-open-network,usd-coin,dai,solana,dogecoin,arbitrum,optimism,"
        "avalanche-2,ripple,cardano,bitcoin-cash,sui,kaspa,aerodrome-finance,"
        "fantom,crypto-com-chain,zksync,mantle,celo,moonbeam,aptos,cosmos,near,stellar,algorand,blast"
    )
    async with session.get(
        "https://api.coingecko.com/api/v3/simple/price",
        params={"ids": ids, "vs_currencies": "usd"},
        timeout=aiohttp.ClientTimeout(total=10),
    ) as r:
        d = await r.json(content_type=None)
    pol_price = d.get("polygon-ecosystem-token", {}).get("usd", 0)
    return {
        "ETH":   d.get("ethereum",          {}).get("usd", 0),
        "BNB":   d.get("binancecoin",       {}).get("usd", 0),
        "POL":   pol_price,
        "MATIC": pol_price,
        "BTC":   d.get("bitcoin",           {}).get("usd", 0),
        "LTC":   d.get("litecoin",          {}).get("usd", 0),
        "TRX":   d.get("tron",              {}).get("usd", 0),
        "TON":   d.get("the-open-network",  {}).get("usd", 0),
        "USDT":  1.0,
        "USDC":  d.get("usd-coin",          {}).get("usd", 1.0),
        "DAI":   d.get("dai",               {}).get("usd", 1.0),
        "SOL":   d.get("solana",            {}).get("usd", 0),
        "DOGE":  d.get("dogecoin",          {}).get("usd", 0),
        "ARB":   d.get("arbitrum",          {}).get("usd", 0),
        "OP":    d.get("optimism",          {}).get("usd", 0),
        "AVAX":  d.get("avalanche-2",       {}).get("usd", 0),
        "XRP":   d.get("ripple",            {}).get("usd", 0),
        "ADA":   d.get("cardano",           {}).get("usd", 0),
        "BCH":   d.get("bitcoin-cash",      {}).get("usd", 0),
        "SUI":   d.get("sui",               {}).get("usd", 0),
        "KAS":   d.get("kaspa",             {}).get("usd", 0),
        "AERO":  d.get("aerodrome-finance", {}).get("usd", 0),
        "FTM":   d.get("fantom",            {}).get("usd", 0),
        "CRO":   d.get("crypto-com-chain",  {}).get("usd", 0),
        "ZK":    d.get("zksync",            {}).get("usd", 0),
        "MNT":   d.get("mantle",            {}).get("usd", 0),
        "CELO":  d.get("celo",              {}).get("usd", 0),
        "GLMR":  d.get("moonbeam",          {}).get("usd", 0),
        "APT":   d.get("aptos",             {}).get("usd", 0),
        "ATOM":  d.get("cosmos",            {}).get("usd", 0),
        "NEAR":  d.get("near",              {}).get("usd", 0),
        "XLM":   d.get("stellar",           {}).get("usd", 0),
        "ALGO":  d.get("algorand",          {}).get("usd", 0),
        "BLAST": d.get("blast",             {}).get("usd", 0),
    }


async def _prices_coinbase(session: aiohttp.ClientSession) -> dict[str, float]:
    coins = {
        "BTC": "BTC", "ETH": "ETH", "BNB": "BNB", "LTC": "LTC", "TRX": "TRX", "TON": "TON",
        "POL": "POL", "USDC": "USDC", "DAI": "DAI", "SOL": "SOL", "DOGE": "DOGE",
        "ARB": "ARB", "OP": "OP", "AVAX": "AVAX", "XRP": "XRP", "ADA": "ADA", "BCH": "BCH",
        "SUI": "SUI", "FTM": "FTM", "CRO": "CRO", "CELO": "CELO", "APT": "APT", "ATOM": "ATOM",
        "NEAR": "NEAR", "XLM": "XLM", "ALGO": "ALGO"
    }
    out: dict[str, float] = {"USDT": 1.0, "USDC": 1.0, "DAI": 1.0}

    async def _fetch_one(coin_code, api_symbol):
        try:
            url = f"https://api.coinbase.com/v2/prices/{api_symbol}-USD/spot"
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
                if r.status == 200:
                    d = await r.json(content_type=None)
                    val = float(d.get("data", {}).get("amount", 0))
                    if val > 0:
                        out[coin_code] = val
                        if coin_code == "POL":
                            out["MATIC"] = val
        except Exception:
            pass

    await asyncio.gather(*[_fetch_one(k, v) for k, v in coins.items()])
    return out


async def _prices_binance(session: aiohttp.ClientSession) -> dict[str, float]:
    sym_map = {
        "ETHUSDT": "ETH", "BNBUSDT": "BNB", "BTCUSDT": "BTC",
        "LTCUSDT": "LTC", "TRXUSDT": "TRX", "TONUSDT": "TON",
        "POLUSDT": "POL", "USDCUSDT": "USDC", "DAIUSDT": "DAI",
        "SOLUSDT": "SOL", "DOGEUSDT": "DOGE", "ARBUSDT": "ARB",
        "OPUSDT": "OP", "AVAXUSDT": "AVAX", "XRPUSDT": "XRP",
        "ADAUSDT": "ADA", "BCHUSDT": "BCH", "SUIUSDT": "SUI",
        "KASUSDT": "KAS", "FTMUSDT": "FTM", "CROUSDT": "CRO",
        "ZKUSDT": "ZK", "MNTUSDT": "MNT", "CELOUSDT": "CELO",
        "GLMRUSDT": "GLMR", "APTUSDT": "APT", "ATOMUSDT": "ATOM",
        "NEARUSDT": "NEAR", "XLMUSDT": "XLM", "ALGOUSDT": "ALGO"
    }
    symbols = list(sym_map.keys())
    async with session.get(
        "https://api.binance.com/api/v3/ticker/price",
        params={"symbols": str(symbols).replace("'", '"').replace(" ", "")},
        timeout=aiohttp.ClientTimeout(total=10),
    ) as r:
        data = await r.json(content_type=None)
    out: dict[str, float] = {"USDT": 1.0, "USDC": 1.0, "DAI": 1.0}
    for item in data:
        key = sym_map.get(item.get("symbol", ""))
        if key:
            val = float(item.get("price", 0))
            out[key] = val
            if key == "POL":
                out["MATIC"] = val
    return out


async def fetch_prices(session: aiohttp.ClientSession) -> dict[str, float]:
    sources = await asyncio.gather(
        _prices_coingecko(session),
        _prices_coinbase(session),
        _prices_binance(session),
        return_exceptions=True,
    )
    merged: dict[str, float] = {"USDT": 1.0}
    for res in sources:
        if isinstance(res, Exception):
            continue
        for k in ALL_PRICE_KEYS:
            if merged.get(k, 0) == 0:
                v = res.get(k, 0)
                if v and v > 0:
                    merged[k] = v
    for k in ALL_PRICE_KEYS:
        merged.setdefault(k, 0.0)
    return merged


async def ensure_prices(session: aiohttp.ClientSession):
    global PRICES, _prices_fetched_at
    if time.monotonic() - _prices_fetched_at > PRICE_REFRESH_SECONDS or not PRICES:
        PRICES = await fetch_prices(session)
        _prices_fetched_at = time.monotonic()


# ═══════════════════════════════════════════════════════════════════════════════
#  ASYNC RPC HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def _rpc_one(session: aiohttp.ClientSession, url: str,
                   method: str, params: list, timeout: int = 8) -> any:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(url, json=payload,
                            timeout=aiohttp.ClientTimeout(total=timeout)) as r:
        data = await r.json(content_type=None)
    if "result" in data:
        return data["result"]
    raise ApiError(data.get("error", "no result"))


async def rpc_race(session: aiohttp.ClientSession, chain: str,
                   method: str, params: list, timeout: int = 8) -> any:
    tasks = [
        asyncio.create_task(_rpc_one(session, url, method, params, timeout))
        for url in RPC_ENDPOINTS[chain]
    ]
    errors = []
    for coro in asyncio.as_completed(tasks):
        try:
            result = await coro
            for t in tasks:
                t.cancel()
            return result
        except Exception as e:
            errors.append(str(e))
    raise ApiError(f"all RPCs failed for {chain}/{method}: {errors[-1] if errors else 'unknown'}")


# ═══════════════════════════════════════════════════════════════════════════════
#  EVM HELPERS (15 Chains)
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_evm_address(address: str) -> str:
    addr = address.strip()
    if addr.lower().startswith("0x"):
        addr = addr[2:]
    addr = addr.lower()
    if len(addr) != 40 or any(c not in "0123456789abcdef" for c in addr):
        raise ValueError(f"'{address}' is not a valid EVM address")
    return "0x" + addr


async def get_native_balance(session, address, chain) -> float:
    res = await rpc_race(session, chain, "eth_getBalance", [address, "latest"])
    return int(res, 16) / 1e18


async def get_erc20_balance(session, address, chain, contract_address, decimals) -> float:
    padded     = address[2:].rjust(64, "0")
    data_field = "0x70a08231" + padded
    res = await rpc_race(session, chain, "eth_call",
                         [{"to": contract_address, "data": data_field}, "latest"])
    if not res or res == "0x":
        return 0.0
    return int(res, 16) / (10 ** decimals)


async def check_evm_chain(session, addr, chain) -> dict:
    info   = CHAIN_INFO[chain]
    result = {
        "chain": chain, "name": info["name"],
        "native_sym": info["native"],
        "native": 0.0,
        "tokens": [],
        "errors": [],
    }

    native_task = asyncio.create_task(get_native_balance(session, addr, chain))
    tokens_to_check = EVM_TOKENS.get(chain, [])
    token_tasks = [
        asyncio.create_task(get_erc20_balance(session, addr, chain, token["contract"], token["decimals"]))
        for token in tokens_to_check
    ]

    await asyncio.gather(native_task, *token_tasks, return_exceptions=True)

    try:
        result["native"] = native_task.result()
    except Exception as e:
        result["errors"].append(f"{info['native']}: {e}")

    for token, task in zip(tokens_to_check, token_tasks):
        try:
            bal = task.result()
            if bal > 0.000001:
                result["tokens"].append({
                    "symbol": token["symbol"],
                    "balance": bal,
                    "price_key": token["price_key"],
                })
        except Exception as e:
            result["errors"].append(f"{token['symbol']}: {e}")

    return result


async def format_evm_wallet(session, raw_address):
    try:
        addr = normalize_evm_address(raw_address)
    except ValueError as e:
        return [f"Skipping '{raw_address}': {e}"], 0.0

    lines = [
        f"Address : {addr}",
        f"Type    : EVM Multi-Chain (15 Networks)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
    ]

    evm_chains = list(RPC_ENDPOINTS.keys())
    chain_results = await asyncio.gather(*[check_evm_chain(session, addr, c) for c in evm_chains])

    native_key = {
        "eth": "ETH", "bsc": "BNB", "polygon": "POL", "arbitrum": "ETH",
        "optimism": "ETH", "base": "ETH", "avalanche": "AVAX", "fantom": "FTM",
        "cronos": "CRO", "zksync": "ETH", "linea": "ETH", "blast": "ETH",
        "mantle": "MNT", "celo": "CELO", "moonbeam": "GLMR"
    }
    grand_totals = {}
    grand_usd  = 0.0
    any_error  = False

    for r in chain_results:
        chain        = r["chain"]
        np           = PRICES.get(native_key[chain], 0.0)
        native_usd   = r["native"] * np
        chain_usd    = native_usd

        if r["native"] > 0.000001 or r["tokens"]:
            grand_totals[native_key[chain]] = grand_totals.get(native_key[chain], 0.0) + r["native"]

            lines.append("")
            lines.append(f"[{r['name']}]")
            lines.append(f"  {r['native_sym']:<10}: {r['native']:.8f}  (~${native_usd:.2f})")

            for t in r["tokens"]:
                tp = PRICES.get(t["price_key"], 0.0)
                t_usd = t["balance"] * tp
                chain_usd += t_usd
                grand_totals[t["symbol"]] = grand_totals.get(t["symbol"], 0.0) + t["balance"]
                lines.append(f"  {t['symbol']:<10}: {t['balance']:.6f}  (~${t_usd:.2f})")

            lines.append(f"  Chain USD : ${chain_usd:.2f}")

        grand_usd += chain_usd

    lines.append("")
    lines.append("─" * 38)
    for sym, tot in sorted(grand_totals.items()):
        if tot > 0.000001:
            lines.append(f"TOTAL {sym:<7} : {tot:.6f}")

    lines.append(f"TOTAL USD  : ${grand_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if grand_usd > 0.01 else "No funds detected")

    return lines, grand_usd


# ═══════════════════════════════════════════════════════════════════════════════
#  SOLANA HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

async def sol_rpc_race(session: aiohttp.ClientSession, method: str, params: list) -> any:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    for url in SOL_RPC_ENDPOINTS:
        try:
            async with session.post(url, json=payload, headers=headers, timeout=aiohttp.ClientTimeout(total=8)) as r:
                data = await r.json(content_type=None)
                if "result" in data:
                    return data["result"]
        except Exception:
            continue
    raise ApiError("all Solana RPCs failed")


async def format_sol_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Solana (SOL + SPL Tokens)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    sol_bal  = 0.0
    tokens   = []
    total_usd = 0.0

    async def _sol_balance():
        res = await sol_rpc_race(session, "getBalance", [address])
        return (res.get("value", 0) or 0) / 1e9

    async def _spl_tokens():
        res = await sol_rpc_race(session, "getTokenAccountsByOwner", [
            address,
            {"programId": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
            {"encoding": "jsonParsed"}
        ])
        parsed_tokens = []
        for account in res.get("value", []):
            try:
                info = account["account"]["data"]["parsed"]["info"]
                mint = info["mint"]
                token_amount = info["tokenAmount"]
                ui_amount = token_amount.get("uiAmount", 0) or 0
                if ui_amount > 0:
                    sym = "SPL"
                    if mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v":
                        sym = "USDC"
                    elif mint == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB":
                        sym = "USDT"
                    elif mint == "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263":
                        sym = "BONK"
                    parsed_tokens.append((sym, ui_amount))
            except Exception:
                continue
        return parsed_tokens

    bal_task, tokens_task = await asyncio.gather(_sol_balance(), _spl_tokens(), return_exceptions=True)

    if not isinstance(bal_task, Exception):
        sol_bal = bal_task
        sol_usd = sol_bal * PRICES.get("SOL", 0.0)
        total_usd += sol_usd
        lines.append(f"SOL        : {sol_bal:.6f}  (~${sol_usd:.2f})")
    else:
        lines.append(f"SOL Balance Error: {bal_task}")

    if not isinstance(tokens_task, Exception) and tokens_task:
        for sym, amt in tokens_task:
            price = PRICES.get(sym, 1.0 if sym in ("USDC", "USDT") else 0.0)
            t_usd = amt * price
            total_usd += t_usd
            lines.append(f"{sym:<10}: {amt:.6f}  (~${t_usd:.2f})")

    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL SOL  : {sol_bal:.6f}")
    lines.append(f"TOTAL USD  : ${total_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if total_usd > 0.01 else "No SOL balance")

    return lines, total_usd


# ═══════════════════════════════════════════════════════════════════════════════
#  ADDITIONAL NON-EVM CHAINS (APT, ATOM, NEAR, XLM, ALGO, DOGE, XRP, ADA, BCH, SUI, KAS)
# ═══════════════════════════════════════════════════════════════════════════════

async def format_apt_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Aptos (APT)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    try:
        url = f"https://fullnode.mainnet.aptoslabs.com/v1/accounts/{address}/resources"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                for res in data:
                    if res.get("type") == "0x1::coin::CoinStore<0x1::aptos_coin::AptosCoin>":
                        balance = int(res.get("data", {}).get("coin", {}).get("value", 0)) / 1e8
                        break
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("APT", 0.0)
    lines.append(f"APT        : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL APT  : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No APT balance")

    return lines, balance_usd


async def format_atom_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Cosmos Hub (ATOM)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    try:
        url = f"https://cosmos-rest.publicnode.com/cosmos/bank/v1beta1/balances/{address}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                for b in d.get("balances", []):
                    if b.get("denom") == "uatom":
                        balance = int(b.get("amount", 0)) / 1e6
                        break
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("ATOM", 0.0)
    lines.append(f"ATOM       : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL ATOM : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No ATOM balance")

    return lines, balance_usd


async def format_near_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Near Protocol (NEAR)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "query",
        "params": {"request_type": "view_account", "finality": "final", "account_id": address}
    }
    try:
        async with session.post("https://rpc.mainnet.near.org", json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json(content_type=None)
            if "result" in d:
                balance = int(d["result"].get("amount", 0)) / 1e24
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("NEAR", 0.0)
    lines.append(f"NEAR       : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL NEAR : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No NEAR balance")

    return lines, balance_usd


async def format_xlm_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Stellar (XLM)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    try:
        url = f"https://horizon.stellar.org/accounts/{address}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                for b in d.get("balances", []):
                    if b.get("asset_type") == "native":
                        balance = float(b.get("balance", 0))
                        break
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("XLM", 0.0)
    lines.append(f"XLM        : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL XLM  : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No XLM balance")

    return lines, balance_usd


async def format_algo_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Algorand (ALGO)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    try:
        url = f"https://mainnet-api.algonode.cloud/v2/accounts/{address}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                balance = int(d.get("account", {}).get("amount", 0)) / 1e6
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("ALGO", 0.0)
    lines.append(f"ALGO       : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL ALGO : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No ALGO balance")

    return lines, balance_usd


async def format_doge_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : DOGE / Dogecoin",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    try:
        url = f"https://api.blockcypher.com/v1/doge/main/addrs/{address}/balance"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                balance = d.get("balance", 0) / 1e8
            else:
                url_alt = f"https://dogechain.info/api/v1/address/balance/{address}"
                async with session.get(url_alt, timeout=aiohttp.ClientTimeout(total=10)) as r2:
                    d2 = await r2.json(content_type=None)
                    balance = float(d2.get("balance", 0))
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("DOGE", 0.0)
    lines.append(f"DOGE       : {balance:.8f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL DOGE : {balance:.8f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No DOGE balance")

    return lines, balance_usd


async def format_xrp_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Ripple (XRP)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    payload = {"method": "account_info", "params": [{"account": address, "ledger_index": "validated"}]}
    endpoints = ["https://s1.ripple.com:51234/", "https://xrplcluster.com"]
    for ep in endpoints:
        try:
            async with session.post(ep, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
                d = await r.json(content_type=None)
                if "result" in d and "account_data" in d["result"]:
                    balance = int(d["result"]["account_data"]["Balance"]) / 1e6
                    break
        except Exception:
            continue

    balance_usd = balance * PRICES.get("XRP", 0.0)
    lines.append(f"XRP        : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL XRP  : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No XRP balance")

    return lines, balance_usd


async def format_ada_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Cardano (ADA)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    try:
        url = "https://api.koios.rest/api/v1/address_info"
        async with session.post(url, json={"_addresses": [address]}, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                if data and isinstance(data, list):
                    balance = int(data[0].get("balance", 0)) / 1e6
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("ADA", 0.0)
    lines.append(f"ADA        : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL ADA  : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No ADA balance")

    return lines, balance_usd


async def format_bch_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Bitcoin Cash (BCH)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    clean_addr  = address.replace("bitcoincash:", "")
    try:
        url = f"https://api.blockchair.com/bitcoin-cash/dashboards/address/{clean_addr}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                addr_data = d.get("data", {}).get(clean_addr, {}).get("address", {})
                balance = addr_data.get("balance", 0) / 1e8
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("BCH", 0.0)
    lines.append(f"BCH        : {balance:.8f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL BCH  : {balance:.8f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No BCH balance")

    return lines, balance_usd


async def format_kas_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Kaspa (KAS)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    try:
        url = f"https://api.kaspa.org/addresses/{address}/balance"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                balance = int(d.get("balance", 0)) / 1e8
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("KAS", 0.0)
    lines.append(f"KAS        : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL KAS  : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No KAS balance")

    return lines, balance_usd


async def format_sui_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : Sui Network (SUI)",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = 0.0
    balance_usd = 0.0
    payload = {"jsonrpc": "2.0", "id": 1, "method": "suix_getBalance", "params": [address, "0x2::sui::SUI"]}
    try:
        async with session.post("https://fullnode.mainnet.sui.io:443", json=payload, timeout=aiohttp.ClientTimeout(total=8)) as r:
            d = await r.json(content_type=None)
            if "result" in d:
                balance = int(d["result"].get("totalBalance", 0)) / 1e9
    except Exception as e:
        lines.append(f"ERROR: {e}")

    balance_usd = balance * PRICES.get("SUI", 0.0)
    lines.append(f"SUI        : {balance:.6f}  (~${balance_usd:.2f})")
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL SUI  : {balance:.6f}")
    lines.append(f"TOTAL USD  : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance_usd > 0.01 else "No SUI balance")

    return lines, balance_usd


async def format_btc_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : BTC",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = None
    balance_usd = 0.0

    async def _bal():
        async with session.get(
            f"https://blockstream.info/api/address/{address}",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            d = await r.json(content_type=None)
        s = d.get("chain_stats", {})
        return (s.get("funded_txo_sum", 0) - s.get("spent_txo_sum", 0)) / 1e8

    async def _txs():
        async with session.get(
            f"https://blockstream.info/api/address/{address}/txs",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return await r.json(content_type=None)

    bal_task, txs_task = await asyncio.gather(_bal(), _txs(), return_exceptions=True)

    if isinstance(bal_task, Exception):
        lines.append(f"BTC: ERROR ({bal_task})")
    else:
        balance     = bal_task
        balance_usd = balance * PRICES.get("BTC", 0)
        lines.append(f"BTC: {balance:.8f}  (~${balance_usd:.2f})")

    if isinstance(txs_task, Exception):
        lines.append(f"Recent txs: ERROR ({txs_task})")
    else:
        txs = txs_task
        if isinstance(txs, list) and txs:
            lines.append("")
            lines.append("Recent transactions:")
            for tx in txs[:5]:
                txid       = tx.get("txid", "")
                block_time = tx.get("status", {}).get("block_time")
                when       = (datetime.fromtimestamp(block_time, tz=timezone.utc)
                              .strftime("%Y-%m-%d %H:%M UTC") if block_time else "unconfirmed")
                received   = sum(o["value"] for o in tx.get("vout", [])
                                 if o.get("scriptpubkey_address") == address) / 1e8
                lines.append(f"  tx:{txid[:12]}... +{received:.8f} BTC  {when}")
        else:
            lines.append("No recent transactions")

    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL BTC : {balance or 0:.8f}")
    lines.append(f"TOTAL USD : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance and balance > 0 else "No BTC balance")

    return lines, balance_usd


async def format_ltc_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : LTC",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = None
    balance_usd = 0.0

    async def _bal():
        async with session.get(
            f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}/balance",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return (await r.json(content_type=None)).get("balance", 0) / 1e8

    async def _txs():
        async with session.get(
            f"https://api.blockcypher.com/v1/ltc/main/addrs/{address}",
            params={"limit": 5},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            d = await r.json(content_type=None)
        return (d.get("txrefs") or []) + (d.get("unconfirmed_txrefs") or [])

    bal_task, txs_task = await asyncio.gather(_bal(), _txs(), return_exceptions=True)

    if isinstance(bal_task, Exception):
        lines.append(f"LTC: ERROR ({bal_task})")
    else:
        balance     = bal_task
        balance_usd = balance * PRICES.get("LTC", 0)
        lines.append(f"LTC: {balance:.8f}  (~${balance_usd:.2f})")

    if isinstance(txs_task, Exception):
        lines.append(f"Recent txs: ERROR ({txs_task})")
    else:
        txrefs = txs_task
        if txrefs:
            lines.append("")
            lines.append("Recent transactions:")
            for tx in txrefs[:5]:
                value     = tx.get("value", 0) / 1e8
                direction = "received" if tx.get("tx_input_n", -1) == -1 else "sent"
                confirmed = tx.get("confirmed", "unconfirmed")
                lines.append(f"  tx:{tx.get('tx_hash','')[:12]}... {direction} {value:.8f} LTC  {confirmed}")
        else:
            lines.append("No recent transactions")

    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL LTC : {balance or 0:.8f}")
    lines.append(f"TOTAL USD : ${balance_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if balance and balance > 0 else "No LTC balance")

    return lines, balance_usd


async def format_trx_wallet(session, address):
    lines = [
        f"Address : {address}",
        f"Type    : TRX / Tron",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    trx_bal = 0.0
    usdt    = 0.0
    usdc    = 0.0

    async def _account():
        async with session.get(
            f"https://api.trongrid.io/v1/accounts/{address}",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return await r.json(content_type=None)

    async def _txs(contract_address):
        async with session.get(
            f"https://api.trongrid.io/v1/accounts/{address}/transactions/trc20",
            params={"contract_address": contract_address, "limit": 5, "only_to": "true"},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return (await r.json(content_type=None)).get("data", [])

    acc_task = asyncio.create_task(_account())
    await asyncio.gather(acc_task, return_exceptions=True)

    try:
        d = acc_task.result()
        accounts = d.get("data", [])
        if accounts:
            acc     = accounts[0]
            trx_bal = acc.get("balance", 0) / 1e6
            for token in acc.get("trc20", []):
                if TRX_USDT_CONTRACT in token:
                    usdt = int(token[TRX_USDT_CONTRACT]) / 1e6
                if TRX_USDC_CONTRACT in token:
                    usdc = int(token[TRX_USDC_CONTRACT]) / 1e6
    except Exception as e:
        lines.append(f"ERROR: {e}")

    trx_usd  = trx_bal * PRICES.get("TRX", 0.0)
    usdt_usd = usdt    * PRICES.get("USDT", 1.0)
    usdc_usd = usdc    * PRICES.get("USDC", 1.0)
    total_usd = trx_usd + usdt_usd + usdc_usd

    lines.append(f"TRX        : {trx_bal:.6f}  (~${trx_usd:.2f})")
    if usdt > 0:
        lines.append(f"USDT(TRC20): {usdt:.6f}  (~${usdt_usd:.2f})")
    if usdc > 0:
        lines.append(f"USDC(TRC20): {usdc:.6f}  (~${usdc_usd:.2f})")

    tx_tasks = {}
    if usdt > 0:
        tx_tasks["USDT"] = asyncio.create_task(_txs(TRX_USDT_CONTRACT))
    if usdc > 0:
        tx_tasks["USDC"] = asyncio.create_task(_txs(TRX_USDC_CONTRACT))

    if tx_tasks:
        await asyncio.gather(*tx_tasks.values(), return_exceptions=True)
        for name, task in tx_tasks.items():
            try:
                txs = task.result()
                if txs:
                    lines.append("")
                    lines.append(f"Recent incoming {name}:")
                    for tx in txs:
                        decimals = int(tx.get("token_info", {}).get("decimals", 6))
                        amount   = int(tx["value"]) / (10 ** decimals)
                        ts       = datetime.fromtimestamp(tx["block_timestamp"] / 1000, tz=timezone.utc)
                        lines.append(f"  +{amount:.6f} {name} from {tx['from']} "
                                     f"{ts.strftime('%Y-%m-%d %H:%M UTC')} tx:{tx['transaction_id'][:12]}...")
                else:
                    lines.append(f"No recent incoming {name}")
            except Exception as e:
                lines.append(f"Recent {name} txs error: {e}")

    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL TRX  : {trx_bal:.6f}")
    if usdt > 0:
        lines.append(f"TOTAL USDT : {usdt:.6f}")
    if usdc > 0:
        lines.append(f"TOTAL USDC : {usdc:.6f}")
    lines.append(f"TOTAL USD  : ${total_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if total_usd > 0.01 else "No funds detected")

    return lines, total_usd


async def format_ton_wallet(session, address):
    try:
        raw_addr = normalize_ton_address(address)
    except Exception as e:
        return [f"Skipping '{address}': {e}"], 0.0

    lines = [
        f"Address : {address}",
        f"Type    : TON",
        f"Checked : {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "",
    ]
    balance     = None
    balance_usd = 0.0
    jetton_usd  = 0.0
    jetton_lines = []

    async def _bal():
        async with session.get(
            "https://toncenter.com/api/v2/getAddressBalance",
            params={"address": raw_addr},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return int((await r.json(content_type=None)).get("result", 0)) / 1e9

    async def _txs():
        async with session.get(
            "https://toncenter.com/api/v2/getTransactions",
            params={"address": raw_addr, "limit": 5},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return (await r.json(content_type=None)).get("result", [])

    async def _jettons():
        async with session.get(
            f"https://tonapi.io/v2/accounts/{raw_addr}/jettons",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            if r.status == 200:
                return await r.json(content_type=None)
            return None

    bal_task, txs_task, jettons_task = await asyncio.gather(
        _bal(), _txs(), _jettons(), return_exceptions=True
    )

    if isinstance(bal_task, Exception):
        lines.append(f"TON: ERROR ({bal_task})")
    else:
        balance     = bal_task
        balance_usd = balance * PRICES.get("TON", 0.0)
        lines.append(f"TON        : {balance:.9f}  (~${balance_usd:.2f})")

    balances = []
    if not isinstance(jettons_task, Exception) and jettons_task:
        balances = jettons_task.get("balances", [])
        for item in balances:
            bal_raw = int(item.get("balance", "0"))
            if bal_raw > 0:
                jetton_meta = item.get("jetton", {})
                symbol = jetton_meta.get("symbol", "UNKNOWN").upper()
                decimals = int(jetton_meta.get("decimals", 9))
                amount = bal_raw / (10 ** decimals)

                price = 0.0
                if symbol in ("USDT", "USD", "USDC"):
                    price = 1.0
                else:
                    price = PRICES.get(symbol, 0.0)
                    if price == 0.0:
                        price = float(item.get("price", {}).get("prices", {}).get("USD", 0.0))

                usd_val = amount * price
                jetton_usd += usd_val

                price_str = f"  (~${usd_val:.2f})" if price > 0 else ""
                jetton_lines.append(f"  {symbol:<10}: {amount:.6f}{price_str}")

    if jetton_lines:
        lines.extend(jetton_lines)

    if isinstance(txs_task, Exception):
        lines.append(f"Recent txs: ERROR ({txs_task})")
    else:
        shown = []
        for tx in txs_task:
            in_msg = tx.get("in_msg", {}) or {}
            value  = int(in_msg.get("value", 0) or 0) / 1e9
            src    = in_msg.get("source", "") or "?"
            utime  = tx.get("utime")
            when   = (datetime.fromtimestamp(utime, tz=timezone.utc)
                      .strftime("%Y-%m-%d %H:%M UTC") if utime else "?")
            if value > 0:
                shown.append(f"  +{value:.9f} TON from {src}  {when}")
        if shown:
            lines.append("")
            lines.append("Recent transactions:")
            lines.extend(shown)
        else:
            lines.append("No recent incoming transactions")

    total_usd = balance_usd + jetton_usd
    lines.append("")
    lines.append("─" * 38)
    lines.append(f"TOTAL TON  : {balance or 0:.9f}")
    if balances:
        for item in balances:
            bal_raw = int(item.get("balance", "0"))
            if bal_raw > 0:
                symbol = item.get("jetton", {}).get("symbol", "UNKNOWN").upper()
                decimals = int(item.get("jetton", {}).get("decimals", 9))
                amount = bal_raw / (10 ** decimals)
                lines.append(f"TOTAL {symbol:<5}: {amount:.6f}")

    lines.append(f"TOTAL USD  : ${total_usd:.2f}")
    lines.append("─" * 38)
    lines.append("✓ FUNDS FOUND" if total_usd > 0.01 else "No TON balance")

    return lines, balance_usd


def friendly_to_raw(addr_str: str) -> str:
    normalized = addr_str.replace('-', '+').replace('_', '/')
    padding = '=' * ((4 - len(normalized) % 4) % 4)
    try:
        decoded = base64.b64decode(normalized + padding)
    except Exception:
        raise ValueError("Invalid friendly address encoding")

    if len(decoded) != 36:
        raise ValueError("Invalid friendly address length")

    data = decoded[:34]
    crc_expected = struct.unpack(">H", decoded[34:])[0]

    crc = 0
    for byte in data:
        crc ^= (byte << 8)
        for _ in range(8):
            if crc & 0x8000:
                crc = (crc << 1) ^ 0x1021
            else:
                crc = crc << 1
            crc &= 0xffff

    if crc != crc_expected:
        raise ValueError("CRC checksum mismatch")

    workchain = decoded[1]
    workchain_id = workchain if workchain < 128 else workchain - 256
    account_id = decoded[2:34].hex()
    return f"{workchain_id}:{account_id}"


def normalize_ton_address(address: str) -> str:
    addr = address.strip()
    if re.match(r"^-?\d+:[0-9a-fA-F]{64}$", addr):
        return addr
    return friendly_to_raw(addr)


# ═══════════════════════════════════════════════════════════════════════════════
#  ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

def detect_address_type(address: str) -> str:
    addr = address.strip()
    # Cosmos (cosmos1...)
    if addr.startswith("cosmos1"):
        return "atom"
    # Stellar (G... 56 chars)
    if re.match(r"^G[A-Z2-7]{55}$", addr):
        return "xlm"
    # Algorand (58 uppercase chars)
    if re.match(r"^[A-Z2-7]{58}$", addr):
        return "algo"
    # Near (.near)
    if addr.endswith(".near"):
        return "near"
    # Sui / Aptos / Near 64-hex chars starting with 0x
    if re.match(r"^0x[0-9a-fA-F]{64}$", addr):
        return "sui_apt"
    # EVM (40 hex characters starting with 0x)
    if re.match(r"^0x[0-9a-fA-F]{40}$", addr):
        return "evm"
    # Tron (34 chars starting with T)
    if re.match(r"^T[1-9A-HJ-NP-Za-km-z]{33}$", addr):
        return "trx"
    # Dogecoin (34 chars starting with D)
    if re.match(r"^D[1-9A-HJ-NP-Za-km-z]{32,33}$", addr):
        return "doge"
    # Ripple XRP (24-34 chars starting with r)
    if re.match(r"^r[0-9a-zA-Z]{24,34}$", addr):
        return "xrp"
    # Cardano (addr1...)
    if re.match(r"^addr1[a-z0-9]{50,100}$", addr):
        return "ada"
    # Bitcoin Cash
    if addr.startswith("bitcoincash:") or re.match(r"^[qp][a-z0-9]{41}$", addr):
        return "bch"
    # Kaspa (kaspa:...)
    if addr.startswith("kaspa:"):
        return "kas"
    # TON (48 chars base64url or raw format)
    if re.match(r"^[A-Za-z0-9_-]{48}$", addr) or re.match(r"^-?\d+:[0-9a-fA-F]{64}$", addr):
        return "ton"
    # Bitcoin
    if addr.lower().startswith("bc1") or re.match(r"^[13][a-zA-HJ-NP-Z0-9]{25,34}$", addr):
        return "btc"
    # Litecoin
    if addr.lower().startswith("ltc1") or re.match(r"^[LM][a-zA-HJ-NP-Z0-9]{25,34}$", addr):
        return "ltc"
    # Solana (Base58 32-44 chars)
    if re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", addr):
        return "sol"
    return "unknown"


async def format_wallet(session, raw_address) -> tuple[list[str], float] | None:
    addr      = raw_address.strip()
    addr_type = detect_address_type(addr)

    if addr_type == "evm":
        return await format_evm_wallet(session, addr)
    if addr_type == "sol":
        return await format_sol_wallet(session, addr)
    if addr_type == "doge":
        return await format_doge_wallet(session, addr)
    if addr_type == "xrp":
        return await format_xrp_wallet(session, addr)
    if addr_type == "ada":
        return await format_ada_wallet(session, addr)
    if addr_type == "bch":
        return await format_bch_wallet(session, addr)
    if addr_type == "kas":
        return await format_kas_wallet(session, addr)
    if addr_type == "sui_apt":
        # Check Sui & Aptos in parallel
        sui_res, apt_res = await asyncio.gather(
            format_sui_wallet(session, addr),
            format_apt_wallet(session, addr),
            return_exceptions=True
        )
        lines = []
        tot_usd = 0.0
        if not isinstance(sui_res, Exception) and sui_res:
            l, u = sui_res
            lines.extend(l)
            tot_usd += u
        if not isinstance(apt_res, Exception) and apt_res:
            l, u = apt_res
            if lines:
                lines.append("")
            lines.extend(l)
            tot_usd += u
        return lines, tot_usd
    if addr_type == "atom":
        return await format_atom_wallet(session, addr)
    if addr_type == "near":
        return await format_near_wallet(session, addr)
    if addr_type == "xlm":
        return await format_xlm_wallet(session, addr)
    if addr_type == "algo":
        return await format_algo_wallet(session, addr)
    if addr_type == "trx":
        return await format_trx_wallet(session, addr)
    if addr_type == "ton":
        return await format_ton_wallet(session, addr)
    if addr_type == "btc":
        return await format_btc_wallet(session, addr)
    if addr_type == "ltc":
        return await format_ltc_wallet(session, addr)
    return None


def find_addresses(text: str) -> list[str]:
    found = []
    for token in re.split(r"\s+", text):
        cleaned = token.strip(".,!?;:()[]{}'\"<>")
        if cleaned and detect_address_type(cleaned) != "unknown" and cleaned not in found:
            found.append(cleaned)
    return found


def chunk_message(text: str, limit: int = 4000) -> list[str]:
    chunks = []
    while len(text) > limit:
        idx = text.rfind("\n", 0, limit)
        if idx == -1:
            idx = limit
        chunks.append(text[:idx])
        text = text[idx:].lstrip("\n")
    chunks.append(text)
    return chunks


# ═══════════════════════════════════════════════════════════════════════════════
#  UI HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def get_referral_link(user_id: int) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={user_id}"


def build_start_message(user_id: int, credits: int) -> str:
    return (
        "👋 Welcome to Wallet Balance Checker!\n\n"
        f"💳 Credits Remaining: {credits}\n\n"
        "⚡ Quick Start:\n"
        "Just paste any personal crypto wallet address into the chat to check balances & USD value instantly.\n\n"
        "ℹ️ Type /help to view all 25 supported blockchain networks.\n"
        "🔗 Type /refer to get your referral link & earn free credits."
    )


def build_no_credits_message(user_id: int) -> str:
    ref_link = get_referral_link(user_id)
    return (
        "No credits remaining.\n\n"
        "Invite friends to earn free credits.\n"
        f"You get +{REFERRAL_REWARD_CREDITS} credits for every person who joins through your link.\n\n"
        f"`{ref_link}`\n\n"
        "/refer to see your link again."
    )


# ═══════════════════════════════════════════════════════════════════════════════
#  ANIMATION
# ═══════════════════════════════════════════════════════════════════════════════

async def send_typing_text(event, full_text: str, words_per_step: int = 7, step_delay: float = 0.07):
    word_ends = [m.end() for m in re.finditer(r"\S+", full_text)]
    if not word_ends:
        return await event.reply(full_text)
    sent    = await event.reply("▍")
    indices = list(range(words_per_step - 1, len(word_ends), words_per_step))
    if not indices or indices[-1] != len(word_ends) - 1:
        indices.append(len(word_ends) - 1)
    for step, wi in enumerate(indices):
        pos     = word_ends[wi]
        is_last = step == len(indices) - 1
        display = full_text[:pos] if is_last else full_text[:pos] + " ▍"
        try:
            await sent.edit(display)
        except Exception:
            pass
        if not is_last:
            await asyncio.sleep(step_delay)
    return sent


async def send_typing_code(event, full_text: str, words_per_step: int = 6, step_delay: float = 0.10, sent_message=None):
    word_ends = [m.end() for m in re.finditer(r"\S+", full_text)]
    if not word_ends:
        if sent_message:
            await sent_message.edit(f"```\n{full_text}\n```")
            return sent_message
        return await event.reply(f"```\n{full_text}\n```")
    sent    = sent_message or await event.reply("`...`")
    indices = list(range(words_per_step - 1, len(word_ends), words_per_step))
    if not indices or indices[-1] != len(word_ends) - 1:
        indices.append(len(word_ends) - 1)
    for step, wi in enumerate(indices):
        pos     = word_ends[wi]
        is_last = step == len(indices) - 1
        cursor  = "" if is_last else " ▍"
        display = f"```\n{full_text[:pos]}{cursor}\n```"
        try:
            await sent.edit(display)
        except Exception:
            pass
        if not is_last:
            await asyncio.sleep(step_delay)
    return sent


# ═══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM BOT
# ═══════════════════════════════════════════════════════════════════════════════

client = TelegramClient(MemorySession(), API_ID, API_HASH)


# ── Notification Helpers ───────────────────────────────────────────────────────

async def notify_user(user_id: int, message: str):
    try:
        await client.send_message(user_id, message)
    except Exception:
        pass


async def notify_referral_reward(referrer_id: int, referred_user_id: int, new_balance: int):
    await notify_user(
        referrer_id,
        f"Referral Reward\n\n"
        f"A new user joined through your referral link.\n\n"
        f"+{REFERRAL_REWARD_CREDITS} Credits Added\n"
        f"New Balance: {new_balance} Credits\n\n"
        f"Keep sharing your link to earn more."
    )
    await notify_user(
        referred_user_id,
        f"Welcome!\n\n"
        f"You joined through a referral link and your account is ready.\n"
        f"Starting Credits: {STARTING_CREDITS}\n\n"
        f"Paste any personal wallet address to run your first check.\n"
        f"Use /refer to get your own referral link."
    )


async def notify_credit_added(user_id: int, amount: int, new_balance: int):
    await notify_user(
        user_id,
        f"Credits Added\n\n"
        f"+{amount} Credits have been added to your account.\n"
        f"New Balance: {new_balance} Credits"
    )


async def notify_credit_removed(user_id: int, amount: int, new_balance: int):
    await notify_user(
        user_id,
        f"Credits Updated\n\n"
        f"{amount} Credits have been removed from your account.\n"
        f"New Balance: {new_balance} Credits"
    )


# ── DB helpers for admin stats ─────────────────────────────────────────────────

async def get_total_user_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM users") as cursor:
            row = await cursor.fetchone()
        return row[0] if row else 0


async def get_total_referral_count() -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM referrals") as cursor:
            row = await cursor.fetchone()
        return row[0] if row else 0


async def get_all_user_ids() -> list[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id FROM users") as cursor:
            rows = await cursor.fetchall()
        return [r[0] for r in rows]


# ── /start ─────────────────────────────────────────────────────────────────────

@client.on(events.NewMessage(pattern=r"^/start(?:\s+(.+))?$"))
async def start_handler(event):
    user_id = event.sender_id
    args    = event.pattern_match.group(1)
    referred_by = None

    if args:
        try:
            referrer_id = int(args.strip())
            if referrer_id != user_id:
                referred_by = referrer_id
        except (ValueError, TypeError):
            pass

    is_new_user = (await get_user(user_id)) is None
    await get_or_create_user(user_id, referred_by)

    if referred_by and is_new_user:
        rewarded = await process_referral(referred_by, user_id)
        if rewarded:
            referrer = await get_user(referred_by)
            referrer_balance = referrer["credits"] if referrer else REFERRAL_REWARD_CREDITS
            await notify_referral_reward(referred_by, user_id, referrer_balance)

    user    = await get_user(user_id)
    credits = user["credits"] if user else STARTING_CREDITS

    if user_id in ADMIN_IDS:
        total_users     = await get_total_user_count()
        total_referrals = await get_total_referral_count()
        admin_text = (
            "Admin Control Panel\n\n"
            f"Total Users    : {total_users}\n"
            f"Total Referrals: {total_referrals}\n"
            f"Your Balance   : Unlimited (Admin)\n\n"
            "Select an admin option below or use direct slash commands:"
        )
        buttons = [
            [
                Button.inline("Broadcast", data=b"admin_broadcast"),
                Button.inline("Statistics", data=b"admin_stats"),
            ],
            [
                Button.inline("Add Credits", data=b"admin_add_credits"),
                Button.inline("Remove Credits", data=b"admin_remove_credits"),
            ],
            [
                Button.inline("User Lookup", data=b"admin_user_lookup"),
                Button.inline("Force Join Config", data=b"admin_force_join"),
            ]
        ]
        await event.reply(admin_text, buttons=buttons)
        return

    if user_id not in ADMIN_IDS and not (await is_user_in_channel(user_id)):
        channel = await get_setting("force_join_channel", "")
        text, buttons = build_force_join_prompt(channel)
        await event.reply(text, buttons=buttons)
        return

    await send_typing_text(event, build_start_message(user_id, credits))


@client.on(events.CallbackQuery)
async def callback_query_handler(event):
    if event.sender_id not in ADMIN_IDS:
        await event.answer("Access denied. Admin only.", alert=True)
        return

    data = event.data

    if data == b"admin_panel" or data == b"admin_cancel":
        ADMIN_STATES.pop(event.sender_id, None)
        total_users     = await get_total_user_count()
        total_referrals = await get_total_referral_count()
        admin_text = (
            "Admin Control Panel\n\n"
            f"Total Users    : {total_users}\n"
            f"Total Referrals: {total_referrals}\n"
            f"Your Balance   : Unlimited (Admin)\n\n"
            "Select an admin action below:"
        )
        buttons = [
            [
                Button.inline("Broadcast", data=b"admin_broadcast"),
                Button.inline("Statistics", data=b"admin_stats"),
            ],
            [
                Button.inline("Add Credits", data=b"admin_add_credits"),
                Button.inline("Remove Credits", data=b"admin_remove_credits"),
            ],
            [
                Button.inline("User Lookup", data=b"admin_user_lookup"),
                Button.inline("Force Join Config", data=b"admin_force_join"),
            ]
        ]
        await event.answer()
        await event.edit(admin_text, buttons=buttons)

    elif data == b"admin_stats":
        total_users     = await get_total_user_count()
        total_referrals = await get_total_referral_count()
        await event.answer()
        await event.edit(
            f"Bot Statistics\n\n"
            f"Total Users    : {total_users}\n"
            f"Total Referrals: {total_referrals}",
            buttons=[[Button.inline("Back to Admin Panel", data=b"admin_panel")]]
        )

    elif data == b"admin_broadcast":
        ADMIN_STATES[event.sender_id] = "awaiting_broadcast"
        await event.answer()
        await event.edit(
            "Broadcast Announcement\n\n"
            "Please type or send the message you want to broadcast to all users below:",
            buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
        )

    elif data == b"admin_add_credits":
        ADMIN_STATES[event.sender_id] = "awaiting_add_credits"
        await event.answer()
        await event.edit(
            "Add Credits\n\n"
            "Please type the User ID and Credit Amount (e.g. 5048281046 50):",
            buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
        )

    elif data == b"admin_remove_credits":
        ADMIN_STATES[event.sender_id] = "awaiting_remove_credits"
        await event.answer()
        await event.edit(
            "Remove Credits\n\n"
            "Please type the User ID and Credit Amount to remove (e.g. 5048281046 10):",
            buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
        )

    elif data == b"admin_user_lookup":
        ADMIN_STATES[event.sender_id] = "awaiting_user_lookup"
        await event.answer()
        await event.edit(
            "User Lookup\n\n"
            "Please type the User ID to lookup (e.g. 5048281046):",
            buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
        )

    elif data == b"admin_set_channel_prompt":
        ADMIN_STATES[event.sender_id] = "awaiting_set_channel"
        await event.answer()
        await event.edit(
            "Set Force Join Channel\n\n"
            "Please type the Channel username or link (e.g. @MyChannel or https://t.me/MyChannel):",
            buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
        )

    elif data == b"admin_force_join" or data == b"toggle_force_join_on" or data == b"toggle_force_join_off":
        if data == b"toggle_force_join_on":
            await set_setting("force_join_enabled", "1")
            await event.answer("Force Join Enabled!")
        elif data == b"toggle_force_join_off":
            await set_setting("force_join_enabled", "0")
            await event.answer("Force Join Disabled!")
        else:
            await event.answer()

        enabled = await get_setting("force_join_enabled", "0")
        channel = await get_setting("force_join_channel", "Not Set")
        status_str = "ON" if enabled == "1" else "OFF"
        text = (
            "Force Join Configuration\n\n"
            f"Status  : {status_str}\n"
            f"Channel : {channel}\n\n"
            "Select an option below:"
        )
        toggle_data = b"toggle_force_join_off" if enabled == "1" else b"toggle_force_join_on"
        toggle_text = "Turn OFF Force Join" if enabled == "1" else "Turn ON Force Join"
        buttons = [
            [Button.inline(toggle_text, data=toggle_data)],
            [Button.inline("Set Channel", data=b"admin_set_channel_prompt")],
            [Button.inline("Back to Admin Panel", data=b"admin_panel")]
        ]
        await event.edit(text, buttons=buttons)

    elif data == b"verify_join":
        if await is_user_in_channel(event.sender_id):
            await event.answer("Verification successful! You can now use the bot.", alert=True)
            try:
                await event.delete()
            except Exception:
                pass
        else:
            await event.answer("You have not joined the required channel yet! Please join first.", alert=True)


# ── User slash commands ────────────────────────────────────────────────────────

@client.on(events.NewMessage(pattern=r"^/balance$"))
async def cmd_balance(event):
    user_id = event.sender_id
    await get_or_create_user(user_id)
    user    = await get_user(user_id)
    credits = user["credits"] if user else 0
    msg = await event.reply("▍")
    text = (
        f"Credits: {credits}\n"
        f"Checks Remaining: {credits}"
    )
    await msg.edit(text)


@client.on(events.NewMessage(pattern=r"^/refer$"))
async def cmd_refer(event):
    user_id   = event.sender_id
    await get_or_create_user(user_id)
    ref_link  = get_referral_link(user_id)
    ref_count = await get_referral_count(user_id)
    msg  = await event.reply("▍")
    text = (
        f"Your Referral Link\n\n"
        f"`{ref_link}`\n\n"
        f"Reward: +{REFERRAL_REWARD_CREDITS} Credits per successful referral\n"
        f"Total Referrals: {ref_count}\n\n"
        f"Share your link and earn credits every time someone joins and uses the bot."
    )
    await msg.edit(text)


@client.on(events.NewMessage(pattern=r"^/help$"))
async def cmd_help(event):
    msg  = await event.reply("▍")
    text = (
        "📖 How to Use & Supported Chains\n\n"
        "Paste any personal crypto wallet address into the chat.\n"
        "The bot automatically detects the blockchain and returns full balances and USD values.\n\n"
        "🌐 25 Supported Blockchain Networks & Cryptos:\n\n"
        "🔹 EVM Networks (15 Chains):\n"
        "  • Ethereum (ETH & ERC-20 Tokens)\n"
        "  • BNB Smart Chain (BNB & BEP-20 Tokens)\n"
        "  • Polygon (MATIC / POL & Tokens)\n"
        "  • Arbitrum One (ETH, ARB, USDT, USDC)\n"
        "  • OP Mainnet (ETH, OP, USDT, USDC)\n"
        "  • Base (ETH, USDC, AERO)\n"
        "  • Avalanche C-Chain (AVAX, USDT.e, USDC.e)\n"
        "  • Fantom Opera (FTM, USDT, USDC)\n"
        "  • Cronos EVM (CRO, USDC)\n"
        "  • zkSync Era (ETH, ZK, USDC)\n"
        "  • Linea (ETH, USDC)\n"
        "  • Blast (ETH, USDB, BLAST)\n"
        "  • Mantle (MNT, USDT, USDC)\n"
        "  • Celo (CELO, cUSD)\n"
        "  • Moonbeam (GLMR, USDC)\n\n"
        "🔹 Non-EVM Networks (10 Chains):\n"
        "  • Solana (SOL & SPL Tokens: USDC, USDT, BONK)\n"
        "  • Bitcoin (BTC Balance & Recent Txs)\n"
        "  • Bitcoin Cash (BCH Balance)\n"
        "  • Dogecoin (DOGE Balance)\n"
        "  • Litecoin (LTC Balance & Txs)\n"
        "  • Ripple (XRP Balance)\n"
        "  • Cardano (ADA Balance)\n"
        "  • Cosmos Hub (ATOM Balance)\n"
        "  • Near Protocol (NEAR Balance)\n"
        "  • Stellar (XLM Balance)\n"
        "  • Algorand (ALGO Balance)\n"
        "  • TON (TON & Jetton Tokens)\n"
        "  • Sui Network (SUI Balance)\n"
        "  • Kaspa (KAS Balance)\n"
        "  • Aptos (APT Balance)\n\n"
        "📌 Usage Info:\n"
        "• Each check costs 1 credit.\n"
        "• You can paste up to 3 addresses in a single message.\n"
        "⚠️ Do not use exchange deposit addresses (Binance, OKX, etc.), as they will not reflect correct balances."
    )
    await msg.edit(text)


@client.on(events.NewMessage(pattern=r"^/commands$"))
async def cmd_commands(event):
    msg  = await event.reply("▍")
    text = (
        "All Commands\n\n"
        "/start      — welcome message and bot info\n"
        "/balance    — check your credit balance\n"
        "/refer      — get your referral link\n"
        "/help       — how to use the bot and supported wallets\n"
        "/commands   — this list\n\n"
        "To check a wallet, just paste the address directly into the chat.\n"
        "No command needed."
    )
    await msg.edit(text)


# ── Admin commands ─────────────────────────────────────────────────────────────

@client.on(events.NewMessage(pattern=r"^/admin$"))
async def admin_panel(event):
    if event.sender_id not in ADMIN_IDS:
        return
    total_users     = await get_total_user_count()
    total_referrals = await get_total_referral_count()
    text = (
        "Admin Panel\n\n"
        f"Total Users    : {total_users}\n"
        f"Total Referrals: {total_referrals}\n\n"
        "Commands:\n\n"
        "/addcredit USER_ID AMOUNT\n"
        "  Example: /addcredit 55555555 50\n\n"
        "/removecredit USER_ID AMOUNT\n"
        "  Example: /removecredit 55555555 5\n\n"
        "/user USER_ID\n"
        "  Example: /user 55555555\n\n"
        "/stats\n"
        "  Full bot statistics\n\n"
        "/broadcast MESSAGE\n"
        "  Send a message to all users\n"
        "  Example: /broadcast Hello everyone!"
    )
    await event.reply(text)


@client.on(events.NewMessage(pattern=r"^/addcredit\s+(\d+)\s+(\d+)$"))
async def admin_add_credit(event):
    if event.sender_id not in ADMIN_IDS:
        return
    target_id = int(event.pattern_match.group(1))
    amount    = int(event.pattern_match.group(2))
    new_balance = await add_credits(target_id, amount)
    await event.reply(
        f"Done.\n\n"
        f"User       : {target_id}\n"
        f"Added      : +{amount}\n"
        f"New Balance: {new_balance} Credits\n\n"
        f"User notified."
    )
    await notify_credit_added(target_id, amount, new_balance)


@client.on(events.NewMessage(pattern=r"^/removecredit\s+(\d+)\s+(\d+)$"))
async def admin_remove_credit(event):
    if event.sender_id not in ADMIN_IDS:
        return
    target_id = int(event.pattern_match.group(1))
    amount    = int(event.pattern_match.group(2))
    new_balance = await remove_credits(target_id, amount)
    await event.reply(
        f"Done.\n\n"
        f"User       : {target_id}\n"
        f"Removed    : -{amount}\n"
        f"New Balance: {new_balance} Credits\n\n"
        f"User notified."
    )
    await notify_credit_removed(target_id, amount, new_balance)


@client.on(events.NewMessage(pattern=r"^/user\s+(\d+)$"))
async def admin_user_info(event):
    if event.sender_id not in ADMIN_IDS:
        return
    target_id  = int(event.pattern_match.group(1))
    user       = await get_user(target_id)
    if not user:
        await event.reply(f"User {target_id} not found.")
        return
    ref_count  = await get_referral_count(target_id)
    created_at = user.get("created_at", "Unknown")
    await event.reply(
        f"User Information\n\n"
        f"User ID     : {user['user_id']}\n"
        f"Credits     : {user['credits']}\n"
        f"Referrals   : {ref_count}\n"
        f"Referred By : {user['referred_by'] or 'None'}\n"
        f"Join Date   : {created_at}"
    )


@client.on(events.NewMessage(pattern=r"^/stats$"))
async def admin_stats(event):
    if event.sender_id not in ADMIN_IDS:
        return
    total_users     = await get_total_user_count()
    total_referrals = await get_total_referral_count()
    await event.reply(
        f"Bot Statistics\n\n"
        f"Total Users    : {total_users}\n"
        f"Total Referrals: {total_referrals}"
    )


@client.on(events.NewMessage(pattern=r"^/broadcast\s+(.+)$", func=lambda e: not e.out))
async def admin_broadcast(event):
    if event.sender_id not in ADMIN_IDS:
        return
    message  = event.pattern_match.group(1).strip()
    user_ids = await get_all_user_ids()
    sent     = 0
    failed   = 0
    status_msg = await event.reply(f"Sending to {len(user_ids)} users...")
    for uid in user_ids:
        try:
            await client.send_message(uid, message)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    try:
        await status_msg.edit(
            f"Broadcast Complete\n\n"
            f"Sent   : {sent}\n"
            f"Failed : {failed}\n"
            f"Total  : {len(user_ids)}"
        )
    except Exception:
        pass


@client.on(events.NewMessage(pattern=r"^/setchannel(?:\s+(.+))?$"))
async def admin_set_channel(event):
    if event.sender_id not in ADMIN_IDS:
        return
    ch = event.pattern_match.group(1)
    if not ch:
        current = await get_setting("force_join_channel", "Not Set")
        await event.reply(f"Current Force Join Channel: {current}\n\nUsage: /setchannel @channelname")
        return
    ch_clean = ch.strip()
    await set_setting("force_join_channel", ch_clean)
    await event.reply(
        f"Force Join Channel Updated!\n\n"
        f"Channel: {ch_clean}\n\n"
        f"Use /forcejoin on to enable Force Join mode."
    )


@client.on(events.NewMessage(pattern=r"^/forcejoin(?:\s+(on|off))?$", func=lambda e: not e.out))
async def admin_force_join_cmd(event):
    if event.sender_id not in ADMIN_IDS:
        return
    arg = event.pattern_match.group(1)
    if not arg:
        enabled = await get_setting("force_join_enabled", "0")
        ch      = await get_setting("force_join_channel", "Not Set")
        status  = "ON" if enabled == "1" else "OFF"
        await event.reply(f"Force Join Status: {status}\nChannel: {ch}\n\nUsage: /forcejoin on or /forcejoin off")
        return
    val = "1" if arg.lower() == "on" else "0"
    await set_setting("force_join_enabled", val)
    status_str = "ENABLED (ON)" if val == "1" else "DISABLED (OFF)"
    await event.reply(f"Force Join is now {status_str}.")


# ── Main wallet address handler ────────────────────────────────────────────────

@client.on(events.NewMessage(incoming=True))
async def handler(event):
    if event.out:
        return

    text = event.raw_text or ""

    if re.match(r"^/(start|balance|refer|help|commands|admin|addcredit|removecredit|user|stats|broadcast|setchannel|forcejoin)", text.strip()):
        return

    user_id = event.sender_id
    await get_or_create_user(user_id)

    is_admin = user_id in ADMIN_IDS

    if is_admin and user_id in ADMIN_STATES:
        state = ADMIN_STATES.pop(user_id)
        input_text = text.strip()

        if state == "awaiting_broadcast":
            user_ids = await get_all_user_ids()
            status_msg = await event.reply(f"Broadcasting message to {len(user_ids)} users...")
            sent, failed = 0, 0
            for uid in user_ids:
                try:
                    await client.send_message(uid, input_text)
                    sent += 1
                    await asyncio.sleep(0.05)
                except Exception:
                    failed += 1
            await status_msg.edit(
                f"Broadcast Complete!\n\n"
                f"Sent   : {sent}\n"
                f"Failed : {failed}\n"
                f"Total  : {len(user_ids)}",
                buttons=[[Button.inline("Back to Admin Panel", data=b"admin_panel")]]
            )
            return

        elif state == "awaiting_add_credits":
            parts = input_text.split()
            if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                t_id = int(parts[0])
                amt  = int(parts[1])
                new_bal = await add_credits(t_id, amt)
                await notify_credit_added(t_id, amt, new_bal)
                await event.reply(
                    f"Credits Added Successfully!\n\n"
                    f"User ID    : {t_id}\n"
                    f"Added      : +{amt}\n"
                    f"New Balance: {new_bal} Credits",
                    buttons=[[Button.inline("Back to Admin Panel", data=b"admin_panel")]]
                )
            else:
                await event.reply(
                    "Invalid input format.\nPlease send User ID and Amount (e.g. 5048281046 50):",
                    buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
                )
                ADMIN_STATES[user_id] = "awaiting_add_credits"
            return

        elif state == "awaiting_remove_credits":
            parts = input_text.split()
            if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                t_id = int(parts[0])
                amt  = int(parts[1])
                new_bal = await remove_credits(t_id, amt)
                await notify_credit_removed(t_id, amt, new_bal)
                await event.reply(
                    f"Credits Removed Successfully!\n\n"
                    f"User ID    : {t_id}\n"
                    f"Removed    : -{amt}\n"
                    f"New Balance: {new_bal} Credits",
                    buttons=[[Button.inline("Back to Admin Panel", data=b"admin_panel")]]
                )
            else:
                await event.reply(
                    "Invalid input format.\nPlease send User ID and Amount (e.g. 5048281046 10):",
                    buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
                )
                ADMIN_STATES[user_id] = "awaiting_remove_credits"
            return

        elif state == "awaiting_user_lookup":
            if input_text.isdigit():
                t_id = int(input_text)
                user = await get_user(t_id)
                if not user:
                    await event.reply(
                        f"User {t_id} not found.",
                        buttons=[[Button.inline("Back to Admin Panel", data=b"admin_panel")]]
                    )
                else:
                    ref_count  = await get_referral_count(t_id)
                    created_at = user.get("created_at", "Unknown")
                    await event.reply(
                        f"User Information\n\n"
                        f"User ID     : {user['user_id']}\n"
                        f"Credits     : {user['credits']}\n"
                        f"Referrals   : {ref_count}\n"
                        f"Referred By : {user['referred_by'] or 'None'}\n"
                        f"Join Date   : {created_at}",
                        buttons=[[Button.inline("Back to Admin Panel", data=b"admin_panel")]]
                    )
            else:
                await event.reply(
                    "Invalid User ID. Please send a numeric User ID:",
                    buttons=[[Button.inline("Cancel", data=b"admin_cancel")]]
                )
                ADMIN_STATES[user_id] = "awaiting_user_lookup"
            return

        elif state == "awaiting_set_channel":
            await set_setting("force_join_channel", input_text)
            await event.reply(
                f"Force Join Channel Saved!\n\nChannel: {input_text}\n\nDon't forget to turn Force Join ON in Force Join Config.",
                buttons=[[Button.inline("Back to Admin Panel", data=b"admin_panel")]]
            )
            return

    addresses = find_addresses(text)
    if not addresses:
        return

    if not is_admin and not (await is_user_in_channel(user_id)):
        channel = await get_setting("force_join_channel", "")
        text, buttons = build_force_join_prompt(channel)
        await event.reply(text, buttons=buttons)
        return

    credits = await get_credits(user_id)
    if not is_admin and credits <= 0:
        msg  = await event.reply("▍")
        await msg.edit(build_no_credits_message(user_id))
        return

    status_msg = await event.reply("🔎 `Checking wallet address...`\n⏳ `Querying balances across 25 multi-chain networks...`")

    connector = aiohttp.TCPConnector(limit=40, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        await ensure_prices(session)

        addr_list = addresses[:MAX_ADDRESSES_PER_MESSAGE]
        tasks     = [format_wallet(session, addr) for addr in addr_list]
        results   = await asyncio.gather(*tasks, return_exceptions=True)

        first_result = True
        for addr, res in zip(addr_list, results):
            if isinstance(res, Exception) or res is None:
                continue

            if not is_admin:
                success = await deduct_credit(user_id)
                if not success:
                    await status_msg.edit(build_no_credits_message(user_id))
                    break

            lines, total_usd = res
            lines.append("")
            lines.append("═" * 38)
            lines.append(f"GRAND TOTAL USD: ${total_usd:.2f}")
            lines.append("═" * 38)
            if is_admin:
                lines.append("Credits Remaining: Unlimited (Admin)")
            else:
                remaining = await get_credits(user_id)
                lines.append(f"Credits Remaining: {remaining}")

            full_text = "\n".join(lines)
            chunks = list(chunk_message(full_text))
            for i, chunk in enumerate(chunks):
                if first_result and i == 0:
                    await send_typing_code(event, chunk, sent_message=status_msg)
                    first_result = False
                else:
                    await send_typing_code(event, chunk)
                await asyncio.sleep(0.3)


# ═══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("Starting 25-network multi-chain wallet checker bot...")

    async def main():
        # Initialize database
        await init_db()
        print("Database initialized.")

        # Pre-fetch prices
        print("Pre-fetching prices...")
        async with aiohttp.ClientSession() as s:
            global PRICES, _prices_fetched_at
            PRICES             = await fetch_prices(s)
            _prices_fetched_at = time.monotonic()

        p = PRICES
        print("Prices: " + "  ".join(
            f"{k}=${v:,.4f}" for k, v in p.items() if k != "USDT" and v > 0
        ))

        await client.start(bot_token=BOT_TOKEN)
        me = await client.get_me()
        if me and me.username:
            global BOT_USERNAME
            BOT_USERNAME = me.username
            print(f"Bot connected as @{BOT_USERNAME}")
        else:
            print("Bot is running.")
        await client.run_until_disconnected()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Bot stopped.")
