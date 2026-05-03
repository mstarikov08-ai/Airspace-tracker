#!/usr/bin/env python3
"""
Airspace NOTAM Tracker Bot
Monitors global airspace closures and NOTAMs via Telegram.
Data source: aviationweather.gov (free, no API key required)
"""

import os
import re
import json
import logging
import sqlite3
import hashlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Tuple, Any

import requests
from dotenv import load_dotenv
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
from telegram.error import TelegramError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHANNEL_ID: str = os.getenv("TELEGRAM_CHANNEL_ID", "")
DB_PATH: str = os.getenv("DB_PATH", "notams.db")
POLL_INTERVAL: int = int(os.getenv("POLL_INTERVAL_MINUTES", "15"))

AVIATIONWEATHER_API = "https://aviationweather.gov/api/data/notam"

# Regions with FIR codes, emoji flags, and priority level
REGIONS: Dict[str, Dict] = {
    "Russia": {
        "firs": ["UURR", "ULLL", "UUWW", "URWW", "USSS", "UHHH", "UNKL", "UOOO"],
        "flag": "\U0001f1f7\U0001f1fa",
        "priority": "high",
    },
    "Ukraine": {
        "firs": ["UKBV", "UKOO", "UKHH", "UKFV", "UKDV"],
        "flag": "\U0001f1fa\U0001f1e6",
        "priority": "high",
    },
    "Belarus": {
        "firs": ["UMMV"],
        "flag": "\U0001f1e7\U0001f1fe",
        "priority": "high",
    },
    "Israel": {
        "firs": ["LLLL"],
        "flag": "\U0001f1ee\U0001f1f1",
        "priority": "high",
    },
    "Iran": {
        "firs": ["OIIX", "OIIG", "OIIS", "OIAF"],
        "flag": "\U0001f1ee\U0001f1f7",
        "priority": "high",
    },
    "Iraq": {
        "firs": ["ORBB"],
        "flag": "\U0001f1ee\U0001f1f6",
        "priority": "high",
    },
    "Lebanon": {
        "firs": ["OLBA"],
        "flag": "\U0001f1f1\U0001f1e7",
        "priority": "high",
    },
    "Syria": {
        "firs": ["OSTT"],
        "flag": "\U0001f1f8\U0001f1fe",
        "priority": "high",
    },
    "Moldova": {
        "firs": ["LUUU"],
        "flag": "\U0001f1f2\U0001f1e9",
        "priority": "medium",
    },
    "Georgia": {
        "firs": ["UGGG"],
        "flag": "\U0001f1ec\U0001f1ea",
        "priority": "medium",
    },
    "Armenia": {
        "firs": ["UDDD"],
        "flag": "\U0001f1e6\U0001f1f2",
        "priority": "medium",
    },
    "Azerbaijan": {
        "firs": ["UBBA"],
        "flag": "\U0001f1e6\U0001f1ff",
        "priority": "medium",
    },
    "Turkey": {
        "firs": ["LTAA", "LTBB"],
        "flag": "\U0001f1f9\U0001f1f7",
        "priority": "medium",
    },
    "Jordan": {
        "firs": ["OJAC"],
        "flag": "\U0001f1ef\U0001f1f4",
        "priority": "medium",
    },
    "Saudi Arabia": {
        "firs": ["OEJD", "OERR"],
        "flag": "\U0001f1f8\U0001f1e6",
        "priority": "medium",
    },
    "Yemen": {
        "firs": ["OYSC"],
        "flag": "\U0001f1fe\U0001f1ea",
        "priority": "medium",
    },
    "Libya": {
        "firs": ["HLLL"],
        "flag": "\U0001f1f1\U0001f1fe",
        "priority": "medium",
    },
    "Afghanistan": {
        "firs": ["OAKX"],
        "flag": "\U0001f1e6\U0001f1eb",
        "priority": "low",
    },
    "Pakistan": {
        "firs": ["OPKR"],
        "flag": "\U0001f1f5\U0001f1f0",
        "priority": "low",
    },
}

# Reverse lookup: FIR code -> region name
FIR_TO_REGION: Dict[str, str] = {}
for _region, _data in REGIONS.items():
    for _fir in _data["firs"]:
        FIR_TO_REGION[_fir] = _region

ALL_FIRS: List[str] = list(FIR_TO_REGION.keys())

# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS notams (
                notam_id      TEXT PRIMARY KEY,
                location      TEXT NOT NULL,
                region        TEXT,
                raw_text      TEXT,
                description   TEXT,
                altitude_lower TEXT,
                altitude_upper TEXT,
                valid_from    TEXT,
                valid_to      TEXT,
                reason        TEXT,
                severity      TEXT,
                alerted       INTEGER DEFAULT 0,
                first_seen    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS subscriptions (
                chat_id  INTEGER NOT NULL,
                region   TEXT    NOT NULL,
                PRIMARY KEY (chat_id, region)
            );

            CREATE INDEX IF NOT EXISTS idx_notams_location  ON notams(location);
            CREATE INDEX IF NOT EXISTS idx_notams_region    ON notams(region);
            CREATE INDEX IF NOT EXISTS idx_notams_valid_to  ON notams(valid_to);
            CREATE INDEX IF NOT EXISTS idx_notams_alerted   ON notams(alerted);
        """)
    logger.info("Database ready at %s", DB_PATH)


# ─────────────────────────────────────────────────────────────────────────────
# NOTAM FETCHING & PARSING
# ─────────────────────────────────────────────────────────────────────────────

def fetch_notams(icao_codes: List[str]) -> List[Any]:
    """Fetch NOTAMs from aviationweather.gov for a space-separated list of ICAO codes."""
    try:
        resp = requests.get(
            AVIATIONWEATHER_API,
            params={"format": "json", "location": " ".join(icao_codes)},
            timeout=30,
            headers={"User-Agent": "AirspaceTrackerBot/1.0 (github.com/airspace-tracker)"},
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return data.get("features", [])
        return []
    except requests.RequestException as exc:
        logger.warning("NOTAM fetch failed for %s: %s", icao_codes, exc)
        return []
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning("NOTAM parse error for %s: %s", icao_codes, exc)
        return []


def parse_time(raw: Optional[str]) -> Optional[datetime]:
    """Parse a NOTAM timestamp (ISO or YYMMDDHHMM) to UTC datetime."""
    if not raw:
        return None
    raw = raw.strip()
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y%m%d%H%M",
        "%y%m%d%H%M",
    ):
        try:
            return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None


def extract_raw_fields(raw: str) -> Dict[str, str]:
    """Pull structured fields out of raw NOTAM text."""
    f: Dict[str, str] = {}

    m = re.search(r"Q\)\s*([^\n]+)", raw)
    if m:
        f["q_line"] = m.group(1).strip()
        parts = f["q_line"].split("/")
        if len(parts) >= 7:
            f["fir"] = parts[0].strip()
            f["qcode"] = parts[1].strip()
            f["q_lower"] = parts[5].strip()
            f["q_upper"] = parts[6].strip().split()[0]  # strip trailing coords

    m = re.search(r"A\)\s*([A-Z0-9]{4})", raw)
    if m:
        f["location"] = m.group(1)

    m = re.search(r"B\)\s*(\d{8,12})", raw)
    if m:
        f["start"] = m.group(1)

    m = re.search(r"C\)\s*(\d{8,12}|PERM)", raw)
    if m:
        f["end"] = m.group(1)

    m = re.search(r"E\)\s*(.*?)(?=\nF\)|\nG\)|\Z)", raw, re.DOTALL)
    if m:
        f["desc"] = m.group(1).strip()

    m = re.search(r"F\)\s*([^\n]+)", raw)
    if m:
        f["alt_lower"] = m.group(1).strip()

    m = re.search(r"G\)\s*([^\n]+)", raw)
    if m:
        f["alt_upper"] = m.group(1).strip()

    return f


def classify_reason(
    description: str,
    location: str,
    valid_from: Optional[datetime],
    valid_to: Optional[datetime],
) -> str:
    """Infer the most likely reason for the airspace restriction."""
    text = (description or "").lower()

    hours: Optional[float] = None
    if valid_from and valid_to and valid_to > valid_from:
        hours = (valid_to - valid_from).total_seconds() / 3600

    military_kw = [
        "military", "mil ops", "armed forces", "air defence", "air defense",
        "restricted area", "danger area", "prohibited area", "weapons",
        "exercise", "nato", "combat", "hostile", "missile", "artillery",
        "drone", "uav", "uas", "wartime", "tsa", "tra", "moa", "firing",
    ]
    if any(kw in text for kw in military_kw):
        return "Military activity"

    vip_kw = [
        "vip", "head of state", "state aircraft", "royal flight",
        "president", "prime minister", "government flight",
    ]
    if any(kw in text for kw in vip_kw):
        return "VIP movement"

    # Short-duration closure near known VIP capitals → likely VIP
    vip_firs = {"UUWW", "UURR", "LLLL", "LTBA", "LTAC", "EIDW", "EGLL"}
    if hours is not None and hours <= 3 and location in vip_firs:
        return "VIP movement (suspected)"

    maintenance_kw = [
        "maintenance", "work in progress", "wip", "construction",
        "runway clsd", "rwy clsd", "taxiway clsd", "twy clsd",
        "obstacle", "crane", "building work", "repaving", "lighting",
        "nav aid", "navaid", "ils", "vor", "ndb", "apron", "stand",
    ]
    if any(kw in text for kw in maintenance_kw):
        return "Airport maintenance"

    weather_kw = [
        "weather", "wind shear", "severe turbulence", "icing",
        "thunderstorm", "ash cloud", "volcanic ash", "fog", "blizzard",
        "sigmet",
    ]
    if any(kw in text for kw in weather_kw):
        return "Weather-related"

    # Known active conflict FIRs → assume military
    conflict_firs = {
        "UKBV", "UKHH", "UKOO", "UKFV", "UKDV",
        "OSTT", "ORBB", "OYSC", "HLLL", "OAKX",
    }
    if location in conflict_firs:
        return "Military activity (conflict zone)"

    return "Unknown / restricted"


def detect_severity(lower: str, upper: str) -> Tuple[str, str]:
    """Return (label, emoji) for the airspace restriction extent."""

    def to_fl(s: str) -> int:
        s = (s or "").upper().strip()
        if s in ("GND", "SFC", "000", "0", ""):
            return 0
        m = re.search(r"FL\s*(\d+)", s)
        if m:
            return int(m.group(1))
        m = re.search(r"(\d+)\s*FT", s)
        if m:
            return int(m.group(1)) // 100
        try:
            return int(s)
        except ValueError:
            return 0

    lo = to_fl(lower)
    hi = to_fl(upper)

    if lo == 0 and hi >= 600:
        return "Full airspace closure", "⛔"
    if lo == 0 and hi >= 200:
        return "Major closure (surface–high alt)", "\U0001f534"
    if lo == 0:
        return "Low-level closure", "\U0001f7e0"
    if hi >= 600:
        return "High-altitude restriction", "\U0001f7e1"
    return "Partial closure", "\U0001f7e1"


def fmt_alt(lower: str, upper: str) -> str:
    lo = (lower or "SFC").upper().strip()
    hi = (upper or "UNL").upper().strip()
    if lo in ("000", "0", "GND"):
        lo = "Surface"
    return f"{lo} to {hi}"


def fmt_dt(dt: Optional[datetime]) -> str:
    return dt.strftime("%Y-%m-%d %H:%M UTC") if dt else "Unknown"


def fmt_duration(valid_from: Optional[datetime], valid_to: Optional[datetime]) -> str:
    if not valid_from or not valid_to or valid_to <= valid_from:
        return ""
    delta = valid_to - valid_from
    h = int(delta.total_seconds() // 3600)
    m = int((delta.total_seconds() % 3600) // 60)
    if h > 0:
        return f" ({h}h {m:02d}m)" if m else f" ({h}h)"
    return f" ({m}m)"


def he(text: str) -> str:
    """Escape text for Telegram HTML."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_alert(notam: sqlite3.Row) -> str:
    """Render a NOTAM database row as a formatted HTML Telegram message."""
    region = notam["region"] or "Unknown"
    flag = REGIONS.get(region, {}).get("flag", "\U0001f30d")
    sev_label, sev_emoji = detect_severity(
        notam["altitude_lower"] or "", notam["altitude_upper"] or ""
    )
    vf = parse_time(notam["valid_from"])
    vt = parse_time(notam["valid_to"])

    raw = (notam["raw_text"] or notam["description"] or "").strip()
    if len(raw) > 600:
        raw = raw[:600] + "…"

    return (
        f"{sev_emoji} <b>AIRSPACE CLOSURE — {he(region)} {flag}</b>\n"
        f"\U0001f4cd <b>Location:</b> <code>{he(notam['location'])}</code>\n"
        f"⛔ <b>Altitude:</b> {he(fmt_alt(notam['altitude_lower'], notam['altitude_upper']))} — {he(sev_label)}\n"
        f"⏰ <b>Valid:</b> {he(fmt_dt(vf))} → {he(fmt_dt(vt))}{he(fmt_duration(vf, vt))}\n"
        f"\n"
        f"\U0001f4cb <b>Raw NOTAM:</b>\n"
        f"<pre>{he(raw)}</pre>\n"
        f"\n"
        f"\U0001f4a1 <b>Likely reason:</b> {he(notam['reason'] or 'Unknown')}"
    )


def process_item(item: Any, fallback_location: str) -> Optional[Dict]:
    """Convert a single API response item into a normalised dict."""
    raw_text = ""
    notam_id = ""
    location = fallback_location
    start_raw = end_raw = description = alt_lower = alt_upper = ""

    if isinstance(item, dict) and "properties" in item:
        props = item["properties"]
        raw_text = props.get("all", "") or props.get("message", "") or ""
        notam_id = str(props.get("id") or props.get("notamId") or "")
        location = str(props.get("location") or fallback_location)
        start_raw = str(props.get("startDate") or "")
        end_raw = str(props.get("endDate") or "")
        description = str(props.get("text") or props.get("message") or "")
        alt_lower = str(props.get("lowestAlt") or "")
        alt_upper = str(props.get("highestAlt") or "")
    elif isinstance(item, str):
        raw_text = item
    else:
        return None

    # Supplement from raw NOTAM text when fields are missing
    if raw_text:
        rf = extract_raw_fields(raw_text)
        location = location or rf.get("location", fallback_location)
        start_raw = start_raw or rf.get("start", "")
        end_raw = end_raw or rf.get("end", "")
        description = description or rf.get("desc", "")
        alt_lower = alt_lower or rf.get("q_lower", "") or rf.get("alt_lower", "")
        alt_upper = alt_upper or rf.get("q_upper", "") or rf.get("alt_upper", "")

    if not notam_id:
        notam_id = hashlib.sha256(raw_text.encode()).hexdigest()[:20]

    # Convert Q-line numeric FL codes ("000"/"999") to proper labels
    if re.fullmatch(r"\d{3}", alt_lower):
        alt_lower = "GND" if alt_lower == "000" else f"FL{alt_lower}"
    if re.fullmatch(r"\d{3}", alt_upper):
        alt_upper = "FL999" if alt_upper == "999" else f"FL{alt_upper}"

    vf = parse_time(start_raw)
    vt = parse_time(end_raw)
    region = FIR_TO_REGION.get(location) or FIR_TO_REGION.get(fallback_location)

    return {
        "notam_id": notam_id,
        "location": location or fallback_location,
        "region": region,
        "raw_text": raw_text[:2000],
        "description": description[:600],
        "altitude_lower": alt_lower[:50],
        "altitude_upper": alt_upper[:50],
        "valid_from": vf.isoformat() if vf else "",
        "valid_to": vt.isoformat() if vt else "",
        "reason": classify_reason(description, location, vf, vt),
        "severity": detect_severity(alt_lower, alt_upper)[0],
        "first_seen": datetime.now(timezone.utc).isoformat(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def save_notam(n: Dict) -> bool:
    """Insert NOTAM if new. Returns True when inserted."""
    with get_db() as conn:
        if conn.execute(
            "SELECT 1 FROM notams WHERE notam_id = ?", (n["notam_id"],)
        ).fetchone():
            return False
        conn.execute(
            """
            INSERT INTO notams
                (notam_id, location, region, raw_text, description,
                 altitude_lower, altitude_upper, valid_from, valid_to,
                 reason, severity, alerted, first_seen)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,0,?)
            """,
            (
                n["notam_id"], n["location"], n["region"],
                n["raw_text"], n["description"],
                n["altitude_lower"], n["altitude_upper"],
                n["valid_from"], n["valid_to"],
                n["reason"], n["severity"], n["first_seen"],
            ),
        )
        return True


def get_active_notams(
    region: Optional[str] = None,
    location: Optional[str] = None,
    limit: int = 50,
) -> List[sqlite3.Row]:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        if region:
            return conn.execute(
                "SELECT * FROM notams WHERE region = ? AND (valid_to = '' OR valid_to > ?) "
                "ORDER BY valid_from DESC LIMIT ?",
                (region, now, limit),
            ).fetchall()
        if location:
            return conn.execute(
                "SELECT * FROM notams WHERE location = ? AND (valid_to = '' OR valid_to > ?) "
                "ORDER BY valid_from DESC LIMIT ?",
                (location, now, limit),
            ).fetchall()
        return conn.execute(
            "SELECT * FROM notams WHERE valid_to = '' OR valid_to > ? "
            "ORDER BY valid_from DESC LIMIT ?",
            (now, limit),
        ).fetchall()


def get_unalerted() -> List[sqlite3.Row]:
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        return conn.execute(
            "SELECT * FROM notams WHERE alerted = 0 AND (valid_to = '' OR valid_to > ?) "
            "ORDER BY first_seen DESC",
            (now,),
        ).fetchall()


def mark_alerted(notam_id: str) -> None:
    with get_db() as conn:
        conn.execute("UPDATE notams SET alerted = 1 WHERE notam_id = ?", (notam_id,))


# ─────────────────────────────────────────────────────────────────────────────
# SUBSCRIPTION MANAGEMENT
# ─────────────────────────────────────────────────────────────────────────────

def add_subscription(chat_id: int, region: str) -> None:
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO subscriptions (chat_id, region) VALUES (?, ?)",
            (chat_id, region),
        )


def remove_subscription(chat_id: int, region: str) -> None:
    with get_db() as conn:
        conn.execute(
            "DELETE FROM subscriptions WHERE chat_id = ? AND region = ?",
            (chat_id, region),
        )


def get_region_subscribers(region: str) -> List[int]:
    with get_db() as conn:
        return [
            r["chat_id"]
            for r in conn.execute(
                "SELECT chat_id FROM subscriptions WHERE region = ?", (region,)
            ).fetchall()
        ]


def get_user_subscriptions(chat_id: int) -> List[str]:
    with get_db() as conn:
        return [
            r["region"]
            for r in conn.execute(
                "SELECT region FROM subscriptions WHERE chat_id = ?", (chat_id,)
            ).fetchall()
        ]


# ─────────────────────────────────────────────────────────────────────────────
# BACKGROUND POLL TASK
# ─────────────────────────────────────────────────────────────────────────────

BATCH_SIZE = 10  # number of FIR codes per API call


async def poll_and_alert(app: Application) -> None:
    """Fetch fresh NOTAMs and dispatch alerts for any new entries."""
    logger.info("Poll cycle started — %d FIRs in %d batches",
                len(ALL_FIRS), -(-len(ALL_FIRS) // BATCH_SIZE))

    new_total = 0
    for i in range(0, len(ALL_FIRS), BATCH_SIZE):
        batch = ALL_FIRS[i : i + BATCH_SIZE]
        items = fetch_notams(batch)
        for item in items:
            # Determine best fallback location from batch
            fallback = batch[0]
            n = process_item(item, fallback)
            if n and save_notam(n):
                new_total += 1

    logger.info("Poll complete — %d new NOTAMs stored", new_total)

    unalerted = get_unalerted()
    if not unalerted:
        return

    logger.info("Dispatching alerts for %d NOTAMs", len(unalerted))
    for notam in unalerted:
        text = format_alert(notam)
        region = notam["region"]
        dispatched = False

        if CHANNEL_ID:
            try:
                await app.bot.send_message(
                    chat_id=CHANNEL_ID, text=text, parse_mode=ParseMode.HTML
                )
                dispatched = True
            except TelegramError as exc:
                logger.warning("Channel send failed: %s", exc)

        if region:
            for chat_id in get_region_subscribers(region):
                try:
                    await app.bot.send_message(
                        chat_id=chat_id, text=text, parse_mode=ParseMode.HTML
                    )
                    dispatched = True
                except TelegramError as exc:
                    logger.warning("Subscriber %d send failed: %s", chat_id, exc)

        mark_alerted(notam["notam_id"])

    logger.info("Alert cycle complete")


# ─────────────────────────────────────────────────────────────────────────────
# REGION LOOKUP HELPER
# ─────────────────────────────────────────────────────────────────────────────

def find_region(query: str) -> Optional[str]:
    """Case-insensitive region name lookup with partial match fallback."""
    q = query.strip().lower()
    exact = next((r for r in REGIONS if r.lower() == q), None)
    if exact:
        return exact
    return next((r for r in REGIONS if q in r.lower()), None)


# ─────────────────────────────────────────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────────────────────────────────────────

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "✈️ <b>Airspace NOTAM Tracker</b>\n\n"
        "I monitor global airspace closures and NOTAMs with a focus on conflict regions.\n\n"
        "<b>Priority regions:</b>\n"
        "\U0001f1f7\U0001f1fa Russia · \U0001f1fa\U0001f1e6 Ukraine · \U0001f1e7\U0001f1fe Belarus\n"
        "\U0001f1ee\U0001f1f1 Israel · \U0001f1ee\U0001f1f7 Iran · \U0001f1ee\U0001f1f6 Iraq · "
        "\U0001f1f8\U0001f1fe Syria · \U0001f1f1\U0001f1e7 Lebanon\n\n"
        "<b>Quick start:</b>\n"
        "• /region Russia — active Russian closures\n"
        "• /subscribe Ukraine — live alerts for Ukraine\n"
        "• /active — all active closures worldwide\n"
        "• /help — full command reference\n\n"
        "Data refreshes every 15 minutes from aviationweather.gov."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "✈️ <b>Airspace NOTAM Tracker — Commands</b>\n\n"
        "/start — Welcome message\n"
        "/help — This help text\n\n"
        "<b>Lookups:</b>\n"
        "/notam &lt;ICAO&gt; — NOTAMs for a specific airport/FIR\n"
        "  <i>Example: /notam UUWW</i>\n"
        "/region &lt;name&gt; — Active closures for a region\n"
        "  <i>Example: /region Russia</i>\n"
        "/regions — List all monitored regions\n"
        "/active — All currently active worldwide closures\n\n"
        "<b>Alerts:</b>\n"
        "/subscribe &lt;region&gt; — Auto-alerts for a region\n"
        "  <i>Example: /subscribe Ukraine</i>\n"
        "/unsubscribe &lt;region&gt; — Stop alerts for a region\n\n"
        "<b>Other:</b>\n"
        "/map — Links to live airspace maps\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)


async def cmd_notam(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: <code>/notam ICAO_CODE</code>  e.g. <code>/notam UUWW</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    icao = context.args[0].upper().strip()
    if len(icao) != 4 or not icao.isalnum():
        await update.message.reply_text(
            "❌ Invalid ICAO code. Must be 4 alphanumeric characters, e.g. <code>UUWW</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(
        f"\U0001f50d Fetching NOTAMs for <code>{he(icao)}</code>…",
        parse_mode=ParseMode.HTML,
    )

    # Live fetch then return from DB
    items = fetch_notams([icao])
    for item in items:
        n = process_item(item, icao)
        if n:
            save_notam(n)

    notams = get_active_notams(location=icao)

    if not notams:
        await update.message.reply_text(
            f"✅ No active NOTAMs found for <code>{he(icao)}</code>.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(
        f"Found <b>{len(notams)}</b> active NOTAM(s) for <code>{he(icao)}</code>:",
        parse_mode=ParseMode.HTML,
    )
    for notam in notams[:5]:
        try:
            await update.message.reply_text(
                format_alert(notam), parse_mode=ParseMode.HTML
            )
        except TelegramError as exc:
            logger.warning("Send failed: %s", exc)


async def cmd_region(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        regions_list = "\n".join(
            f"  {REGIONS[r]['flag']} {r}" for r in sorted(REGIONS)
        )
        await update.message.reply_text(
            f"Usage: <code>/region NAME</code>\n\nAvailable regions:\n{he(regions_list)}",
            parse_mode=ParseMode.HTML,
        )
        return

    query = " ".join(context.args)
    matched = find_region(query)
    if not matched:
        await update.message.reply_text(
            f"❌ Unknown region: <code>{he(query)}</code>\n"
            f"Use /regions to see all available regions.",
            parse_mode=ParseMode.HTML,
        )
        return

    notams = get_active_notams(region=matched)
    flag = REGIONS[matched]["flag"]

    if not notams:
        await update.message.reply_text(
            f"✅ No active closures found for {flag} <b>{he(matched)}</b>.",
            parse_mode=ParseMode.HTML,
        )
        return

    await update.message.reply_text(
        f"{flag} <b>{he(matched)}</b> — {len(notams)} active closure(s):",
        parse_mode=ParseMode.HTML,
    )
    for notam in notams[:5]:
        try:
            await update.message.reply_text(
                format_alert(notam), parse_mode=ParseMode.HTML
            )
        except TelegramError as exc:
            logger.warning("Send failed: %s", exc)


async def cmd_subscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not context.args:
        await update.message.reply_text(
            "Usage: <code>/subscribe REGION</code>  e.g. <code>/subscribe Russia</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    query = " ".join(context.args)
    matched = find_region(query)
    if not matched:
        await update.message.reply_text(
            f"❌ Unknown region: <code>{he(query)}</code>\nUse /regions to browse.",
            parse_mode=ParseMode.HTML,
        )
        return

    add_subscription(update.effective_chat.id, matched)
    flag = REGIONS[matched]["flag"]
    await update.message.reply_text(
        f"✅ Subscribed to {flag} <b>{he(matched)}</b> alerts!\n"
        f"You'll receive a message whenever new NOTAMs are detected.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_unsubscribe(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id

    if not context.args:
        subs = get_user_subscriptions(chat_id)
        if not subs:
            await update.message.reply_text("You have no active subscriptions.")
            return
        sub_list = "\n".join(
            f"  {REGIONS.get(r, {}).get('flag', '')} {r}" for r in subs
        )
        await update.message.reply_text(
            f"Your subscriptions:\n{he(sub_list)}\n\n"
            f"Use <code>/unsubscribe REGION</code> to remove one.",
            parse_mode=ParseMode.HTML,
        )
        return

    query = " ".join(context.args)
    matched = find_region(query)
    if not matched:
        await update.message.reply_text(
            f"❌ Unknown region: <code>{he(query)}</code>",
            parse_mode=ParseMode.HTML,
        )
        return

    remove_subscription(chat_id, matched)
    flag = REGIONS[matched]["flag"]
    await update.message.reply_text(
        f"✅ Unsubscribed from {flag} <b>{he(matched)}</b> alerts.",
        parse_mode=ParseMode.HTML,
    )


async def cmd_active(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    notams = get_active_notams(limit=200)

    if not notams:
        await update.message.reply_text(
            "✅ No active airspace closures in the database.\n\n"
            "Data refreshes every 15 minutes. Use /notam &lt;ICAO&gt; for a live query.",
            parse_mode=ParseMode.HTML,
        )
        return

    by_region: Dict[str, list] = {}
    for n in notams:
        by_region.setdefault(n["region"] or "Other", []).append(n)

    def _priority(r: str) -> int:
        p = REGIONS.get(r, {}).get("priority", "low")
        return {"high": 0, "medium": 1, "low": 2}.get(p, 3)

    sorted_regions = sorted(by_region, key=lambda r: (_priority(r), r))

    lines = [f"\U0001f30d <b>Active Airspace Closures</b> ({len(notams)} total)\n"]
    for region in sorted_regions[:20]:
        rnots = by_region[region]
        flag = REGIONS.get(region, {}).get("flag", "\U0001f30d")
        _, sev_emoji = detect_severity(
            rnots[0]["altitude_lower"] or "", rnots[0]["altitude_upper"] or ""
        )
        reason = rnots[0]["reason"] or "Unknown"
        lines.append(
            f"{sev_emoji} {flag} <b>{he(region)}</b> — {len(rnots)} NOTAM(s)"
        )
        lines.append(f"   └ {he(reason)}")

    lines.append("\nUse <code>/region NAME</code> for details.")

    await update.message.reply_text(
        "\n".join(lines), parse_mode=ParseMode.HTML
    )


async def cmd_regions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    by_priority: Dict[str, list] = {"high": [], "medium": [], "low": []}
    for region, data in REGIONS.items():
        by_priority[data["priority"]].append((region, data))

    lines = ["\U0001f5fa️ <b>Monitored Regions</b>\n"]
    labels = [("high", "\U0001f534 High Priority"), ("medium", "\U0001f7e1 Medium Priority"), ("low", "\U0001f535 Low Priority")]
    for key, label in labels:
        lines.append(f"<b>{label}:</b>")
        for region, data in sorted(by_priority[key]):
            firs = ", ".join(f"<code>{f}</code>" for f in data["firs"])
            lines.append(f"{data['flag']} {he(region)} — {firs}")
        lines.append("")

    await update.message.reply_text("\n".join(lines), parse_mode=ParseMode.HTML)


async def cmd_map(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    text = (
        "\U0001f5fa️ <b>Live Airspace Maps</b>\n\n"
        '• <a href="https://skyvector.com">SkyVector</a> — Full NOTAM overlay worldwide\n'
        '• <a href="https://notams.aim.faa.gov/notamSearch/">FAA NOTAM Search</a> — Official US/global NOTAMs\n'
        '• <a href="https://www.public.nm.eurocontrol.int/">Eurocontrol NOP</a> — European flow management\n'
        '• <a href="https://aviationweather.gov/notam">AviationWeather.gov</a> — Source data for this bot\n'
        '• <a href="https://flightaware.com/misery/">FlightAware Misery Map</a> — Real-time delay tracker\n'
        '• <a href="https://www.flightradar24.com">Flightradar24</a> — Live flight tracking'
    )
    await update.message.reply_text(
        text, parse_mode=ParseMode.HTML, disable_web_page_preview=True
    )


# ─────────────────────────────────────────────────────────────────────────────
# APPLICATION LIFECYCLE
# ─────────────────────────────────────────────────────────────────────────────

async def _post_init(app: Application) -> None:
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(
        poll_and_alert,
        trigger="interval",
        minutes=POLL_INTERVAL,
        args=[app],
        id="notam_poll",
        next_run_time=datetime.now(timezone.utc),  # run immediately on startup
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    app.bot_data["scheduler"] = scheduler
    logger.info("Scheduler started — polling every %d minutes", POLL_INTERVAL)


async def _post_shutdown(app: Application) -> None:
    scheduler: Optional[AsyncIOScheduler] = app.bot_data.get("scheduler")
    if scheduler:
        scheduler.shutdown(wait=False)


def main() -> None:
    if not BOT_TOKEN:
        raise SystemExit(
            "TELEGRAM_BOT_TOKEN is not set.\n"
            "Copy .env.example to .env and fill in your token."
        )

    init_db()

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(_post_init)
        .post_shutdown(_post_shutdown)
        .build()
    )

    for cmd, handler in [
        ("start", cmd_start),
        ("help", cmd_help),
        ("notam", cmd_notam),
        ("region", cmd_region),
        ("regions", cmd_regions),
        ("subscribe", cmd_subscribe),
        ("unsubscribe", cmd_unsubscribe),
        ("active", cmd_active),
        ("map", cmd_map),
    ]:
        app.add_handler(CommandHandler(cmd, handler))

    logger.info(
        "Bot starting up — %d regions, %d FIRs monitored",
        len(REGIONS), len(ALL_FIRS),
    )
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
