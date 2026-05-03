# ✈️ Airspace NOTAM Tracker Bot

A Telegram bot that monitors global airspace closures and NOTAMs in real time,
with a focus on Russia, Ukraine, the Middle East, and other frequently affected
regions. Data is sourced from [aviationweather.gov](https://aviationweather.gov)
(free, no API key required) and refreshed every 15 minutes.

---

## Features

- **Live NOTAM fetching** from aviationweather.gov (GeoJSON API)
- **SQLite storage** with deduplication — no duplicate alerts
- **Smart reason detection** — classifies NOTAMs as military activity, VIP
  movement, airport maintenance, weather, or unknown
- **Severity classification** — full closure, major closure, partial, high-alt
- **Per-region subscriptions** — get automatic push alerts for the regions you
  care about
- **Telegram channel support** — optionally broadcast all alerts to a channel
- **15-minute polling** via APScheduler (configurable)
- **All conflict zones pre-loaded** — Russia, Ukraine, Belarus, Israel, Iran,
  Iraq, Syria, Lebanon, Caucasus, and more

---

## Quick Start

### 1. Get a Telegram Bot Token

1. Open Telegram and search for **@BotFather**.
2. Send `/newbot` and follow the prompts (choose a name and username).
3. Copy the token that BotFather gives you (looks like `123456:ABC-DEF...`).

### 2. Clone & Install

```bash
git clone https://github.com/mstarikov08-ai/airspace-tracker.git
cd airspace-tracker
pip install -r requirements.txt
```

> Python 3.10+ recommended.

### 3. Configure

```bash
cp .env.example .env
```

Open `.env` and set at minimum:

```
TELEGRAM_BOT_TOKEN=your_token_here
```

Optionally set `TELEGRAM_CHANNEL_ID` to post all alerts to a channel
(e.g. `@my_airspace_channel` or the numeric ID `-1001234567890`).
The bot must be a channel admin with **Post Messages** permission.

### 4. Run

```bash
python bot.py
```

The bot will:
1. Initialise the SQLite database (`notams.db`).
2. Immediately fetch NOTAMs for all monitored FIR codes.
3. Start polling every 15 minutes.
4. Listen for Telegram commands.

To run as a background service:

```bash
# systemd, screen, tmux, or simply:
nohup python bot.py &> bot.log &
```

---

## Bot Commands

| Command | Description |
|---|---|
| `/start` | Welcome message and feature overview |
| `/help` | Full command reference |
| `/notam ICAO` | Fetch active NOTAMs for a specific airport or FIR (live + cached) |
| `/region NAME` | List active closures for a monitored region |
| `/regions` | Show all monitored regions with their FIR codes |
| `/subscribe REGION` | Subscribe to automatic alerts for a region |
| `/unsubscribe REGION` | Cancel alerts for a region |
| `/active` | Summary of all currently active worldwide closures |
| `/map` | Links to live web maps (SkyVector, Flightradar24, etc.) |

**Examples:**
```
/notam UUWW          — Vnukovo airport (Moscow)
/notam LLLL          — Ben Gurion (Tel Aviv)
/region Russia       — All active Russian NOTAMs
/subscribe Ukraine   — Push alerts for Ukrainian airspace
/unsubscribe Iran
```

---

## Alert Format

Every alert sent by the bot looks like this:

```
⛔ AIRSPACE CLOSURE — Russia 🇷🇺
📍 Location: UUWW
⛔ Altitude: Surface to FL999 — Full airspace closure
⏰ Valid: 2024-01-15 14:00 UTC → 2024-01-15 18:00 UTC (4h)

📋 Raw NOTAM:
A1234/24 NOTAMN
Q) UURR/QRRCA/IV/BO/E/000/999/5553N03722E999
A) UUWW B) 2401150800 C) 2401151400
E) AIRSPACE CLOSED DUE TO MILITARY EXERCISE

💡 Likely reason: Military activity
```

### Severity Levels

| Emoji | Label | Condition |
|---|---|---|
| ⛔ | Full airspace closure | Surface to FL600+ |
| 🔴 | Major closure | Surface to FL200–FL599 |
| 🟠 | Low-level closure | Surface to below FL200 |
| 🟡 | High-altitude restriction | Above surface, FL600+ |
| 🟡 | Partial closure | Any other restriction |

---

## Monitored Regions & ICAO FIR Codes

### 🔴 High Priority

| Region | Flag | FIR Codes |
|---|---|---|
| Russia | 🇷🇺 | UURR, ULLL, UUWW, URWW, USSS, UHHH, UNKL, UOOO |
| Ukraine | 🇺🇦 | UKBV, UKOO, UKHH, UKFV, UKDV |
| Belarus | 🇧🇾 | UMMV |
| Israel | 🇮🇱 | LLLL |
| Iran | 🇮🇷 | OIIX, OIIG, OIIS, OIAF |
| Iraq | 🇮🇶 | ORBB |
| Lebanon | 🇱🇧 | OLBA |
| Syria | 🇸🇾 | OSTT |

### 🟡 Medium Priority

| Region | Flag | FIR Codes |
|---|---|---|
| Moldova | 🇲🇩 | LUUU |
| Georgia | 🇬🇪 | UGGG |
| Armenia | 🇦🇲 | UDDD |
| Azerbaijan | 🇦🇿 | UBBA |
| Turkey | 🇹🇷 | LTAA, LTBB |
| Jordan | 🇯🇴 | OJAC |
| Saudi Arabia | 🇸🇦 | OEJD, OERR |
| Yemen | 🇾🇪 | OYSC |
| Libya | 🇱🇾 | HLLL |

### 🔵 Low Priority

| Region | Flag | FIR Codes |
|---|---|---|
| Afghanistan | 🇦🇫 | OAKX |
| Pakistan | 🇵🇰 | OPKR |

---

## Project Structure

```
airspace-tracker/
├── bot.py            # Single-file bot implementation
├── requirements.txt  # Python dependencies
├── .env.example      # Configuration template
├── .env              # Your config (not committed)
└── notams.db         # SQLite database (auto-created)
```

---

## Data Sources

| Source | Coverage | Notes |
|---|---|---|
| [aviationweather.gov ADDS API](https://aviationweather.gov/api/data/notam) | Global | Free, no key required |

Future sources that could be added: ICAO iNOTAM, Eurocontrol B2B, national AIP portals.

---

## Limitations

- aviationweather.gov mirrors ICAO NOTAMs but may lag official national sources
  by a few minutes.
- Some FIR codes (especially conflict zones) may return no data if the country's
  AIS is not submitting to the international network.
- Telegram rate-limits bots to ~30 messages/second; during high-activity events
  alerts may be queued.

---

## License

MIT
