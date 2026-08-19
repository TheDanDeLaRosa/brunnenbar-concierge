"""
BrunnenBar Cloud Concierge, webhook service.

Always on service that answers WhatsApp and Instagram in the house voice, so
the concierge no longer needs Daniel's laptop open. Deploy on Railway.

Auto acknowledge mode, an inbound message is drafted by Claude in the house
rules and sent back automatically. Gmail and calendar are the next phase.

This build logs every inbound POST the moment it arrives, and if the Meta
signature does not match it logs a warning and still processes the message
rather than dropping it silently. That makes misconfiguration visible.

No secrets in this file. Everything comes from environment variables.
"""

import hashlib
import hmac
import json
import logging
import os
import random
import re
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("concierge")

app = FastAPI(title="BrunnenBar Cloud Concierge")

META_VERIFY_TOKEN = os.environ.get("META_VERIFY_TOKEN", "")
META_APP_SECRET = os.environ.get("META_APP_SECRET", "")
WHATSAPP_TOKEN = os.environ.get("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID", "")
WHATSAPP_BUSINESS_ACCOUNT_ID = os.environ.get("WHATSAPP_BUSINESS_ACCOUNT_ID", "")
INSTAGRAM_TOKEN = os.environ.get("INSTAGRAM_TOKEN", "")
INSTAGRAM_ACCOUNT_ID = os.environ.get("INSTAGRAM_ACCOUNT_ID", "")
# Facebook Messenger, the BrunnenBar Page inbox. Same Meta app and webhook shape
# as Instagram, just a Page access token and the Page id. Inbound arrives as
# object page with entry[].messaging[], the same structure the Instagram lane
# already reads, so it flows through the same handle() brain.
MESSENGER_PAGE_ID = os.environ.get("MESSENGER_PAGE_ID", "")
MESSENGER_TOKEN = os.environ.get("MESSENGER_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v20.0")
AUTO_ACK = os.environ.get("AUTO_ACK", "true").lower() == "true"

# Dualhook coexistence layer. When DUALHOOK_API_KEY is set, WhatsApp sends go
# through Dualhook's Graph compatible runtime (same payloads, different host and
# a dh_live_ key, no Meta token or appsecret_proof). Inbound is unaffected,
# Meta routes it straight to our webhook via Dualhook's Webhook Override. If the
# key is not set we fall back to sending directly through Meta, so nothing breaks
# before the coexistence connection exists.
DUALHOOK_API_KEY = os.environ.get("DUALHOOK_API_KEY", "")
DUALHOOK_BASE_URL = os.environ.get("DUALHOOK_BASE_URL", "https://api.dualhook.com/v25.0")

# Human feel. Replies wait a random number of seconds before sending so they do
# not look like an instant bot. Tune with the two env vars, seconds.
REPLY_DELAY_MIN = int(os.environ.get("REPLY_DELAY_MIN", "35"))
REPLY_DELAY_MAX = int(os.environ.get("REPLY_DELAY_MAX", "110"))

# Reservations. Google Calendar credentials already live in Railway.
RESERVIERUNGEN_CALENDAR_ID = os.environ.get("RESERVIERUNGEN_CALENDAR_ID", "")
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")
BOOKING_ENABLED = os.environ.get("BOOKING_ENABLED", "true").lower() == "true"
TURN_HOURS = int(os.environ.get("TURN_HOURS", "3"))
BAR_TZ = ZoneInfo("Europe/Berlin")

# Email, Gmail lane. Auto replies to genuine guest email in the house voice using
# the same brain and booking tool as chat. Runs as one background poll loop, which
# is safe because the service runs a single process. It uses its own refresh token
# for the bar inbox with gmail.modify and gmail.send scopes, kept separate from the
# calendar token so the working calendar auth is never touched. A start time gate
# means only mail that arrives after the service boots is answered, so the first
# deploy never blasts the existing backlog, and a handled label stops re-answering.
GMAIL_REFRESH_TOKEN = os.environ.get("GMAIL_REFRESH_TOKEN", "")
BAR_EMAIL = os.environ.get("BAR_EMAIL", "brunnenbaraugsburg@gmail.com")
EMAIL_ENABLED = os.environ.get("EMAIL_ENABLED", "true").lower() == "true"
EMAIL_POLL_SECONDS = int(os.environ.get("EMAIL_POLL_SECONDS", "90"))
EMAIL_HANDLED_LABEL = os.environ.get("EMAIL_HANDLED_LABEL", "Concierge-Beantwortet")
_EMAIL_START_MS = int(time.time() * 1000)
_gmail_label_cache = {}

# The bookable tables from the floor plan, name maps to seats and area. Outside
# is the 3XX tables, inside is the real tables. Bar stools and single seats are
# left for walk ins. Edit this to change what the bot can book.
TABLES = {
    "301": (6, "draussen"), "302": (4, "draussen"), "303": (4, "draussen"), "304": (4, "draussen"),
    "305": (2, "draussen"), "306": (2, "draussen"), "307": (2, "draussen"), "308": (2, "draussen"),
    "309": (2, "draussen"), "310": (2, "draussen"), "311": (2, "draussen"), "312": (2, "draussen"),
    "313": (2, "draussen"),
    "Stam": (8, "drinnen"), "HT1": (7, "drinnen"), "HT2": (7, "drinnen"), "HT3": (6, "drinnen"),
    "Sofa": (5, "drinnen"), "ST3": (4, "drinnen"), "Rnd2": (2, "drinnen"),
}

GRAPH = "https://graph.facebook.com/" + GRAPH_VERSION
_handled = set()
_last_msg = {}          # sender -> most recent message id, for debounce
_conv = {}              # sender -> list of {role, content}, short memory
_conv_lock = threading.Lock()


def conv_append(sender: str, role: str, content: str):
    with _conv_lock:
        h = _conv.setdefault(sender, [])
        h.append({"role": role, "content": content})
        del h[:-12]


def conv_history(sender: str):
    with _conv_lock:
        return list(_conv.get(sender, []))


def bar_time_context():
    """A German sentence stating the real current day, date and time in Augsburg
    and whether the bar is open right now, so the model never guesses the weekday.
    Open hours, Donnerstag 18 bis 24 Uhr, Freitag und Samstag 18 bis 2 Uhr, sonst zu.
    Fri and Sat run past midnight, so the early hours of Sat and Sun still count."""
    now = datetime.now(ZoneInfo("Europe/Berlin"))
    wd = now.weekday()  # Monday is 0, Sunday is 6
    h = now.hour
    days = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    open_now = (
        (wd == 3 and h >= 18)               # Donnerstag 18 bis 24
        or (wd == 4 and h >= 18)            # Freitag ab 18
        or (wd == 5 and (h < 2 or h >= 18)) # Samstag, Freitagnacht bis 2 und ab 18
        or (wd == 6 and h < 2)              # Sonntag, Samstagnacht bis 2
    )
    if wd == 3:
        today = "Heute (Donnerstag) hat die Bar von 18 bis 24 Uhr offen."
    elif wd == 4:
        today = "Heute (Freitag) hat die Bar von 18 bis 2 Uhr offen."
    elif wd == 5:
        today = "Heute (Samstag) hat die Bar von 18 bis 2 Uhr offen."
    else:
        today = "Heute ist die Bar geschlossen. Offen ist nur Donnerstag, Freitag und Samstag ab 18 Uhr."
    status = "Die Bar ist gerade GEOEFFNET." if open_now else "Die Bar ist gerade GESCHLOSSEN."
    return (
        "AKTUELLER ZEITPUNKT in Augsburg, verlass dich nur hierauf und rate niemals den Wochentag. "
        f"Es ist {days[wd]}, der {now.strftime('%d.%m.%Y')}, um {now.strftime('%H')} Uhr {now.strftime('%M')}. "
        f"{today} {status}"
    )


_DE_WORDS = set(
    "ich und wir für fuer möchte moechte tisch reservieren reservierung morgen heute "
    "personen leute danke bitte hallo hey servus seid ihr gerne uhr wäre waere hätte "
    "haette kann könnt koennt drinnen draussen draußen abend geöffnet offen euch dich "
    "einen zwei drei vier fünf fuenf sechs".split()
)
_EN_WORDS = set(
    "the a an can could i we you book booking table for tomorrow today please hi hello "
    "would like thanks thank name outside inside evening tonight people person do have "
    "is are how what when your our reserve reservation want get".split()
)


def detect_lang(text: str):
    """Rough English vs German detection for the guest's message, so we can pin
    the reply language in code rather than hope the prompt holds."""
    toks = re.findall(r"[a-zäöüß']+", (text or "").lower())
    de = sum(1 for t in toks if t in _DE_WORDS)
    en = sum(1 for t in toks if t in _EN_WORDS)
    if any(c in (text or "") for c in "äöüß"):
        de += 1
    if de > en:
        return "de"
    if en > de:
        return "en"
    return None


def _calendar_service():
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    creds = Credentials(
        None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=["https://www.googleapis.com/auth/calendar"],
    )
    return build("calendar", "v3", credentials=creds, cache_discovery=False)


_PARTY_RE = re.compile(r"(\d+)\s*Person", re.I)


def parse_event(ev):
    """Read (area, party, start) from a reservation, from the title and the
    structured description. Your entries record the area, drinnen or draussen or
    Bar, rather than a specific table. The area is taken from the last segment of
    the title so words in the Anlass do not confuse it, and a Bar or walk in
    entry returns None so it is not counted against the tables."""
    start = ev.get("start", {}).get("dateTime")
    if not start:
        return None
    summary = ev.get("summary", "") or ""
    desc = ev.get("description", "") or ""
    low_all = (summary + " " + desc).lower()
    m = re.search(r"anzahl personen\s*:?\s*(\d+)", low_all) or re.search(r"(\d+)\s*person", low_all)
    party = int(m.group(1)) if m else None
    segs = re.split(r"\s[–-]\s", summary)
    last = segs[-1].strip().lower() if segs else ""
    if last.startswith("drau"):
        area = "draussen"
    elif last.startswith("drinn"):
        area = "drinnen"
    elif last.startswith("bar"):
        area = None
    else:
        mm = re.search(r"reservierter bereich:\s*(drinnen|drau\w+|bar)", desc.lower())
        if mm and mm.group(1).startswith("drau"):
            area = "draussen"
        elif mm and mm.group(1).startswith("drinn"):
            area = "drinnen"
        else:
            area = None
    if not party or area is None:
        return None
    return (area, party, datetime.fromisoformat(start))


def reservations_on(date_iso: str):
    """Existing reservations for a date as a list of (area, party, start)."""
    svc = _calendar_service()
    day = datetime.fromisoformat(date_iso).replace(tzinfo=BAR_TZ)
    lo = day.replace(hour=0, minute=0, second=0, microsecond=0)
    hi = lo + timedelta(days=1)
    items = svc.events().list(
        calendarId=RESERVIERUNGEN_CALENDAR_ID,
        timeMin=lo.isoformat(), timeMax=hi.isoformat(),
        singleEvents=True, orderBy="startTime",
    ).execute().get("items", [])
    out = []
    for ev in items:
        r = parse_event(ev)
        if r:
            out.append(r)
    return out


def _area_tables(area):
    """Tables in an area, smallest first, as (name, seats)."""
    return sorted(((n, s) for n, (s, a) in TABLES.items() if a == area), key=lambda x: x[1])


def _seat_new_party(existing_parties, new_party, tables):
    """Fit every existing party plus the new one into the tables, largest first,
    each into the smallest table that still fits. Return the table the new party
    lands on, or None if they cannot all be seated. This is what stops overbooking."""
    entries = [("existing", p) for p in existing_parties] + [("new", new_party)]
    entries.sort(key=lambda x: -x[1])
    free = [[n, s] for n, s in tables]
    new_table = None
    for tag, p in entries:
        pick = next((i for i, (n, s) in enumerate(free) if s >= p), None)
        if pick is None:
            return None
        name = free.pop(pick)[0]
        if tag == "new":
            new_table = name
    return new_table


def find_free_table(date_iso: str, start_dt: datetime, party: int, area: str):
    """The table the new party would get in the requested area and 3 hour turn,
    or None if the area cannot seat everyone, so it never overbooks. The turn is
    measured from the actual start time, a 19:30 booking holds until 22:30."""
    turn = timedelta(hours=TURN_HOURS)
    req_end = start_dt + turn
    overlapping = [
        p for (a, p, s) in reservations_on(date_iso)
        if a == area and s < req_end and start_dt < s + turn
    ]
    return _seat_new_party(overlapping, party, _area_tables(area))


def create_reservation(name, contact, party, area, start_dt, occasion, table, note=""):
    svc = _calendar_service()
    end_dt = start_dt + timedelta(hours=TURN_HOURS)
    anlass = occasion or "Schöner Abend"
    # Title matches your convention, name, people, Anlass, and the assigned table,
    # like "Dan - 4 Personen - Schoener Abend - Tisch 302". The area is no longer in
    # the title, so the availability parser reads it from the Reservierter Bereich
    # line in the description below, which is always present.
    summary = f"{name} - {party} Personen - {anlass} - Tisch {table}"
    desc = (
        f"Name: {name}\n"
        f"Telefon/Contact: WhatsApp {contact}\n"
        f"Anzahl Personen: {party}\n"
        f"Besonderer Anlass: {anlass}\n"
        f"Reservierter Bereich: {area}\n"
        f"Zahlung: keine Info\n"
        f"Musik: keine Info\n"
        f"Essen: keine Info\n"
        f"Besondere Wuensche: keine\n"
        f"Wichtige Hinweise fuer das Team: automatisch vom Concierge gebucht, Tisch {table}. "
        f"Endzeit {end_dt.strftime('%H:%M')} ist nur eine Annahme. {note}"
    ).strip()
    body = {
        "summary": summary,
        "description": desc,
        "start": {"dateTime": start_dt.isoformat(), "timeZone": "Europe/Berlin"},
        "end": {"dateTime": end_dt.isoformat(), "timeZone": "Europe/Berlin"},
    }
    return svc.events().insert(calendarId=RESERVIERUNGEN_CALENDAR_ID, body=body).execute()


def process_booking(sender: str, data: dict, lang: str = "de") -> str:
    """Given the details the model gave the tool, check the table map, book if a
    table is free, and return the guest reply in the guest's language. Never
    overbooks."""
    try:
        party = int(data.get("party") or 0)
        area = "draussen" if str(data.get("area", "")).lower().startswith("drau") else "drinnen"
        date_iso = str(data["date"])
        hhmm = str(data["time"])
        start_dt = datetime.fromisoformat(date_iso + "T" + hhmm).replace(tzinfo=BAR_TZ)
        name = (data.get("name") or "").strip() or "Gast"
        occasion = (data.get("occasion") or "").strip() or "Schöner Abend"
        sie = bool(data.get("sie"))
    except Exception as e:
        logger.error("booking parse failed: %s data=%s", e, data)
        return ""
    if not (BOOKING_ENABLED and GOOGLE_REFRESH_TOKEN and RESERVIERUNGEN_CALENDAR_ID):
        logger.warning("booking not configured, handing off")
        return _handoff_line(sie, lang)
    try:
        table = find_free_table(date_iso, start_dt, party, area)
    except Exception as e:
        logger.error("availability check failed: %s", e)
        return _handoff_line(sie, lang)
    if not table:
        logger.info("no free table for %s %s party %s %s", date_iso, hhmm, party, area)
        return _full_line(sie, lang)
    try:
        create_reservation(name, sender, party, area, start_dt, occasion, table)
    except Exception as e:
        logger.error("create_reservation failed: %s", e)
        return _handoff_line(sie, lang)
    logger.info("booked table %s for %s party %s %s %s", table, name, party, date_iso, hhmm)
    h = start_dt.strftime("%H")
    if lang == "en":
        days_en = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        where = "outside" if area == "draussen" else "inside"
        h12 = start_dt.hour % 12 or 12
        mins = f":{start_dt.strftime('%M')}" if start_dt.minute else ""
        time_en = f"{h12}{mins} {'am' if start_dt.hour < 12 else 'pm'}"
        return (f"Perfect, your table for {party} on {days_en[start_dt.weekday()]} at {time_en} {where} "
                f"is confirmed, we look forward to having you and see you soon LG Dan")
    days_de = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]
    bereich = "draussen" if area == "draussen" else "drinnen"
    wd = days_de[start_dt.weekday()]
    if sie:
        return (f"Sehr gerne, Ihr Tisch fuer {party} am {wd} um {h} Uhr {bereich} "
                f"ist fest reserviert. Wir freuen uns auf Sie und bis bald LG Dan")
    return (f"Sehr gerne, dein Tisch fuer {party} am {wd} um {h} Uhr {bereich} "
            f"ist fest reserviert, wir freuen uns auf euch und bis bald LG Dan")


def _handoff_line(sie=False, lang="de"):
    if lang == "en":
        return "I'll pass this straight to Dan and he'll get back to you personally very soon LG Dan"
    if sie:
        return "Ich gebe Ihre Anfrage direkt an Dan weiter, er meldet sich gleich persoenlich bei Ihnen LG Dan"
    return "Ich geb deine Anfrage direkt an Dan weiter, er meldet sich gleich persoenlich bei dir LG Dan"


def _full_line(sie=False, lang="de"):
    if lang == "en":
        return ("It is unfortunately already quite full at that time, I'll pass it straight to Dan "
                "and he'll see if something can still be arranged LG Dan")
    if sie:
        return ("Fuer den Zeitpunkt ist es leider schon recht voll, ich gebe es direkt an Dan weiter "
                "und er schaut ob sich doch noch etwas machen laesst LG Dan")
    return ("Fuer den Zeitpunkt ist es leider schon recht voll, ich geb es direkt an Dan weiter "
            "und er schaut ob sich doch noch was machen laesst LG Dan")

SYSTEM_PROMPT = """You are the concierge for BrunnenBar, a neighbourhood cocktail bar in Augsburg, Germany. You reply to guest messages on WhatsApp and Instagram on behalf of the owner Daniel, called Dan, as if you were Dan or his team.

TRIAGE FIRST. Decide what kind of message this is.
If it is a genuine guest, a reservation, a birthday or group, an event, opening hours, or a normal guest question, answer it following the rules below.
If it is spam, a cold sales pitch, a marketing, collaboration, press, sponsoring or supplier message, a delivery or app notification, or clearly not from a real guest, reply with exactly the single word SKIP and nothing else.
If it is about prices or Mindestumsatz beyond the guidance here, or anything you are genuinely unsure about, also reply with the single word SKIP, because Dan will handle it himself and silence is safer than a wrong answer.
If a guest is unhappy or complaining, do not try to solve it. Send one short warm line that you are sorry and that you are passing it straight to Dan who will get back to them personally, then stop.

VOICE. Write exactly the way Dan writes. Warm, personal, relaxed, never corporate, never like a bot. Use informal du and euch by default, but if the guest clearly writes formally with Sie, mirror that and answer in the Sie form throughout. Greet with a friendly opener and their first name when you know it, like Hey Lisa or Hallo Tobias. In a German reply use warm touches like sehr gerne and danke dir, in an English reply use the natural English equivalent and never those German words. Close warmly, in German usually Wir freuen uns auf euch and then LG Dan, in English the natural equivalent and then LG Dan. Small human touches are good, like mentioning the weather for an outside table. Keep it to two or three short flowing sentences.

HARD FORMAT RULES, no exceptions. Never use hyphens, dashes, bullet points, numbered lists, colons or semicolons. Clock times like 19 Uhr are fine. Connect thoughts with und and dann and the odd comma the way Dan does. No emoji. Always sign LG Dan.

LANGUAGE. Reply completely in the language the guest wrote in, and never mix two languages in one message. If the guest writes English, the whole reply must be natural English, so write inside or outside, not drinnen or draussen, and do not drop in German phrases like sehr gerne. The only German you keep in an English reply is the sign off LG Dan. If the guest writes German, reply fully in German. The words drinnen and draussen only ever appear inside the book_table tool call, never in an English guest message.

CONTEXT AND OWNING MISTAKES. You can see the whole conversation, so read it before you reply and fit where the chat already is. Do not greet a returning guest as if this is the first message, do not ask something that was already answered, and pick up naturally from what was said. Very important, if YOU said something wrong earlier, for example the wrong day or wrong hours, and the guest corrects you, own it warmly and apologise, something like sorry, da hab ich mich vertan, and then give the right answer. Never act as if the guest made the mistake and never pretend it did not happen.

TIME AND OPENING HOURS. For anything about whether the bar is open, or what day or time it is, rely ONLY on the AKTUELLER ZEITPUNKT line given to you and never guess the weekday. Opening hours are Donnerstag 18 bis 24 Uhr, Freitag und Samstag 18 bis 2 Uhr, sonst geschlossen. There is a Happy Hour bis 20 Uhr, mention it warmly but never quote prices. If today is a closed day, say so kindly and name the next open day.

RESERVATIONS up to six people. You need six things, the date, the time, the number of people, a name, whether they would like inside or outside, and whether it is for a special occasion. Always ask about the occasion, warmly, even if they have not mentioned one, because we like to note it, and if there is none that is completely fine. If any of these is missing, ask for what is missing warmly in one short flowing message, never as a list, and do not book yet. Once you have all six, do NOT write a confirmation yourself. Instead call the book_table tool with the details. Resolve the date to YYYY-MM-DD using the AKTUELLER ZEITPUNKT line, use 24 hour time as HH:MM, area is exactly drinnen or draussen, occasion is the Anlass or an empty string, and sie is true only if you are speaking to the guest in the formal Sie form. The system then checks the real table availability, books an actual table and sends the guest the confirmation for you, so when you call book_table you do not also write any message. Only ever call book_table for parties of up to six people, never for seven or more.

SAME DAY BY PHONE. This rule comes before the booking rule. If a guest wants a table for today AND the bar is open today AND it is after 18 Uhr right now, never call book_table. Instead warmly tell them to please call the bar directly under 0821 47019035 rather than WhatsApp, because a table for tonight is arranged fastest by phone.

GROUPS AND EVENTS, seven people or more, or any birthday, party or private booking. Treat it as an event and do not confirm anything. Warmly work through the occasion, whether they have been to BrunnenBar before, the date and time, how many people, drinnen or draussen, and roughly what they have in mind. Then tell them Dan gets back to them personally with an Angebot by email. Never mention Mindestumsatz or prices. If a guest asks about price straight away, do not give a number, instead ask warmly how many people they are and whether they have been to the bar before.

FACTS YOU MAY SHARE. BrunnenBar is on Am Brunnenlech in Augsburg. There is no kitchen, so guests are welcome to bring their own food and cake, and caterers like Thassos are possible. Dogs are welcome. There is WLAN. You can pay by card or cash. Parking is easiest at the City Galerie. Getting into the bar is barrier free, but there is a small step up to the toilets and the toilets are quite tight for a wheelchair, so be honest about that. Never invent capacity, deposit, cancellation or any policy not listed here, if you do not know, say you will check and Dan will come back to them.

Never congratulate in advance for a birthday, wedding or anything that has not happened yet, that is bad luck, show excitement about hosting instead. Never put a bank account, IBAN or card number into a message.

When you are not calling the book_table tool, output only the message text to send, or the single word SKIP. Never mention the tool or JSON to the guest."""


BOOK_TOOL = {
    "name": "book_table",
    "description": (
        "Reserve a table. Only call this once you have ALL of these from the guest, the date, "
        "the time, the number of people which must be six or fewer, the area drinnen or draussen, "
        "the guest's name, and whether it is for a special occasion. Do NOT call it for seven or "
        "more people, for an event, or for a same day request after 18 Uhr. Calling it books a real "
        "table and sends the guest the confirmation, so when you call it you write no message yourself."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "The guest's name"},
            "party": {"type": "integer", "description": "Number of people, 1 to 6"},
            "area": {"type": "string", "enum": ["drinnen", "draussen"]},
            "date": {"type": "string", "description": "YYYY-MM-DD, resolved from the AKTUELLER ZEITPUNKT line"},
            "time": {"type": "string", "description": "HH:MM in 24 hour time"},
            "occasion": {"type": "string", "description": "The Anlass, or an empty string if none"},
            "sie": {"type": "boolean", "description": "true only if addressing the guest formally with Sie"},
        },
        "required": ["name", "party", "area", "date", "time", "occasion", "sie"],
    },
}


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/debug")
def debug():
    """Which config is present, booleans only, never the values. Safe to open in a browser."""
    return {
        "META_VERIFY_TOKEN": bool(META_VERIFY_TOKEN),
        "META_APP_SECRET": bool(META_APP_SECRET),
        "WHATSAPP_TOKEN": bool(WHATSAPP_TOKEN),
        "WHATSAPP_PHONE_NUMBER_ID": WHATSAPP_PHONE_NUMBER_ID or None,
        "WHATSAPP_BUSINESS_ACCOUNT_ID": WHATSAPP_BUSINESS_ACCOUNT_ID or None,
        "INSTAGRAM_TOKEN": bool(INSTAGRAM_TOKEN),
        "INSTAGRAM_ACCOUNT_ID": INSTAGRAM_ACCOUNT_ID or None,
        "MESSENGER_TOKEN": bool(MESSENGER_TOKEN),
        "MESSENGER_PAGE_ID": MESSENGER_PAGE_ID or None,
        "GMAIL_REFRESH_TOKEN": bool(GMAIL_REFRESH_TOKEN),
        "EMAIL_ENABLED": EMAIL_ENABLED,
        "EMAIL_POLL_SECONDS": EMAIL_POLL_SECONDS,
        "BAR_EMAIL": BAR_EMAIL or None,
        "ANTHROPIC_API_KEY": bool(ANTHROPIC_API_KEY),
        "GRAPH_VERSION": GRAPH_VERSION,
        "AUTO_ACK": AUTO_ACK,
        "DUALHOOK_API_KEY": bool(DUALHOOK_API_KEY),
        "DUALHOOK_BASE_URL": DUALHOOK_BASE_URL,
        "whatsapp_send_path": (DUALHOOK_BASE_URL if DUALHOOK_API_KEY else GRAPH) + "/" + (WHATSAPP_PHONE_NUMBER_ID or "<PHONE_NUMBER_ID>") + "/messages",
        "BOOKING_ENABLED": BOOKING_ENABLED,
        "GOOGLE_REFRESH_TOKEN": bool(GOOGLE_REFRESH_TOKEN),
        "RESERVIERUNGEN_CALENDAR_ID": bool(RESERVIERUNGEN_CALENDAR_ID),
        "TURN_HOURS": TURN_HOURS,
        "bookable_tables": len(TABLES),
    }


@app.get("/calendars")
def calendars_debug():
    """List the calendars this connection can see, so we can confirm the
    RESERVIERUNGEN_CALENDAR_ID points at the real reservations calendar."""
    try:
        svc = _calendar_service()
        items = svc.calendarList().list().execute().get("items", [])
        return {
            "configured_id": RESERVIERUNGEN_CALENDAR_ID,
            "calendars": [{"id": c.get("id"), "summary": c.get("summary")} for c in items],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/reservations")
def reservations_debug(date: str = ""):
    """Read the Reservierungen calendar for a date, YYYY-MM-DD, default today.
    Shows both the raw events and how the parser reads their tables, so we can
    confirm the calendar connection and the table parsing before trusting it."""
    if not date:
        date = datetime.now(BAR_TZ).strftime("%Y-%m-%d")
    try:
        svc = _calendar_service()
        day = datetime.fromisoformat(date).replace(tzinfo=BAR_TZ)
        lo = day.replace(hour=0, minute=0, second=0, microsecond=0)
        hi = lo + timedelta(days=1)
        items = svc.events().list(
            calendarId=RESERVIERUNGEN_CALENDAR_ID,
            timeMin=lo.isoformat(), timeMax=hi.isoformat(),
            singleEvents=True, orderBy="startTime",
        ).execute().get("items", [])
        raw = []
        for e in items:
            p = parse_event(e)
            raw.append({
                "summary": e.get("summary", ""),
                "start": e.get("start", {}).get("dateTime") or e.get("start", {}).get("date"),
                "read_as": ({"area": p[0], "party": p[1]} if p else "not counted, bar or unspecified"),
            })
        parsed = reservations_on(date)
        return {
            "date": date,
            "raw_event_count": len(items),
            "raw": raw,
            "counted_count": len(parsed),
            "counted": [{"area": a, "party": pp, "start": s.isoformat()} for a, pp, s in parsed],
        }
    except Exception as e:
        return {"error": str(e)}


def subscribe_waba():
    """Subscribe this app to the WhatsApp Business Account so inbound message
    webhooks are actually delivered. Without this, the messages field can be
    subscribed at app level yet no messages arrive. Idempotent, safe to repeat."""
    if not (WHATSAPP_BUSINESS_ACCOUNT_ID and WHATSAPP_TOKEN):
        logger.warning("subscribe_waba: missing WHATSAPP_BUSINESS_ACCOUNT_ID or WHATSAPP_TOKEN")
        return {"error": "missing WHATSAPP_BUSINESS_ACCOUNT_ID or WHATSAPP_TOKEN"}
    try:
        r = httpx.post(
            GRAPH + "/" + WHATSAPP_BUSINESS_ACCOUNT_ID + "/subscribed_apps",
            headers={"Authorization": "Bearer " + WHATSAPP_TOKEN},
            timeout=30,
        )
        logger.info("subscribe_waba response %s %s", r.status_code, r.text)
        return {"status": r.status_code, "body": r.text}
    except Exception as e:
        detail = getattr(e, "response", None)
        logger.error("subscribe_waba failed: %s %s", e, detail.text if detail is not None else "")
        return {"error": str(e)}


@app.get("/subscribe")
def subscribe_route():
    """Open in a browser to force the subscription and see the result."""
    return subscribe_waba()


def subscribe_page():
    """Subscribe the app to the Facebook Page so Messenger message webhooks are
    delivered. Needs the Page access token and Page id. Idempotent."""
    if not (MESSENGER_PAGE_ID and MESSENGER_TOKEN):
        logger.warning("subscribe_page: missing MESSENGER_PAGE_ID or MESSENGER_TOKEN")
        return {"error": "missing MESSENGER_PAGE_ID or MESSENGER_TOKEN"}
    try:
        r = httpx.post(
            GRAPH + "/" + MESSENGER_PAGE_ID + "/subscribed_apps",
            headers={"Authorization": "Bearer " + MESSENGER_TOKEN},
            params={"subscribed_fields": "messages,messaging_postbacks"},
            timeout=30,
        )
        logger.info("subscribe_page response %s %s", r.status_code, r.text)
        return {"status": r.status_code, "body": r.text}
    except Exception as e:
        detail = getattr(e, "response", None)
        logger.error("subscribe_page failed: %s %s", e, detail.text if detail is not None else "")
        return {"error": str(e)}


@app.get("/subscribe_messenger")
def subscribe_messenger_route():
    """Open in a browser to force the Page subscription and see the result."""
    return subscribe_page()


@app.on_event("startup")
def _startup_subscribe():
    logger.info("Startup: subscribing app to WhatsApp Business Account %s", WHATSAPP_BUSINESS_ACCOUNT_ID or "(not set)")
    subscribe_waba()
    if MESSENGER_PAGE_ID and MESSENGER_TOKEN:
        logger.info("Startup: subscribing app to Facebook Page %s", MESSENGER_PAGE_ID)
        subscribe_page()
    if EMAIL_ENABLED and GMAIL_REFRESH_TOKEN:
        logger.info("Startup: starting Gmail poll loop every %s s for %s", EMAIL_POLL_SECONDS, BAR_EMAIL)
        threading.Thread(target=poll_gmail_loop, daemon=True).start()
    else:
        logger.info("Startup: Gmail lane off (no GMAIL_REFRESH_TOKEN or EMAIL_ENABLED false)")


def challenge(request: Request):
    from fastapi import Response
    p = request.query_params
    if p.get("hub.mode") == "subscribe" and p.get("hub.verify_token") == META_VERIFY_TOKEN:
        return Response(content=p.get("hub.challenge", ""), media_type="text/plain")
    return Response(content="verification failed", status_code=403)


def signature_matches(body: bytes, header: str) -> bool:
    if not META_APP_SECRET or not header:
        return False
    expected = "sha256=" + hmac.new(META_APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


def check_sig(kind: str, body: bytes, header: str):
    if not META_APP_SECRET:
        logger.warning("%s: META_APP_SECRET not set, skipping signature check", kind)
        return
    if not signature_matches(body, header):
        logger.warning("%s: signature did NOT match. Check that META_APP_SECRET in Railway equals the app's App secret. Processing anyway.", kind)
    else:
        logger.info("%s: signature ok", kind)


@app.get("/webhook/whatsapp")
async def whatsapp_verify(request: Request):
    return challenge(request)


@app.post("/webhook/whatsapp")
async def whatsapp_receive(request: Request):
    body = await request.body()
    logger.info("WhatsApp POST received, %d bytes", len(body))
    check_sig("WhatsApp", body, request.headers.get("X-Hub-Signature-256", ""))
    try:
        data = await request.json()
    except Exception:
        logger.error("WhatsApp POST body was not JSON")
        return {"received": True}
    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            value = change.get("value", {})
            for msg in value.get("messages", []):
                mid = msg.get("id")
                if not mid or mid in _handled:
                    continue
                _handled.add(mid)
                if msg.get("type") != "text":
                    logger.info("WhatsApp non text message, skipping")
                    continue
                sender = msg.get("from")
                text = (msg.get("text") or {}).get("body", "")
                logger.info("WhatsApp in from %s: %s", sender, text[:120])
                conv_append(sender, "user", text)
                _last_msg[sender] = mid
                threading.Thread(target=handle_later, args=("whatsapp", sender, text, mid), daemon=True).start()
    return {"received": True}


@app.get("/webhook/instagram")
async def instagram_verify(request: Request):
    return challenge(request)


@app.post("/webhook/instagram")
async def instagram_receive(request: Request):
    body = await request.body()
    logger.info("Instagram POST received, %d bytes", len(body))
    check_sig("Instagram", body, request.headers.get("X-Hub-Signature-256", ""))
    try:
        data = await request.json()
    except Exception:
        logger.error("Instagram POST body was not JSON")
        return {"received": True}
    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            message = event.get("message") or {}
            mid = message.get("mid")
            text = message.get("text", "")
            if message.get("is_echo") or not text or not mid or mid in _handled:
                continue
            _handled.add(mid)
            sender = event.get("sender", {}).get("id")
            logger.info("Instagram in from %s: %s", sender, text[:120])
            conv_append(sender, "user", text)
            _last_msg[sender] = mid
            threading.Thread(target=handle_later, args=("instagram", sender, text, mid), daemon=True).start()
    return {"received": True}


@app.get("/webhook/messenger")
async def messenger_verify(request: Request):
    return challenge(request)


@app.post("/webhook/messenger")
async def messenger_receive(request: Request):
    body = await request.body()
    logger.info("Messenger POST received, %d bytes", len(body))
    check_sig("Messenger", body, request.headers.get("X-Hub-Signature-256", ""))
    try:
        data = await request.json()
    except Exception:
        logger.error("Messenger POST body was not JSON")
        return {"received": True}
    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            message = event.get("message") or {}
            mid = message.get("mid")
            text = message.get("text", "")
            if message.get("is_echo") or not text or not mid or mid in _handled:
                continue
            _handled.add(mid)
            sender = event.get("sender", {}).get("id")
            logger.info("Messenger in from %s: %s", sender, text[:120])
            conv_append(sender, "user", text)
            _last_msg[sender] = mid
            threading.Thread(target=handle_later, args=("messenger", sender, text, mid), daemon=True).start()
    return {"received": True}


def handle_later(channel: str, sender: str, text: str, mid: str = None):
    """Wait a random human feeling pause, then draft and send. Runs in its own
    thread so the webhook can return 200 to Meta straight away. If a newer
    message from the same sender arrived meanwhile, this older one steps aside
    so the newest turn answers with the full context."""
    lo, hi = sorted((REPLY_DELAY_MIN, REPLY_DELAY_MAX))
    delay = random.uniform(lo, hi)
    logger.info("%s reply to %s scheduled in %.0f s", channel, sender, delay)
    time.sleep(delay)
    if mid is not None and _last_msg.get(sender) != mid:
        logger.info("newer message from %s arrived, skipping older scheduled reply", sender)
        return
    handle(channel, sender, text)


def handle(channel: str, sender: str, text: str):
    action, value, lang = claude_decide(sender, text)
    if action == "book":
        logger.info("book_table called by model: %s", value)
        reply = process_booking(sender, value, lang) or _handoff_line(bool(value.get("sie")), lang)
    else:
        reply = value
    if not reply or reply.strip().upper() == "SKIP":
        logger.info("No reply, classified as not a guest inquiry or empty draft")
        return
    logger.info("Draft reply (%s): %s", channel, reply[:200])
    conv_append(sender, "assistant", reply)
    if not AUTO_ACK:
        logger.info("AUTO_ACK off, draft logged only, not sending")
        return
    if channel == "whatsapp":
        send_whatsapp(sender, reply)
    elif channel == "instagram":
        send_instagram(sender, reply)
    elif channel == "messenger":
        send_messenger(sender, reply)


def _clean_messages(history):
    """Collapse consecutive same role turns and make sure it starts with user,
    so the Anthropic messages array is always valid."""
    msgs = []
    for m in history:
        role = m.get("role")
        content = (m.get("content") or "").strip()
        if not content:
            continue
        if msgs and msgs[-1]["role"] == role:
            msgs[-1]["content"] += "\n" + content
        else:
            msgs.append({"role": role, "content": content})
    while msgs and msgs[0]["role"] != "user":
        msgs.pop(0)
    return msgs


def claude_decide(sender: str, text: str):
    """Ask the model what to do. Returns (action, value, lang), where action is
    'book' with value a details dict, 'text' with value the reply string, or
    'none'. Booking goes through the book_table tool, which is far more reliable
    than asking the model to emit a special line."""
    lang = detect_lang(text)
    if not ANTHROPIC_API_KEY:
        logger.warning("No ANTHROPIC_API_KEY, cannot draft")
        return ("none", "", lang)
    messages = _clean_messages(conv_history(sender)) or [{"role": "user", "content": text}]
    system = SYSTEM_PROMPT + "\n\n" + bar_time_context()
    if lang == "en":
        system += (
            "\n\nLANGUAGE OVERRIDE for this reply. The guest is writing in ENGLISH. "
            "Write your entire reply in natural English. Do not use any German words such as "
            "sehr gerne, drinnen or draussen, write inside and outside instead. The only German "
            "allowed anywhere in the reply is the sign off LG Dan."
        )
    elif lang == "de":
        system += "\n\nLANGUAGE OVERRIDE for this reply. The guest is writing in German, so reply fully in German."
    try:
        r = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-5",
                "max_tokens": 500,
                "system": system,
                "tools": [BOOK_TOOL],
                "messages": messages,
            },
            timeout=30,
        )
        r.raise_for_status()
        parts = r.json().get("content", [])
        for p in parts:
            if p.get("type") == "tool_use" and p.get("name") == "book_table":
                return ("book", p.get("input", {}) or {}, lang)
        text_out = "".join(p.get("text", "") for p in parts if p.get("type") == "text").strip()
        return ("text", text_out, lang)
    except Exception as e:
        detail = getattr(e, "response", None)
        logger.error("Claude decide failed: %s %s", e, detail.text if detail is not None else "")
        return ("none", "", lang)


def send_whatsapp(to: str, text: str):
    if DUALHOOK_API_KEY:
        url = DUALHOOK_BASE_URL + "/" + WHATSAPP_PHONE_NUMBER_ID + "/messages"
        headers = {"Authorization": "Bearer " + DUALHOOK_API_KEY}
        via = "Dualhook"
    else:
        url = GRAPH + "/" + WHATSAPP_PHONE_NUMBER_ID + "/messages"
        headers = {"Authorization": "Bearer " + WHATSAPP_TOKEN}
        via = "Meta direct"
    try:
        r = httpx.post(
            url,
            headers=headers,
            json={"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": text}},
            timeout=30,
        )
        r.raise_for_status()
        logger.info("WhatsApp reply sent to %s via %s", to, via)
    except Exception as e:
        detail = getattr(e, "response", None)
        logger.error("WhatsApp send failed via %s: %s %s", via, e, detail.text if detail is not None else "")


def send_instagram(recipient_id: str, text: str):
    try:
        r = httpx.post(
            GRAPH + "/" + INSTAGRAM_ACCOUNT_ID + "/messages",
            headers={"Authorization": "Bearer " + INSTAGRAM_TOKEN},
            json={"recipient": {"id": recipient_id}, "message": {"text": text}},
            timeout=30,
        )
        r.raise_for_status()
        logger.info("Instagram reply sent to %s", recipient_id)
    except Exception as e:
        detail = getattr(e, "response", None)
        logger.error("Instagram send failed: %s %s", e, detail.text if detail is not None else "")


def send_messenger(recipient_id: str, text: str):
    """Send a Facebook Messenger reply from the Page. messaging_type RESPONSE marks
    it as a direct reply to the guest, which is what Messenger expects inside the
    standard messaging window."""
    try:
        r = httpx.post(
            GRAPH + "/" + (MESSENGER_PAGE_ID or "me") + "/messages",
            headers={"Authorization": "Bearer " + MESSENGER_TOKEN},
            json={
                "recipient": {"id": recipient_id},
                "messaging_type": "RESPONSE",
                "message": {"text": text},
            },
            timeout=30,
        )
        r.raise_for_status()
        logger.info("Messenger reply sent to %s", recipient_id)
    except Exception as e:
        detail = getattr(e, "response", None)
        logger.error("Messenger send failed: %s %s", e, detail.text if detail is not None else "")


# ---------------------------------------------------------------------------
# Email, Gmail lane
# ---------------------------------------------------------------------------

def _gmail_service():
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    creds = Credentials(
        None,
        refresh_token=GMAIL_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=[
            "https://www.googleapis.com/auth/gmail.modify",
            "https://www.googleapis.com/auth/gmail.send",
        ],
    )
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _gmail_label_id(svc):
    """Id of the handled label, created once if missing, cached for the process."""
    if "id" in _gmail_label_cache:
        return _gmail_label_cache["id"]
    labels = svc.users().labels().list(userId="me").execute().get("labels", [])
    for l in labels:
        if l.get("name") == EMAIL_HANDLED_LABEL:
            _gmail_label_cache["id"] = l["id"]
            return l["id"]
    created = svc.users().labels().create(
        userId="me",
        body={"name": EMAIL_HANDLED_LABEL, "labelListVisibility": "labelShow", "messageListVisibility": "show"},
    ).execute()
    _gmail_label_cache["id"] = created["id"]
    return created["id"]


def _hdr(headers, name):
    for h in headers:
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def _addr(value):
    m = re.search(r"<([^>]+)>", value or "")
    return (m.group(1) if m else (value or "")).strip()


def _extract_plain(payload):
    """Best effort plain text from a Gmail payload, preferring text/plain, falling
    back to stripped html."""
    import base64

    def decode(data):
        if not data:
            return ""
        return base64.urlsafe_b64decode(data.encode()).decode("utf-8", "replace")

    mt = payload.get("mimeType", "")
    body = payload.get("body", {})
    if mt == "text/plain" and body.get("data"):
        return decode(body["data"])
    if mt.startswith("multipart"):
        for p in payload.get("parts", []):
            if p.get("mimeType") == "text/plain" and p.get("body", {}).get("data"):
                return decode(p["body"]["data"])
        for p in payload.get("parts", []):
            r = _extract_plain(p)
            if r:
                return r
    if mt == "text/html" and body.get("data"):
        return re.sub(r"<[^>]+>", " ", decode(body["data"]))
    return ""


def _is_automated_or_self(headers, from_addr):
    """True for our own address, no reply senders, bounces, newsletters and any
    machine mail, so we never auto answer things a real guest did not send."""
    fa = (from_addr or "").lower()
    if BAR_EMAIL.lower() and BAR_EMAIL.lower() in fa:
        return True
    for bad in ("no-reply", "noreply", "no_reply", "donotreply", "do-not-reply",
                "mailer-daemon", "postmaster", "notification", "notifications", "bounce"):
        if bad in fa:
            return True
    if _hdr(headers, "List-Unsubscribe") or _hdr(headers, "List-Id"):
        return True
    if _hdr(headers, "Precedence").lower() in ("bulk", "list", "junk"):
        return True
    auto = _hdr(headers, "Auto-Submitted").lower()
    if auto and auto != "no":
        return True
    return False


def send_email(svc, to, subject, body, headers, thread_id):
    import base64
    from email.mime.text import MIMEText
    msg_id_hdr = _hdr(headers, "Message-ID")
    refs = _hdr(headers, "References")
    if subject and subject.lower().startswith("re:"):
        subj = subject
    elif subject:
        subj = "Re: " + subject
    else:
        subj = "Deine Nachricht an BrunnenBar"
    mime = MIMEText(body, "plain", "utf-8")
    mime["To"] = to
    mime["From"] = BAR_EMAIL
    mime["Subject"] = subj
    if msg_id_hdr:
        mime["In-Reply-To"] = msg_id_hdr
        mime["References"] = (refs + " " + msg_id_hdr).strip() if refs else msg_id_hdr
    raw = base64.urlsafe_b64encode(mime.as_bytes()).decode()
    send_body = {"raw": raw}
    if thread_id:
        send_body["threadId"] = thread_id
    svc.users().messages().send(userId="me", body=send_body).execute()
    logger.info("Email reply sent to %s", to)


def handle_email(svc, msg_id):
    full = svc.users().messages().get(userId="me", id=msg_id, format="full").execute()
    payload = full.get("payload", {})
    headers = payload.get("headers", [])
    label_id = _gmail_label_id(svc)

    def mark_handled():
        try:
            svc.users().messages().modify(userId="me", id=msg_id, body={"addLabelIds": [label_id]}).execute()
        except Exception as e:
            logger.error("email label failed %s: %s", msg_id, e)

    if int(full.get("internalDate", "0")) < _EMAIL_START_MS:
        mark_handled()  # pre existing mail, never answer the backlog
        return
    from_addr = _addr(_hdr(headers, "From"))
    if _is_automated_or_self(headers, from_addr):
        logger.info("email skip automated/self from %s", from_addr)
        mark_handled()
        return
    subject = _hdr(headers, "Subject")
    body = _extract_plain(payload).strip()
    if not body:
        mark_handled()
        return
    text = (subject + "\n\n" + body) if subject else body
    text = text[:4000]
    sender_key = "email:" + from_addr
    logger.info("Email in from %s: %s", from_addr, (subject or body)[:120])
    conv_append(sender_key, "user", text)
    _last_msg[sender_key] = msg_id
    action, value, lang = claude_decide(sender_key, text)
    if action == "book":
        logger.info("book_table called by model from email: %s", value)
        reply = process_booking(sender_key, value, lang) or _handoff_line(bool(value.get("sie")), lang)
    else:
        reply = value
    if not reply or reply.strip().upper() == "SKIP":
        logger.info("email classified SKIP or empty from %s", from_addr)
        mark_handled()
        return
    conv_append(sender_key, "assistant", reply)
    if AUTO_ACK:
        try:
            send_email(svc, to=from_addr, subject=subject, body=reply, headers=headers, thread_id=full.get("threadId"))
        except Exception as e:
            detail = getattr(e, "response", None)
            logger.error("email send failed to %s: %s %s", from_addr, e, detail.text if detail is not None else "")
    else:
        logger.info("AUTO_ACK off, email draft logged only, not sending")
    mark_handled()


def poll_gmail_once():
    svc = _gmail_service()
    q = f"in:inbox -label:{EMAIL_HANDLED_LABEL} newer_than:2d"
    res = svc.users().messages().list(userId="me", q=q, maxResults=15).execute()
    ids = [m["id"] for m in res.get("messages", [])]
    for mid in ids:
        try:
            handle_email(svc, mid)
        except Exception as e:
            logger.error("handle_email failed for %s: %s", mid, e)
    return len(ids)


def poll_gmail_loop():
    while True:
        try:
            if EMAIL_ENABLED and GMAIL_REFRESH_TOKEN:
                poll_gmail_once()
        except Exception as e:
            logger.error("gmail poll loop error: %s", e)
        time.sleep(EMAIL_POLL_SECONDS)


@app.get("/poll_email")
def poll_email_route():
    """Force one Gmail poll and see how many new messages it looked at. Safe to open
    in a browser once email is configured, for testing."""
    if not (EMAIL_ENABLED and GMAIL_REFRESH_TOKEN):
        return {"error": "email not configured, set GMAIL_REFRESH_TOKEN and EMAIL_ENABLED"}
    try:
        n = poll_gmail_once()
        return {"ok": True, "looked_at": n}
    except Exception as e:
        return {"error": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
