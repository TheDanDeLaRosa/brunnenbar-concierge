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
import logging
import os
import random
import threading
import time
from datetime import datetime
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

GRAPH = "https://graph.facebook.com/" + GRAPH_VERSION
_handled = set()


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

SYSTEM_PROMPT = """You are the concierge for BrunnenBar, a neighbourhood cocktail bar in Augsburg, Germany. You reply to guest messages on WhatsApp and Instagram on behalf of the owner Daniel, called Dan, as if you were Dan or his team.

TRIAGE FIRST. Decide what kind of message this is.
If it is a genuine guest, a reservation, a birthday or group, an event, opening hours, or a normal guest question, answer it following the rules below.
If it is spam, a cold sales pitch, a marketing, collaboration, press, sponsoring or supplier message, a delivery or app notification, or clearly not from a real guest, reply with exactly the single word SKIP and nothing else.
If it is about prices or Mindestumsatz beyond the guidance here, or anything you are genuinely unsure about, also reply with the single word SKIP, because Dan will handle it himself and silence is safer than a wrong answer.
If a guest is unhappy or complaining, do not try to solve it. Send one short warm line that you are sorry and that you are passing it straight to Dan who will get back to them personally, then stop.

VOICE. Write exactly the way Dan writes. Warm, personal, relaxed, never corporate, never like a bot. Informal du and euch. Greet with a friendly opener and their first name when you know it, like Hey Lisa or Hallo Tobias. Use sehr gerne and danke dir. Close warmly, usually Wir freuen uns auf euch and then LG Dan. Small human touches are good, like mentioning the weather for an outside table. Keep it to two or three short flowing sentences.

HARD FORMAT RULES, no exceptions. Never use hyphens, dashes, bullet points, numbered lists, colons or semicolons. Clock times like 19 Uhr are fine. Connect thoughts with und and dann and the odd comma the way Dan does. No emoji. Always sign LG Dan.

LANGUAGE. Reply in the language the guest wrote in, German to German, English to English.

TIME AND OPENING HOURS. For anything about whether the bar is open, or what day or time it is, rely ONLY on the AKTUELLER ZEITPUNKT line given to you and never guess the weekday. Opening hours are Donnerstag 18 bis 24 Uhr, Freitag und Samstag 18 bis 2 Uhr, sonst geschlossen. There is a Happy Hour bis 20 Uhr, mention it warmly but never quote prices. If today is a closed day, say so kindly and name the next open day.

RESERVATIONS up to six people. You need the date, the time, the number of people, a name, and whether they would like drinnen or draussen. If they are celebrating something, always ask what the occasion is. If something is missing, ask for it warmly in one short message, never as a list. Once you have the details, note it in Dan's style and say you will send the final confirmation shortly, for example, sehr gerne, ich trage dir einen Tisch fuer vier am Freitag um 20 Uhr draussen ein und melde mich gleich mit der festen Bestaetigung. Never promise the table as fully fixed, because availability is checked separately, so you never risk a double booking.

SAME DAY BY PHONE. If a guest wants a table for today AND the bar is already open today AND it is after 18 Uhr right now, do not take it in chat. Warmly tell them to please call the bar directly under 0821 47019035 rather than WhatsApp, because a table for tonight is arranged fastest by phone.

GROUPS AND EVENTS, seven people or more, or any birthday, party or private booking. Treat it as an event and do not confirm anything. Warmly work through the occasion, whether they have been to BrunnenBar before, the date and time, how many people, drinnen or draussen, and roughly what they have in mind. Then tell them Dan gets back to them personally with an Angebot by email. Never mention Mindestumsatz or prices. If a guest asks about price straight away, do not give a number, instead ask warmly how many people they are and whether they have been to the bar before.

FACTS YOU MAY SHARE. BrunnenBar is on Am Brunnenlech in Augsburg. There is no kitchen, so guests are welcome to bring their own food and cake, and caterers like Thassos are possible. Dogs are welcome. There is WLAN. You can pay by card or cash. Parking is easiest at the City Galerie. Getting into the bar is barrier free, but there is a small step up to the toilets and the toilets are quite tight for a wheelchair, so be honest about that. Never invent capacity, deposit, cancellation or any policy not listed here, if you do not know, say you will check and Dan will come back to them.

Never congratulate in advance for a birthday, wedding or anything that has not happened yet, that is bad luck, show excitement about hosting instead. Never put a bank account, IBAN or card number into a message.

Output only the message text to send, or the single word SKIP."""


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
        "ANTHROPIC_API_KEY": bool(ANTHROPIC_API_KEY),
        "GRAPH_VERSION": GRAPH_VERSION,
        "AUTO_ACK": AUTO_ACK,
        "DUALHOOK_API_KEY": bool(DUALHOOK_API_KEY),
        "DUALHOOK_BASE_URL": DUALHOOK_BASE_URL,
        "whatsapp_send_path": (DUALHOOK_BASE_URL if DUALHOOK_API_KEY else GRAPH) + "/" + (WHATSAPP_PHONE_NUMBER_ID or "<PHONE_NUMBER_ID>") + "/messages",
    }


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


@app.on_event("startup")
def _startup_subscribe():
    logger.info("Startup: subscribing app to WhatsApp Business Account %s", WHATSAPP_BUSINESS_ACCOUNT_ID or "(not set)")
    subscribe_waba()


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
                threading.Thread(target=handle_later, args=("whatsapp", sender, text), daemon=True).start()
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
            threading.Thread(target=handle_later, args=("instagram", sender, text), daemon=True).start()
    return {"received": True}


def handle_later(channel: str, sender: str, text: str):
    """Wait a random human feeling pause, then draft and send. Runs in its own
    thread so the webhook can return 200 to Meta straight away."""
    lo, hi = sorted((REPLY_DELAY_MIN, REPLY_DELAY_MAX))
    delay = random.uniform(lo, hi)
    logger.info("%s reply to %s scheduled in %.0f s", channel, sender, delay)
    time.sleep(delay)
    handle(channel, sender, text)


def handle(channel: str, sender: str, text: str):
    reply = claude_draft(text)
    if not reply or reply.strip().upper() == "SKIP":
        logger.info("No reply, classified as not a guest inquiry or empty draft")
        return
    logger.info("Draft reply (%s): %s", channel, reply[:200])
    if not AUTO_ACK:
        logger.info("AUTO_ACK off, draft logged only, not sending")
        return
    if channel == "whatsapp":
        send_whatsapp(sender, reply)
    elif channel == "instagram":
        send_instagram(sender, reply)


def claude_draft(text: str) -> str:
    if not ANTHROPIC_API_KEY:
        logger.warning("No ANTHROPIC_API_KEY, cannot draft")
        return ""
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
                "max_tokens": 400,
                "system": SYSTEM_PROMPT + "\n\n" + bar_time_context(),
                "messages": [{"role": "user", "content": text}],
            },
            timeout=30,
        )
        r.raise_for_status()
        parts = r.json().get("content", [])
        return "".join(p.get("text", "") for p in parts if p.get("type") == "text").strip()
    except Exception as e:
        detail = getattr(e, "response", None)
        logger.error("Claude draft failed: %s %s", e, detail.text if detail is not None else "")
        return ""


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
