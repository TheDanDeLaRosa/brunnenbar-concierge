"""
BrunnenBar Cloud Concierge, webhook service.

Always on service that answers WhatsApp and Instagram in the house voice, so
the concierge no longer needs Daniel's laptop open. Deploy on Railway.

This version implements the core reply loop, WhatsApp and Instagram inbound
to a Claude drafted reply that is sent automatically within the house rules,
auto acknowledge mode. Gmail and calendar are the next phase, marked below.

No secrets in this file. Everything comes from environment variables.
No hyphens in guest facing text, that rule lives in the system prompt.
"""

import hashlib
import hmac
import logging
import os

import httpx
from fastapi import FastAPI, Request, Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("concierge")

app = FastAPI(title="BrunnenBar Cloud Concierge")

# ---- Config, all from Railway Variables ----
META_VERIFY_TOKEN = os.environ.get("META_VERIFY_TOKEN", "")
META_APP_SECRET = os.environ.get("META_APP_SECRET", "")
WHATSAPP_TOKEN = os.environ.get("WHATSAPP_TOKEN", "")
WHATSAPP_PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID", "")
INSTAGRAM_TOKEN = os.environ.get("INSTAGRAM_TOKEN", "")
INSTAGRAM_ACCOUNT_ID = os.environ.get("INSTAGRAM_ACCOUNT_ID", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GRAPH_VERSION = os.environ.get("GRAPH_VERSION", "v20.0")
# AUTO_ACK true means send the drafted reply automatically. Set to false to
# only log the draft and not send, if Daniel ever wants approve before send.
AUTO_ACK = os.environ.get("AUTO_ACK", "true").lower() == "true"

GRAPH = "https://graph.facebook.com/" + GRAPH_VERSION

# Simple in memory dedupe of handled message ids. Resets on redeploy, which is
# fine, Meta rarely redelivers and a duplicate acknowledgment is low harm.
_handled = set()

# The house voice. These are the same rules the manual concierge follows.
SYSTEM_PROMPT = """You are the concierge for BrunnenBar, a neighbourhood bar in Augsburg, Germany, replying to a guest message on behalf of the owner Daniel.

First decide if this message is a genuine guest inquiry that wants a reply, a reservation, a birthday or group, an event question, opening hours, or a general guest question. If it is spam, a cold sales pitch, a marketing or collaboration partner, a delivery or app notification, or anything not from a real guest, reply with exactly the single word SKIP and nothing else.

If it is a real guest, write a short warm reply in German that Daniel would send. Hard rules, no exceptions:
Never use hyphens, dashes, bullet points, numbered lists, colons, or semicolons. Clock times like 19 Uhr are fine.
Warm, natural, personal, informal du or ihr, never corporate, never sound like a bot.
Use und and dann as connectors instead of punctuation lists.
Do not give prices, and do not mention Mindestumsatz, Barkeeper, or Trinkgeld in a first reply. If the guest asks about price straight away, ask instead how many people they are and whether they have been to the bar before.
If it is a reservation or group and they have not said what they are celebrating, ask what the occasion is.
Never congratulate in advance for a birthday, wedding or similar that has not happened yet, that is bad luck, show enthusiasm about hosting instead.
Only state facts that are known. Opening hours are Donnerstag 18 bis 24 Uhr, Freitag und Samstag 18 bis 2 Uhr, Sonntag bis Mittwoch geschlossen. There is a Happy Hour until 20 Uhr. Do not invent capacity, deposit, or cancellation policy, if asked something undocumented say you will confirm and come back to them.
Keep it to two or three short sentences. End warmly, often with something like Wir freuen uns auf euch and LG Dan.

Output only the message text to send, or the single word SKIP."""


@app.get("/health")
def health():
    return {"ok": True}


# ---- Webhook verification, Meta GET handshake ----
def verify_subscription(request: Request):
    p = request.query_params
    if p.get("hub.mode") == "subscribe" and p.get("hub.verify_token") == META_VERIFY_TOKEN:
        return Response(content=p.get("hub.challenge", ""), media_type="text/plain")
    return Response(content="verification failed", status_code=403)


def signature_ok(body: bytes, header: str) -> bool:
    if not META_APP_SECRET or not header:
        return False
    expected = "sha256=" + hmac.new(META_APP_SECRET.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


# ---- WhatsApp ----
@app.get("/webhook/whatsapp")
async def whatsapp_verify(request: Request):
    return verify_subscription(request)


@app.post("/webhook/whatsapp")
async def whatsapp_receive(request: Request):
    body = await request.body()
    if not signature_ok(body, request.headers.get("X-Hub-Signature-256", "")):
        return Response(status_code=403)
    data = await request.json()
    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            for msg in change.get("value", {}).get("messages", []):
                mid = msg.get("id")
                if not mid or mid in _handled:
                    continue
                _handled.add(mid)
                if msg.get("type") != "text":
                    continue
                sender = msg.get("from")
                text = (msg.get("text") or {}).get("body", "")
                logger.info("WhatsApp in from %s: %s", sender, text[:120])
                handle(channel="whatsapp", sender=sender, text=text)
    return {"received": True}


# ---- Instagram ----
@app.get("/webhook/instagram")
async def instagram_verify(request: Request):
    return verify_subscription(request)


@app.post("/webhook/instagram")
async def instagram_receive(request: Request):
    body = await request.body()
    if not signature_ok(body, request.headers.get("X-Hub-Signature-256", "")):
        return Response(status_code=403)
    data = await request.json()
    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            message = event.get("message") or {}
            mid = message.get("mid")
            text = message.get("text", "")
            # Ignore echoes of our own outgoing messages.
            if message.get("is_echo") or not text or not mid or mid in _handled:
                continue
            _handled.add(mid)
            sender = event.get("sender", {}).get("id")
            logger.info("Instagram in from %s: %s", sender, text[:120])
            handle(channel="instagram", sender=sender, text=text)
    return {"received": True}


# ---- The brain ----
def handle(channel: str, sender: str, text: str):
    reply = claude_draft(text)
    if not reply or reply.strip().upper() == "SKIP":
        logger.info("No reply, classified as not a guest inquiry or empty")
        return
    logger.info("Draft reply (%s): %s", channel, reply[:200])
    if not AUTO_ACK:
        logger.info("AUTO_ACK off, not sending, draft logged only")
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
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": text}],
            },
            timeout=30,
        )
        r.raise_for_status()
        parts = r.json().get("content", [])
        return "".join(p.get("text", "") for p in parts if p.get("type") == "text").strip()
    except Exception as e:
        logger.error("Claude draft failed: %s", e)
        return ""


def send_whatsapp(to: str, text: str):
    try:
        r = httpx.post(
            GRAPH + "/" + WHATSAPP_PHONE_NUMBER_ID + "/messages",
            headers={"Authorization": "Bearer " + WHATSAPP_TOKEN},
            json={"messaging_product": "whatsapp", "to": to, "type": "text", "text": {"body": text}},
            timeout=30,
        )
        r.raise_for_status()
        logger.info("WhatsApp reply sent to %s", to)
    except Exception as e:
        logger.error("WhatsApp send failed: %s %s", e, getattr(e, "response", None) and e.response.text)


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
        logger.error("Instagram send failed: %s %s", e, getattr(e, "response", None) and e.response.text)


# ---- Next phase, not wired yet ----
# Gmail poll, read guest inquiries from brunnenbaraugsburg and reply.
# Calendar, create the reservation in the BrunnenBar Reservierungen calendar.
# Escalation, send Daniel the ones that commit the bar for a human yes.


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
