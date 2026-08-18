"""
BrunnenBar Cloud Concierge, webhook service.

One always on service that receives WhatsApp, Instagram and email messages
through the official APIs and replies in the house voice, so nothing depends
on Daniel's laptop being open. Deploy on Railway, same pattern as BarPatrol.

This is the SKELETON. The webhook verification and signature checks are real
and testable. The message handling calls stubs that get filled in once Meta
verification clears and the credentials exist in Railway's Variables tab, and
once Daniel picks the autonomy policy (see PLAN.md, open decision).

No secrets live in this file. Everything comes from environment variables.
"""

import hashlib
import hmac
import logging
import os

from fastapi import FastAPI, Request, Response

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("concierge")

app = FastAPI(title="BrunnenBar Cloud Concierge")

# Config from environment, set these in Railway's Variables tab.
META_VERIFY_TOKEN = os.environ.get("META_VERIFY_TOKEN", "")      # a string you choose, given to Meta when subscribing the webhook
META_APP_SECRET = os.environ.get("META_APP_SECRET", "")          # from the Meta app, used to verify signatures
WHATSAPP_TOKEN = os.environ.get("WHATSAPP_TOKEN", "")            # permanent access token
WHATSAPP_PHONE_NUMBER_ID = os.environ.get("WHATSAPP_PHONE_NUMBER_ID", "")
INSTAGRAM_TOKEN = os.environ.get("INSTAGRAM_TOKEN", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
RESERVIERUNGEN_CALENDAR_ID = os.environ.get(
    "RESERVIERUNGEN_CALENDAR_ID",
    "6787ba64e770c7ee8a42d4bd88fd76c3a4e75e1f8e71469ff32dbc0f45668a2f@group.calendar.google.com",
)


@app.get("/health")
def health():
    return {"ok": True}


# ------------------------- Webhook verification -------------------------
# Meta calls the webhook with a GET once, to confirm we own the endpoint.
# We echo hub.challenge back if the verify token matches.

def verify_subscription(request: Request):
    params = request.query_params
    if params.get("hub.mode") == "subscribe" and params.get("hub.verify_token") == META_VERIFY_TOKEN:
        return Response(content=params.get("hub.challenge", ""), media_type="text/plain")
    return Response(content="verification failed", status_code=403)


def signature_ok(app_secret: str, body: bytes, header: str) -> bool:
    """Meta signs every POST with X-Hub-Signature-256. Verify it so we only
    act on genuine Meta calls, never a spoofed request."""
    if not app_secret or not header:
        return False
    expected = "sha256=" + hmac.new(app_secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)


# ------------------------- WhatsApp -------------------------

@app.get("/webhook/whatsapp")
async def whatsapp_verify(request: Request):
    return verify_subscription(request)


@app.post("/webhook/whatsapp")
async def whatsapp_receive(request: Request):
    body = await request.body()
    if not signature_ok(META_APP_SECRET, body, request.headers.get("X-Hub-Signature-256", "")):
        return Response(status_code=403)
    data = await request.json()
    # Meta shape: entry[].changes[].value.messages[]
    for entry in data.get("entry", []):
        for change in entry.get("changes", []):
            value = change.get("value", {})
            for msg in value.get("messages", []):
                sender = msg.get("from")
                text = (msg.get("text") or {}).get("body", "")
                logger.info("WhatsApp in from %s: %s", sender, text[:80])
                process_guest_message(channel="whatsapp", sender=sender, text=text, raw=msg)
    return {"received": True}


# ------------------------- Instagram -------------------------

@app.get("/webhook/instagram")
async def instagram_verify(request: Request):
    return verify_subscription(request)


@app.post("/webhook/instagram")
async def instagram_receive(request: Request):
    body = await request.body()
    if not signature_ok(META_APP_SECRET, body, request.headers.get("X-Hub-Signature-256", "")):
        return Response(status_code=403)
    data = await request.json()
    # Instagram shape: entry[].messaging[] with sender.id and message.text
    for entry in data.get("entry", []):
        for event in entry.get("messaging", []):
            sender = event.get("sender", {}).get("id")
            text = (event.get("message") or {}).get("text", "")
            if text:
                logger.info("Instagram in from %s: %s", sender, text[:80])
                process_guest_message(channel="instagram", sender=sender, text=text, raw=event)
    return {"received": True}


# ------------------------- Email -------------------------
# Gmail does not push webhooks the same way. Two options, decided at build time:
#  a) Gmail API push via Pub/Sub, real time.
#  b) A background poll every couple of minutes.
# Stubbed here, wired in Phase 1 since it needs no Meta verification.

def poll_gmail():
    """TODO Phase 1: read new guest/event inquiries from brunnenbaraugsburg,
    then call process_guest_message(channel='email', ...)."""
    pass


# ------------------------- The brain, shared by all channels -------------------------

def process_guest_message(channel: str, sender: str, text: str, raw: dict):
    """One path for every channel.
    TODO once credentials + autonomy policy exist:
      1. classify with Claude (guest inquiry, event, spam, partner)
      2. check the Reservierungen calendar for an existing booking, avoid duplicates
      3. draft a reply in the house voice with claude_draft()
      4. per policy, either auto send a safe acknowledgment or queue for Daniel's approval
      5. send via send_reply()
    Guardrails carry over from the current concierge prompts, no prices in the
    first or second message, du tone, never invent policies, never pre wish.
    """
    logger.info("process_guest_message stub, channel=%s sender=%s", channel, sender)


def claude_draft(context: str) -> str:
    """TODO: call the Claude API with ANTHROPIC_API_KEY to draft in the house
    voice, reusing the BrunnenBar guest reply rules."""
    return ""


def send_reply(channel: str, recipient: str, text: str):
    """TODO: send via the WhatsApp Cloud API, the Instagram Messaging API, or
    Gmail, depending on channel. Respects the 24 hour messaging window."""
    pass


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
