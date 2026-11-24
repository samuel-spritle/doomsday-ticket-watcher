#!/usr/bin/env python3
"""
Serverless ticket watcher (BookMyShow + District) for GitHub Actions.

Each run:
  1. Polls Telegram once for new messages/commands (subscribe, /bms, /district, ping ...)
  2. Scrapes both sites and alerts every subscriber when a NEW theater appears
  3. Saves everything to state/state.json (the workflow commits it back to the repo)

Env vars:
  TELEGRAM_BOT_TOKEN   (required)
  TELEGRAM_CHAT_ID     (optional admin chat, always subscribed)
  BMS_URL, DISTRICT_URL (optional, override default URLs)
  RUN_SECONDS          (optional, keep looping inside one run; 0 = single pass)
  STATE_FILE           (optional, default state/state.json)
"""
import json
import os
import random
import re
import time
from pathlib import Path

from curl_cffi import requests as cr

try:
    from patchright.sync_api import sync_playwright
    HAS_PATCHRIGHT = True
except ImportError:
    HAS_PATCHRIGHT = False

# --- CONFIG ---------------------------------------------------------------
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
RUN_SECONDS = int(os.getenv("RUN_SECONDS", "0") or 0)
STATE_FILE = Path(os.getenv("STATE_FILE", "state/state.json"))
HEARTBEAT_MINUTES = int(os.getenv("HEARTBEAT_MINUTES", "60") or 0)  # 0 = no periodic status message

MIN_INTERVAL, MAX_INTERVAL = 25, 45      # only used when RUN_SECONDS > 0
FAIL_ALERT_THRESHOLD = 10                # consecutive failed fetches before warning you

DEFAULT_BMS_URL = os.getenv("BMS_URL") or (
    "https://in.bookmyshow.com/movies/chennai/avengers-doomsday/buytickets/"
    "ET00526306/20261217?etCodes=*&language=english&refEventCode=ET00526306"
)
DEFAULT_DISTRICT_URL = os.getenv("DISTRICT_URL") or (
    "https://www.district.in/movies/avengers-doomsday-movie-tickets-in-chennai-"
    "MV176735?frmtid=Mm5dpqTqJz"
)

HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

CHAIN_RE = re.compile(
    r"^(PVR|INOX|AGS|Cinepolis|Sathyam|SPI|Rohini|Kamala|Vettri|Sangam|Devi|Woodlands|Mayajaal|Palazzo|Luxe)\b",
    re.IGNORECASE,
)
CHALLENGE_MARKERS = ("just a moment", "cf-challenge", "access denied", "attention required")


def log(msg: str):
    print(msg, flush=True)


# --- EXTRACTORS -----------------------------------------------------------
def _visible_lines(html: str):
    clean = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", html, flags=re.DOTALL | re.IGNORECASE)
    for line in re.sub(r"<[^>]+>", "\n", clean).splitlines():
        yield line.strip()


def extract_district(html: str) -> set[str]:
    out: set[str] = set()
    m = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, flags=re.DOTALL | re.IGNORECASE)
    if m:
        try:
            raw = json.dumps(json.loads(m.group(1)))
            for x in re.finditer(r'"(?:cinemaName|venueName|theaterName|cinema_name)"\s*:\s*"([^"]+)"', raw):
                if len(x.group(1).strip()) > 3:
                    out.add(x.group(1).strip())
        except json.JSONDecodeError:
            pass
    if not out:
        for x in re.finditer(r'href="[^"]*/movies/[^"]*cinema[^"]*"[^>]*>([^<]+)<', html, flags=re.IGNORECASE):
            name = re.sub(r"\s+", " ", x.group(1)).strip()
            if len(name) > 3:
                out.add(name)
    if not out:
        out = {l for l in _visible_lines(html) if CHAIN_RE.match(l)}
    return out


def extract_bms(html: str) -> set[str]:
    out: set[str] = set()
    for x in re.finditer(r'"(?:VenueName|venueName)"\s*:\s*"([^"]+)"', html):
        if len(x.group(1).strip()) > 3:
            out.add(x.group(1).strip())
    if not out:
        for x in re.finditer(r'data-name="([^"]+)"[^>]*data-sub-region-code', html, flags=re.IGNORECASE):
            if len(x.group(1).strip()) > 3:
                out.add(x.group(1).strip())
    if not out:
        for x in re.finditer(r'href="[^"]*/buytickets/[^"]+/venue/[^"]*"[^>]*>([^<]+)<', html, flags=re.IGNORECASE):
            name = re.sub(r"\s+", " ", x.group(1)).strip()
            if len(name) > 3:
                out.add(name)
    if not out:
        out = {l for l in _visible_lines(html) if CHAIN_RE.match(l)}
    return out


SITES = {
    "district": dict(label="District", domain="district.in",
                     referer="https://www.district.in/", extract=extract_district,
                     default_url=DEFAULT_DISTRICT_URL),
    "bms": dict(label="BookMyShow", domain="bookmyshow.com",
                referer="https://in.bookmyshow.com/", extract=extract_bms,
                default_url=DEFAULT_BMS_URL),
}


# --- STATE ----------------------------------------------------------------
def fresh_site(url: str) -> dict:
    return {"url": url, "env_url": url, "known": None, "new": [], "fails": 0}


def load_state() -> dict:
    state = {"offset": 0, "subscribers": [], "sites": {}}
    if STATE_FILE.exists():
        try:
            state.update(json.loads(STATE_FILE.read_text(encoding="utf-8")))
        except Exception as e:
            log(f"[STATE LOAD ERROR] {e}")

    for key, cfg in SITES.items():
        s = state["sites"].get(key)
        # new site, or the env/variable URL was changed -> adopt it and reset baseline
        if not s or s.get("env_url") != cfg["default_url"]:
            state["sites"][key] = fresh_site(cfg["default_url"])

    if ADMIN_CHAT_ID and ADMIN_CHAT_ID not in state["subscribers"]:
        state["subscribers"].append(ADMIN_CHAT_ID)
    return state


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state["subscribers"] = sorted(set(state["subscribers"]))
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")


# --- TELEGRAM -------------------------------------------------------------
def tg(method: str, **payload) -> dict:
    r = cr.post(f"https://api.telegram.org/bot{TOKEN}/{method}", json=payload, timeout=25)
    r.raise_for_status()
    return r.json()


def send(chat_id: str, text: str):
    for i in range(0, len(text), 4000):  # Telegram limit is 4096
        try:
            tg("sendMessage", chat_id=chat_id, text=text[i:i + 4000], disable_web_page_preview=True)
        except Exception as e:
            log(f"[TELEGRAM ERROR {chat_id}] {e}")


def broadcast(state: dict, text: str):
    if not state["subscribers"]:
        log("[WARN] No subscribers yet — message your bot first.")
    for cid in state["subscribers"]:
        send(cid, text)


def fmt_list(items, empty="None yet") -> str:
    return "\n".join(f"   • {t}" for t in sorted(set(items))) if items else f"   • {empty}"


HELP = (
    "Commands:\n"
    "• ping / status — theater status\n"
    "• /bms <url> — watch a new BookMyShow URL\n"
    "• /district <url> — watch a new District URL\n"
    "• /urls — show watched URLs\n"
    "• /stop — unsubscribe\n\n"
    "Note: runs on GitHub Actions, so replies can take a few minutes."
)


def status_text(state: dict) -> str:
    parts = ["Watcher is alive on GitHub Actions",
             f"Subscribers: {len(state['subscribers'])}\n"]
    for k, c in SITES.items():
        s = state["sites"][k]
        known = s["known"] or []
        parts.append(
            f"{c['label'].upper()} ({len(known)} active)\n"
            f"Newly added:\n{fmt_list(s['new'])}\n"
            f"All active:\n{fmt_list(known)}\n")
    return "\n".join(parts)


def process_updates(state: dict):
    if not TOKEN:
        return
    try:
        data = tg("getUpdates", offset=state["offset"], timeout=0, allowed_updates=["message"])
    except Exception as e:
        log(f"[GETUPDATES ERROR] {e}")
        return

    for upd in data.get("result", []):
        state["offset"] = upd["update_id"] + 1
        msg = upd.get("message") or {}
        raw = (msg.get("text") or "").strip()
        chat = msg.get("chat") or {}
        cid = str(chat.get("id", ""))
        name = chat.get("first_name") or chat.get("title") or cid
        if not cid or not raw:
            continue

        raw = re.sub(r"^(/\w+)@\w+", r"\1", raw)  # /cmd@botname -> /cmd
        text = raw.lower()

        if text in {"/stop", "stop", "unsubscribe", "/unsubscribe"}:
            if cid in state["subscribers"]:
                state["subscribers"].remove(cid)
                send(cid, "Unsubscribed. Send /start anytime to rejoin.")
                log(f"[UNSUBSCRIBED] {name} ({cid})")
            continue

        if cid not in state["subscribers"]:
            state["subscribers"].append(cid)
            log(f"[NEW SUBSCRIBER] {name} ({cid})")
            send(cid, f"Subscribed, {name}! You'll get an alert whenever a new theater opens.\n\n{HELP}")

        for key, cmd in (("bms", "/bms"), ("district", "/district")):
            if text.startswith(cmd + " "):
                cfg, new_url = SITES[key], raw.split(" ", 1)[1].strip()
                if cfg["domain"] not in new_url.lower():
                    send(cid, f"Invalid {cfg['label']} URL.")
                else:
                    state["sites"][key].update(url=new_url, known=None, new=[], fails=0)
                    send(cid, f"Updated {cfg['label']} URL & reset baseline:\n{new_url}")
                break
        else:
            if text in {"/urls", "urls"}:
                send(cid, "Active URLs:\n\n" + "\n\n".join(
                    f"{c['label']}:\n{state['sites'][k]['url']}" for k, c in SITES.items()))
            elif text in {"ping", "/ping", "status", "/status"}:
                send(cid, status_text(state))
            elif text in {"/start", "/help", "help"}:
                send(cid, HELP)


# --- FETCHING -------------------------------------------------------------
def render_with_browser(url: str) -> str:
    """Fallback: real Chrome (preinstalled on GitHub runners) via Patchright."""
    if not HAS_PATCHRIGHT:
        return ""
    args = ["--disable-blink-features=AutomationControlled", "--no-sandbox"]
    try:
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(channel="chrome", headless=True, args=args)
            except Exception:
                browser = p.chromium.launch(headless=True, args=args)
            ctx = browser.new_context(viewport={"width": 1366, "height": 768},
                                      locale="en-IN", timezone_id="Asia/Kolkata")
            page = ctx.new_page()
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=40000)
                page.wait_for_timeout(4500)
                page.mouse.wheel(0, 1200)
                page.wait_for_timeout(1500)
                return page.content()
            finally:
                ctx.close()
                browser.close()
    except Exception as e:
        log(f"[BROWSER ERROR] {e}")
        return ""


def fetch_theaters(session, cfg: dict, url: str) -> tuple[bool, set[str]]:
    """Returns (fetch_ok, theaters). fetch_ok=False means blocked/failed (NOT 'zero theaters')."""
    ok, theaters = False, set()
    try:
        r = session.get(url, impersonate="chrome", headers={**HEADERS, "Referer": cfg["referer"]}, timeout=25)
        if r.status_code == 200:
            ok, theaters = True, cfg["extract"](r.text)
        else:
            log(f"[HTTP {r.status_code}] {cfg['label']}")
    except Exception as e:
        log(f"[FETCH ERROR] {cfg['label']}: {e}")

    if (not ok or not theaters) and HAS_PATCHRIGHT:
        log(f"[FALLBACK] {cfg['label']} via headless Chrome...")
        html = render_with_browser(url)
        if html and not any(m in html[:5000].lower() for m in CHALLENGE_MARKERS):
            found = cfg["extract"](html)
            if found or not ok:
                ok, theaters = True, found
    return ok, theaters


def check_site(session, state: dict, key: str):
    cfg, s = SITES[key], state["sites"][key]
    url = (s.get("url") or "").strip()
    if not url:
        return

    ok, current = fetch_theaters(session, cfg, url)

    if not ok:
        s["fails"] += 1
        log(f"[BLOCKED] {cfg['label']} (consecutive fails: {s['fails']})")
        if s["fails"] == FAIL_ALERT_THRESHOLD:
            broadcast(state, f"{cfg['label']} has failed/blocked {FAIL_ALERT_THRESHOLD} checks in a row "
                             f"(GitHub's IPs may be blocked). Alerts for it are paused until it recovers.")
        return

    if s["fails"] >= FAIL_ALERT_THRESHOLD:
        broadcast(state, f"{cfg['label']} checks are working again.")
    s["fails"] = 0

    # First successful read -> baseline (silent apart from a confirmation)
    if s["known"] is None:
        s["known"] = sorted(current)
        log(f"[BASELINE] {cfg['label']}: {len(current)} theaters")
        broadcast(state, f"Now watching {cfg['label']}. Currently {len(current)} theater(s) listed.\n"
                         f"I'll alert you when a new one appears.")
        return

    known = set(s["known"])
    if not current and known:
        log(f"[WARN] {cfg['label']} returned 0 theaters — keeping baseline.")
        return

    added = sorted(current - known)
    if added:
        s["new"] = (s["new"] + added)[-50:]
        s["known"] = sorted(known | current)
        listing = "\n".join(f"- {t}" for t in added)
        log(f"[NEW THEATER] {cfg['label']}:\n{listing}")
        broadcast(state, f"NEW THEATER ADDED ON {cfg['label'].upper()}!\n\n"
                         f"Newly added ({len(added)}):\n{listing}\n\n"
                         f"Total active: {len(s['known'])}\nBook now: {url}")
    else:
        log(f"[WAITING] {cfg['label']} — {len(current)} theaters, nothing new.")


# --- MAIN -----------------------------------------------------------------
def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN is not set.")

    state = load_state()
    deadline = time.time() + RUN_SECONDS
    session = cr.Session()

    while True:
        process_updates(state)
        for key in SITES:
            try:
                check_site(session, state, key)
            except Exception as e:
                log(f"[ERROR] {key}: {e}")
            time.sleep(random.uniform(2, 4))
        if HEARTBEAT_MINUTES and time.time() - state.get("last_heartbeat", 0) >= HEARTBEAT_MINUTES * 60:
            broadcast(state, "Hourly update\n\n" + status_text(state))
            state["last_heartbeat"] = time.time()
        save_state(state)

        if time.time() + MAX_INTERVAL >= deadline:
            break
        wait = random.uniform(MIN_INTERVAL, MAX_INTERVAL)
        log(f"Sleeping {wait:.0f}s...")
        time.sleep(wait)

    log("Run complete.")


if __name__ == "__main__":
    main()
