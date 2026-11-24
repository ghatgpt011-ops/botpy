import asyncio
import json
import logging
import os
import re
import time
from collections import deque
from datetime import datetime
from threading import Lock, Thread
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from flask import Flask, request
from splusthon import SoroushClient
from splusthon.sessions import StringSession

# ================= تنظیمات =================
STATE_FILE = os.path.join(os.environ.get("DATA_DIR", "."), "spam_3groups_state.json")
PORT = int(os.environ.get("PORT", 8080))
PANEL_PATH = os.environ.get("PANEL_PATH", "/panel")
if not PANEL_PATH.startswith("/"):
    PANEL_PATH = "/" + PANEL_PATH

KEEPALIVE_URL = (os.environ.get("KEEPALIVE_URL") or "").strip()
KEEPALIVE_INTERVAL = max(60, int(os.environ.get("KEEPALIVE_INTERVAL", "240")))

MAX_SLOTS = 3
MAX_CONSOLE_LINES = 140

for _name in ("splusthon", "asyncio", "werkzeug"):
    logging.getLogger(_name).setLevel(logging.CRITICAL)

data = {
    "phone": "",
    "session": "",
    "code_hash": None,
    "auth_msg": "شماره را وارد کنید.",
    "connected": False,
    "account_name": "-",
    "speed": 1.0,
    "last_error": "-",
    "slots": {
        "1": {"text": "پیام ۱", "target_chat": "", "target_name": "-", "running": False, "sent": 0, "last_at": "-"},
        "2": {"text": "پیام ۲", "target_chat": "", "target_name": "-", "running": False, "sent": 0, "last_at": "-"},
        "3": {"text": "پیام ۳", "target_chat": "", "target_name": "-", "running": False, "sent": 0, "last_at": "-"},
    },
}

dialogs_cache = []
console_logs = deque(maxlen=MAX_CONSOLE_LINES)
console_lock = Lock()

client = None
bot_loop = None
io_lock = asyncio.Lock()
connect_lock = asyncio.Lock()
_last_connect_try = 0.0


def now_t() -> str:
    return datetime.now().strftime("%H:%M:%S")


def add_console_log(msg: str):
    line = f"[{now_t()}] {str(msg).strip()}"
    with console_lock:
        console_logs.append(line)


def get_console_logs() -> list:
    with console_lock:
        return list(console_logs)


add_console_log("سرویس سرور با موتور خوداصلاح خطا راه‌اندازی شد.")


def save_data():
    try:
        parent_dir = os.path.dirname(os.path.abspath(STATE_FILE))
        if parent_dir:
            os.makedirs(parent_dir, exist_ok=True)
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {k: v for k, v in data.items() if k != "code_hash"},
                f,
                ensure_ascii=False,
                indent=2,
            )
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def load_data():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if isinstance(raw, dict):
                for k in ("phone", "session", "auth_msg", "account_name", "speed", "last_error"):
                    if k in raw:
                        data[k] = raw[k]
                raw_slots = raw.get("slots") or {}
                for k in ("1", "2", "3"):
                    if isinstance(raw_slots.get(k), dict):
                        data["slots"][k].update(raw_slots[k])
                data["connected"] = False
                add_console_log("وضعیت و سشن ذخیره‌شده بدون پاک شدن بارگذاری شد.")
        except Exception:
            pass


def parse_target(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if "splus.ir" in s or s.startswith("http://") or s.startswith("https://"):
        url_s = s if s.startswith("http") else "https://" + s
        try:
            parsed = urlparse(url_s)
            parts = [p for p in parsed.path.split("/") if p]
            if parts:
                if len(parts) >= 2 and parts[-1].isdigit():
                    if parts[0] == "c" and parts[1].lstrip("-").isdigit():
                        cid = parts[1]
                        return f"-100{cid}" if not cid.startswith("-") else cid
                    return parts[-2]
                return parts[0]
        except Exception:
            pass
    m = re.search(r"(?:https?://)?(?:www\.)?splus\.ir/([^/\s?#]+)", s)
    if m:
        return m.group(1).strip()
    if s.startswith("@"):
        return s[1:].strip()
    return s


def to_peer(raw: str):
    t = parse_target(raw)
    return int(t) if t.lstrip("-").isdigit() else t


def active_running_count(exclude_slot: str = "") -> int:
    return sum(
        1
        for k, s in data["slots"].items()
        if k != exclude_slot and bool(s.get("running"))
    )


def extract_dialog_item(d):
    ent = getattr(d, "entity", None)
    did = getattr(d, "id", None) or getattr(ent, "id", None)
    if did is None:
        return None
    name = (
        getattr(d, "name", None)
        or getattr(d, "title", None)
        or getattr(ent, "title", None)
        or getattr(ent, "name", None)
        or getattr(ent, "first_name", None)
        or str(did)
    )
    is_grp = bool(
        getattr(d, "is_group", False)
        or getattr(ent, "megagroup", False)
        or getattr(ent, "gigagroup", False)
    )
    is_chn = bool(getattr(d, "is_channel", False) or getattr(ent, "broadcast", False))
    dtype = "کانال" if is_chn else ("گروه" if is_grp else "چت")
    return {"id": str(did), "name": str(name), "type": dtype, "is_gc": bool(is_grp or is_chn)}


load_data()


# ================= اتصال سروش و دریافت گروه‌ها =================
async def _safe_disconnect(c):
    if c is None:
        return
    try:
        if hasattr(c, "disconnect"):
            res = c.disconnect()
            if asyncio.iscoroutine(res):
                await asyncio.wait_for(res, timeout=3)
    except Exception:
        pass


async def connect_client(force: bool = False) -> bool:
    global client, _last_connect_try
    sess = (data.get("session") or "").strip()
    if not sess:
        data["connected"] = False
        return False

    async with connect_lock:
        if data.get("connected") and client is not None and not force:
            return True

        now = time.monotonic()
        if not force and (now - _last_connect_try) < 1.5:
            return bool(data.get("connected"))
        _last_connect_try = now

        try:
            if force or client is None:
                await _safe_disconnect(client)
                client = SoroushClient(StringSession(sess))

            if hasattr(client, "is_connected"):
                try:
                    already = client.is_connected()
                    if asyncio.iscoroutine(already):
                        already = await already
                    if not already:
                        await asyncio.wait_for(client.connect(), timeout=12)
                except Exception:
                    await asyncio.wait_for(client.connect(), timeout=12)
            else:
                await asyncio.wait_for(client.connect(), timeout=12)

            me = await asyncio.wait_for(client.get_me(), timeout=8)
            fn = (getattr(me, "first_name", "") or "").strip()
            ln = (getattr(me, "last_name", "") or "").strip()
            data["account_name"] = (
                f"{fn} {ln}".strip()
                or getattr(me, "username", None)
                or data["phone"]
                or "متصل"
            )
            was_connected = bool(data.get("connected"))
            data["connected"] = True
            data["last_error"] = "-"
            if not was_connected:
                add_console_log(f"اکانت با حفظ سشن متصل شد: {data['account_name']}")
            return True
        except Exception as e:
            data["connected"] = False
            err_s = str(e)[:120]
            data["last_error"] = err_s
            add_console_log(f"خطای اتصال: {err_s}")
            await _safe_disconnect(client)
            client = None
            return False


async def fetch_groups_list():
    global dialogs_cache
    add_console_log("در حال دریافت لیست گروه‌ها و کانال‌ها...")
    ok = await connect_client()
    if not ok or client is None:
        add_console_log("دریافت لیست ناموفق بود؛ اکانت متصل نیست.")
        return dialogs_cache
    try:
        raw = []
        if hasattr(client, "get_dialogs"):
            res = await asyncio.wait_for(client.get_dialogs(limit=200), timeout=15)
            raw = list(res) if res else []
        elif hasattr(client, "iter_dialogs"):
            async for d in client.iter_dialogs(limit=200):
                raw.append(d)

        gc_only = []
        all_items = []
        for d in raw:
            info = extract_dialog_item(d)
            if not info:
                continue
            item = {"id": info["id"], "name": info["name"], "type": info["type"]}
            all_items.append(item)
            if info["is_gc"]:
                gc_only.append(item)

        dialogs_cache = gc_only if gc_only else all_items
        add_console_log(f"لیست دریافت شد ({len(dialogs_cache)} مورد).")
        return dialogs_cache
    except Exception as e:
        err_s = str(e)[:120]
        data["last_error"] = err_s
        data["connected"] = False
        add_console_log(f"خطا در دریافت لیست: {err_s}")
        return dialogs_cache


async def do_send_code(phone: str):
    global client
    data["phone"] = phone
    data["auth_msg"] = "در حال ارسال کد..."
    add_console_log(f"درخواست ارسال کد برای {phone}...")
    try:
        async with connect_lock:
            await _safe_disconnect(client)
            client = SoroushClient(StringSession())
            await asyncio.wait_for(client.connect(), timeout=15)
            if hasattr(client, "send_code_request"):
                res = await asyncio.wait_for(client.send_code_request(phone), timeout=15)
                data["code_hash"] = getattr(res, "phone_code_hash", None)
            elif hasattr(client, "send_code"):
                res = await asyncio.wait_for(client.send_code(phone), timeout=15)
                data["code_hash"] = getattr(res, "phone_code_hash", None)
            else:
                await asyncio.wait_for(client.sign_in(phone=phone), timeout=15)
        data["auth_msg"] = "کد ارسال شد؛ کد را وارد کنید."
        add_console_log("کد تأیید ارسال شد.")
    except Exception as e:
        data["auth_msg"] = f"خطا: {e}"
        add_console_log(f"خطای ارسال کد: {str(e)[:120]}")


async def do_verify_code(code: str):
    phone = data.get("phone", "").strip()
    add_console_log("در حال بررسی کد تأیید...")
    try:
        if client is None:
            raise RuntimeError("ابتدا شماره را بفرستید.")
        async with connect_lock:
            if data.get("code_hash"):
                try:
                    await asyncio.wait_for(
                        client.sign_in(phone=phone, code=code, phone_code_hash=data["code_hash"]),
                        timeout=15,
                    )
                except TypeError:
                    await asyncio.wait_for(client.sign_in(phone, code, data["code_hash"]), timeout=15)
            else:
                await asyncio.wait_for(client.sign_in(phone=phone, code=code), timeout=15)

        data["session"] = client.session.save() if getattr(client, "session", None) else ""
        data["connected"] = True
        data["last_error"] = "-"
        data["auth_msg"] = "با موفقیت وارد شد."
        save_data()
        add_console_log("ورود به اکانت با موفقیت انجام شد.")
    except Exception as e:
        data["auth_msg"] = f"خطا در کد: {e}"
        add_console_log(f"خطای تأیید کد: {str(e)[:120]}")


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)
    return None


# ================= موتور تشخیص و اصلاح خودکار خطا =================
async def auto_heal_slot_error(slot_num: str, slot: dict, err: Exception, consecutive_errors: int):
    """
    خطای رخ‌داده را بدون پاک کردن سشن تحلیل و اصلاح می‌کند.
    خروجی: (backoff_seconds: float, should_retry_immediately: bool)
    """
    err_raw = str(err or "")
    err_low = err_raw.lower()
    err_short = err_raw[:120]

    # ۱. محدودیت سرعت / Flood
    if any(k in err_low for k in ("flood", "rate", "too many", "limit", "429", "slow")):
        wait_sec = min(15.0, max(2.5, 2.0 * consecutive_errors))
        add_console_log(f"[اصلاح خودکار جای {slot_num}] محدودیت ارسال تشخیص داده شد؛ مکث {wait_sec:.1f}s و ادامه همان پیام.")
        return wait_sec, False

    # ۲. خطای مقصد / Peer / آیدی / یوزرنیم
    if any(k in err_low for k in ("peer", "chat", "user", "resolve", "not found", "invalid", "target")):
        current_tgt = str(slot.get("target_chat") or "").strip()
        fixed_tgt = parse_target(current_tgt)
        if not fixed_tgt.lstrip("-").isdigit() and dialogs_cache:
            name_key = str(slot.get("target_name") or "").strip()
            matched = next(
                (g.get("id") for g in dialogs_cache if name_key and str(g.get("name", "")).strip() == name_key),
                None,
            )
            if matched:
                fixed_tgt = str(matched)
        if fixed_tgt and fixed_tgt != current_tgt:
            slot["target_chat"] = fixed_tgt
            save_data()
            add_console_log(f"[اصلاح خودکار جای {slot_num}] فرمت مقصد اصلاح شد ({current_tgt} -> {fixed_tgt})؛ تلاش مجدد برای همان پیام.")
            return 0.3, True

    # ۳. خطای اتصال / شبکه / تایم‌اوت / کلاینت (با حفظ کامل سشن)
    if any(
        k in err_low
        for k in (
            "timeout",
            "timed out",
            "connect",
            "connection",
            "network",
            "socket",
            "closed",
            "reset",
            "broken pipe",
            "client",
            "session",
            "auth",
            "eof",
        )
    ) or consecutive_errors >= 1:
        data["connected"] = False
        add_console_log(f"[اصلاح خودکار جای {slot_num}] خطای «{err_short}» گرفته شد؛ در حال بازاتصال با سشن فعلی...")
        ok = await connect_client(force=True)
        if ok:
            add_console_log(f"[اصلاح خودکار جای {slot_num}] اتصال اصلاح شد؛ ادامه ارسال همان پیام.")
            return 0.2, True
        wait_sec = min(6.0, 1.0 * consecutive_errors)
        return wait_sec, False

    return min(4.0, 0.7 * consecutive_errors), False


async def slot_worker(slot_num: str):
    slot = data["slots"][slot_num]
    consecutive_errors = 0
    retry_now = False

    while True:
        try:
            if not slot.get("running"):
                consecutive_errors = 0
                retry_now = False
                await asyncio.sleep(0.15)
                continue

            if not str(slot.get("target_chat") or "").strip() or not str(slot.get("text") or "").strip():
                slot["running"] = False
                data["last_error"] = "مقصد یا متن خالی است."
                add_console_log(f"[اصلاح خودکار جای {slot_num}] مقصد یا متن خالی بود؛ اسلات ایمن متوقف شد.")
                save_data()
                await asyncio.sleep(0.2)
                continue

            if not data.get("connected") or client is None:
                ok = await connect_client()
                await asyncio.sleep(0.3 if ok else 1.0)
                continue

            # دستور در حال اجرا (همان مقصد و همان متن تا زمان ارسال موفق)
            peer = to_peer(str(slot["target_chat"]))
            msg = str(slot["text"])

            async with io_lock:
                await asyncio.wait_for(client.send_message(peer, msg), timeout=10)

            consecutive_errors = 0
            retry_now = False
            new_count = int(slot.get("sent") or 0) + 1
            slot["sent"] = new_count
            slot["last_at"] = now_t()
            data["last_error"] = "-"
            dest_label = slot.get("target_name") or slot.get("target_chat")
            add_console_log(f"[جای {slot_num}] پیام #{new_count} به «{dest_label}» ارسال شد.")

            if new_count % 5 == 0:
                save_data()

            await asyncio.sleep(max(0.1, float(data.get("speed") or 1.0)))

        except Exception as e:
            consecutive_errors += 1
            err_s = str(e)[:120]
            data["last_error"] = err_s
            wait_sec, retry_now = await auto_heal_slot_error(slot_num, slot, e, consecutive_errors)
            if not retry_now:
                await asyncio.sleep(wait_sec)


async def runner_main():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    add_console_log("حلقه اصلی ارسال و خوداصلاح‌کننده خطا فعال شد.")

    if (data.get("session") or "").strip():
        await connect_client(force=True)

    for s_num in ("1", "2", "3"):
        asyncio.create_task(slot_worker(s_num))

    while True:
        await asyncio.sleep(3)
        if (data.get("session") or "").strip() and not data.get("connected"):
            await connect_client()


def start_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(runner_main())
        except Exception as e:
            data["connected"] = False
            err_s = str(e)[:120]
            data["last_error"] = err_s
            add_console_log(f"[اصلاح خودکار حلقه اصلی] خطای «{err_s}» گرفته شد؛ در حال بازیابی...")
            time.sleep(1)


def run_keepalive():
    if not KEEPALIVE_URL:
        return
    while True:
        time.sleep(KEEPALIVE_INTERVAL)
        try:
            req = Request(KEEPALIVE_URL, headers={"User-Agent": "KeepAliveBot/1.0"})
            with urlopen(req, timeout=10) as _:
                pass
        except Exception:
            pass


# ================= پنل ساده تک‌رنگ + ترمینال سرور =================
HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>اسپم ۳ گروه</title>
<style>
  body { font-family: Tahoma, sans-serif; background: #121212; color: #eaeaea; margin: 0; padding: 14px; }
  .box { max-width: 480px; margin: 0 auto 12px; background: #1c1c1c; border: 1px solid #333; border-radius: 6px; padding: 12px; }
  h3 { margin: 0 0 10px; font-size: 14px; border-bottom: 1px solid #2e2e2e; padding-bottom: 6px; }
  label { display: block; font-size: 12px; color: #b5b5b5; margin: 8px 0 4px; }
  input, select, textarea { width: 100%; box-sizing: border-box; padding: 8px; border: 1px solid #3b3b3b; border-radius: 4px; background: #111; color: #fff; font-family: Tahoma, sans-serif; }
  button { width: 100%; padding: 8px; margin-top: 8px; border: 1px solid #4c4c4c; border-radius: 4px; background: #262626; color: #fff; font-family: Tahoma, sans-serif; cursor: pointer; }
  button:hover { background: #333; }
  .slot-box { border: 1px solid #2e2e2e; background: #151515; border-radius: 4px; padding: 8px; margin-top: 8px; font-size: 12px; line-height: 1.7; }
  .stat-line { font-size: 13px; margin: 6px 0; line-height: 1.7; }
  .term-box {
    background: #0b0b0b;
    border: 1px solid #2c2c2c;
    border-radius: 4px;
    padding: 8px;
    height: 220px;
    overflow-y: auto;
    font-family: Consolas, monospace;
    font-size: 11px;
    line-height: 1.6;
    color: #d6d6d6;
    direction: ltr;
    text-align: left;
    white-space: pre-wrap;
    word-break: break-word;
  }
  .term-row { display: flex; gap: 8px; }
  .term-row button { margin-top: 6px; }
</style>
</head>
<body>

<div class="box">
  <h3>آمار و وضعیت سرور</h3>
  <div class="stat-line">اکانت: <b id="st_acc">-</b> | اتصال: <b id="st_conn">قطع</b> | فعال: <b id="st_act">0 / 3</b></div>
  <div class="stat-line">خطا: <span id="st_err" dir="ltr">-</span></div>
  <div id="slots_stats"></div>
  <button onclick="stopAll()">توقف همه</button>
</div>

<div class="box">
  <h3>ترمینال زنده و خوداصلاح‌کننده سرور</h3>
  <div id="term_view" class="term-box">در حال اتصال به لاگ سرور...</div>
  <div class="term-row">
    <button onclick="clearTerm()">پاک کردن لاگ</button>
    <button onclick="stopAll()">توقف فوری ارسال</button>
  </div>
</div>

<div class="box">
  <h3>ورود اکانت سروش</h3>
  <label>شماره تلفن:</label>
  <input id="inp_phone" type="text" placeholder="+98912..." dir="ltr">
  <button onclick="sendPhone()">ارسال کد</button>
  <label>کد تأیید:</label>
  <input id="inp_code" type="text" placeholder="12345" dir="ltr">
  <button onclick="sendCode()">تأیید کد</button>
  <div class="stat-line" id="auth_msg">-</div>
</div>

<div class="box">
  <h3>گروه‌ها و شروع اسپم (حداکثر ۳ جا)</h3>
  <button onclick="loadGroups()">دریافت لیست گروه‌ها و کانال‌ها</button>
  <label>سرعت ارسال (ثانیه):</label>
  <input id="inp_speed" type="number" step="0.1" min="0.1" value="1">

  <div class="slot-box">
    <b>جای اول (۱)</b>
    <label>انتخاب گروه/کانال:</label>
    <select id="sel_1" onchange="document.getElementById('tgt_1').value=this.value"><option value="">-- لیست گروه‌ها --</option></select>
    <input id="tgt_1" type="text" placeholder="آیدی، لینک یا @یوزرنیم" style="margin-top:6px;">
    <label>متن ۱:</label>
    <textarea id="txt_1"></textarea>
    <button onclick="startSlot('1')">شروع در جای ۱</button>
    <button onclick="stopSlot('1')">توقف جای ۱</button>
  </div>

  <div class="slot-box">
    <b>جای دوم (۲)</b>
    <label>انتخاب گروه/کانال:</label>
    <select id="sel_2" onchange="document.getElementById('tgt_2').value=this.value"><option value="">-- لیست گروه‌ها --</option></select>
    <input id="tgt_2" type="text" placeholder="آیدی، لینک یا @یوزرنیم" style="margin-top:6px;">
    <label>متن ۲:</label>
    <textarea id="txt_2"></textarea>
    <button onclick="startSlot('2')">شروع در جای ۲</button>
    <button onclick="stopSlot('2')">توقف جای ۲</button>
  </div>

  <div class="slot-box">
    <b>جای سوم (۳)</b>
    <label>انتخاب گروه/کانال:</label>
    <select id="sel_3" onchange="document.getElementById('tgt_3').value=this.value"><option value="">-- لیست گروه‌ها --</option></select>
    <input id="tgt_3" type="text" placeholder="آیدی، لینک یا @یوزرنیم" style="margin-top:6px;">
    <label>متن ۳:</label>
    <textarea id="txt_3"></textarea>
    <button onclick="startSlot('3')">شروع در جای ۳</button>
    <button onclick="stopSlot('3')">توقف جای ۳</button>
  </div>
</div>

<script>
function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

async function sendPhone(){
  const p = document.getElementById('inp_phone').value.trim();
  if(!p) return;
  await fetch('/api/phone?phone=' + encodeURIComponent(p));
  setTimeout(refresh, 400);
}
async function sendCode(){
  const c = document.getElementById('inp_code').value.trim();
  if(!c) return;
  await fetch('/api/code?code=' + encodeURIComponent(c));
  setTimeout(refresh, 600);
}
async function loadGroups(){
  const r = await fetch('/api/groups');
  const arr = await r.json();
  const opts = '<option value="">-- لیست گروه‌ها --</option>' + (arr || []).map(g =>
    `<option value="${esc(g.id)}">[${esc(g.type || 'گروه')}] ${esc(g.name)} (${esc(g.id)})</option>`
  ).join('');
  ['1','2','3'].forEach(n => {
    const sel = document.getElementById('sel_' + n);
    const cur = document.getElementById('tgt_' + n).value;
    sel.innerHTML = opts;
    if(cur) sel.value = cur;
  });
}
async function startSlot(n){
  const payload = {
    slot: n,
    target: document.getElementById('tgt_' + n).value.trim(),
    text: document.getElementById('txt_' + n).value,
    speed: document.getElementById('inp_speed').value
  };
  await fetch('/api/slot_start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(payload)
  });
  refresh();
}
async function stopSlot(n){
  await fetch('/api/slot_stop?slot=' + n);
  refresh();
}
async function stopAll(){
  await fetch('/api/stop_all');
  refresh();
}
async function clearTerm(){
  await fetch('/api/logs_clear', {method: 'POST'});
  refresh();
}

let firstLoad = true;
async function refresh(){
  try {
    const r = await fetch('/api/stats');
    const d = await r.json();
    const slots = d.slots || {};
    let activeCnt = 0;
    let html = '';

    ['1','2','3'].forEach(n => {
      const s = slots[n] || {};
      if(s.running) activeCnt++;
      html += `<div class="slot-box">
        <div><b>جای ${n}</b>: <b>${s.running ? 'در حال اسپم' : 'متوقف'}</b> | ارسال: <b>${s.sent || 0}</b> | آخرین: <b>${esc(s.last_at || '-')}</b></div>
        <div>محل: <b>${esc(s.target_name || s.target_chat || '-')}</b> | متن: <b>${esc(s.text || '-')}</b></div>
      </div>`;
    });

    document.getElementById('slots_stats').innerHTML = html;
    document.getElementById('st_acc').innerText = d.account_name || '-';
    document.getElementById('st_conn').innerText = d.connected ? 'متصل' : 'قطع';
    document.getElementById('st_act').innerText = activeCnt + ' / 3';
    document.getElementById('st_err').innerText = d.last_error || '-';
    document.getElementById('auth_msg').innerText = d.auth_msg || '-';

    const logs = Array.isArray(d.logs) ? d.logs : [];
    const termEl = document.getElementById('term_view');
    const nearBottom = (termEl.scrollHeight - termEl.scrollTop - termEl.clientHeight) < 28;
    termEl.innerText = logs.length ? logs.join('\\n') : 'هنوز لاگی ثبت نشده است.';
    if (firstLoad || nearBottom) {
      termEl.scrollTop = termEl.scrollHeight;
    }

    if(firstLoad){
      firstLoad = false;
      document.getElementById('inp_speed').value = d.speed || 1;
      ['1','2','3'].forEach(n => {
        const s = slots[n] || {};
        if(s.target_chat) document.getElementById('tgt_' + n).value = s.target_chat;
        if(s.text) document.getElementById('txt_' + n).value = s.text;
      });
    }
  } catch(e) {}
}
setInterval(refresh, 1500);
refresh();
</script>
</body>
</html>
"""

app = Flask(__name__)


@app.route("/")
@app.route(PANEL_PATH)
def page():
    return HTML


@app.route("/health")
def health():
    return "OK", 200


@app.route("/api/stats")
def api_stats():
    payload = {k: v for k, v in data.items() if k not in ("code_hash", "session")}
    payload["logs"] = get_console_logs()
    return app.response_class(json.dumps(payload, ensure_ascii=False), mimetype="application/json")


@app.route("/api/logs_clear", methods=["POST"])
def api_logs_clear():
    with console_lock:
        console_logs.clear()
    add_console_log("لاگ ترمینال پاک شد.")
    return "", 200


@app.route("/api/phone")
def api_phone():
    p = (request.args.get("phone") or "").strip()
    if p:
        run_async(do_send_code(p))
    return "", 200


@app.route("/api/code")
def api_code():
    c = (request.args.get("code") or "").strip()
    if c:
        run_async(do_verify_code(c))
    return "", 200


@app.route("/api/groups")
def api_groups():
    fut = run_async(fetch_groups_list())
    if fut:
        try:
            res = fut.result(timeout=16)
            return app.response_class(json.dumps(res or [], ensure_ascii=False), mimetype="application/json")
        except Exception:
            pass
    return app.response_class(json.dumps(dialogs_cache, ensure_ascii=False), mimetype="application/json")


@app.route("/api/slot_start", methods=["POST"])
def api_slot_start():
    body = request.get_json(silent=True) or {}
    n = str(body.get("slot") or "").strip()
    if n in ("1", "2", "3"):
        if active_running_count(exclude_slot=n) >= MAX_SLOTS:
            data["last_error"] = "هم‌زمان بیشتر از ۳ جا نمی‌توان اسپم زد."
            add_console_log("درخواست رد شد: هم‌زمان حداکثر ۳ اسلات مجاز است.")
            save_data()
            return "", 200
        raw_tgt = str(body.get("target") or "").strip()
        parsed_tgt = parse_target(raw_tgt)
        txt = str(body.get("text") or "").strip()
        try:
            data["speed"] = max(0.1, float(body.get("speed") or 1.0))
        except Exception:
            data["speed"] = 1.0

        s = data["slots"][n]
        if parsed_tgt:
            s["target_chat"] = parsed_tgt
            matched = next((g.get("name") for g in dialogs_cache if str(g.get("id")) == str(parsed_tgt)), "")
            s["target_name"] = matched or raw_tgt
        if txt:
            s["text"] = txt
        if s.get("target_chat") and s.get("text"):
            s["running"] = True
            data["last_error"] = "-"
            add_console_log(f"[جای {n}] ارسال شروع شد -> {s.get('target_name') or s.get('target_chat')}")
        else:
            data["last_error"] = "مقصد و متن را وارد کنید."
            add_console_log(f"[جای {n}] خطا: مقصد یا متن خالی است.")
        save_data()
    return "", 200


@app.route("/api/slot_stop")
def api_slot_stop():
    n = (request.args.get("slot") or "").strip()
    if n in ("1", "2", "3"):
        data["slots"][n]["running"] = False
        add_console_log(f"[جای {n}] ارسال متوقف شد.")
        save_data()
    return "", 200


@app.route("/api/stop_all")
def api_stop_all():
    for s in data["slots"].values():
        s["running"] = False
    add_console_log("همه اسلات‌ها متوقف شدند.")
    save_data()
    return "", 200


def run_web():
    while True:
        try:
            app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)
        except Exception:
            time.sleep(1)


if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    Thread(target=start_bot, daemon=True).start()
    Thread(target=run_keepalive, daemon=True).start()
    while True:
        time.sleep(1)
