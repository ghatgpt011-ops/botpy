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

STATE_FILE = os.path.join(os.environ.get("DATA_DIR", "."), "spam_3groups_state.json")
PORT = int(os.environ.get("PORT", 8080))
PANEL_PATH = os.environ.get("PANEL_PATH", "/panel")
if not PANEL_PATH.startswith("/"):
    PANEL_PATH = "/" + PANEL_PATH

KEEPALIVE_URL = (os.environ.get("KEEPALIVE_URL") or "").strip()
KEEPALIVE_INTERVAL = max(60, int(os.environ.get("KEEPALIVE_INTERVAL", "240")))
MAX_SLOTS = 3

for name in ("splusthon", "asyncio", "werkzeug"):
    logging.getLogger(name).setLevel(logging.CRITICAL)


def make_slot(idx: str) -> dict:
    return {
        "text": f"پیام {idx}",
        "target_chat": "",
        "target_name": "-",
        "running": False,
        "sent": 0,
        "last_at": "-",
    }


data = {
    "phone": "",
    "session": "",
    "code_hash": None,
    "auth_msg": "شماره را وارد کنید.",
    "connected": False,
    "account_name": "-",
    "speed": 1.0,
    "last_error": "-",
    "slots": {k: make_slot(k) for k in ("1", "2", "3")},
}

dialogs_cache = []
console_logs = deque(maxlen=140)
console_lock = Lock()
state_lock = Lock()

client = None
bot_loop = None
io_lock = None
connect_lock = None
_last_connect_try = 0.0


def now_t() -> str:
    return datetime.now().strftime("%H:%M:%S")


def log_line(msg: str):
    with console_lock:
        console_logs.append(f"[{now_t()}] {str(msg).strip()}")


def get_logs() -> list:
    with console_lock:
        return list(console_logs)


log_line("سرویس سرور راه‌اندازی شد.")


def save_data():
    with state_lock:
        try:
            parent = os.path.dirname(os.path.abspath(STATE_FILE))
            if parent:
                os.makedirs(parent, exist_ok=True)
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
    if not os.path.exists(STATE_FILE):
        return
    with state_lock:
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
            if not isinstance(raw, dict):
                return
            for k in ("phone", "session", "auth_msg", "account_name", "speed", "last_error"):
                if k in raw and raw[k] is not None:
                    data[k] = raw[k]
            raw_slots = raw.get("slots") or {}
            for k in ("1", "2", "3"):
                if isinstance(raw_slots.get(k), dict):
                    cur = data["slots"][k]
                    for sk, sv in raw_slots[k].items():
                        if sk in ("target_chat", "target_name") and not sv and cur.get(sk):
                            continue
                        cur[sk] = sv
            data["connected"] = False
            log_line("وضعیت و سشن ذخیره‌شده بارگذاری شد.")
        except Exception as e:
            log_line(f"خطا در خواندن فایل: {str(e)[:100]}")


def parse_target(raw: str) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    if "splus.ir" in s or s.startswith(("http://", "https://")):
        url_s = s if s.startswith("http") else "https://" + s
        try:
            parts = [p for p in urlparse(url_s).path.split("/") if p]
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
    return s[1:].strip() if s.startswith("@") else s


def to_peer(raw: str):
    t = parse_target(raw)
    return int(t) if t.lstrip("-").isdigit() else t


def sync_target_name(slot: dict):
    tgt = str(slot.get("target_chat") or "").strip()
    if not tgt:
        return
    matched = next((g.get("name") for g in dialogs_cache if str(g.get("id")) == tgt), None)
    if matched:
        slot["target_name"] = matched
    elif not slot.get("target_name") or slot.get("target_name") == "-":
        slot["target_name"] = tgt


def extract_dialog(d):
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


async def _disconnect(c):
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
    global client, _last_connect_try, connect_lock
    if connect_lock is None:
        connect_lock = asyncio.Lock()

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
                await _disconnect(client)
                client = SoroushClient(StringSession(sess))

            if hasattr(client, "is_connected"):
                try:
                    conn = client.is_connected()
                    if asyncio.iscoroutine(conn):
                        conn = await conn
                    if not conn:
                        await asyncio.wait_for(client.connect(), timeout=12)
                except Exception:
                    await asyncio.wait_for(client.connect(), timeout=12)
            else:
                await asyncio.wait_for(client.connect(), timeout=12)

            me = await asyncio.wait_for(client.get_me(), timeout=8)
            full_name = f"{getattr(me, 'first_name', '') or ''} {getattr(me, 'last_name', '') or ''}".strip()
            data["account_name"] = full_name or getattr(me, "username", None) or data["phone"] or "متصل"
            was_conn = bool(data.get("connected"))
            data["connected"] = True
            data["last_error"] = "-"
            if not was_conn:
                log_line(f"اکانت متصل شد: {data['account_name']}")
            return True
        except Exception as e:
            data["connected"] = False
            err = str(e)[:120]
            data["last_error"] = err
            log_line(f"خطای اتصال: {err}")
            await _disconnect(client)
            client = None
            return False


async def fetch_groups():
    global dialogs_cache
    log_line("در حال دریافت لیست گروه‌ها و کانال‌ها...")
    if not await connect_client() or client is None:
        log_line("دریافت لیست ناموفق بود؛ اکانت متصل نیست.")
        return dialogs_cache
    try:
        raw = []
        if hasattr(client, "get_dialogs"):
            res = await asyncio.wait_for(client.get_dialogs(limit=200), timeout=15)
            raw = list(res) if res else []
        elif hasattr(client, "iter_dialogs"):
            async for d in client.iter_dialogs(limit=200):
                raw.append(d)

        gc_only, all_items = [], []
        for d in raw:
            info = extract_dialog(d)
            if not info:
                continue
            item = {"id": info["id"], "name": info["name"], "type": info["type"]}
            all_items.append(item)
            if info["is_gc"]:
                gc_only.append(item)

        if gc_only or all_items:
            dialogs_cache = gc_only if gc_only else all_items
        for s in data["slots"].values():
            sync_target_name(s)
        save_data()
        log_line(f"لیست دریافت شد ({len(dialogs_cache)} مورد).")
        return dialogs_cache
    except Exception as e:
        err = str(e)[:120]
        data["last_error"] = err
        data["connected"] = False
        log_line(f"خطا در دریافت لیست: {err}")
        return dialogs_cache


async def do_send_code(phone: str):
    global client, connect_lock
    if connect_lock is None:
        connect_lock = asyncio.Lock()
    data["phone"] = phone
    data["auth_msg"] = "در حال ارسال کد..."
    log_line(f"ارسال کد برای {phone}...")
    try:
        async with connect_lock:
            await _disconnect(client)
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
        log_line("کد تأیید ارسال شد.")
    except Exception as e:
        data["auth_msg"] = f"خطا: {e}"
        log_line(f"خطای ارسال کد: {str(e)[:120]}")


async def do_verify_code(code: str):
    global connect_lock
    if connect_lock is None:
        connect_lock = asyncio.Lock()
    phone = data.get("phone", "").strip()
    log_line("در حال بررسی کد تأیید...")
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
        log_line("ورود با موفقیت انجام شد.")
    except Exception as e:
        data["auth_msg"] = f"خطا در کد: {e}"
        log_line(f"خطای تأیید کد: {str(e)[:120]}")


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)
    return None


async def heal_error(slot_num: str, slot: dict, err: Exception, errs: int):
    raw = str(err or "")
    low = raw.lower()
    if any(k in low for k in ("flood", "rate", "too many", "limit", "429", "slow")):
        wait = min(15.0, max(2.5, 2.0 * errs))
        log_line(f"[اصلاح جای {slot_num}] محدودیت سرعت؛ مکث {wait:.1f}s.")
        return wait, False

    if any(k in low for k in ("peer", "chat", "user", "resolve", "not found", "invalid", "target")):
        cur = str(slot.get("target_chat") or "").strip()
        fixed = parse_target(cur)
        if not fixed.lstrip("-").isdigit() and dialogs_cache:
            name_key = str(slot.get("target_name") or "").strip()
            matched = next(
                (g.get("id") for g in dialogs_cache if name_key and str(g.get("name", "")).strip() == name_key),
                None,
            )
            if matched:
                fixed = str(matched)
        if fixed and fixed != cur:
            slot["target_chat"] = fixed
            sync_target_name(slot)
            save_data()
            log_line(f"[اصلاح جای {slot_num}] مقصد اصلاح شد ({cur} -> {fixed}).")
            return 0.3, True

    data["connected"] = False
    log_line(f"[اصلاح جای {slot_num}] خطای «{raw[:100]}»؛ بازاتصال با حفظ سشن...")
    if await connect_client(force=True):
        log_line(f"[اصلاح جای {slot_num}] اتصال برقرار شد؛ ادامه همان پیام.")
        return 0.2, True
    return min(6.0, 1.0 * errs), False


async def slot_worker(slot_num: str):
    global io_lock
    if io_lock is None:
        io_lock = asyncio.Lock()

    slot = data["slots"][slot_num]
    errs = 0
    retry_now = False
    while True:
        try:
            if not slot.get("running"):
                errs, retry_now = 0, False
                await asyncio.sleep(0.15)
                continue

            tgt = str(slot.get("target_chat") or "").strip()
            txt = str(slot.get("text") or "").strip()
            if not tgt or not txt:
                slot["running"] = False
                data["last_error"] = "مقصد یا متن خالی است."
                log_line(f"[اصلاح جای {slot_num}] مقصد یا متن خالی بود؛ متوقف شد.")
                save_data()
                await asyncio.sleep(0.2)
                continue

            if not data.get("connected") or client is None:
                ok = await connect_client()
                await asyncio.sleep(0.3 if ok else 1.0)
                continue

            sync_target_name(slot)
            async with io_lock:
                await asyncio.wait_for(client.send_message(to_peer(tgt), txt), timeout=10)

            errs, retry_now = 0, False
            cnt = int(slot.get("sent") or 0) + 1
            slot["sent"] = cnt
            slot["last_at"] = now_t()
            data["last_error"] = "-"
            log_line(f"[جای {slot_num}] پیام #{cnt} به «{slot.get('target_name') or tgt}» ارسال شد.")
            if cnt % 5 == 0:
                save_data()
            await asyncio.sleep(max(0.1, float(data.get("speed") or 1.0)))
        except Exception as e:
            errs += 1
            data["last_error"] = str(e)[:120]
            wait, retry_now = await heal_error(slot_num, slot, e, errs)
            if not retry_now:
                await asyncio.sleep(wait)


async def runner_main():
    global bot_loop, io_lock, connect_lock
    bot_loop = asyncio.get_running_loop()
    io_lock = asyncio.Lock()
    connect_lock = asyncio.Lock()
    log_line("حلقه ارسال فعال شد.")
    if (data.get("session") or "").strip():
        await connect_client(force=True)
    for k in ("1", "2", "3"):
        asyncio.create_task(slot_worker(k))
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
            data["last_error"] = str(e)[:120]
            log_line(f"[بازیابی حلقه] {str(e)[:100]}")
            time.sleep(1)


def run_keepalive():
    if not KEEPALIVE_URL:
        return
    while True:
        time.sleep(KEEPALIVE_INTERVAL)
        try:
            with urlopen(Request(KEEPALIVE_URL, headers={"User-Agent": "KeepAliveBot/1.0"}), timeout=10):
                pass
        except Exception:
            pass


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
  .term-box { background: #0b0b0b; border: 1px solid #2c2c2c; border-radius: 4px; padding: 8px; height: 210px; overflow-y: auto; font-family: Consolas, monospace; font-size: 11px; line-height: 1.6; color: #d6d6d6; direction: ltr; text-align: left; white-space: pre-wrap; word-break: break-word; }
  .row { display: flex; gap: 8px; }
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
  <h3>ترمینال زنده سرور</h3>
  <div id="term_view" class="term-box">در حال اتصال...</div>
  <div class="row"><button onclick="clearTerm()">پاک کردن لاگ</button><button onclick="stopAll()">توقف فوری</button></div>
</div>

<div class="box">
  <h3>ورود اکانت سروش</h3>
  <label>شماره تلفن:</label><input id="inp_phone" type="text" placeholder="+98912..." dir="ltr">
  <button onclick="sendAuth('/api/phone?phone=', 'inp_phone')">ارسال کد</button>
  <label>کد تأیید:</label><input id="inp_code" type="text" placeholder="12345" dir="ltr">
  <button onclick="sendAuth('/api/code?code=', 'inp_code')">تأیید کد</button>
  <div class="stat-line" id="auth_msg">-</div>
</div>

<div class="box">
  <h3>گروه‌ها و شروع اسپم (حداکثر ۳ جا)</h3>
  <button onclick="loadGroups()">دریافت لیست گروه‌ها و کانال‌ها</button>
  <label>سرعت ارسال (ثانیه):</label><input id="inp_speed" type="number" step="0.1" min="0.1" value="1">
  <div id="slots_form"></div>
</div>

<script>
const SLOTS = ['1', '2', '3'];
const esc = s => String(s || '').replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

document.getElementById('slots_form').innerHTML = SLOTS.map(n => `
  <div class="slot-box">
    <b>جای ${n}</b>
    <label>انتخاب گروه/کانال:</label>
    <select id="sel_${n}" onchange="document.getElementById('tgt_${n}').value=this.value"><option value="">-- لیست گروه‌ها --</option></select>
    <input id="tgt_${n}" type="text" placeholder="آیدی، لینک یا @یوزرنیم" style="margin-top:6px;">
    <label>متن ${n}:</label><textarea id="txt_${n}"></textarea>
    <button onclick="startSlot('${n}')">شروع در جای ${n}</button>
    <button onclick="stopSlot('${n}')">توقف جای ${n}</button>
  </div>
`).join('');

async function sendAuth(url, id){
  const v = document.getElementById(id).value.trim();
  if(!v) return;
  await fetch(url + encodeURIComponent(v));
  setTimeout(refresh, 400);
}
async function loadGroups(){
  const arr = await (await fetch('/api/groups')).json();
  const opts = '<option value="">-- لیست گروه‌ها --</option>' + (arr || []).map(g =>
    `<option value="${esc(g.id)}">[${esc(g.type || 'گروه')}] ${esc(g.name)} (${esc(g.id)})</option>`
  ).join('');
  SLOTS.forEach(n => {
    const sel = document.getElementById('sel_' + n);
    const cur = document.getElementById('tgt_' + n).value;
    sel.innerHTML = opts;
    if(cur) sel.value = cur;
  });
}
async function startSlot(n){
  await fetch('/api/slot_start', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      slot: n,
      target: document.getElementById('tgt_' + n).value.trim(),
      text: document.getElementById('txt_' + n).value,
      speed: document.getElementById('inp_speed').value
    })
  });
  refresh();
}
const stopSlot = async n => { await fetch('/api/slot_stop?slot=' + n); refresh(); };
const stopAll = async () => { await fetch('/api/stop_all'); refresh(); };
const clearTerm = async () => { await fetch('/api/logs_clear', {method: 'POST'}); refresh(); };

let firstLoad = true;
async function refresh(){
  try {
    const d = await (await fetch('/api/stats')).json();
    const slots = d.slots || {};
    let active = 0;
    document.getElementById('slots_stats').innerHTML = SLOTS.map(n => {
      const s = slots[n] || {};
      if(s.running) active++;
      const dest = s.target_name && s.target_name !== '-' && s.target_chat && s.target_name !== s.target_chat
        ? `${s.target_name} (${s.target_chat})` : (s.target_name || s.target_chat || '-');
      return `<div class="slot-box">
        <div><b>جای ${n}</b>: <b>${s.running ? 'در حال اسپم' : 'متوقف'}</b> | ارسال: <b>${s.sent || 0}</b> | آخرین: <b>${esc(s.last_at || '-')}</b></div>
        <div>محل: <b>${esc(dest)}</b> | متن: <b>${esc(s.text || '-')}</b></div>
      </div>`;
    }).join('');

    document.getElementById('st_acc').innerText = d.account_name || '-';
    document.getElementById('st_conn').innerText = d.connected ? 'متصل' : 'قطع';
    document.getElementById('st_act').innerText = active + ' / 3';
    document.getElementById('st_err').innerText = d.last_error || '-';
    document.getElementById('auth_msg').innerText = d.auth_msg || '-';

    const logs = Array.isArray(d.logs) ? d.logs : [];
    const term = document.getElementById('term_view');
    const nearBottom = (term.scrollHeight - term.scrollTop - term.clientHeight) < 28;
    term.innerText = logs.length ? logs.join('\n') : 'هنوز لاگی ثبت نشده است.';
    if(firstLoad || nearBottom) term.scrollTop = term.scrollHeight;

    SLOTS.forEach(n => {
      const s = slots[n] || {};
      const tgt = document.getElementById('tgt_' + n);
      const txt = document.getElementById('txt_' + n);
      if((firstLoad || !tgt.value.trim()) && s.target_chat) tgt.value = s.target_chat;
      if((firstLoad || !txt.value.trim()) && s.text) txt.value = s.text;
    });
    if(firstLoad){
      firstLoad = false;
      document.getElementById('inp_speed').value = d.speed || 1;
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


def json_resp(obj):
    return app.response_class(json.dumps(obj, ensure_ascii=False), mimetype="application/json")


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
    payload["logs"] = get_logs()
    return json_resp(payload)


@app.route("/api/logs_clear", methods=["POST"])
def api_logs_clear():
    with console_lock:
        console_logs.clear()
    log_line("لاگ ترمینال پاک شد.")
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
    fut = run_async(fetch_groups())
    if fut:
        try:
            return json_resp(fut.result(timeout=16) or [])
        except Exception:
            pass
    return json_resp(dialogs_cache)


@app.route("/api/slot_start", methods=["POST"])
def api_slot_start():
    body = request.get_json(silent=True) or {}
    n = str(body.get("slot") or "").strip()
    if n in ("1", "2", "3"):
        active = sum(1 for k, s in data["slots"].items() if k != n and bool(s.get("running")))
        if active >= MAX_SLOTS:
            data["last_error"] = "هم‌زمان بیشتر از ۳ جا نمی‌توان اسپم زد."
            log_line("درخواست رد شد: حداکثر ۳ اسلات مجاز است.")
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
            s["target_name"] = next((g.get("name") for g in dialogs_cache if str(g.get("id")) == str(parsed_tgt)), "") or raw_tgt
        if txt:
            s["text"] = txt
        if s.get("target_chat") and s.get("text"):
            sync_target_name(s)
            s["running"] = True
            data["last_error"] = "-"
            log_line(f"[جای {n}] ارسال شروع شد -> {s.get('target_name') or s.get('target_chat')}")
        else:
            data["last_error"] = "مقصد و متن را وارد کنید."
            log_line(f"[جای {n}] خطا: مقصد یا متن خالی است.")
        save_data()
    return "", 200


@app.route("/api/slot_stop")
def api_slot_stop():
    n = (request.args.get("slot") or "").strip()
    if n in ("1", "2", "3"):
        data["slots"][n]["running"] = False
        log_line(f"[جای {n}] ارسال متوقف شد.")
        save_data()
    return "", 200


@app.route("/api/stop_all")
def api_stop_all():
    for s in data["slots"].values():
        s["running"] = False
    log_line("همه اسلات‌ها متوقف شدند.")
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
