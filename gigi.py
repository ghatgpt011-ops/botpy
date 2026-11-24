import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from threading import Thread
from urllib.parse import urlparse

from flask import Flask, request
from splusthon import SoroushClient
from splusthon.sessions import StringSession

# ================= تنظیمات =================
STATE_FILE = os.path.join(os.environ.get("DATA_DIR", "."), "spam_3groups_state.json")
PORT = int(os.environ.get("PORT", 8080))
PANEL_PATH = os.environ.get("PANEL_PATH", "/panel")
if not PANEL_PATH.startswith("/"):
    PANEL_PATH = "/" + PANEL_PATH

MAX_SLOTS = 3

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
client = None
bot_loop = None
io_lock = asyncio.Lock()


def now_t() -> str:
    return datetime.now().strftime("%H:%M:%S")


def save_data():
    try:
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
async def connect_client() -> bool:
    global client
    sess = (data.get("session") or "").strip()
    if not sess:
        data["connected"] = False
        return False
    try:
        if client is None:
            client = SoroushClient(StringSession(sess))
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
        data["connected"] = True
        data["last_error"] = "-"
        return True
    except Exception as e:
        data["connected"] = False
        data["last_error"] = str(e)[:120]
        return False


async def fetch_groups_list():
    global dialogs_cache
    ok = await connect_client()
    if not ok or client is None:
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
        return dialogs_cache
    except Exception as e:
        data["last_error"] = str(e)[:120]
        return dialogs_cache


async def do_send_code(phone: str):
    global client
    data["phone"] = phone
    data["auth_msg"] = "در حال ارسال کد..."
    try:
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
    except Exception as e:
        data["auth_msg"] = f"خطا: {e}"


async def do_verify_code(code: str):
    phone = data.get("phone", "").strip()
    try:
        if client is None:
            raise RuntimeError("ابتدا شماره را بفرستید.")
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
        data["auth_msg"] = "با موفقیت وارد شد."
        save_data()
    except Exception as e:
        data["auth_msg"] = f"خطا در کد: {e}"


def run_async(coro):
    if bot_loop and not bot_loop.is_closed():
        return asyncio.run_coroutine_threadsafe(coro, bot_loop)
    return None


async def slot_worker(slot_num: str):
    slot = data["slots"][slot_num]
    while True:
        try:
            if not slot.get("running") or not slot.get("target_chat") or not slot.get("text"):
                await asyncio.sleep(0.15)
                continue

            if not data.get("connected") or client is None:
                await connect_client()
                await asyncio.sleep(0.8)
                continue

            peer = to_peer(str(slot["target_chat"]))
            msg = str(slot["text"])

            async with io_lock:
                await asyncio.wait_for(client.send_message(peer, msg), timeout=10)

            slot["sent"] = int(slot.get("sent") or 0) + 1
            slot["last_at"] = now_t()
            data["last_error"] = "-"
            if slot["sent"] % 10 == 0:
                save_data()

            await asyncio.sleep(max(0.1, float(data.get("speed") or 1.0)))
        except Exception as e:
            data["last_error"] = str(e)[:120]
            data["connected"] = False
            await asyncio.sleep(0.8)


async def runner_main():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    if (data.get("session") or "").strip():
        await connect_client()
    for s_num in ("1", "2", "3"):
        asyncio.create_task(slot_worker(s_num))
    while True:
        await asyncio.sleep(4)
        if (data.get("session") or "").strip() and not data.get("connected"):
            await connect_client()


def start_bot():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(runner_main())
        except Exception:
            time.sleep(1)


# ================= پنل ساده تک‌رنگ =================
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
</style>
</head>
<body>

<div class="box">
  <h3>آمار و وضعیت سرور</h3>
  <div class="stat-line">اکانت: <b id="st_acc">-</b> | فعال: <b id="st_act">0 / 3</b></div>
  <div class="stat-line">خطا: <span id="st_err" dir="ltr">-</span></div>
  <div id="slots_stats"></div>
  <button onclick="stopAll()">توقف همه</button>
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
        <div><b>جای ${n}</b>: <b>${s.running ? 'در حال اسپم' : 'متوقف'}</b> | ارسال: <b>${s.sent || 0}</b></div>
        <div>محل: <b>${esc(s.target_name || s.target_chat || '-')}</b> | متن: <b>${esc(s.text || '-')}</b></div>
      </div>`;
    });

    document.getElementById('slots_stats').innerHTML = html;
    document.getElementById('st_acc').innerText = d.account_name || '-';
    document.getElementById('st_act').innerText = activeCnt + ' / 3';
    document.getElementById('st_err').innerText = d.last_error || '-';
    document.getElementById('auth_msg').innerText = d.auth_msg || '-';

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


@app.route(PANEL_PATH)
def page():
    return HTML


@app.route("/health")
def health():
    return "OK", 200


@app.route("/api/stats")
def api_stats():
    payload = {k: v for k, v in data.items() if k not in ("code_hash", "session")}
    return app.response_class(json.dumps(payload, ensure_ascii=False), mimetype="application/json")


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
        else:
            data["last_error"] = "مقصد و متن را وارد کنید."
        save_data()
    return "", 200


@app.route("/api/slot_stop")
def api_slot_stop():
    n = (request.args.get("slot") or "").strip()
    if n in ("1", "2", "3"):
        data["slots"][n]["running"] = False
        save_data()
    return "", 200


@app.route("/api/stop_all")
def api_stop_all():
    for s in data["slots"].values():
        s["running"] = False
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
    while True:
        time.sleep(1)
