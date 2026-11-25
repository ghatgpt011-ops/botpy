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
SESSION_FILE = "soroush_session.txt"
JOBS_FILE = "spam_jobs.json"
PORT = int(os.environ.get("PORT", 8080))
PANEL_PATH = os.environ.get("PANEL_PATH", "/my-panel")
if not PANEL_PATH.startswith("/"):
    PANEL_PATH = "/" + PANEL_PATH

spam_jobs = []
job_seq = 0

state = {
    "status": "در حال راه‌اندازی...",
    "ping": "در حال بررسی...",
    "auth_status": "disconnected",  # disconnected | code_sent | connected
    "auth_msg": "شماره تلفن را وارد کنید (مثال: +98912...)",
    "phone": "",
}

client = SoroushClient(StringSession())
group_list = []
bot_loop = None
phone_code_hash = None
login_in_progress = False
client_ready = False
connect_lock = asyncio.Lock()


def now_clock() -> str:
    return datetime.now().strftime("%H:%M:%S")


def load_session_str() -> str:
    if os.path.exists(SESSION_FILE):
        try:
            with open(SESSION_FILE, "r", encoding="utf-8") as f:
                return f.read().strip()
        except Exception:
            pass
    return ""


def save_session_str(s: str):
    try:
        with open(SESSION_FILE, "w", encoding="utf-8") as f:
            f.write(s or "")
    except Exception:
        pass


def load_jobs():
    global spam_jobs, job_seq
    if os.path.exists(JOBS_FILE):
        try:
            with open(JOBS_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f)
                if isinstance(raw, list):
                    spam_jobs = raw
                    for j in spam_jobs:
                        j["idx"] = int(j.get("idx") or 0)
                        j["sent"] = int(j.get("sent") or 0)
                        j["max_count"] = int(j.get("max_count") or 0)
                        j["speed"] = max(0.01, float(j.get("speed") or 1.0))
                        if j.get("running"):
                            j["last_status"] = "ادامه خودکار تسک..."
                            j["last_err"] = ""
                    job_seq = len(spam_jobs)
        except Exception:
            pass


def save_jobs():
    try:
        with open(JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(spam_jobs, f, ensure_ascii=False, indent=2)
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
                    if parts[0] == "c" and len(parts) >= 2 and parts[1].lstrip("-").isdigit():
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


def resolve_group_name(target: str) -> str:
    t = str(target).strip()
    for g in group_list:
        if str(g.get("id")) == t or str(g.get("name")) == t:
            return g.get("name") or t
    return t


def parse_texts(encoded: str) -> list:
    if not encoded:
        return []
    txt = encoded.replace("_NEWLINE_", "\n")
    try:
        arr = json.loads(txt)
        if isinstance(arr, list):
            items = [str(x).strip() for x in arr if str(x).strip()]
            if items:
                return items
    except Exception:
        pass
    return [txt.strip()] if txt.strip() else []


load_jobs()


# ================= مدیریت کلاینت و لاگین =================
async def _close_client_safely(c):
    if c is None:
        return
    for attr in ("_client", "client", "_session_http", "http_session"):
        sub = getattr(c, attr, None)
        if sub is not None and hasattr(sub, "close"):
            try:
                res = sub.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
    if hasattr(c, "disconnect"):
        try:
            res = c.disconnect()
            if asyncio.iscoroutine(res):
                await res
        except Exception:
            pass


def _extract_code_hash(sent_obj, phone: str):
    if sent_obj is not None:
        h = getattr(sent_obj, "phone_code_hash", None)
        if h:
            return str(h)
        if isinstance(sent_obj, dict) and sent_obj.get("phone_code_hash"):
            return str(sent_obj["phone_code_hash"])
    for attr in ("_phone_code_hash", "phone_code_hash", "code_hash"):
        val = getattr(client, attr, None)
        if isinstance(val, dict) and phone in val:
            return str(val[phone])
        if isinstance(val, str) and val:
            return val
    return None


async def _ensure_live_session(force_fresh: bool = False) -> bool:
    global client, client_ready
    if login_in_progress:
        return False

    sess = load_session_str()
    if not sess:
        client_ready = False
        state["auth_status"] = "disconnected"
        state["auth_msg"] = "سشن وجود ندارد؛ شماره را وارد کنید."
        return False

    async with connect_lock:
        if not force_fresh and client_ready:
            try:
                t0 = time.perf_counter()
                await asyncio.wait_for(client.get_me(), timeout=6)
                state["ping"] = f"{int((time.perf_counter() - t0) * 1000)} ms"
                state["auth_status"] = "connected"
                return True
            except Exception:
                pass

        try:
            if force_fresh or not client_ready:
                await _close_client_safely(client)
                client = SoroushClient(StringSession(sess))

            t0 = time.perf_counter()
            await asyncio.wait_for(client.connect(), timeout=15)
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            ms = int((time.perf_counter() - t0) * 1000)

            client_ready = True
            state["ping"] = f"{ms} ms"
            state["auth_status"] = "connected"
            name = getattr(me, "first_name", None) or "اکانت"
            state["auth_msg"] = f"متصل و فعال: {name}"
            state["status"] = f"{name} آنلاین و در حال اجراست."
            return True
        except Exception as e:
            client_ready = False
            state["ping"] = "اختلال لحظه‌ای"
            state["status"] = f"بررسی مجدد اتصال: {e}"
            return False


async def _do_send_code(phone: str):
    global client, phone_code_hash, login_in_progress, client_ready
    login_in_progress = True
    client_ready = False
    phone_code_hash = None
    state["phone"] = phone
    state["auth_status"] = "code_sent"
    state["auth_msg"] = "در حال ارسال کد..."
    try:
        async with connect_lock:
            await _close_client_safely(client)
            client = SoroushClient(StringSession())
            await asyncio.wait_for(client.connect(), timeout=20)

            sent_res = None
            if hasattr(client, "send_code_request"):
                sent_res = await asyncio.wait_for(client.send_code_request(phone), timeout=20)
            elif hasattr(client, "send_code"):
                sent_res = await asyncio.wait_for(client.send_code(phone), timeout=20)
            else:
                sent_res = await asyncio.wait_for(client.sign_in(phone=phone), timeout=20)

            phone_code_hash = _extract_code_hash(sent_res, phone)
        state["auth_status"] = "code_sent"
        state["auth_msg"] = "کد ارسال شد؛ کد را وارد کنید."
    except Exception as e:
        state["auth_status"] = "code_sent"
        state["auth_msg"] = f"خطا در ارسال کد: {e}"


async def _do_verify_code(code: str):
    global phone_code_hash, login_in_progress, client_ready
    state["auth_status"] = "code_sent"
    state["auth_msg"] = "در حال تأیید کد..."
    phone = state.get("phone", "").strip()
    try:
        async with connect_lock:
            if not hasattr(client, "sign_in"):
                raise RuntimeError("متد sign_in در کلاینت موجود نیست.")

            signed = False
            last_exc = None

            if phone_code_hash:
                for call in (
                    lambda: client.sign_in(phone=phone, code=code, phone_code_hash=phone_code_hash),
                    lambda: client.sign_in(phone, code, phone_code_hash),
                ):
                    try:
                        await asyncio.wait_for(call(), timeout=20)
                        signed = True
                        break
                    except TypeError:
                        continue
                    except Exception as e:
                        last_exc = e

            if not signed:
                try:
                    await asyncio.wait_for(client.sign_in(phone=phone, code=code), timeout=20)
                    signed = True
                except Exception as e:
                    last_exc = e

            if not signed:
                raise RuntimeError(str(last_exc or "ورود ناموفق بود."))

            sess = client.session.save() if getattr(client, "session", None) else ""
            if not sess:
                raise RuntimeError("سشن خالی است.")

            save_session_str(sess)
            phone_code_hash = None
            login_in_progress = False
            client_ready = True
            state["auth_status"] = "connected"
            state["auth_msg"] = "ورود موفق انجام شد؛ اتصال برقرار است."
            state["status"] = "متصل و آماده ارسال."
    except Exception as e:
        state["auth_status"] = "code_sent"
        state["auth_msg"] = f"خطا در کد: {e}"


def run_in_bot_loop(coro):
    if bot_loop and not bot_loop.is_closed():
        asyncio.run_coroutine_threadsafe(coro, bot_loop)


# ================= رابط کاربری ساده =================
PANEL_HTML = """<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>پنل اسپم</title>
<style>
  body { font-family: Tahoma, sans-serif; background: #111; color: #eee; margin: 0; padding: 12px; }
  .box { max-width: 460px; margin: 0 auto 12px; background: #1b1b1b; border: 1px solid #333; border-radius: 8px; padding: 12px; }
  h3 { margin: 0 0 10px; font-size: 15px; border-bottom: 1px solid #2c2c2c; padding-bottom: 6px; }
  label { display: block; font-size: 12px; color: #aaa; margin: 8px 0 4px; }
  input, textarea { width: 100%; box-sizing: border-box; padding: 8px; border-radius: 6px; border: 1px solid #3a3a3a; background: #111; color: #fff; font-family: Tahoma, sans-serif; }
  textarea { min-height: 50px; resize: vertical; }
  button { width: 100%; padding: 9px; margin-top: 8px; border: 0; border-radius: 6px; font-family: Tahoma, sans-serif; font-weight: bold; cursor: pointer; background: #2b6cb0; color: #fff; }
  .btn-green { background: #2f855a; }
  .btn-amber { background: #b7791f; }
  .btn-red { background: #c53030; }
  .mini { width: auto; padding: 5px 10px; font-size: 12px; margin-top: 0; }
  .row { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; margin-top: 6px; }
  .card { background: #121212; border: 1px solid #2e2e2e; border-radius: 6px; padding: 8px; margin-top: 8px; font-size: 12px; line-height: 1.7; }
  .small { font-size: 12px; color: #bbb; margin-top: 6px; word-break: break-word; }
  .groups { display: flex; flex-wrap: wrap; gap: 5px; margin-top: 6px; max-height: 100px; overflow-y: auto; }
  .gbtn { width: auto; margin: 0; padding: 4px 8px; font-size: 11px; background: #252525; border: 1px solid #3d3d3d; }
</style>
</head>
<body>

<div class="box">
  <h3>اطلاعات اسپم (پینگ: <span id="ping">-</span>)</h3>
  <div id="info_list"><div class="small">اسپمی ثبت نشده است.</div></div>
  <div class="small" id="status_txt">-</div>
</div>

<div class="box">
  <h3>ورود با شماره</h3>
  <label>شماره تلفن:</label>
  <input id="phone" type="text" placeholder="+98912..." dir="ltr">
  <button onclick="sendPhone()">ارسال کد</button>
  <div id="code_box" style="display:none; margin-top:8px;">
    <label>کد تأیید:</label>
    <input id="code" type="text" placeholder="12345" dir="ltr">
    <button class="btn-green" onclick="sendCode()">تأیید کد</button>
  </div>
  <div class="small" id="auth_msg">-</div>
</div>

<div class="box">
  <h3>اسپم جدید</h3>
  <button class="btn-amber" onclick="loadGroups()">دریافت لیست گروه‌ها/کانال‌ها</button>
  <div id="groups" class="groups"></div>

  <label>مقصد (لینک چنل/پیام، @یوزرنیم یا آیدی):</label>
  <input id="target" type="text" placeholder="لینک، @username یا آیدی...">

  <label>متن‌های اسپم:</label>
  <div id="texts">
    <div class="row">
      <textarea class="t-inp" style="flex:1" placeholder="متن ۱..."></textarea>
      <button type="button" class="mini btn-red" onclick="delText(this)">X</button>
    </div>
  </div>
  <button type="button" onclick="addText()">افزودن متن دیگر</button>

  <label>سرعت (ثانیه بین هر ارسال):</label>
  <input id="speed" type="number" value="1" step="0.05" min="0.01">

  <label>تعداد ارسال (0 = نامحدود):</label>
  <input id="max_count" type="number" value="0" step="1" min="0">

  <button class="btn-green" onclick="addJob()">شروع اسپم</button>
  <div class="small" id="add_msg"></div>

  <h3 style="margin-top:14px;">لیست اسپم‌ها</h3>
  <div id="jobs_list"><div class="small">هنوز اسپمی ساخته نشده است.</div></div>
</div>

<script>
let manualCodeBox = false;

function esc(s){ return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }

function addText(){
  const n = document.querySelectorAll('.t-inp').length + 1;
  const d = document.createElement('div');
  d.className = 'row';
  d.innerHTML = `<textarea class="t-inp" style="flex:1" placeholder="متن ${n}..."></textarea><button type="button" class="mini btn-red" onclick="delText(this)">X</button>`;
  document.getElementById('texts').appendChild(d);
}
function delText(btn){
  const rows = document.querySelectorAll('#texts .row');
  if(rows.length <= 1){ rows[0].querySelector('textarea').value=''; return; }
  btn.parentElement.remove();
}
function getTexts(){
  return Array.from(document.querySelectorAll('.t-inp')).map(x=>x.value.trim()).filter(Boolean);
}

async function sendPhone(){
  const p = document.getElementById('phone').value.trim();
  if(!p) return alert('شماره را وارد کن');
  manualCodeBox = true;
  document.getElementById('code_box').style.display = 'block';
  document.getElementById('auth_msg').innerText = 'در حال ارسال کد...';
  await fetch('/api/send_phone?phone=' + encodeURIComponent(p));
  setTimeout(tick, 600);
}
async function sendCode(){
  const c = document.getElementById('code').value.trim();
  if(!c) return alert('کد را وارد کن');
  document.getElementById('auth_msg').innerText = 'در حال تأیید کد...';
  await fetch('/api/verify_code?code=' + encodeURIComponent(c));
  setTimeout(tick, 800);
}

async function loadGroups(){
  document.getElementById('groups').innerHTML = '<span class="small">در حال دریافت...</span>';
  const r = await fetch('/api/groups');
  const arr = await r.json();
  if(!arr.length){ document.getElementById('groups').innerHTML = '<span class="small">موردی یافت نشد</span>'; return; }
  document.getElementById('groups').innerHTML = arr.map(g =>
    `<button type="button" class="gbtn" onclick="document.getElementById('target').value='${String(g.id).replace(/'/g,"\\'")}'">${esc(g.name)}</button>`
  ).join('');
}

async function addJob(){
  const t = document.getElementById('target').value.trim();
  const tx = getTexts();
  const sp = document.getElementById('speed').value;
  const mc = document.getElementById('max_count').value;
  if(!t || !tx.length){ document.getElementById('add_msg').innerText = 'مقصد و حداقل یک متن وارد کن.'; return; }
  const enc = encodeURIComponent(JSON.stringify(tx).replace(/\\n/g,'_NEWLINE_'));
  const r = await fetch(`/api/add_job?target=${encodeURIComponent(t)}&texts=${enc}&speed=${encodeURIComponent(sp)}&max_count=${encodeURIComponent(mc)}`);
  const out = await r.json();
  document.getElementById('add_msg').innerText = out.msg || 'ثبت شد.';
  tick();
}

async function ctrlJob(id, act){
  await fetch(`/api/${act}_job?id=${encodeURIComponent(id)}`);
  tick();
}

function renderJobs(jobs){
  const info = document.getElementById('info_list');
  const list = document.getElementById('jobs_list');
  if(!jobs.length){
    info.innerHTML = '<div class="small">اسپمی ثبت نشده است.</div>';
    list.innerHTML = '<div class="small">هنوز اسپمی ساخته نشده است.</div>';
    return;
  }
  info.innerHTML = jobs.map(j => {
    const lim = j.max_count > 0 ? `${j.sent} / ${j.max_count}` : `${j.sent} / ∞`;
    return `<div class="card">
      <b>مقصد:</b> ${esc(j.target_name)} (${esc(j.target)})<br>
      <b>سرعت:</b> ${j.speed}s | <b>ارسال:</b> ${lim} | <b>آخرین ارسال:</b> ${esc(j.last_time||'-')}<br>
      <b>وضعیت:</b> ${j.running ? 'فعال' : 'متوقف'} (${esc(j.last_status||'')})
      ${j.last_err ? `<br><span style="color:#feb2b2" dir="ltr">${esc(j.last_err)}</span>` : ''}
    </div>`;
  }).join('');

  list.innerHTML = jobs.map(j => `
    <div class="card">
      <b>${esc(j.target_name)}</b> — سرعت: ${j.speed}s — متن‌ها: ${(j.texts||[]).length}<br>
      <div class="row">
        ${j.running
          ? `<button type="button" class="mini btn-amber" onclick="ctrlJob('${j.id}','pause')">توقف</button>`
          : `<button type="button" class="mini btn-green" onclick="ctrlJob('${j.id}','resume')">ادامه</button>`}
        <button type="button" class="mini btn-red" onclick="ctrlJob('${j.id}','del')">حذف</button>
      </div>
    </div>
  `).join('');
}

async function tick(){
  try{
    const r = await fetch('/api/state');
    const d = await r.json();
    document.getElementById('ping').innerText = d.ping;
    document.getElementById('status_txt').innerText = d.status;
    document.getElementById('auth_msg').innerText = d.auth_msg;
    if(d.auth_status === 'connected'){ manualCodeBox = false; }
    const showCode = manualCodeBox || (d.auth_status === 'code_sent');
    document.getElementById('code_box').style.display = showCode ? 'block' : 'none';
    renderJobs(d.jobs || []);
  }catch(e){}
}

setInterval(tick, 1500);
tick();
</script>
</body>
</html>
"""


# ================= سرور وب =================
logging.getLogger("werkzeug").setLevel(logging.ERROR)
app = Flask(__name__)


def json_resp(obj, code=200):
    return app.response_class(
        response=json.dumps(obj, ensure_ascii=False),
        status=code,
        mimetype="application/json; charset=utf-8",
    )


@app.route(PANEL_PATH)
def panel_page():
    return PANEL_HTML


@app.route("/")
def root_page():
    return f"OK - Open {PANEL_PATH}", 200


@app.route("/health")
def health_page():
    return "OK", 200


@app.route("/api/send_phone")
def api_send_phone():
    p = (request.args.get("phone") or "").strip()
    if p:
        state["phone"] = p
        state["auth_status"] = "code_sent"
        state["auth_msg"] = "در حال ارسال کد..."
        run_in_bot_loop(_do_send_code(p))
    return "", 200


@app.route("/api/verify_code")
def api_verify_code():
    c = (request.args.get("code") or "").strip()
    if c:
        run_in_bot_loop(_do_verify_code(c))
    return "", 200


@app.route("/api/groups")
def api_groups():
    return json_resp(group_list)


@app.route("/api/add_job")
def api_add_job():
    global job_seq
    raw_t = (request.args.get("target") or "").strip()
    tgt = parse_target(raw_t)
    txts = parse_texts(request.args.get("texts") or "")
    try:
        spd = max(0.01, float(request.args.get("speed") or "1"))
    except Exception:
        spd = 1.0
    try:
        mc = max(0, int(request.args.get("max_count") or "0"))
    except Exception:
        mc = 0

    if not tgt or not txts:
        return json_resp({"ok": False, "msg": "مقصد یا متن معتبر نیست."})

    job_seq += 1
    job = {
        "id": f"j_{job_seq}_{int(time.time()*1000)%10000}",
        "target": tgt,
        "target_name": resolve_group_name(tgt),
        "texts": txts,
        "idx": 0,
        "speed": spd,
        "max_count": mc,
        "running": True,
        "sent": 0,
        "last_time": "-",
        "last_err": "",
        "last_status": "در حال شروع",
    }
    spam_jobs.append(job)
    save_jobs()
    state["status"] = f"اسپم روی {job['target_name']} شروع شد."
    return json_resp({"ok": True, "msg": "اسپم اضافه شد."})


@app.route("/api/pause_job")
def api_pause_job():
    jid = (request.args.get("id") or "").strip()
    for j in spam_jobs:
        if j["id"] == jid:
            j["running"] = False
            j["last_status"] = "متوقف"
    save_jobs()
    return "", 200


@app.route("/api/resume_job")
def api_resume_job():
    jid = (request.args.get("id") or "").strip()
    for j in spam_jobs:
        if j["id"] == jid:
            if int(j.get("max_count") or 0) > 0 and int(j.get("sent") or 0) >= int(j["max_count"]):
                j["sent"] = 0
            j["running"] = True
            j["last_err"] = ""
            j["last_status"] = "در حال ارسال"
    save_jobs()
    return "", 200


@app.route("/api/del_job")
def api_del_job():
    jid = (request.args.get("id") or "").strip()
    spam_jobs[:] = [j for j in spam_jobs if j["id"] != jid]
    save_jobs()
    return "", 200


@app.route("/api/state")
def api_state():
    return json_resp({
        "ping": state["ping"],
        "status": state["status"],
        "auth_status": state["auth_status"],
        "auth_msg": state["auth_msg"],
        "jobs": spam_jobs,
    })


def run_web():
    app.run(host="0.0.0.0", port=PORT, use_reloader=False, threaded=True)


# ================= موتور ارسال و نگهداری اتصال پایدار =================
async def refresh_groups():
    global group_list
    try:
        if not client_ready:
            return
        dialogs = await asyncio.wait_for(client.get_dialogs(limit=200), timeout=10)
        group_list = [
            {"id": d.id, "name": d.name or str(d.id)}
            for d in dialogs
            if getattr(d, "is_group", False) or getattr(d, "is_channel", False)
        ]
        for j in spam_jobs:
            j["target_name"] = resolve_group_name(j["target"])
    except Exception:
        pass


async def job_worker(job: dict):
    global client_ready
    err_count = 0
    while job in spam_jobs:
        try:
            if not job.get("running"):
                await asyncio.sleep(0.2)
                continue

            mc = int(job.get("max_count") or 0)
            sent = int(job.get("sent") or 0)
            if mc > 0 and sent >= mc:
                job["running"] = False
                job["last_status"] = f"تمام شد ({mc})"
                save_jobs()
                await asyncio.sleep(0.3)
                continue

            if login_in_progress or not client_ready:
                job["last_status"] = "در حال حفظ اتصال..."
                await asyncio.sleep(0.5)
                continue

            t0 = time.perf_counter()
            txts = job.get("texts") or ["تست"]
            i = int(job.get("idx") or 0) % len(txts)
            msg = txts[i]

            t_parsed = parse_target(job["target"])
            peer = int(t_parsed) if t_parsed.lstrip("-").isdigit() else t_parsed

            await asyncio.wait_for(client.send_message(peer, msg), timeout=12)

            # فقط پس از موفقیت ارسال، شمارنده و ایندکس را جلو می‌بریم
            job["idx"] = (i + 1) % len(txts)
            job["sent"] = sent + 1
            job["last_time"] = now_clock()
            job["last_err"] = ""
            job["last_status"] = f"ارسال #{job['sent']}"
            err_count = 0

            if job["sent"] % 10 == 0:
                save_jobs()

            if mc > 0 and job["sent"] >= mc:
                job["running"] = False
                job["last_status"] = f"تمام شد ({mc}/{mc})"
                save_jobs()
                continue

            elapsed = time.perf_counter() - t0
            spd = max(0.01, float(job.get("speed") or 1.0))
            if spd - elapsed > 0:
                await asyncio.sleep(spd - elapsed)

        except Exception as e:
            err_count += 1
            job["last_err"] = str(e)[:120]
            job["last_status"] = f"اختلال کوتاه؛ ادامه ({err_count})"

            # اگر اختلال طولانی‌تر شد، نشست را تازه می‌کنیم بدون اینکه تسک متوقف شود
            if err_count >= 5:
                client_ready = False
                await _ensure_live_session(force_fresh=True)
                err_count = 0

            await asyncio.sleep(0.6)


async def supervisor_loop():
    running_tasks = {}
    while True:
        try:
            active_ids = {j["id"] for j in spam_jobs}
            for jid in list(running_tasks.keys()):
                if jid not in active_ids or running_tasks[jid].done():
                    running_tasks.pop(jid, None)

            for j in list(spam_jobs):
                jid = j["id"]
                if jid not in running_tasks:
                    running_tasks[jid] = asyncio.create_task(job_worker(j))
        except Exception:
            pass
        await asyncio.sleep(0.3)


async def heartbeat_loop():
    while True:
        try:
            if not login_in_progress:
                ok = await _ensure_live_session(force_fresh=False)
                if ok and not group_list:
                    await refresh_groups()
        except Exception:
            pass
        await asyncio.sleep(5)


async def main_loop():
    global bot_loop
    bot_loop = asyncio.get_running_loop()
    asyncio.create_task(supervisor_loop())
    asyncio.create_task(heartbeat_loop())

    # تلاش اولیه برای اتصال سریع با سشن ذخیره‌شده
    await _ensure_live_session(force_fresh=True)
    await refresh_groups()

    while True:
        await asyncio.sleep(1)


def start_asyncio_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    while True:
        try:
            loop.run_until_complete(main_loop())
        except Exception:
            time.sleep(1)


if __name__ == "__main__":
    Thread(target=run_web, daemon=True).start()
    Thread(target=start_asyncio_thread, daemon=True).start()
    while True:
        time.sleep(1)
