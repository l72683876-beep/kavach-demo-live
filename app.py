#!/usr/bin/env python3
"""Aurora Nights Live: a real three-tier demo of Kavach in production shape.

    browser -> this app (Flask) -> Kavach engine (HTTP + HMAC)

The app is written the way a third-party developer would integrate the
engine: it talks to Kavach only through the Python SDK (sdk/python),
holds seats with reserve() while the customer "pays", confirms on
success, releases on failure, and falls back to the waitlist when the
show sells out. Pricing and the (simulated) payment live in THIS layer
on purpose: the engine never sees money.

Run it:
    cd engine && ./kavach -seats 200 -ip-rate 0 \
        -api-keys appkey:aurora-app -hmac-secret appsecret -hold-ttl 60 &
    cd examples/demo-app && python3 app.py &   # http://127.0.0.1:9000
    python3 loadtest_app.py                     # end-to-end load test
"""
import json, os, secrets, sys, threading, time, uuid

WEBHOOK_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webhooks.jsonl")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "sdk", "python"))
from flask import Flask, Response, jsonify, request, send_from_directory, session, redirect, render_template_string
from kavach import Kavach, KavachError

KAVACH_URL = os.environ.get("KAVACH_URL", "http://127.0.0.1:8080")
KAVACH_KEY = os.environ.get("KAVACH_KEY", "appkey")
KAVACH_SECRET = os.environ.get("KAVACH_SECRET", "appsecret")
WEBHOOK_TOKEN = os.environ.get("KAVACH_WEBHOOK_TOKEN", "")

app = Flask(__name__, static_folder="static", static_url_path="")
kv = Kavach(KAVACH_URL, api_key=KAVACH_KEY, hmac_secret=KAVACH_SECRET)

@app.get("/")
def home():
    return send_from_directory("static", "index.html")

_board_cache = {"data": None, "ts": 0.0, "lock": threading.Lock()}

def _refresh_board():
    """Real flash-sale apps serve the seat board from a short-TTL cache:
    a 20k-seat show has thousands of spectators refreshing per second, and
    every one of them hitting the engine would waste it. Kavach still
    decides every hold, pay and waitlist event live; only the read-only
    board is eventually consistent (500 ms)."""
    while True:
        try:
            snap = json.dumps(kv.seats())
            with _board_cache["lock"]:
                _board_cache["data"] = snap
                _board_cache["ts"] = time.time()
        except Exception:
            pass
        try:
            state = json.dumps(kv.seats_state())
            with _board_cache["lock"]:
                _board_cache["state"] = state
        except Exception:
            pass
        time.sleep(0.5)

_board_cache.setdefault("state", None)
threading.Thread(target=_refresh_board, daemon=True).start()

@app.get("/api/seats")
def seats():
    with _board_cache["lock"]:
        data = _board_cache["data"]
    if not data:  # fresh worker whose refresh thread has not ticked yet
        try:
            data = json.dumps(kv.seats())
        except Exception:
            data = '{"total":0,"free":0,"held":0,"sold":0}'
    return Response(data, mimetype="application/json")


@app.get("/api/seats/state")
def seats_state():
    """Live per-seat map for the venue view: '0' free, '1' held, '2' sold."""
    with _board_cache["lock"]:
        data = _board_cache.get("state")
    if not data:
        try:
            data = json.dumps(kv.seats_state())
        except Exception:
            return jsonify({"states": "", "offset": 0}), 503
    return Response(data, mimetype="application/json")

@app.post("/api/hold")
def hold():
    """Reserve seats through Kavach; the app keeps its own order record."""
    b = request.get_json(silent=True) or {}
    try:
        n = int(b.get("seats", 1))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "status": "bad_request"}), 400
    user = b.get("user_id")
    key = b.get("key") or uuid.uuid4().hex
    try:
        r = kv.reserve(user, key, seats=n)
    except KavachError as e:
        return jsonify({"ok": False, "status": e.body.get("status", "error")}), e.status
    if r.get("status") != "held":
        return jsonify({"ok": False, **r}), 200
    return jsonify({"ok": True, "reservation_id": r["reservation_id"], "seats": r["seats"],
                    "hold_expires_at": r["hold_expires_at"], "amount": 499.0 * n, "key": key})

@app.post("/api/pay")
def pay():
    """Simulated payment gateway. Success -> confirm the hold in Kavach.
    Failure or the customer walking away -> release the seats back."""
    b = request.get_json(silent=True) or {}
    rid = b.get("reservation_id")
    if b.get("outcome", "success") != "success":
        try:
            kv.release(rid)
        except KavachError:
            pass  # hold already expired: the engine freed the seats itself
        return jsonify({"ok": False, "status": "payment_failed"}), 200
    try:
        c = kv.confirm(rid, b.get("user_id"))
    except KavachError as e:
        return jsonify({"ok": False, "status": e.body.get("status", "error")}), e.status
    return jsonify({"ok": True, "booking_id": c["booking_id"], "seats": c["seats"]})

@app.post("/api/waitlist")
def waitlist():
    b = request.get_json(silent=True) or {}
    try:
        try:
            n = int(b.get("seats", 1))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "status": "bad_request"}), 400
        w = kv.waitlist_join(b.get("user_id"), b.get("key") or uuid.uuid4().hex, seats=n)
    except KavachError as e:
        return jsonify({"ok": False, "status": e.body.get("status", "error")}), e.status
    return jsonify({"ok": True, "waitlist_id": w.get("waitlist_id"), "position": w.get("position")})

@app.post("/api/kavach-webhook")
def kavach_webhook():
    """Engine-to-app callback layer: Kavach POSTs waitlist_fulfilled events
    here when a released/expired seat promotes a waitlisted fan."""
    if WEBHOOK_TOKEN and request.args.get("token") != WEBHOOK_TOKEN:
        return jsonify({"ok": False, "status": "forbidden"}), 403
    ev = request.get_json(silent=True)
    if not isinstance(ev, dict):
        return jsonify({"ok": False, "status": "bad_json"}), 400
    with open(WEBHOOK_LOG, "a") as f:
        f.write(json.dumps(ev) + "\n")
    return jsonify({"ok": True})

@app.get("/api/metrics")
def metrics():
    return jsonify(kv.metrics())

# ============ Marketplace: profiles + flash deals (Supabase Postgres) ============
import sqlite3 as _sqlite3

from flask import session, redirect, render_template_string
from werkzeug.security import generate_password_hash, check_password_hash

SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", "")
PG = bool(SUPABASE_DB_URL)
SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "")
SB_READY = bool(SUPABASE_URL and SUPABASE_ANON_KEY)
SQLITE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "platform.db")
PH = "%s" if PG else "?"
_db_lock = threading.Lock()
_db_ready = False

_SCHEMA_PG = """
CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL DEFAULT '',
  pw_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS deals (
  id SERIAL PRIMARY KEY,
  seller_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  price_inr DOUBLE PRECISION NOT NULL,
  seats_total INTEGER NOT NULL,
  sold INTEGER NOT NULL DEFAULT 0,
  starts_at TEXT,
  ends_at TEXT,
  created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS bookings (
  id SERIAL PRIMARY KEY,
  deal_id INTEGER NOT NULL,
  buyer_id INTEGER NOT NULL,
  seats INTEGER NOT NULL,
  amount DOUBLE PRECISION NOT NULL,
  payment_status TEXT NOT NULL DEFAULT 'pending',
  razorpay_order_id TEXT,
  created_at TIMESTAMPTZ DEFAULT now());
"""

_SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  email TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL DEFAULT '',
  pw_hash TEXT NOT NULL,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS deals (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  seller_id INTEGER NOT NULL,
  title TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  price_inr REAL NOT NULL,
  seats_total INTEGER NOT NULL,
  sold INTEGER NOT NULL DEFAULT 0,
  starts_at TEXT,
  ends_at TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE IF NOT EXISTS bookings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  deal_id INTEGER NOT NULL,
  buyer_id INTEGER NOT NULL,
  seats INTEGER NOT NULL,
  amount REAL NOT NULL,
  payment_status TEXT NOT NULL DEFAULT 'pending',
  razorpay_order_id TEXT,
  created_at TEXT DEFAULT CURRENT_TIMESTAMP);
"""


def _connect():
    if PG:
        import psycopg2
        return psycopg2.connect(SUPABASE_DB_URL)
    return _sqlite3.connect(SQLITE_PATH)


def _init_db():
    global _db_ready
    with _db_lock:
        if _db_ready:
            return
        conn = _connect()
        cur = conn.cursor()
        schema = _SCHEMA_PG if PG else _SCHEMA_SQLITE
        for stmt in [s.strip() for s in schema.split(";") if s.strip()]:
            cur.execute(stmt)
        conn.commit()
        cur.close()
        conn.close()
        _db_ready = True


def _q(sql, args=(), fetch=False, commit=False):
    _init_db()
    conn = _connect()
    cur = conn.cursor()
    cur.execute(sql, args)
    rows = None
    if fetch:
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    if commit:
        conn.commit()
    cur.close()
    conn.close()
    return rows


def _current_user():
    uid = session.get("uid")
    if not uid:
        return None
    rows = _q("SELECT id, email, name FROM users WHERE id = " + PH, (uid,), fetch=True)
    return rows[0] if rows else None


_BASE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Aurora Deals</title><style>
:root{--c:#22d3ee;--g:#4ade80;--bg:#0b1020;--card:#111832}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:#e5e7eb;
font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif}
nav{display:flex;gap:18px;align-items:center;padding:14px 24px;background:#0e1430;
border-bottom:1px solid #1e2a4d;position:sticky;top:0}
nav a{color:#e5e7eb;text-decoration:none;font-weight:600}
nav a.brand{color:var(--c);font-size:1.2rem}
nav .sp{flex:1}
main{max-width:980px;margin:0 auto;padding:28px 20px}
.card{background:var(--card);border:1px solid #1e2a4d;border-radius:14px;padding:20px;margin-bottom:18px}
h1{color:var(--c);font-size:1.5rem;margin:0 0 14px}
h2{font-size:1.1rem;margin:0 0 10px;color:var(--g)}
input,textarea,button{font:inherit;border-radius:10px;border:1px solid #2a3a66;
background:#0b1020;color:#e5e7eb;padding:10px 12px;width:100%;margin-bottom:12px}
button{background:linear-gradient(90deg,var(--c),var(--g));color:#04121f;font-weight:700;
border:none;cursor:pointer;width:auto;padding:10px 22px}
button:disabled{opacity:.5;cursor:default}
.badge{display:inline-block;background:#0b2a35;color:var(--c);border-radius:8px;
padding:2px 10px;font-size:.8rem;margin-left:8px}
.err{color:#f87171}.ok{color:var(--g)}
.muted{color:#94a3b8;font-size:.9rem}
.price{color:var(--g);font-weight:700}
label{font-size:.85rem;color:#94a3b8;display:block;margin-bottom:4px}
</style></head><body>
<nav><a class="brand" href="/">Aurora</a><a href="/deals">Deals</a>
{% if u %}<a href="/profile">{{ u['name'] or u['email'] }}</a><a href="/logout">Logout</a>
{% else %}<a href="/login">Login</a><a href="/signup">Sign up</a>{% endif %}
<span class="sp"></span><a href="/post-deal">+ Post a deal</a></nav>
<main>{% block body %}{% endblock %}</main></body></html>"""

_T_SIGNUP = _BASE.replace("{% block body %}{% endblock %}", """
<div class="card" style="max-width:420px;margin:40px auto"><h1>Create your profile</h1>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
{% if note %}<p class="ok">{{ note }}</p>{% endif %}
<form method="post"><label>Name</label><input name="name" required>
<label>Email</label><input name="email" type="email" required>
<label>Password (8+ characters)</label><input name="password" type="password" required minlength="8">
<button>Sign up</button></form>
<p class="muted">Already have a profile? <a href="/login" style="color:var(--c)">Login</a></p></div>""")

_T_LOGIN = _BASE.replace("{% block body %}{% endblock %}", """
<div class="card" style="max-width:420px;margin:40px auto"><h1>Login</h1>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
<form method="post"><label>Email</label><input name="email" type="email" required>
<label>Password</label><input name="password" type="password" required>
<button>Login</button></form>
<p class="muted">New here? <a href="/signup" style="color:var(--c)">Create a profile</a></p></div>""")

_T_DEALS = _BASE.replace("{% block body %}{% endblock %}", """
<h1>Live flash deals <span id="live-dot" style="display:none;color:var(--g);font-size:.75rem;font-weight:400">&#9679; LIVE via Supabase Realtime</span></h1>
{% if not deals %}<div class="card">No deals yet. Be the first: post one!</div>{% endif %}
{% for d in deals %}<div class="card" data-deal="{{ d['id'] }}">{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="" style="max-width:100%;max-height:220px;object-fit:cover;border-radius:10px;margin-bottom:10px">{% endif %}<h2><a href="/deal/{{ d['id'] }}" style="color:var(--g);text-decoration:none">{{ d['title'] }}</a></h2>
<p>{{ d['description'][:160] }}</p>
<p>by {{ d['seller'] }} <span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span>
<span class="badge">{{ d['seats_total'] - d['sold'] }} of {{ d['seats_total'] }} left</span></p></div>{% endfor %}
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script>window.SB_URL={{ sb_url|tojson }};window.SB_KEY={{ sb_key|tojson }};</script>
{{ _RT_JS }}
""")

_T_DEAL = _BASE.replace("{% block body %}{% endblock %}", """
<div class="card" data-deal="{{ d['id'] }}"><h1>{{ d['title'] }} <span id="live-dot" style="display:none;color:var(--g);font-size:.75rem;font-weight:400">&#9679; LIVE</span></h1>
{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="" style="max-width:100%;max-height:320px;object-fit:cover;border-radius:10px">{% endif %}
<p>by {{ d['seller'] }} &middot; <span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span> per booking</p>
<p>{{ d['description'] }}</p>
<p><span class="badge">{{ left }} of {{ d['seats_total'] }} left</span>
{% if d['sold'] > 0 %}<span class="badge">{{ d['sold'] }} sold</span>{% endif %}</p>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
{% if ok %}<p class="ok">{{ ok }}</p>{% endif %}
{% if u and left > 0 %}<form method="post" action="/deal/{{ d['id'] }}/book">
<label>Seats / quantity</label><input name="seats" type="number" min="1" max="{{ left }}" value="1" style="max-width:120px">
<button>Book now</button></form>
{% elif not u %}<p><a href="/login" style="color:var(--c)">Login</a> to book.</p>
{% else %}<p class="muted">Sold out.</p>{% endif %}</div>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script>window.SB_URL={{ sb_url|tojson }};window.SB_KEY={{ sb_key|tojson }};</script>
{{ _RT_JS }}
""")

_T_POST = _BASE.replace("{% block body %}{% endblock %}", """
<div class="card"><h1>Post a flash deal</h1>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
<form method="post" enctype="multipart/form-data"><label>Deal title</label><input name="title" required maxlength="120">
<label>Description</label><textarea name="description" rows="4"></textarea>
<label>Price per booking (INR)</label><input name="price" type="number" min="1" step="1" required>
<label>Total quantity / seats</label><input name="seats" type="number" min="1" step="1" required>
<label>Image (optional, PNG/JPG/WebP, max 5 MB)</label><input name="image" type="file" accept="image/*" style="color:#94a3b8">
<label>Sale starts (optional, YYYY-MM-DD HH:MM)</label><input name="starts_at" placeholder="2026-11-01 10:00">
<label>Sale ends (optional)</label><input name="ends_at">
<button>Publish deal</button></form></div>""")

_T_PROFILE = _BASE.replace("{% block body %}{% endblock %}", """
<div class="card"><h1>{{ u['name'] or u['email'] }}</h1><p class="muted">{{ u['email'] }}</p></div>
<div class="card"><h2>Your deals</h2>
{% if not mydeals %}<p class="muted">You have not posted any deals yet.</p>{% endif %}
{% for d in mydeals %}<p><a href="/deal/{{ d['id'] }}" style="color:var(--c)">{{ d['title'] }}</a>
<span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span>
<span class="badge">{{ d['sold'] }}/{{ d['seats_total'] }} sold</span></p>{% endfor %}</div>
<div class="card"><h2>Your bookings</h2>
{% if not mybookings %}<p class="muted">No bookings yet.</p>{% endif %}
{% for b in mybookings %}<p><a href="/deal/{{ b['deal_id'] }}" style="color:var(--c)">{{ b['title'] }}</a>
&middot; {{ b['seats'] }} &times; &#8377;{{ '%.0f'|format(b['amount'] / b['seats']) if b['seats'] else '' }}
<span class="badge">{{ b['payment_status'] }}</span></p>{% endfor %}</div>""")


def _sb_auth(path, payload):
    try:
        r = requests.post(SUPABASE_URL + path, data=json.dumps(payload),
                          headers={"apikey": SUPABASE_ANON_KEY,
                                   "Authorization": "Bearer " + SUPABASE_ANON_KEY,
                                   "Content-Type": "application/json"}, timeout=15)
        try:
            body = r.json()
        except Exception:
            body = {}
        return r.status_code, body
    except Exception as e:
        return 503, {"msg": "auth service unreachable: " + str(e)}


def _sb_sync_user(email, name, sb_uid=None):
    rows = _q("SELECT id FROM users WHERE email = " + PH, (email,), fetch=True)
    if not rows:
        _q("INSERT INTO users (email, name, pw_hash) VALUES (" + ",".join([PH] * 3) + ")",
           (email, name or email.split("@")[0], generate_password_hash(secrets.token_hex(16))), commit=True)
    elif sb_uid:
        _q("UPDATE users SET name = " + PH + " WHERE email = " + PH + " AND (name IS NULL OR name = '')",
           (name, email), commit=True)


def _sb_local_uid(email):
    return _q("SELECT id FROM users WHERE email = " + PH, (email,), fetch=True)[0]["id"]


def _sb_upload_image(f):
    ext = (f.filename.rsplit(".", 1)[-1] or "").lower() if f.filename else ""
    if ext not in ("png", "jpg", "jpeg", "webp", "gif"):
        return None
    data = f.read()
    if len(data) > 5 * 1024 * 1024:
        return None
    mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "webp": "image/webp", "gif": "image/gif"}[ext]
    fn = "deal-%d-%s.%s" % (int(time.time()), secrets.token_hex(4), ext)
    r = requests.post(SUPABASE_URL + "/storage/v1/object/deal-images/" + fn, data=data,
                      headers={"apikey": SUPABASE_ANON_KEY,
                               "Authorization": "Bearer " + SUPABASE_ANON_KEY,
                               "Content-Type": mime}, timeout=30)
    if r.status_code in (200, 201):
        return SUPABASE_URL + "/storage/v1/object/public/deal-images/" + fn
    return None


_RT_JS = """
(function(){
  try{
    var sb = window.supabase.createClient(window.SB_URL, window.SB_KEY);
    sb.channel("deals-live").on("postgres_changes", {event:"*", schema:"public", table:"deals"}, function(p){
      var d = p.new; if(!d || !d.id) return;
      var el = document.querySelector('[data-deal="'+d.id+'"]');
      if(el){
        var b = el.querySelector(".badge");
        if(b){ b.textContent = (d.seats_total - d.sold) + " of " + d.seats_total + " left";
               b.style.background = "rgba(74,222,128,.35)"; setTimeout(function(){ b.style.background = ""; }, 1200); }
        var inp = el.querySelector("input[name=seats]"); if(inp && d.seats_total - d.sold > 0) inp.max = d.seats_total - d.sold;
        var fm = el.querySelector("form[action*='/book']"); if(d.sold >= d.seats_total && fm){ fm.outerHTML = '<p class="muted">Sold out.</p>'; }
      } else { location.reload(); }
    }).subscribe(function(status){
      if(status === "SUBSCRIBED"){ var l = document.getElementById("live-dot"); if(l) l.style.display = "inline-block"; }
    });
  }catch(e){}
})();
</script>"""

@app.route("/signup", methods=["GET", "POST"])
def signup():
    err = note = None
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        name = (request.form.get("name") or "").strip()[:60]
        pw = request.form.get("password") or ""
        if not email or "@" not in email or len(pw) < 8:
            err = "Valid email and an 8+ character password, please."
        elif not SB_READY:
            err = "Auth service is not configured yet. Please try again later."
        else:
            s, body = _sb_auth("/auth/v1/signup", {"email": email, "password": pw, "data": {"name": name}})
            if s == 200 and (body.get("access_token") or body.get("session")):
                _sb_sync_user(email, name, (body.get("user") or {}).get("id"))
                session["uid"] = _sb_local_uid(email)
                return redirect("/deals")
            elif s == 200:
                note = ("Almost done: Supabase sent a confirmation email to " + email +
                        ". Click the link inside it, then login.")
            else:
                msg = str(body.get("msg") or body.get("error_description") or body.get("error") or "Signup failed.")
                low = msg.lower()
                if "already" in low:
                    err = "This email already has a profile."
                elif "rate limit" in low:
                    err = "The email service is briefly rate-limited (free tier). Please try again in a few minutes."
                else:
                    err = "Signup failed: " + msg
    return render_template_string(_T_SIGNUP, err=err, note=note, u=_current_user())


@app.route("/login", methods=["GET", "POST"])
def login():
    err = None
    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        pw = request.form.get("password") or ""
        if not SB_READY:
            err = "Auth service is not configured yet."
        else:
            s, body = _sb_auth("/auth/v1/token?grant_type=password", {"email": email, "password": pw})
            if s == 200:
                meta = (body.get("user") or {}).get("user_metadata") or {}
                _sb_sync_user(email, meta.get("name") or "", (body.get("user") or {}).get("id"))
                session["uid"] = _sb_local_uid(email)
                return redirect("/deals")
            msg = str(body.get("msg") or body.get("error_description") or body.get("error") or "")
            low = msg.lower()
            if "not confirmed" in low:
                err = "Email not confirmed yet: click the confirmation link in your inbox, then login."
            elif "rate limit" in low:
                err = "The email service is briefly rate-limited (free tier). Please try again in a few minutes."
            else:
                err = "Email or password did not match."
    return render_template_string(_T_LOGIN, err=err, u=_current_user())


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/deals")


@app.route("/post-deal", methods=["GET", "POST"])
def post_deal():
    u = _current_user()
    if not u:
        return redirect("/login")
    err = None
    if request.method == "POST":
        try:
            price = float(request.form.get("price") or 0)
            seats = int(request.form.get("seats") or 0)
        except ValueError:
            price = seats = 0
        title = (request.form.get("title") or "").strip()[:120]
        if not title or price <= 0 or seats <= 0:
            err = "Title, price and quantity are needed."
        else:
            img = None
            f = request.files.get("image")
            if f and f.filename and SB_READY:
                img = _sb_upload_image(f)
            _q("INSERT INTO deals (seller_id, title, description, price_inr, seats_total, starts_at, ends_at, image_url)"
               " VALUES (" + ",".join([PH] * 8) + ")",
               (u["id"], title, (request.form.get("description") or "").strip()[:2000],
                price, seats, request.form.get("starts_at") or None,
                request.form.get("ends_at") or None, img), commit=True)
            return redirect("/deals")
    return render_template_string(_T_POST, err=err, u=u)


@app.route("/deals")
def deals_list():
    rows = _q("SELECT d.id, d.title, d.description, d.price_inr, d.seats_total, d.sold, d.image_url, u.name AS seller"
              " FROM deals d JOIN users u ON u.id = d.seller_id"
              " ORDER BY d.created_at DESC LIMIT 50", fetch=True)
    return render_template_string(_T_DEALS, deals=rows or [], u=_current_user(),
                                  sb_url=SUPABASE_URL, sb_key=SUPABASE_ANON_KEY, _RT_JS=_RT_JS)


@app.route("/deal/<int:did>", methods=["GET", "POST"])
def deal_page(did):
    u = _current_user()
    err = ok = None
    if request.method == "POST":
        if not u:
            return redirect("/login")
        try:
            n = max(1, int(request.form.get("seats") or 1))
        except ValueError:
            n = 1
        rows = _q("SELECT * FROM deals WHERE id = " + PH, (did,), fetch=True)
        if not rows:
            return redirect("/deals")
        d = rows[0]
        _q("UPDATE deals SET sold = sold + {0} WHERE id = {0} AND sold + {0} <= seats_total".format(PH),
           (n, did, n), commit=True)
        after = _q("SELECT sold FROM deals WHERE id = " + PH, (did,), fetch=True)[0]["sold"]
        if after > d["sold"]:
            amount = round(d["price_inr"] * n, 2)
            order_id = None
            if RAZORPAY_KEY_ID:
                order_id = _razorpay_order(amount)
            _q("INSERT INTO bookings (deal_id, buyer_id, seats, amount, payment_status, razorpay_order_id)"
               " VALUES (" + ",".join([PH] * 6) + ")",
               (did, u["id"], n, amount, "pending" if order_id else "paid", order_id), commit=True)
            ok = ("Booked! Booking pending payment." if order_id
                  else "Booked! Booking confirmed (demo payment).")
        else:
            err = "Not enough seats left."
    rows = _q("SELECT d.*, u.name AS seller FROM deals d JOIN users u ON u.id = d.seller_id"
              " WHERE d.id = " + PH, (did,), fetch=True)
    if not rows:
        return redirect("/deals")
    d = rows[0]
    return render_template_string(_T_DEAL, d=d, left=d["seats_total"] - d["sold"],
                                  u=u, err=err, ok=ok, sb_url=SUPABASE_URL, sb_key=SUPABASE_ANON_KEY, _RT_JS=_RT_JS)


@app.route("/profile")
def profile():
    u = _current_user()
    if not u:
        return redirect("/login")
    mydeals = _q("SELECT id, title, price_inr, sold, seats_total FROM deals WHERE seller_id = " + PH +
                 " ORDER BY created_at DESC", (u["id"],), fetch=True)
    mybookings = _q("SELECT b.*, d.title FROM bookings b JOIN deals d ON d.id = b.deal_id"
                    " WHERE b.buyer_id = " + PH + " ORDER BY b.created_at DESC", (u["id"],), fetch=True)
    return render_template_string(_T_PROFILE, u=u, mydeals=mydeals or [], mybookings=mybookings or [])


import hashlib, hmac, requests

RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET", "")
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "aurora-dev-secret-2026")

def _razorpay_order(amount):
    r = requests.post("https://api.razorpay.com/v1/orders",
                      auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
                      json={"amount": int(amount * 100), "currency": "INR"},
                      timeout=10)
    r.raise_for_status()
    return r.json()["id"]

@app.post("/api/deal/<int:did>/verify")
def deal_verify(did):
    b = request.get_json(silent=True) or {}
    order_id = b.get("razorpay_order_id")
    payment_id = b.get("razorpay_payment_id")
    sig = b.get("razorpay_signature")
    if not (order_id and payment_id and sig and RAZORPAY_KEY_SECRET):
        return jsonify({"ok": False, "status": "bad_request"}), 400
    expected = hmac.new(RAZORPAY_KEY_SECRET.encode(),
                        (order_id + "|" + payment_id).encode(), hashlib.sha256).hexdigest()
    if expected != sig:
        return jsonify({"ok": False, "status": "bad_signature"}), 400
    _q("UPDATE bookings SET payment_status = 'paid' WHERE razorpay_order_id = " + PH,
       (order_id,), commit=True)
    return jsonify({"ok": True})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "9000")), threaded=True)
