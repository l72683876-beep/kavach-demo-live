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
from markupsafe import Markup
from kavach import Kavach, KavachError

KAVACH_URL = os.environ.get("KAVACH_URL", "http://127.0.0.1:8080")
KAVACH_KEY = os.environ.get("KAVACH_KEY", "appkey")
KAVACH_SECRET = os.environ.get("KAVACH_SECRET", "appsecret")
WEBHOOK_TOKEN = os.environ.get("KAVACH_WEBHOOK_TOKEN", "")

app = Flask(__name__, static_folder="static", static_url_path="")
kv = Kavach(KAVACH_URL, api_key=KAVACH_KEY, hmac_secret=KAVACH_SECRET)

@app.get("/")
def home():
    stats = _q("SELECT COUNT(*) AS n, COALESCE(SUM(sold),0) AS sold,"
               " COUNT(DISTINCT seller_id) AS nsellers FROM deals", fetch=True)[0]
    featured = _q("SELECT d.id, d.title, d.description, d.price_inr, d.seats_total, d.sold,"
                  " d.image_url, u.name AS seller"
                  " FROM deals d JOIN users u ON u.id = d.seller_id"
                  " ORDER BY d.created_at DESC LIMIT 3", fetch=True)
    return render_template_string(_T_HOME, u=_current_user(), ndeals=stats["n"],
                                  seats_sold=stats["sold"], nsellers=stats["nsellers"],
                                  deals=featured or [])


@app.get("/event")
def event_page():
    # Public: visitors can browse the live 3D venue; holds/bookings still need an account.
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
    rows = _q("SELECT id, email, name, created_at FROM users WHERE id = " + PH, (uid,), fetch=True)
    return rows[0] if rows else None


def _safe_next(default="/deals"):
    nx = request.values.get("next") or ""
    if nx.startswith("/") and not nx.startswith("//"):
        return nx
    return default


def _need_login():
    from urllib.parse import quote
    target = request.full_path if request.query_string else request.path
    if target.endswith("?"):
        target = target[:-1]
    return redirect("/login?next=" + quote(target, safe=""))


_BASE = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{% block title %}Aurora{% endblock %} &middot; Kavach</title>
<style>
:root{--c:#22d3ee;--g:#4ade80;--bg:#0b1020;--card:#111832;--line:#1e2a4d;--muted:#94a3b8}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:#e5e7eb;font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif}
a{color:var(--c);text-decoration:none}
nav{display:flex;gap:20px;align-items:center;padding:12px 22px;background:rgba(14,20,48,.92);backdrop-filter:blur(8px);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:50;flex-wrap:wrap}
nav a{color:#e5e7eb;font-weight:600;font-size:.95rem}
nav a:hover{color:var(--c)}
nav a.brand{color:var(--c);font-size:1.25rem;display:flex;align-items:center;gap:8px}
.brand-dot{width:10px;height:10px;border-radius:50%;background:var(--g);box-shadow:0 0 12px var(--g);display:inline-block}
nav .sp{flex:1}
.btn{display:inline-block;background:linear-gradient(90deg,var(--c),var(--g));color:#04121f;font-weight:700;border:none;border-radius:10px;padding:9px 20px;cursor:pointer;font-size:.95rem;text-decoration:none}
.btn.ghost{background:transparent;color:var(--c);border:1px solid #2a3a66}
.btn.small{padding:7px 16px;font-size:.88rem}
main{max-width:1080px;margin:0 auto;padding:28px 20px}
footer{border-top:1px solid var(--line);color:var(--muted);text-align:center;padding:22px;font-size:.85rem;margin-top:48px}
footer b{color:var(--c)}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:22px;margin-bottom:18px}
h1{color:#fff;font-size:1.7rem;margin:0 0 14px}
h2{font-size:1.15rem;margin:0 0 10px;color:var(--g)}
input,textarea,button{font:inherit;border-radius:10px;border:1px solid #2a3a66;background:#0b1020;color:#e5e7eb;padding:10px 12px;width:100%;margin-bottom:12px}
button{background:linear-gradient(90deg,var(--c),var(--g));color:#04121f;font-weight:700;border:none;cursor:pointer;width:auto;padding:10px 22px}
button:disabled{opacity:.5;cursor:default}
.badge{display:inline-block;background:#0b2a35;color:var(--c);border-radius:8px;padding:2px 10px;font-size:.78rem;margin-left:6px}
.err{color:#f87171}.ok{color:var(--g)}
.muted{color:var(--muted);font-size:.9rem}
.price{color:var(--g);font-weight:700}
label{font-size:.85rem;color:var(--muted);display:block;margin-bottom:4px}
.hero{padding:60px 12px 40px;text-align:center}
.hero h1{font-size:2.2rem;line-height:1.25;margin:12px 0 14px}
.hero .sub{color:var(--muted);max-width:640px;margin:0 auto 26px;font-size:1.05rem}
.kicker{color:var(--c);letter-spacing:.14em;text-transform:uppercase;font-size:.78rem;font-weight:700;margin:0}
.cta{display:flex;gap:14px;justify-content:center;flex-wrap:wrap}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin:6px 0 32px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:18px;text-align:center}
.stat b{display:block;color:var(--c);font-size:1.55rem}
.stat span{color:var(--muted);font-size:.85rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(250px,1fr));gap:16px}
.deal{background:var(--card);border:1px solid var(--line);border-radius:14px;overflow:hidden;display:flex;flex-direction:column}
.deal img{width:100%;height:150px;object-fit:cover;display:block}
.deal .pad{padding:14px 16px 16px;display:flex;flex-direction:column;gap:8px;flex:1}
.deal h3{margin:0;font-size:1.02rem}
.deal h3 a{color:#fff}
.deal h3 a:hover{color:var(--c)}
.deal .meta{color:var(--muted);font-size:.85rem}
.deal .grow{flex:1}
.bar{height:6px;background:#0b1020;border-radius:3px;overflow:hidden}
.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--c),var(--g))}
.steps{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}
.step b{color:var(--c);display:block;margin-bottom:6px}
.avatar{width:52px;height:52px;border-radius:50%;background:linear-gradient(135deg,var(--c),var(--g));color:#04121f;font-weight:800;font-size:1.4rem;display:flex;align-items:center;justify-content:center;flex-shrink:0}
.row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.auth{max-width:400px;margin:56px auto}
.auth .brand{text-align:center;color:var(--c);font-size:1.4rem;font-weight:800;margin-bottom:14px}
.search{display:flex;gap:10px;margin-bottom:20px}
.backlink{display:inline-block;margin-bottom:14px;color:var(--muted);font-weight:600;font-size:.9rem}
.backlink:hover{color:var(--c)}
.search input{margin-bottom:0}
@media(max-width:640px){.hero h1{font-size:1.55rem}main{padding:20px 14px}}
</style>
</head>
<body>
<nav>
<a class="brand" href="/"><i class="brand-dot"></i>Aurora</a>
<a href="/deals">Deals</a>
<a href="/event">Live Event</a>
<a href="/sell">Sell</a>
<span class="sp"></span>
{% if u %}
<a href="/profile">{{ u['name'] or u['email'] }}</a>
<a class="btn ghost small" href="/logout">Logout</a>
{% else %}
<a class="btn ghost small" href="/login">Sign in</a>
<a class="btn small" href="/signup">Sign up</a>
{% endif %}
</nav>
<main>{% block body %}{% endblock %}</main>
<footer>Powered by <b>Kavach</b> &middot; flash-sale engine with zero double-sells</footer>
</body>
</html>
"""

_T_HOME = _BASE.replace("{% block body %}{% endblock %}", """
<section class="hero">
<p class="kicker">Flash-sale marketplace, live on Kavach</p>
<h1>Deals that sell out in seconds.<br>Inventory that never oversells.</h1>
<p class="sub">Every booking on Aurora is held, confirmed or released through the Kavach engine in real time. Sold means sold: no double-bookings, ever.</p>
<div class="cta"><a class="btn" href="/deals">Explore deals</a><a class="btn ghost" href="/event">Book live event seats</a></div>
</section>
<div class="stats">
<div class="stat"><b>{{ ndeals }}</b><span>live deals</span></div>
<div class="stat"><b>{{ seats_sold }}</b><span>seats sold</span></div>
<div class="stat"><b>{{ nsellers }}</b><span>sellers</span></div>
</div>
{% if deals %}
<h2>Fresh drops</h2>
<div class="grid">
{% for d in deals %}
<div class="deal">{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="">{% endif %}
<div class="pad"><h3><a href="/deal/{{ d['id'] }}">{{ d['title'] }}</a></h3>
<div class="meta">by {{ d['seller'] }}</div>
<div class="row"><span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span><span class="badge">{{ d['seats_total'] - d['sold'] }} of {{ d['seats_total'] }} left</span></div></div></div>
{% endfor %}
</div>
<p style="margin:18px 0 28px"><a href="/deals">See all deals &rarr;</a></p>
{% else %}
<div class="card"><p class="muted" style="margin:0">No deals on the board yet. Be the first: <a href="/post-deal">post one</a>.</p></div>
{% endif %}
<div class="card">
<h2>How it works</h2>
<div class="steps">
<div class="step"><b>1 &middot; Post</b>Sellers list a flash deal with a fixed quantity of seats or units.</div>
<div class="step"><b>2 &middot; Hold</b>When you book, Kavach holds your seat atomically while payment completes.</div>
<div class="step"><b>3 &middot; Confirm or release</b>Success confirms the seat; failure releases it instantly for the next buyer.</div>
</div>
</div>
""")

_T_SIGNUP = _BASE.replace("{% block body %}{% endblock %}", """
<div class="auth"><div class="card"><div class="brand">Aurora</div><h1>Create your account</h1>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
{% if note %}<p class="ok">{{ note }}</p>{% endif %}
<form method="post"><input type="hidden" name="next" value="{{ nx }}"><label>Name</label><input name="name" required>
<label>Email</label><input name="email" type="email" required>
<label>Password (8+ characters)</label><input name="password" type="password" required minlength="8">
<button>Sign up</button></form>
<div style="display:flex;gap:10px;margin-top:12px">
<a class="btn ghost small" href="/auth/google?next={{ nx }}" style="flex:1;text-align:center">Continue with Google</a>
<a class="btn ghost small" href="/auth/github?next={{ nx }}" style="flex:1;text-align:center">Continue with GitHub</a>
</div>
<p class="muted">Already have an account? <a href="/login">Sign in</a></p></div></div>
""").replace("<title>{% block title %}Aurora{% endblock %}", "<title>Sign up &middot; Aurora")

_T_LOGIN = _BASE.replace("{% block body %}{% endblock %}", """
<div class="auth"><div class="card"><div class="brand">Aurora</div><h1>Welcome back</h1>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
<form method="post"><input type="hidden" name="next" value="{{ nx }}"><label>Email</label><input name="email" type="email" required>
<label>Password</label><input name="password" type="password" required>
<button>Sign in</button></form>
<div style="display:flex;gap:10px;margin-top:12px">
<a class="btn ghost small" href="/auth/google?next={{ nx }}" style="flex:1;text-align:center">Continue with Google</a>
<a class="btn ghost small" href="/auth/github?next={{ nx }}" style="flex:1;text-align:center">Continue with GitHub</a>
</div>
<p class="muted">New here? <a href="/signup">Create an account</a></p></div></div>
""")

_T_DEALS = _BASE.replace("{% block body %}{% endblock %}", """
<h1>Live flash deals <span id="live-dot" style="display:none;color:var(--g);font-size:.75rem;font-weight:400">&#9679; LIVE</span></h1>
<form class="search" method="get" action="/deals"><input name="q" value="{{ q }}" placeholder="Search deals&hellip;"><button class="btn">Search</button></form>
{% if not deals %}<div class="card"><p class="muted" style="margin:0">No deals{{ ' match your search' if q else ' yet' }}. <a href="/post-deal">Post one</a>.</p></div>{% endif %}
<div class="grid">
{% for d in deals %}
<div class="deal" data-deal="{{ d['id'] }}">{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="">{% endif %}
<div class="pad"><h3><a href="/deal/{{ d['id'] }}">{{ d['title'] }}</a></h3>
<div class="meta">by {{ d['seller'] }}</div>
<div class="grow"><p style="margin:0" class="muted">{{ d['description'][:110] }}</p></div>
<div class="row"><span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span><span class="badge">{{ d['seats_total'] - d['sold'] }} of {{ d['seats_total'] }} left</span></div>
<div class="bar"><i style="width:{{ ((100 * d['sold'] / d['seats_total'])|round)|int }}%"></i></div></div></div>
{% endfor %}
</div>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script>window.SB_URL={{ sb_url|tojson }};window.SB_KEY={{ sb_key|tojson }};</script>
{{ _RT_JS }}
""")

_T_DEAL = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="#" onclick="history.back();return false">&larr; Back</a>
<div class="card deal-page" data-deal="{{ d['id'] }}" style="max-width:680px;margin:0 auto">
<h1>{{ d['title'] }} <span id="live-dot" style="display:none;color:var(--g);font-size:.75rem;font-weight:400">&#9679; LIVE</span></h1>
{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="" style="max-width:100%;max-height:320px;object-fit:cover;border-radius:10px;margin-bottom:14px">{% endif %}
<p>by {{ d['seller'] }} &middot; <span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span> per booking</p>
<p>{{ d['description'] }}</p>
<p><span class="badge">{{ left }} of {{ d['seats_total'] }} left</span>
{% if d['sold'] > 0 %}<span class="badge">{{ d['sold'] }} sold</span>{% endif %}</p>
<div class="bar" style="margin-bottom:16px"><i style="width:{{ ((100 * d['sold'] / d['seats_total'])|round)|int }}%"></i></div>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
{% if ok %}<p class="ok">{{ ok }}</p>{% endif %}
{% if u and left > 0 %}<form method="post" action="/deal/{{ d['id'] }}/book">
<label>Seats / quantity</label><input name="seats" type="number" min="1" max="{{ left }}" value="1" style="max-width:140px">
<button>Book now</button></form>
{% elif not u %}<p><a href="/login">Sign in</a> to book.</p>
{% else %}<p class="muted">Sold out.</p>{% endif %}
</div>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script>window.SB_URL={{ sb_url|tojson }};window.SB_KEY={{ sb_key|tojson }};</script>
{{ _RT_JS }}
""").replace("<title>{% block title %}{{ d['title'] }}{% endblock %}", "<title>{{ d['title'] }} &middot; Aurora")

_T_POST = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="#" onclick="history.back();return false">&larr; Back</a>
<div class="card" style="max-width:560px;margin:0 auto"><h1>Post a flash deal</h1>
<p class="muted">Set a fixed quantity. Kavach guarantees it never oversells.</p>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
<form method="post" enctype="multipart/form-data"><label>Deal title</label><input name="title" required maxlength="120">
<label>Description</label><textarea name="description" rows="4"></textarea>
<label>Price per booking (INR)</label><input name="price" type="number" min="1" step="1" required>
<label>Total quantity / seats</label><input name="seats" type="number" min="1" step="1" required>
<label>Image (optional, PNG/JPG/WebP, max 5 MB)</label><input name="image" type="file" accept="image/*" style="color:#94a3b8">
<label>Sale starts (optional, YYYY-MM-DD HH:MM)</label><input name="starts_at" placeholder="2026-11-01 10:00">
<label>Sale ends (optional)</label><input name="ends_at">
<button>Publish deal</button></form></div>
""")

_T_SELL = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="#" onclick="history.back();return false">&larr; Back</a>
<h1>Seller dashboard</h1>
<div class="stats">
<div class="stat"><b>{{ n_active }}</b><span>active listings</span></div>
<div class="stat"><b>{{ n_sold }}</b><span>units sold</span></div>
<div class="stat"><b>&#8377;{{ '%.0f'|format(revenue) }}</b><span>revenue from sold units</span></div>
</div>
<p><a class="btn" href="/post-deal">+ Post a new deal</a></p>
<h2>Your listings</h2>
{% if not mydeals %}<div class="card"><p class="muted" style="margin:0">No listings yet. Post your first deal.</p></div>{% endif %}
{% for d in mydeals %}
<div class="card"><div class="row">
<div class="grow" style="flex:1">
<h3 style="margin:0 0 6px"><a href="/deal/{{ d['id'] }}" style="color:#fff">{{ d['title'] }}</a></h3>
<div class="muted">&#8377;{{ '%.0f'|format(d['price_inr']) }} per booking &middot; {{ d['sold'] }}/{{ d['seats_total'] }} sold</div>
<div class="bar" style="max-width:320px;margin-top:8px"><i style="width:{{ ((100 * d['sold'] / d['seats_total'])|round)|int }}%"></i></div>
</div>
{% if d['sold'] < d['seats_total'] %}<span class="badge">active</span>{% else %}<span class="badge" style="background:#2a1b35;color:#f87171">sold out</span>{% endif %}
</div></div>
{% endfor %}
""")

_T_PROFILE = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="#" onclick="history.back();return false">&larr; Back</a>
<div class="card"><div class="row">
<div class="avatar">{{ (u['name'] or u['email'])[:1]|upper }}</div>
<div><h1 style="margin:0 0 4px;font-size:1.35rem">{{ u['name'] or u['email'] }}</h1>
<div class="muted">{{ u['email'] }}{% if joined %} &middot; member since {{ joined }}{% endif %}</div></div></div></div>
<div class="stats" style="margin-top:0">
<div class="stat"><b>{{ mydeals|length }}</b><span>deals posted</span></div>
<div class="stat"><b>{{ mybookings|length }}</b><span>bookings made</span></div>
</div>
<div class="card"><h2>Account settings</h2>
<form method="post" action="/profile"><label>Display name</label>
<input name="name" value="{{ u['name'] or '' }}" maxlength="60" required>
<button>Save changes</button></form></div>
<div class="card"><h2>Your deals</h2>
{% if not mydeals %}<p class="muted">You have not posted any deals yet. <a href="/post-deal">Post one</a>.</p>{% endif %}
{% for d in mydeals %}<div class="row" style="margin-bottom:10px">
<div class="grow" style="flex:1"><a href="/deal/{{ d['id'] }}" style="color:#fff">{{ d['title'] }}</a>
<div class="muted">&#8377;{{ '%.0f'|format(d['price_inr']) }} &middot; {{ d['sold'] }}/{{ d['seats_total'] }} sold</div></div>
<a class="btn ghost small" href="/sell">Manage</a></div>{% endfor %}</div>
<div class="card"><h2>Your bookings</h2>
{% if not mybookings %}<p class="muted">No bookings yet.</p>{% endif %}
{% for b in mybookings %}<div class="row" style="margin-bottom:10px">
<div class="grow" style="flex:1"><a href="/deal/{{ b['deal_id'] }}" style="color:#fff">{{ b['title'] }}</a>
<div class="muted">{{ b['seats'] }} &times; booking</div></div>
<span class="badge" {% if b['payment_status'] == 'paid' %}style="background:#0b2a20;color:var(--g)"{% endif %}>{{ b['payment_status'] }}</span></div>{% endfor %}</div>
""")


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


_RT_JS = Markup("""
<script>
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
</script>""")

@app.route("/signup", methods=["GET", "POST"])
def signup():
    err = note = None
    nx = request.values.get("next") or ""
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
                return redirect(_safe_next())
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
    return render_template_string(_T_SIGNUP, err=err, note=note, u=_current_user(), nx=nx)


@app.route("/login", methods=["GET", "POST"])
def login():
    err = None
    nx = request.values.get("next") or ""
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
                return redirect(_safe_next())
            msg = str(body.get("msg") or body.get("error_description") or body.get("error") or "")
            low = msg.lower()
            if "not confirmed" in low:
                err = "Email not confirmed yet: click the confirmation link in your inbox, then login."
            elif "rate limit" in low:
                err = "The email service is briefly rate-limited (free tier). Please try again in a few minutes."
            else:
                err = "Email or password did not match."
    return render_template_string(_T_LOGIN, err=err, u=_current_user(), nx=nx)


@app.route("/auth/<provider>")
def oauth_start(provider):
    if provider not in ("google", "github") or not SB_READY:
        return redirect("/login")
    base = request.host_url.rstrip("/")
    if "onrender.com" in request.host:
        base = base.replace("http://", "https://", 1)
    from urllib.parse import quote
    session["oauth_state"] = secrets.token_urlsafe(16)
    session["oauth_next"] = _safe_next()
    q = ("provider=" + provider + "&redirect_to=" + quote(base + "/auth/callback", safe="")
         + "&state=" + quote(session["oauth_state"], safe=""))
    return redirect(SUPABASE_URL + "/auth/v1/authorize?" + q)


@app.route("/auth/callback")
def oauth_callback():
    err = request.values.get("error_description") or request.values.get("error") or ""
    return render_template_string(_T_OAUTH_CB, cb_err=err, u=_current_user(),
                                  sb_url=SUPABASE_URL, sb_key=SUPABASE_ANON_KEY)


_T_OAUTH_CB = _BASE.replace("{% block body %}{% endblock %}", """
<div class="auth"><div class="card"><div class="brand">Aurora</div><h1>Finishing sign-in&hellip;</h1>
<p id="m" class="muted">Verifying your Google/GitHub account, one moment.</p>
<p><a href="/login">Back to sign in</a></p></div></div>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script>window.SB_URL={{ sb_url|tojson }};window.SB_KEY={{ sb_key|tojson }};
(async function(){
  var msg = document.getElementById("m");
  var err = {{ (cb_err or '')|tojson }};
  try {
    var sb = window.supabase.createClient(window.SB_URL, window.SB_KEY);
    var sess = null;
    for (var i = 0; i < 10 && !sess; i++) {
      var g = await sb.auth.getSession();
      sess = g && g.data && g.data.session;
      if (!sess) await new Promise(function(r){ setTimeout(r, 500); });
    }
    if (!sess) {
      msg.textContent = err ? ("Sign-in did not complete: " + err + " The provider may not be switched on yet.")
                            : "Sign-in did not complete. Please try again.";
      return;
    }
    var res = await fetch("/auth/finish", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ access_token: sess.access_token }) });
    var j = await res.json();
    if (j.ok) { location.replace(j.redirect || "/deals"); }
    else { msg.textContent = "Sign-in failed: " + (j.error || "unknown error"); }
  } catch (e) { msg.textContent = "Sign-in error: " + e; }
})();
</script>
""")


@app.route("/auth/finish", methods=["POST"])
def oauth_finish():
    if not SB_READY:
        return {"ok": False, "error": "auth not configured"}, 503
    tok = (request.get_json(silent=True) or {}).get("access_token") or ""
    if not tok:
        return {"ok": False, "error": "missing token"}, 400
    try:
        r = requests.get(SUPABASE_URL + "/auth/v1/user",
                         headers={"apikey": SUPABASE_ANON_KEY, "Authorization": "Bearer " + tok}, timeout=15)
        body = r.json() if r.status_code == 200 else {}
    except Exception:
        body = {}
        r = None
    email = (body.get("email") or "").strip().lower()
    if not r or r.status_code != 200 or not email:
        return {"ok": False, "error": "could not verify the sign-in"}, 401
    meta = body.get("user_metadata") or {}
    name = meta.get("full_name") or meta.get("name") or ""
    if not name:
        for it in (body.get("identities") or []):
            idd = (it or {}).get("identity_data") or {}
            if idd.get("name"):
                name = idd.get("name")
                break
    _sb_sync_user(email, name, body.get("id"))
    session["uid"] = _sb_local_uid(email)
    session.pop("oauth_state", None)
    return {"ok": True, "redirect": session.pop("oauth_next", "/deals")}


@app.route("/sell")
def sell_dash():
    u = _current_user()
    if not u:
        return _need_login()
    mydeals = _q("SELECT id, title, price_inr, sold, seats_total FROM deals WHERE seller_id = " + PH +
                 " ORDER BY created_at DESC", (u["id"],), fetch=True) or []
    n_active = sum(1 for d in mydeals if d["sold"] < d["seats_total"])
    n_sold = sum(d["sold"] for d in mydeals)
    revenue = sum(d["sold"] * d["price_inr"] for d in mydeals)
    return render_template_string(_T_SELL, u=u, mydeals=mydeals,
                                  n_active=n_active, n_sold=n_sold, revenue=revenue)


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/deals")


@app.route("/post-deal", methods=["GET", "POST"])
def post_deal():
    u = _current_user()
    if not u:
        return _need_login()
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
            return redirect("/sell")
    return render_template_string(_T_POST, err=err, u=u)


@app.route("/deals")
def deals_list():
    # Public: anyone can browse deals; booking still needs an account.
    q = (request.args.get("q") or "").strip().lower()
    rows = _q("SELECT d.id, d.title, d.description, d.price_inr, d.seats_total, d.sold, d.image_url, u.name AS seller"
              " FROM deals d JOIN users u ON u.id = d.seller_id"
              " ORDER BY d.created_at DESC LIMIT 50", fetch=True)
    if q and rows:
        rows = [r for r in rows
                if q in (r["title"] or "").lower() or q in (r["description"] or "").lower()]
    return render_template_string(_T_DEALS, deals=rows or [], q=q, u=_current_user(),
                                  sb_url=SUPABASE_URL, sb_key=SUPABASE_ANON_KEY, _RT_JS=_RT_JS)


@app.route("/deal/<int:did>", methods=["GET", "POST"])
def deal_page(did):
    u = _current_user()
    err = ok = None
    if request.method == "POST":
        if not u:
            return _need_login()
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


@app.route("/profile", methods=["GET", "POST"])
def profile():
    u = _current_user()
    if not u:
        return _need_login()
    saved = None
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()[:60]
        if name:
            _q("UPDATE users SET name = " + PH + " WHERE id = " + PH, (name, u["id"]), commit=True)
            u = _current_user()
            saved = "Profile updated."
    mydeals = _q("SELECT id, title, price_inr, sold, seats_total FROM deals WHERE seller_id = " + PH +
                 " ORDER BY created_at DESC", (u["id"],), fetch=True)
    mybookings = _q("SELECT b.*, d.title FROM bookings b JOIN deals d ON d.id = b.deal_id"
                    " WHERE b.buyer_id = " + PH + " ORDER BY b.created_at DESC", (u["id"],), fetch=True)
    ca = u.get("created_at")
    joined = ca.strftime("%b %d, %Y") if hasattr(ca, "strftime") else str(ca or "")[:10]
    return render_template_string(_T_PROFILE, u=u, mydeals=mydeals or [], mybookings=mybookings or [],
                                  saved=saved, joined=joined)


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
