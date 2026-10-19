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

@app.after_request
def _no_cache_html(resp):
    # Installed PWAs and browsers cache HTML aggressively; app pages must
    # always fetch fresh so deploys show up without clearing app data.
    if resp.mimetype == "text/html":
        resp.headers["Cache-Control"] = "no-store, must-revalidate"
    # Static assets (served at the root path) are content-stable between
    # deploys: cache a day. HTML stays no-store above.
    if resp.status_code == 200 and resp.mimetype in ("image/png", "image/jpeg",
            "image/webp", "image/svg+xml", "application/javascript", "text/css"):
        resp.headers["Cache-Control"] = "public, max-age=86400"
    # Gzip HTML/CSS/JS/JSON bodies over ~512 bytes: free-instance pages are
    # latency-sensitive and text payloads shrink 3-6x.
    try:
        if ("gzip" in request.headers.get("Accept-Encoding", "")
                and resp.status_code == 200
                and resp.mimetype in ("text/html", "text/css", "application/javascript",
                                      "application/json", "text/plain")
                and not resp.direct_passthrough
                and not resp.headers.get("Content-Encoding")):
            data = resp.get_data()
            if len(data) > 512:
                import gzip as _gzip
                packed = _gzip.compress(data, 6)
                resp.set_data(packed)
                resp.headers["Content-Encoding"] = "gzip"
                resp.headers["Content-Length"] = str(len(packed))
    except Exception:
        pass
    return resp


@app.get("/health")
def health():
    """Tiny watchdog ping endpoint (cron-job.org response-size limit is small)."""
    return Response("ok", mimetype="text/plain")


@app.get("/")
def home():
    stats = _q("SELECT COUNT(*) AS n, COALESCE(SUM(sold),0) AS sold,"
               " COUNT(DISTINCT seller_id) AS nsellers FROM deals", fetch=True)[0]
    featured = _q("SELECT d.id, d.title, d.description, d.price_inr, d.seats_total, d.sold,"
                  " d.image_url, u.name AS seller"
                  " FROM deals d JOIN users u ON u.id = d.seller_id"
                  " WHERE d.status != 'deleted'"
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
  status TEXT NOT NULL DEFAULT 'active',
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
  avatar_url TEXT,
  phone TEXT,
  city TEXT,
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
  status TEXT NOT NULL DEFAULT 'active',
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
            try:
                cur.execute(stmt)
            except Exception as e:
                print("init_db stmt failed:", e, file=sys.stderr)
                conn.rollback()
        try:
            cur.execute("ALTER TABLE users ADD COLUMN " + ("IF NOT EXISTS " if PG else "") + "avatar_url TEXT")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("init_db alter failed:", e, file=sys.stderr)
        try:
            cur.execute("ALTER TABLE deals ADD COLUMN " + ("IF NOT EXISTS " if PG else "") + "status TEXT NOT NULL DEFAULT 'active'")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("init_db deals alter failed:", e, file=sys.stderr)
        for col, dflt in (("theme", "'dark'"), ("fx", "'on'"), ("notify", "'on'")):
            try:
                cur.execute("ALTER TABLE users ADD COLUMN " + ("IF NOT EXISTS " if PG else "") + col + " TEXT DEFAULT " + dflt)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print("init_db users pref alter failed:", e, file=sys.stderr)
        try:
            watch_sql = ("CREATE TABLE IF NOT EXISTS watch (user_id INTEGER NOT NULL, deal_id INTEGER NOT NULL, created_at TIMESTAMPTZ DEFAULT now())"
                         if PG else
                         "CREATE TABLE IF NOT EXISTS watch (user_id INTEGER NOT NULL, deal_id INTEGER NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP)")
            cur.execute(watch_sql)
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("init_db watch failed:", e, file=sys.stderr)
        try:
            cur.execute("CREATE TABLE IF NOT EXISTS deal_seats (deal_id INTEGER NOT NULL, seat_no INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'free', booking_id INTEGER, hold_token TEXT, held_until BIGINT, PRIMARY KEY (deal_id, seat_no))")
            conn.commit()
        except Exception as e:
            conn.rollback()
            print("init_db deal_seats failed:", e, file=sys.stderr)
        for col in ("image_url",):
            try:
                cur.execute("ALTER TABLE deals ADD COLUMN " + ("IF NOT EXISTS " if PG else "") + col + " TEXT")
                conn.commit()
            except Exception as e:
                conn.rollback()
                print("init_db deals alter failed:", col, e, file=sys.stderr)
        for col in ("phone", "city"):
            try:
                cur.execute("ALTER TABLE users ADD COLUMN " + ("IF NOT EXISTS " if PG else "") + col + " TEXT")
                conn.commit()
            except Exception as e:
                conn.rollback()
                print("init_db users alter failed:", col, e, file=sys.stderr)
        cur.close()
        conn.close()
        _db_ready = True


_pg_pool = None

def _pg_acquire():
    """Reuse pooled PG connections: a fresh TLS handshake per query made every
    endpoint take ~2s per query on Supabase (~10s per stats poll)."""
    global _pg_pool
    import psycopg2
    from psycopg2.pool import ThreadedConnectionPool, PoolError
    if _pg_pool is None:
        _pg_pool = ThreadedConnectionPool(1, 8, SUPABASE_DB_URL)
    for _ in range(10):
        try:
            return _pg_pool.getconn()
        except PoolError:
            time.sleep(0.15)
        except Exception:
            time.sleep(0.15)
    return psycopg2.connect(SUPABASE_DB_URL)

def _pg_release(conn, broken=False):
    try:
        if _pg_pool is not None:
            _pg_pool.putconn(conn, close=broken)
        else:
            conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass

def _q(sql, args=(), fetch=False, commit=False):
    _init_db()
    if not PG:
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(sql, args)
            rows = None
            if fetch:
                cols = [d[0] for d in cur.description]
                rows = [dict(zip(cols, r)) for r in cur.fetchall()]
            if commit:
                conn.commit()
            cur.close()
        finally:
            conn.close()
        return rows
    conn = _pg_acquire()
    try:
        cur = conn.cursor()
        cur.execute(sql, args)
        rows = None
        if fetch:
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        if commit:
            conn.commit()
        cur.close()
        _pg_release(conn, broken=False)
        return rows
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        _pg_release(conn, broken=True)
        raise


def _current_user():
    uid = session.get("uid")
    if not uid:
        return None
    rows = _q("SELECT id, email, name, avatar_url, created_at, phone, city, theme, fx, notify FROM users WHERE id = " + PH, (uid,), fetch=True)
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
:root{--c:#22d3ee;--g:#4ade80;--p:#a78bfa;--bg:#070b18;--card:rgba(17,24,50,.62);--line:rgba(148,163,184,.16);--muted:#94a3b8}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{margin:0;min-height:100vh;background:var(--bg);color:#e5e7eb;font-family:system-ui,-apple-system,'Segoe UI',Roboto,sans-serif;overflow-x:hidden}
body::before,body::after{content:"";position:fixed;width:60vmax;height:60vmax;border-radius:50%;filter:blur(90px);opacity:.16;z-index:-1;pointer-events:none}
body::before{background:radial-gradient(circle,#22d3ee,transparent 65%);top:-20vmax;left:-15vmax;animation:drift 26s ease-in-out infinite alternate}
body::after{background:radial-gradient(circle,#a78bfa,transparent 65%);bottom:-22vmax;right:-15vmax;animation:drift 32s ease-in-out infinite alternate-reverse}
@keyframes drift{from{transform:translate(0,0) scale(1)}to{transform:translate(6vmax,4vmax) scale(1.15)}}
a{color:var(--c);text-decoration:none}
h1,h2{color:#fff;line-height:1.2}
h1{font-size:1.85rem;margin:0 0 14px;font-weight:800}
.grad{background:linear-gradient(92deg,#22d3ee 5%,#a78bfa 55%,#4ade80 100%);-webkit-background-clip:text;background-clip:text;color:transparent}
nav{display:flex;gap:10px;align-items:center;padding:10px 20px;background:rgba(9,13,32,.78);backdrop-filter:blur(14px);border-bottom:1px solid var(--line);position:sticky;top:0;z-index:50;flex-wrap:wrap}
.navlinks{display:flex;gap:4px;flex-wrap:wrap;align-items:center}
.navlinks a{color:#cbd5e1;font-weight:600;font-size:.9rem;padding:7px 14px;border-radius:999px;transition:color .2s,background .2s}
.navlinks a:hover{color:#fff;background:rgba(34,211,238,.12)}
.navchip{display:flex;align-items:center;gap:8px;padding:4px 14px 4px 5px;border-radius:999px;background:rgba(148,163,184,.08);border:1px solid var(--line);color:#e5e7eb;font-weight:600;font-size:.9rem;transition:border-color .2s,background .2s}
.navchip:hover{border-color:var(--c);background:rgba(34,211,238,.1)}
.navini{display:inline-flex;width:30px;height:30px;border-radius:50%;align-items:center;justify-content:center;background:linear-gradient(135deg,var(--c),var(--g));color:#04121f;font-weight:800;font-size:.9rem}
nav a.brand{color:#fff;font-size:1.22rem;display:flex;align-items:center;gap:8px;font-weight:800}
.brand-dot{width:10px;height:10px;border-radius:50%;background:var(--g);box-shadow:0 0 12px var(--g);display:inline-block;animation:pulse 2.4s infinite}
@keyframes pulse{0%,100%{box-shadow:0 0 8px var(--g)}50%{box-shadow:0 0 18px var(--g)}}
nav .sp{flex:1}
.navuser{display:flex;align-items:center;gap:10px}
.navuser img.navav{width:32px;height:32px;border-radius:50%;object-fit:cover;border:2px solid rgba(74,222,128,.5)}
.btn{display:inline-block;background:linear-gradient(90deg,var(--c),var(--g));color:#04121f;font-weight:800;border:none;border-radius:11px;padding:10px 22px;cursor:pointer;font-size:.95rem;text-decoration:none;transition:transform .15s,box-shadow .15s;box-shadow:0 4px 18px rgba(34,211,238,.25)}
.btn:hover{transform:translateY(-2px);box-shadow:0 8px 26px rgba(34,211,238,.4)}
.btn.ghost{background:rgba(148,163,184,.08);color:#e5e7eb;border:1px solid var(--line);box-shadow:none}
.btn.ghost:hover{border-color:var(--c);color:#fff}
.btn.small{padding:7px 16px;font-size:.87rem}
main{max-width:1120px;margin:0 auto;padding:30px 20px 40px}
footer{border-top:1px solid var(--line);color:var(--muted);text-align:center;padding:24px;font-size:.85rem;margin-top:52px;background:rgba(9,13,32,.5)}
footer b{color:var(--c)}
.card{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:24px;margin-bottom:18px;backdrop-filter:blur(10px);box-shadow:0 10px 34px rgba(2,6,23,.45);transition:border-color .2s}
.card:hover{border-color:rgba(148,163,184,.3)}
.pagehead{padding:26px 0 22px}
.kicker{color:var(--c);letter-spacing:.16em;text-transform:uppercase;font-size:.75rem;font-weight:800;margin:0 0 8px}
.pagehead .sub{color:var(--muted);max-width:660px;margin:6px 0 0;font-size:1rem}
input,textarea,button{font:inherit;border-radius:11px;border:1px solid rgba(148,163,184,.28);background:rgba(7,11,24,.75);color:#e5e7eb;padding:10px 13px;width:100%;margin-bottom:12px;transition:border-color .2s,box-shadow .2s}
input:focus,textarea:focus{outline:none;border-color:var(--c);box-shadow:0 0 0 3px rgba(34,211,238,.15)}
button{background:linear-gradient(90deg,var(--c),var(--g));color:#04121f;font-weight:800;border:none;cursor:pointer;width:auto;padding:10px 24px}
button:hover{transform:translateY(-1px)}
button:disabled{opacity:.5;cursor:default;transform:none}
.badge{display:inline-block;background:rgba(34,211,238,.14);color:var(--c);border-radius:8px;padding:3px 10px;font-size:.78rem;margin-left:6px;font-weight:600}
.badge.green{background:rgba(74,222,128,.16);color:var(--g)}
.badge.red{background:rgba(248,113,113,.14);color:#f87171}
.err{color:#f87171}.ok{color:var(--g)}
.muted{color:var(--muted);font-size:.9rem}
.price{color:var(--g);font-weight:800;font-size:1.05rem}
label{font-size:.84rem;color:var(--muted);display:block;margin-bottom:4px;font-weight:600}
.hero{padding:64px 12px 36px;text-align:center}
.hero h1{font-size:2.5rem;margin:10px 0 16px}
.hero .sub{color:var(--muted);max-width:660px;margin:0 auto 28px;font-size:1.08rem}
.cta{display:flex;gap:14px;justify-content:center;flex-wrap:wrap}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:14px;margin:8px 0 30px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:16px;padding:20px;text-align:center;backdrop-filter:blur(10px)}
.stat b{display:block;font-size:1.6rem;background:linear-gradient(92deg,var(--c),var(--g));-webkit-background-clip:text;background-clip:text;color:transparent}
.stat span{color:var(--muted);font-size:.84rem}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(255px,1fr));gap:18px}
.deal{background:var(--card);border:1px solid var(--line);border-radius:16px;overflow:hidden;display:flex;flex-direction:column;backdrop-filter:blur(10px);transition:transform .2s,box-shadow .2s,border-color .2s}
.deal:hover{transform:translateY(-4px);border-color:rgba(34,211,238,.4);box-shadow:0 14px 34px rgba(2,6,23,.6)}
.deal img{width:100%;height:158px;object-fit:cover;display:block}
.deal .pad{padding:15px 17px 17px;display:flex;flex-direction:column;gap:8px;flex:1}
.deal h3{margin:0;font-size:1.03rem}
.deal h3 a{color:#fff}
.deal h3 a:hover{color:var(--c)}
.deal .meta{color:var(--muted);font-size:.84rem}
.deal .grow{flex:1}
.bar{height:7px;background:rgba(7,11,24,.8);border-radius:4px;overflow:hidden;position:relative}
.bar i{display:block;height:100%;background:linear-gradient(90deg,var(--c),var(--g));border-radius:4px;position:relative;overflow:hidden}
.bar i::after{content:"";position:absolute;inset:0;background:linear-gradient(90deg,transparent,rgba(255,255,255,.35),transparent);animation:sheen 2.6s infinite}
@keyframes sheen{from{transform:translateX(-100%)}to{transform:translateX(100%)}}
.steps{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px}
.step b{color:var(--c);display:block;margin-bottom:6px}
.step{background:rgba(7,11,24,.4);border:1px solid var(--line);border-radius:14px;padding:16px}
.avatar{width:64px;height:64px;border-radius:50%;background:linear-gradient(135deg,var(--c),var(--g));color:#04121f;font-weight:800;font-size:1.6rem;display:flex;align-items:center;justify-content:center;flex-shrink:0;overflow:hidden}
.avatar img{width:100%;height:100%;border-radius:50%;object-fit:cover}
.row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
.auth{max-width:410px;margin:56px auto}
.auth .brand{text-align:center;color:#fff;font-size:1.45rem;font-weight:800;margin-bottom:14px}
.search{display:flex;gap:10px;margin-bottom:22px}
.search input{margin-bottom:0}
.backlink{display:inline-block;margin-bottom:14px;color:var(--muted);font-weight:600;font-size:.9rem}
.backlink:hover{color:var(--c)}
.deal-hero{display:grid;grid-template-columns:1fr 1fr;gap:24px;align-items:start}
.deal-hero img{width:100%;border-radius:14px;object-fit:cover;max-height:360px;border:1px solid var(--line)}
@media(max-width:720px){.deal-hero{grid-template-columns:1fr}.hero h1{font-size:1.7rem}main{padding:20px 14px}}
.divider{height:1px;background:var(--line);margin:18px 0}
.empty{text-align:center;padding:26px}

.scene3d{position:fixed;inset:0;z-index:-2;perspective:1100px;perspective-origin:50% 40%;overflow:hidden;pointer-events:none}
.stage{position:absolute;inset:-35%;transform-style:preserve-3d;will-change:transform}
.orb{position:absolute;border-radius:50%;filter:blur(60px);opacity:.2}
.orb.o1{width:42vmax;height:42vmax;left:8%;top:12%;background:radial-gradient(circle,#22d3ee,transparent 62%);animation:fl1 18s ease-in-out infinite alternate}
.orb.o2{width:34vmax;height:34vmax;right:4%;top:38%;background:radial-gradient(circle,#a78bfa,transparent 62%);animation:fl2 22s ease-in-out infinite alternate}
.orb.o3{width:26vmax;height:26vmax;left:30%;top:60%;background:radial-gradient(circle,#4ade80,transparent 62%);animation:fl1 26s ease-in-out infinite alternate-reverse;opacity:.13}
.orb.o4{width:18vmax;height:18vmax;left:55%;top:8%;background:radial-gradient(circle,#f472b6,transparent 62%);animation:fl2 20s ease-in-out infinite alternate;opacity:.09}
@keyframes fl1{from{transform:translate3d(0,0,-120px)}to{transform:translate3d(5vmax,3vmax,-50px)}}
@keyframes fl2{from{transform:translate3d(0,0,60px)}to{transform:translate3d(-4vmax,-2.5vmax,130px)}}
.gridfloor{position:absolute;left:-70%;right:-70%;top:62%;bottom:-55%;background-image:linear-gradient(rgba(34,211,238,.16) 1.5px,transparent 1.5px),linear-gradient(90deg,rgba(167,139,250,.12) 1.5px,transparent 1.5px);background-size:70px 70px;transform:rotateX(74deg);transform-origin:center top;animation:gridflow 16s linear infinite;-webkit-mask-image:linear-gradient(to bottom,transparent 2%,rgba(0,0,0,.9) 26%,transparent 96%);mask-image:linear-gradient(to bottom,transparent 2%,rgba(0,0,0,.9) 26%,transparent 96%)}
@keyframes gridflow{from{background-position:0 0}to{background-position:0 70px}}
.stars{position:absolute;inset:0;transform:translateZ(260px);animation:twinkle 5s ease-in-out infinite alternate;background-image:radial-gradient(1.5px 1.5px at 12% 22%,rgba(255,255,255,.8),transparent),radial-gradient(1px 1px at 34% 8%,rgba(255,255,255,.6),transparent),radial-gradient(1.5px 1.5px at 68% 30%,rgba(255,255,255,.7),transparent),radial-gradient(1px 1px at 82% 64%,rgba(255,255,255,.5),transparent),radial-gradient(1px 1px at 52% 74%,rgba(255,255,255,.6),transparent),radial-gradient(1.5px 1.5px at 22% 82%,rgba(255,255,255,.55),transparent)}
@keyframes twinkle{from{opacity:.45}to{opacity:1}}
@media (prefers-reduced-motion: reduce){.orb,.gridfloor,.stars{animation:none}}
.navlinks{display:flex;gap:4px;flex-wrap:wrap;align-items:center}
.navlinks a{padding:7px 14px;border-radius:999px;font-weight:600;font-size:.9rem;color:#cbd5e1;text-decoration:none;transition:background .15s,color .15s}
.navlinks a:hover{background:rgba(34,211,238,.12);color:#fff}
.navchip{display:inline-flex;flex-direction:column;align-items:center;gap:3px;padding:7px 12px 5px;border-radius:14px;background:rgba(148,163,184,.10);border:1px solid var(--line);text-decoration:none;color:#e5e7eb;font-weight:600;max-width:92px}
.navchip:hover{border-color:var(--c);color:#fff}
.navav{width:34px;height:34px;border-radius:50%;object-fit:cover;display:block;flex:none}
.navini{width:34px;height:34px;border-radius:50%;background:linear-gradient(135deg,var(--c),var(--g));color:#04121f;display:inline-flex;align-items:center;justify-content:center;font-weight:800;flex:none}
.navname{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.68rem;line-height:1.1;max-width:76px;text-align:center;font-weight:600}
@media(max-width:720px){.navlinks a{padding:6px 10px;font-size:.85rem}}


body.theme-light{--bg:#eef3fa;--card:rgba(255,255,255,.9);--line:rgba(15,23,42,.13);--muted:#5a6a7e;background:#eef3fa;color:#1e293b}
body.theme-light h1,body.theme-light h2,body.theme-light h3{color:#0f172a}
body.theme-light a{color:#0e7490}
body.theme-light nav{background:rgba(247,250,254,.88);border-bottom:1px solid rgba(15,23,42,.1)}
body.theme-light .navlinks a{color:#334155}
body.theme-light .navlinks a:hover{color:#0f172a;background:rgba(34,211,238,.14)}
body.theme-light .navchip{color:#1e293b;background:rgba(148,163,184,.15)}
body.theme-light .navchip:hover{color:#0f172a}
body.theme-light nav a.brand{color:#0f172a}
body.theme-light footer{background:rgba(255,255,255,.65);color:#5a6a7e}
body.theme-light footer b{color:#0e7490}
body.theme-light input,body.theme-light textarea{background:#fff;color:#1e293b;border-color:rgba(15,23,42,.22)}
body.theme-light .btn.ghost{background:rgba(15,23,42,.04);color:#1e293b}
body.theme-light .btn.ghost:hover{color:#0f172a;border-color:var(--c)}
body.theme-light .card,body.theme-light .deal,body.theme-light .stat{box-shadow:0 10px 28px rgba(15,23,42,.08)}
body.theme-light .deal:hover{box-shadow:0 14px 34px rgba(15,23,42,.12)}
body.theme-light .price{color:#16a34a}
body.theme-light .err{color:#dc2626}
body.theme-light .ok{color:#16a34a}
body.theme-light .badge{color:#0e7490}
body.theme-light .badge.green{color:#15803d}
body.theme-light .badge.red{color:#dc2626}
body.theme-light .kicker{color:#0e7490}
body.theme-light .scene3d{opacity:.3}
body.theme-light button{color:#04121f}
@media (prefers-color-scheme: light){body.theme-auto::before,body.theme-auto::after{opacity:.08}
body.theme-auto{--bg:#eef3fa;--card:rgba(255,255,255,.9);--line:rgba(15,23,42,.13);--muted:#5a6a7e;background:#eef3fa;color:#1e293b}
body.theme-auto h1,body.theme-auto h2,body.theme-auto h3{color:#0f172a}
body.theme-auto a{color:#0e7490}
body.theme-auto nav{background:rgba(247,250,254,.88);border-bottom:1px solid rgba(15,23,42,.1)}
body.theme-auto .navlinks a{color:#334155}
body.theme-auto .navlinks a:hover{color:#0f172a;background:rgba(34,211,238,.14)}
body.theme-auto .navchip{color:#1e293b;background:rgba(148,163,184,.15)}
body.theme-auto .navchip:hover{color:#0f172a}
body.theme-auto nav a.brand{color:#0f172a}
body.theme-auto footer{background:rgba(255,255,255,.65);color:#5a6a7e}
body.theme-auto footer b{color:#0e7490}
body.theme-auto input,body.theme-auto textarea{background:#fff;color:#1e293b;border-color:rgba(15,23,42,.22)}
body.theme-auto .btn.ghost{background:rgba(15,23,42,.04);color:#1e293b}
body.theme-auto .btn.ghost:hover{color:#0f172a;border-color:var(--c)}
body.theme-auto .card,body.theme-auto .deal,body.theme-auto .stat{box-shadow:0 10px 28px rgba(15,23,42,.08)}
body.theme-auto .deal:hover{box-shadow:0 14px 34px rgba(15,23,42,.12)}
body.theme-auto .price{color:#16a34a}
body.theme-auto .err{color:#dc2626}
body.theme-auto .ok{color:#16a34a}
body.theme-auto .badge{color:#0e7490}
body.theme-auto .badge.green{color:#15803d}
body.theme-auto .badge.red{color:#dc2626}
body.theme-auto .kicker{color:#0e7490}
body.theme-auto .scene3d{opacity:.3}
body.theme-auto button{color:#04121f}
}
body.theme-light::before,body.theme-light::after{opacity:.08}
body.fx-off .scene3d{display:none}
body.fx-off::before,body.fx-off::after{animation:none}
body.fx-off .orb,body.fx-off .gridfloor,body.fx-off .stars{display:none}
</style>
</head>
<body class="{% if u %}{% if u.get('theme') == 'light' %}theme-light{% elif u.get('theme') == 'auto' %}theme-auto{% else %}theme-dark{% endif %}{% if u.get('fx') == 'off' %} fx-off{% endif %}{% else %}theme-dark{% endif %}">
<div class="scene3d"><div class="stage" id="stage3d">
<div class="orb o1"></div><div class="orb o2"></div><div class="orb o3"></div><div class="orb o4"></div>
<div class="stars"></div><div class="gridfloor"></div>
</div></div>
<nav>
<a class="brand" href="/"><i class="brand-dot"></i>Aurora</a>
<span class="navlinks">
<a href="/">Home</a>
<a href="/deals">Deals</a>
<a href="/event">Live Event</a>
<a href="/sell">Sell</a>
<a href="/post-deal">Post deal</a>
</span>
<span class="sp"></span>
{% if u %}
<a class="navchip" href="/settings" title="Settings">{% if u['avatar_url'] %}<img class="navav" src="{{ u['avatar_url'] }}" alt="">{% else %}<span class="navini">{{ (u['name'] or u['email'])[:1]|upper }}</span>{% endif %}<span class="navname">{{ u['name'] or u['email'] }}</span></a>
{% else %}
<a class="btn ghost small" href="/login">Sign in</a>
<a class="btn small" href="/signup">Sign up</a>
{% endif %}
</nav>
<main>{% block body %}{% endblock %}</main>
<script>
document.querySelectorAll('.deal[data-deal]').forEach(function(c){
 c.addEventListener('click',function(e){
  if(e.target.closest('a,button,form'))return;
  var id=c.getAttribute('data-deal');
  if(id)location.href='/event?deal='+id;
 });
});
document.querySelectorAll('.watch').forEach(function(b){
 b.addEventListener('click',function(e){
  e.preventDefault();e.stopPropagation();
  var id=b.getAttribute('data-deal');
  fetch('/api/deal/'+id+'/watch',{method:'POST'}).then(function(r){
   if(r.status===401){location.href='/login?next='+encodeURIComponent(location.pathname);return null;}
   return r.json();
  }).then(function(j){if(j&&j.ok){b.classList.toggle('on',j.watching);b.innerHTML=j.watching?'\u2665':'\u2661';}});
 });
});
</script>
<footer>Powered by <b>Kavach</b> &middot; flash-sale engine with zero double-sells</footer>
<script>
(function(){
 var st=document.getElementById('stage3d'); if(!st) return;
 var tx=0,ty=0,cx=0,cy=0;
 function loop(){ cx+=(tx-cx)*.06; cy+=(ty-cy)*.06; st.style.transform='rotateX('+cy.toFixed(3)+'deg) rotateY('+cx.toFixed(3)+'deg)'; requestAnimationFrame(loop);} loop();
 var fine=window.matchMedia&&matchMedia('(pointer:fine)').matches;
 if(fine){ window.addEventListener('mousemove',function(e){ tx=(e.clientX/innerWidth-.5)*16; ty=(e.clientY/innerHeight-.5)*-12;}); }

 if(fine){ document.addEventListener('pointermove',function(e){ var c=e.target&&e.target.closest&&e.target.closest('.deal'); if(c){ var r=c.getBoundingClientRect(); var px=(e.clientX-r.left)/r.width-.5, py=(e.clientY-r.top)/r.height-.5; c.style.transform='translateY(-4px) rotateX('+(-py*7).toFixed(2)+'deg) rotateY('+(px*7).toFixed(2)+'deg)'; } }); document.addEventListener('pointerout',function(e){ var c=e.target&&e.target.closest&&e.target.closest('.deal'); if(c){ c.style.transform=''; } }); }
})();
</script>

<button id="aiFab" aria-label="Ask the guide" onclick="aiToggle()">&#10024;</button>
<div id="aiPanel" hidden>
  <div id="aiHead">Aurora guide <span onclick="aiToggle()" style="cursor:pointer;margin-left:auto">&#10005;</span></div>
  <div id="aiLog"><div class="aim bot">Hi! I'm the Aurora guide. Ask me how to book a ticket, post your own deal, or anything about the app.</div></div>
  <form id="aiForm"><input id="aiIn" placeholder="Ask anything..." autocomplete="off"><button type="submit">Ask</button></form>
</div>
<style>
#aiFab{position:fixed;right:18px;bottom:18px;z-index:90;width:54px;height:54px;border-radius:50%;font-size:1.4rem;box-shadow:0 8px 26px rgba(34,211,238,.4);padding:0;margin:0}
#aiPanel[hidden]{display:none}
#aiPanel{position:fixed;right:18px;bottom:82px;z-index:90;width:min(340px,calc(100vw - 36px));max-height:min(480px,72vh);display:flex;flex-direction:column;background:var(--card);backdrop-filter:blur(14px);border:1px solid var(--line);border-radius:16px;box-shadow:0 18px 50px rgba(2,6,23,.55);overflow:hidden}
#aiHead{display:flex;gap:8px;align-items:center;padding:11px 14px;font-weight:700;border-bottom:1px solid var(--line);color:#fff}
#aiLog{flex:1;overflow-y:auto;padding:12px;display:flex;flex-direction:column;gap:8px}
.aim{max-width:86%;padding:9px 12px;border-radius:12px;font-size:.9rem;line-height:1.45;white-space:pre-wrap}
.aim.bot{background:rgba(148,163,184,.13);border:1px solid var(--line);align-self:flex-start}
.aim.me{background:linear-gradient(90deg,var(--c),var(--g));color:#04121f;font-weight:600;align-self:flex-end}
#aiForm{display:flex;gap:8px;padding:10px;border-top:1px solid var(--line)}
#aiForm input{margin:0;flex:1}
#aiForm button{margin:0;width:auto}
</style>
<script>
function aiToggle(){var p=document.getElementById('aiPanel');p.hidden=!p.hidden;if(!p.hidden)document.getElementById('aiIn').focus();}
document.addEventListener('DOMContentLoaded',function(){
  var f=document.getElementById('aiForm');
  f.addEventListener('submit',function(e){
    e.preventDefault();
    var i=document.getElementById('aiIn'),t=i.value.trim();if(!t)return;
    var log=document.getElementById('aiLog');
    var me=document.createElement('div');me.className='aim me';me.textContent=t;log.appendChild(me);i.value='';log.scrollTop=log.scrollHeight;
    var th=document.createElement('div');th.className='aim bot';th.textContent='...';log.appendChild(th);log.scrollTop=log.scrollHeight;
    fetch('/api/assistant',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:t})})
    .then(function(r){return r.json()}).then(function(d){
      th.textContent=(d&&d.reply)||'Sorry, try again.';
      if(d&&d.error)th.textContent=d.error;
      log.scrollTop=log.scrollHeight;
    }).catch(function(){th.textContent='Network issue, try again.';});
  });
});
</script>
</body>
</html>
"""

_T_HOME = _BASE.replace("{% block body %}{% endblock %}", """
<section class="hero">
<p class="kicker">Flash-sale marketplace, live on Kavach</p>
<h1><span class="grad">Deals that sell out in seconds.</span><br>Inventory that never oversells.</h1>
<p class="sub">Every booking on Aurora is held, confirmed or released through the Kavach engine in real time. Sold means sold: no double-bookings, ever.</p>
<div class="cta"><a class="btn" href="/deals">Explore deals</a><a class="btn ghost" href="/event">Book live event seats</a></div>
</section>
<div class="stats">
<div class="stat"><b>{{ ndeals }}</b><span>live deals</span></div>
<div class="stat"><b>{{ seats_sold }}</b><span>seats sold</span></div>
<div class="stat"><b>{{ nsellers }}</b><span>sellers</span></div>
</div>
{% if deals %}
<div class="pagehead" style="padding:6px 0 16px"><p class="kicker">Fresh stock</p><h2>Fresh drops</h2></div>
<div class="grid">
{% for d in deals %}
<div class="deal" data-deal="{{ d['id'] }}">{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="">{% endif %}
<div class="pad"><h3><a href="/event?deal={{ d['id'] }}">{{ d['title'] }}</a></h3>
<div class="meta">by {{ d['seller'] }}</div>
<div class="grow"><p style="margin:0" class="muted">{{ d['description'][:110] }}</p></div>
<div class="row"><span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span>{% if d['status'] == 'paused' %}<span class="badge red">paused</span>{% else %}<span class="badge">{{ d['seats_total'] - d['sold'] }} of {{ d['seats_total'] }} left</span>{% endif %}</div>
<div class="bar"><i style="width:{{ ((100 * d['sold'] / d['seats_total'])|round)|int }}%"></i></div></div></div>
{% endfor %}
</div>
<p style="margin:18px 0 28px"><a class="btn ghost" href="/deals">See all deals &rarr;</a></p>
{% else %}
<div class="card empty"><p class="muted" style="margin:0 0 10px">No deals on the board yet.</p><a class="btn" href="/post-deal">Post the first deal</a></div>
{% endif %}
<div class="card">
<div class="pagehead" style="padding:0 0 14px"><p class="kicker">How it works</p><h2>Three steps, zero oversells</h2></div>
<div class="steps">
<div class="step"><b>1 &middot; Post</b>Sellers list a flash deal with a fixed quantity of seats or units.</div>
<div class="step"><b>2 &middot; Hold</b>When you book, Kavach holds your seat atomically while payment completes.</div>
<div class="step"><b>3 &middot; Confirm or release</b>Success confirms the seat; failure releases it instantly for the next buyer.</div>
</div>
</div>
""")

_T_SIGNUP = _BASE.replace("{% block body %}{% endblock %}", """
<div class="auth"><div class="card"><div class="brand">Aurora</div><h1 class="grad" style="text-align:center">Create your account</h1>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
{% if note %}<p class="ok">{{ note }}</p>{% endif %}
<form method="post"><input type="hidden" name="next" value="{{ nx }}"><label>Name</label><input name="name" required>
<label>Email</label><input name="email" type="email" required>
<label>Password (8+ characters)</label><input name="password" type="password" required minlength="8">
<button style="width:100%">Sign up</button></form>
<div style="display:flex;gap:10px;margin-top:12px">
<a class="btn ghost small" href="/auth/google?next={{ nx }}" style="flex:1;text-align:center">Continue with Google</a>
<a class="btn ghost small" href="/auth/github?next={{ nx }}" style="flex:1;text-align:center">Continue with GitHub</a>
</div>
<p class="muted" style="text-align:center">Already have an account? <a href="/login">Sign in</a></p></div></div>
""").replace("<title>{% block title %}Aurora{% endblock %}", "<title>Sign up &middot; Aurora")

_T_LOGIN = _BASE.replace("{% block body %}{% endblock %}", """
<div class="auth"><div class="card"><div class="brand">Aurora</div><h1 class="grad" style="text-align:center">Welcome back</h1>
{% if err %}<p class="err">{{ err }}</p>{% endif %}
<form method="post"><input type="hidden" name="next" value="{{ nx }}"><label>Email</label><input name="email" type="email" required>
<label>Password</label><input name="password" type="password" required>
<button style="width:100%">Sign in</button></form>
<div style="display:flex;gap:10px;margin-top:12px">
<a class="btn ghost small" href="/auth/google?next={{ nx }}" style="flex:1;text-align:center">Continue with Google</a>
<a class="btn ghost small" href="/auth/github?next={{ nx }}" style="flex:1;text-align:center">Continue with GitHub</a>
</div>
<p class="muted" style="text-align:center">New here? <a href="/signup">Create an account</a></p></div></div>
""")

_T_DEALS = _BASE.replace("{% block body %}{% endblock %}", """
<div class="pagehead"><p class="kicker">Marketplace</p><h1><span class="grad">Live flash deals</span></h1>
<p class="sub">Search the board, open any deal for full details, and book against a live stock count. <span id="live-dot" style="display:none;color:var(--g);font-size:.75rem;font-weight:700">&#9679; LIVE</span></p></div>
<form class="search" method="get" action="/deals"><input name="q" value="{{ q }}" placeholder="Search deals&hellip;"><button class="btn">Search</button></form>
{% if not deals %}<div class="card empty"><p class="muted" style="margin:0 0 10px">No deals{{ ' match your search' if q else ' yet' }}.</p><a class="btn" href="/post-deal">Post one</a></div>{% endif %}
<div class="grid">
{% for d in deals %}
<div class="deal" data-deal="{{ d['id'] }}"><button class="watch" data-deal="{{ d['id'] }}" title="Watch">&#9825;</button>{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="">{% endif %}
<div class="pad"><h3><a href="/event?deal={{ d['id'] }}">{{ d['title'] }}</a></h3>
<div class="meta">by {{ d['seller'] }}</div>
<div class="grow"><p style="margin:0" class="muted">{{ d['description'][:110] }}</p></div>
<div class="row"><span class="price">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span>{% if d['status'] == 'paused' %}<span class="badge red">paused</span>{% else %}<span class="badge">{{ d['seats_total'] - d['sold'] }} of {{ d['seats_total'] }} left</span>{% endif %}</div>
<div class="bar"><i style="width:{{ ((100 * d['sold'] / d['seats_total'])|round)|int }}%"></i></div></div></div>
{% endfor %}
</div>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script>window.SB_URL={{ sb_url|tojson }};window.SB_KEY={{ sb_key|tojson }};</script>
{{ _RT_JS }}
""")

_T_DEAL = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="#" onclick="history.back();return false">&larr; Back</a>
<div class="pagehead" style="padding:6px 0 16px"><p class="kicker">Deal details</p></div>
<div class="card deal-page" data-deal="{{ d['id'] }}">
<div class="deal-hero">
<div>{% if d['image_url'] %}<img src="{{ d['image_url'] }}" alt="">{% else %}<div class="search" style="min-height:200px;align-items:center;justify-content:center;display:flex;border:1px dashed rgba(148,163,184,.35);border-radius:14px;color:var(--muted)">No image</div>{% endif %}</div>
<div>
<h1>{{ d['title'] }} <span id="live-dot" style="display:none;color:var(--g);font-size:.75rem;font-weight:700">&#9679; LIVE</span></h1>
<p class="muted">by {{ d['seller'] }}</p>
<p><span class="price" style="font-size:1.4rem">&#8377;{{ '%.0f'|format(d['price_inr']) }}</span> <span class="muted">per booking</span></p>
<p>{{ d['description'] }}</p>
<div class="divider"></div>
<p style="margin:6px 0">{% if d['status'] == 'paused' %}<span class="badge red">paused</span> {% endif %}<span class="badge green">{{ left }} of {{ d['seats_total'] }} left</span>
{% if d['sold'] > 0 %}<span class="badge">{{ d['sold'] }} sold</span>{% endif %}</p>
<div class="bar" style="margin-bottom:16px"><i style="width:{{ ((100 * d['sold'] / d['seats_total'])|round)|int }}%"></i></div>
{% if owner %}
<div class="row" style="gap:8px;flex-wrap:wrap;margin-bottom:14px">
<a class="btn ghost small" href="/deal/{{ d['id'] }}/edit">Edit deal</a>
{% if d['status'] == 'paused' %}<form method="post" action="/deal/{{ d['id'] }}/resume" style="margin:0"><button class="btn ghost small">Resume bookings</button></form>
{% else %}<form method="post" action="/deal/{{ d['id'] }}/pause" style="margin:0"><button class="btn ghost small">Pause bookings</button></form>{% endif %}
<form method="post" action="/deal/{{ d['id'] }}/delete" style="margin:0" onsubmit="return confirm('Delete this deal? It disappears from listings; past bookings stay in history.')"><button class="btn ghost small" style="color:#f87171;border-color:rgba(248,113,113,.4)">Delete</button></form>
</div>
{% endif %}
{% if err %}<p class="err">{{ err }}</p>{% endif %}
{% if ok %}<p class="ok">{{ ok }}</p>{% endif %}
{% if d['status'] == 'paused' %}<p class="muted">Booking is paused by the seller right now.</p>
{% elif u and left > 0 %}<a class="btn" href="/event?deal={{ d['id'] }}" style="width:100%;text-align:center">Book in the 3D arena &rarr;</a>
<p class="muted" style="font-size:.85rem;margin:8px 0 0">Booking opens in the live 3D venue with real-time counters.</p>
{% elif not u %}<p><a class="btn" href="/login?next=/event%3Fdeal%3D{{ d['id'] }}">Sign in to book</a></p>
{% else %}<p class="muted">Sold out.</p>{% endif %}
</div>
</div>
</div>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script>window.SB_URL={{ sb_url|tojson }};window.SB_KEY={{ sb_key|tojson }};</script>
{{ _RT_JS }}
""").replace("<title>{% block title %}{{ d['title'] }}{% endblock %}", "<title>{{ d['title'] }} &middot; Aurora")

_T_POST = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="/sell">&larr; Back to seller dashboard</a>
<div class="pagehead"><p class="kicker">Seller</p><h1><span class="grad">Post a flash deal</span></h1>
<p class="sub">Set a fixed quantity. Kavach guarantees it never oversells, whatever the traffic.</p></div>
<div class="card" style="max-width:600px;margin:0 auto">
{% if err %}<p class="err">{{ err }}</p>{% endif %}
<form method="post" enctype="multipart/form-data"><label>Deal title</label><input name="title" required maxlength="120">
<label>Description</label><textarea name="description" rows="4" placeholder="What are you selling? Condition, delivery, anything a buyer needs."></textarea>
<label>Price per booking (INR)</label><input name="price" type="number" min="1" step="1" required>
<label>Total quantity / seats</label><input name="seats" type="number" min="1" step="1" required>
<label>Image (optional, PNG/JPG/WebP, max 5 MB)</label><input name="image" type="file" accept="image/*" style="color:#94a3b8">
<label>Sale starts (optional, YYYY-MM-DD HH:MM)</label><input name="starts_at" placeholder="2026-11-01 10:00">
<label>Sale ends (optional)</label><input name="ends_at">
<button style="width:100%">Publish deal</button></form></div>
""")

_T_SELL = _BASE.replace("{% block body %}{% endblock %}", """
<div class="pagehead"><p class="kicker">Seller dashboard</p><h1><span class="grad">Your storefront</span></h1>
<p class="sub">Everything you have listed, and how it is selling, in one place.</p></div>
<div class="stats">
<div class="stat"><b>{{ n_active }}</b><span>active listings</span></div>
<div class="stat"><b>{{ n_sold }}</b><span>units sold</span></div>
<div class="stat"><b>&#8377;{{ '%.0f'|format(revenue) }}</b><span>revenue from sold units</span></div>
</div>
<p style="margin:0 0 20px"><a class="btn" href="/post-deal">+ Post a new deal</a></p>
<div class="pagehead" style="padding:0 0 14px"><h2>Your listings</h2></div>
{% if not mydeals %}<div class="card empty"><p class="muted" style="margin:0 0 10px">No listings yet.</p><a class="btn" href="/post-deal">Post your first deal</a></div>{% endif %}
{% for d in mydeals %}
<div class="card"><div class="row">
<div class="grow" style="flex:1">
<h3 style="margin:0 0 6px"><a href="/deal/{{ d['id'] }}" style="color:#fff">{{ d['title'] }}</a></h3>
<div class="muted">&#8377;{{ '%.0f'|format(d['price_inr']) }} per booking &middot; {{ d['sold'] }}/{{ d['seats_total'] }} sold</div>
<div class="bar" style="max-width:320px;margin-top:8px"><i style="width:{{ ((100 * d['sold'] / d['seats_total'])|round)|int }}%"></i></div>
<div class="row" style="gap:8px;margin-top:12px;flex-wrap:wrap">
<a class="btn ghost small" href="/deal/{{ d['id'] }}/edit">Edit</a>
{% if d['status'] == 'paused' %}<form method="post" action="/deal/{{ d['id'] }}/resume" style="margin:0"><button class="btn ghost small">Resume</button></form>{% else %}<form method="post" action="/deal/{{ d['id'] }}/pause" style="margin:0"><button class="btn ghost small">Pause bookings</button></form>{% endif %}
<form method="post" action="/deal/{{ d['id'] }}/delete" style="margin:0" onsubmit="return confirm('Delete this deal? It disappears from listings; past bookings stay in history.')"><button class="btn ghost small" style="color:#f87171;border-color:rgba(248,113,113,.4)">Delete</button></form>
</div>
</div>
{% if d['status'] == 'paused' %}<span class="badge red">paused</span>{% elif d['sold'] < d['seats_total'] %}<span class="badge green">active</span>{% else %}<span class="badge red">sold out</span>{% endif %}
</div></div>
{% endfor %}
""")

_T_PROFILE = _BASE.replace("{% block body %}{% endblock %}", """
<div class="pagehead"><p class="kicker">Account</p><h1><span class="grad">Your profile</span></h1></div>
<div class="card" style="text-align:center">
<div class="avatar" style="width:96px;height:96px;font-size:2.4rem;margin:0 auto 10px">{% if u['avatar_url'] %}<img src="{{ u['avatar_url'] }}" alt="">{% else %}{{ (u['name'] or u['email'])[:1]|upper }}{% endif %}</div>
<div class="muted" style="font-size:.92rem">{{ u['name'] or u['email'] }}</div>
<div class="muted" style="font-size:.82rem">{{ u['email'] }}{% if u.get('phone') %} &middot; {{ u['phone'] }}{% endif %}{% if u.get('city') %} &middot; {{ u['city'] }}{% endif %}{% if joined %} &middot; member since {{ joined }}{% endif %}</div>
<div style="margin-top:10px"><a class="btn ghost small" href="/settings">Settings</a></div></div>
<div class="stats" style="margin-top:0">
<div class="stat"><b>{{ mydeals|length }}</b><span>deals posted</span></div>
<div class="stat"><b>{{ mybookings|length }}</b><span>bookings made</span></div>
</div>
<div class="card"><h2>Account settings</h2>
{% if saved %}<p class="ok">{{ saved }}</p>{% endif %}
{% if perr %}<p class="err">{{ perr }}</p>{% endif %}
<form method="post" action="/profile" enctype="multipart/form-data"><label>Display name</label>
<input name="name" value="{{ u['name'] or '' }}" maxlength="60" required>
<div class="row" style="gap:12px">
<div style="flex:1"><label>Phone</label>
<input name="phone" value="{{ u.get('phone') or '' }}" maxlength="20" placeholder="+91 98xxxxxxx"></div>
<div style="flex:1"><label>City</label>
<input name="city" value="{{ u.get('city') or '' }}" maxlength="60" placeholder="Faridabad"></div>
</div>
<label>Profile photo (PNG/JPG/WebP, max 5 MB)</label>
<input name="photo" type="file" accept="image/*" style="color:#94a3b8">
<button>Save changes</button></form></div>
<div class="card"><h2>Your deals</h2>
{% if not mydeals %}<p class="muted">You have not posted any deals yet. <a href="/post-deal">Post one</a>.</p>{% endif %}
{% for d in mydeals %}<div class="row" style="margin-bottom:10px">
<div class="grow" style="flex:1"><a href="/deal/{{ d['id'] }}" style="color:#fff">{{ d['title'] }}</a>
<div class="muted">&#8377;{{ '%.0f'|format(d['price_inr']) }} &middot; {{ d['sold'] }}/{{ d['seats_total'] }} sold</div></div>
<a class="btn ghost small" href="/sell">Manage</a></div>{% endfor %}</div>
<div class="card" id="bookings"><h2>Your bookings</h2>
{% if not mybookings %}<p class="muted">No bookings yet.</p>{% endif %}
{% for b in mybookings %}<div class="row" style="margin-bottom:10px">
<div class="grow" style="flex:1"><a href="/deal/{{ b['deal_id'] }}" style="color:#fff">{{ b['title'] }}</a>
<div class="muted">KV-{{ '%06d'|format(b['id']) }} &middot; {{ b['seats'] }} seat{% if b['seats'] != 1 %}s{% endif %} &middot; &#8377;{{ '%.0f'|format(b['amount']) }}{% if b['created_at'] %} &middot; {% if b['created_at'].strftime %}{{ b['created_at'].strftime('%d %b, %H:%M') }}{% else %}{{ b['created_at'] }}{% endif %}{% endif %}</div></div>
<span class="badge {% if b['payment_status'] == 'paid' %}green{% endif %}">{{ b['payment_status'] }}</span></div>{% endfor %}</div>
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


def _sb_upload_image(f, prefix="deal"):
    ext = (f.filename.rsplit(".", 1)[-1] or "").lower() if f.filename else ""
    if ext not in ("png", "jpg", "jpeg", "webp", "gif"):
        return None
    data = f.read()
    if len(data) > 5 * 1024 * 1024:
        return None
    mime = {"png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg",
            "webp": "image/webp", "gif": "image/gif"}[ext]
    fn = prefix + "-%d-%s.%s" % (int(time.time()), secrets.token_hex(4), ext)
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
    mydeals = _q("SELECT id, title, price_inr, sold, seats_total, status FROM deals WHERE seller_id = " + PH +
                 " AND status != 'deleted' ORDER BY created_at DESC", (u["id"],), fetch=True) or []
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
    rows = _q("SELECT d.id, d.title, d.description, d.price_inr, d.seats_total, d.sold, d.status, d.image_url, u.name AS seller"
              " FROM deals d JOIN users u ON u.id = d.seller_id"
              " WHERE d.status != 'deleted'"
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
    prows = _q("SELECT status FROM deals WHERE id = " + PH, (did,), fetch=True)
    if not prows or prows[0]["status"] == "deleted":
        return redirect("/deals")
    if request.method == "POST":
        if not u:
            return _need_login()
        if prows[0]["status"] == "paused":
            err = "This deal is paused by the seller right now."
        else:
            try:
                n = max(1, int(request.form.get("seats") or 1))
            except ValueError:
                n = 1
            pre = _q("SELECT * FROM deals WHERE id = " + PH, (did,), fetch=True)[0]
            _q("UPDATE deals SET sold = sold + {0} WHERE id = {0} AND status = 'active' AND sold + {0} <= seats_total".format(PH),
               (n, did, n), commit=True)
            after = _q("SELECT sold FROM deals WHERE id = " + PH, (did,), fetch=True)[0]["sold"]
            if after > pre["sold"]:
                amount = round(pre["price_inr"] * n, 2)
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
    owner = bool(u and u["id"] == d["seller_id"])
    return render_template_string(_T_DEAL, d=d, left=d["seats_total"] - d["sold"],
                                  u=u, err=err, ok=ok, owner=owner,
                                  sb_url=SUPABASE_URL, sb_key=SUPABASE_ANON_KEY, _RT_JS=_RT_JS)


@app.route("/profile", methods=["GET", "POST"])
def profile():
    u = _current_user()
    if not u:
        return _need_login()
    saved = perr = None
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()[:60]
        phone = (request.form.get("phone") or "").strip()[:20]
        city = (request.form.get("city") or "").strip()[:60]
        f = request.files.get("photo")
        av = None
        if f and f.filename and SB_READY:
            av = _sb_upload_image(f, prefix="avatars/av")
            if not av:
                perr = "Photo skipped: use PNG/JPG/WebP under 5 MB."
        if name or phone or city or av:
            _q("UPDATE users SET name = " + PH + ", phone = " + PH + ", city = " + PH + " WHERE id = " + PH,
               (name or u["name"] or "", phone, city, u["id"]), commit=True)
            if av:
                _q("UPDATE users SET avatar_url = " + PH + " WHERE id = " + PH, (av, u["id"]), commit=True)
            u = _current_user()
            saved = "Profile updated."
    mydeals = _q("SELECT id, title, price_inr, sold, seats_total, status FROM deals WHERE seller_id = " + PH +
                 " AND status != 'deleted' ORDER BY created_at DESC", (u["id"],), fetch=True)
    mybookings = _q("SELECT b.*, d.title FROM bookings b JOIN deals d ON d.id = b.deal_id"
                    " WHERE b.buyer_id = " + PH + " ORDER BY b.created_at DESC", (u["id"],), fetch=True)
    ca = u.get("created_at")
    joined = ca.strftime("%b %d, %Y") if hasattr(ca, "strftime") else str(ca or "")[:10]
    return render_template_string(_T_PROFILE, u=u, mydeals=mydeals or [], mybookings=mybookings or [],
                                  saved=saved, perr=perr, joined=joined)


_T_SETTINGS = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="/deals">&larr; Back to deals</a>
<div class="pagehead"><p class="kicker">Account</p><h1><span class="grad">Settings</span></h1></div>
<div class="card" style="text-align:center">
<div class="avatar" style="width:96px;height:96px;font-size:2.4rem;margin:0 auto 10px">{% if u['avatar_url'] %}<img src="{{ u['avatar_url'] }}" alt="">{% else %}{{ (u['name'] or u['email'])[:1]|upper }}{% endif %}</div>
<div class="muted" style="font-size:.92rem">{{ u['name'] or u['email'] }}</div>
<div class="muted" style="font-size:.82rem">{{ u['email'] }}{% if joined %} &middot; member since {{ joined }}{% endif %}</div>
</div>
<div class="card"><h2>Account details</h2>
{% if saved %}<p class="ok">{{ saved }}</p>{% endif %}{% if perr %}<p class="err">{{ perr }}</p>{% endif %}
<form method="post" action="/settings" enctype="multipart/form-data">
<label>Display name</label><input name="name" value="{{ u['name'] or '' }}" maxlength="60" required>
<div class="row" style="gap:12px">
<div style="flex:1"><label>Phone</label><input name="phone" value="{{ u.get('phone') or '' }}" maxlength="20" placeholder="+91 98xxxxxxx"></div>
<div style="flex:1"><label>City</label><input name="city" value="{{ u.get('city') or '' }}" maxlength="60" placeholder="Faridabad"></div>
</div>
<label>Profile photo (PNG/JPG/WebP, max 5 MB)</label><input name="photo" type="file" accept="image/*" style="color:#94a3b8">
<button>Save changes</button></form></div>
<div class="card"><h2>Sign-in methods</h2>
<div class="row" style="gap:10px;flex-wrap:wrap">
<span class="badge {% if has_pw %}green{% endif %}">Email &amp; password {% if has_pw %}active{% else %}not set{% endif %}</span>
<a class="btn ghost small" href="/auth/google">Google</a>
<a class="btn ghost small" href="/auth/github">GitHub</a>
</div>
<p class="muted" style="font-size:.85rem;margin-bottom:0">Connect an account to sign in with one tap next time.</p></div>
<div class="card"><h2>Appearance &amp; preferences</h2>
<form method="post" action="/settings">
<label>Theme</label>
<div class="row" style="gap:14px;flex-wrap:wrap">
<label style="display:flex;align-items:center;gap:6px;font-weight:500"><input type="radio" name="theme" value="dark" {% if (u.get('theme') or 'dark') == 'dark' %}checked{% endif %}> Dark</label>
<label style="display:flex;align-items:center;gap:6px;font-weight:500"><input type="radio" name="theme" value="light" {% if u.get('theme') == 'light' %}checked{% endif %}> Light</label>
<label style="display:flex;align-items:center;gap:6px;font-weight:500"><input type="radio" name="theme" value="auto" {% if u.get('theme') == 'auto' %}checked{% endif %}> System (auto)</label>
</div>
<label style="margin-top:12px">Background 3D effects</label>
<div class="row" style="gap:14px">
<label style="display:flex;align-items:center;gap:6px;font-weight:500"><input type="radio" name="fx" value="on" {% if (u.get('fx') or 'on') == 'on' %}checked{% endif %}> On</label>
<label style="display:flex;align-items:center;gap:6px;font-weight:500"><input type="radio" name="fx" value="off" {% if u.get('fx') == 'off' %}checked{% endif %}> Off (battery saver)</label>
</div>
<label style="margin-top:12px">Email me booking confirmations</label>
<div class="row" style="gap:14px">
<label style="display:flex;align-items:center;gap:6px;font-weight:500"><input type="radio" name="notify" value="on" {% if (u.get('notify') or 'on') == 'on' %}checked{% endif %}> On</label>
<label style="display:flex;align-items:center;gap:6px;font-weight:500"><input type="radio" name="notify" value="off" {% if u.get('notify') == 'off' %}checked{% endif %}> Off</label>
</div>
<button style="margin-top:16px">Save preferences</button>
</form></div>
<div class="card"><h2>My bookings</h2>
{% if not mybookings %}<p class="muted">No bookings yet. <a href="/deals">Browse deals</a>.</p>{% endif %}
{% for b in mybookings %}<div class="row" style="margin-bottom:10px">
<div class="grow" style="flex:1"><a href="/event?deal={{ b['deal_id'] }}" style="color:#fff">{{ b['title'] }}</a>
<div class="muted">{{ b['seats'] }} &times; &nbsp;&#8377;{{ '%.0f'|format(b['amount']) }}</div></div>
<span class="badge {% if b['payment_status'] == 'paid' %}green{% endif %}">{{ b['payment_status'] }}</span></div>{% endfor %}</div>
<div class="card"><h2>Watchlist</h2>
{% if not watching %}<p class="muted">Nothing saved yet. Tap the heart on any deal to save it here.</p>{% endif %}
{% for d in watching %}<div class="row" style="margin-bottom:10px">
<div class="grow" style="flex:1"><a href="/event?deal={{ d['id'] }}" style="color:#fff">{{ d['title'] }}</a>
<div class="muted">&#8377;{{ '%.0f'|format(d['price_inr']) }} &middot; {{ d['seats_total'] - d['sold'] }} left</div></div>
<a class="btn ghost small" href="/event?deal={{ d['id'] }}">Open live event</a></div>{% endfor %}</div>
<div class="card"><h2>Session</h2>
<p class="muted" style="margin-top:0">Signed in as <b>{{ u['email'] }}</b>.</p>
<a class="btn ghost small" href="/logout" style="color:#f87171;border-color:rgba(248,113,113,.4)">Log out</a>
</div>
""")

_T_EDIT = _BASE.replace("{% block body %}{% endblock %}", """
<a class="backlink" href="/deal/{{ d['id'] }}">&larr; Back to deal</a>
<div class="pagehead"><p class="kicker">Seller</p><h1><span class="grad">Edit deal</span></h1>
<p class="sub">Change the price or quantity any time. Seats can never drop below what is already sold.</p></div>
<div class="card" style="max-width:600px;margin:0 auto">
{% if err %}<p class="err">{{ err }}</p>{% endif %}
<form method="post">
<label>Deal title</label><input name="title" value="{{ d['title'] }}" maxlength="120" required>
<label>Description</label><textarea name="description" rows="4">{{ d['description'] }}</textarea>
<label>Price per booking (INR)</label><input name="price" type="number" min="1" step="1" value="{{ '%.0f'|format(d['price_inr']) }}" required>
<label>Total quantity / seats (min {{ d['sold'] }} already sold)</label><input name="seats" type="number" min="{{ d['sold'] }}" step="1" value="{{ d['seats_total'] }}" required>
<label>Sale starts (optional, YYYY-MM-DD HH:MM)</label><input name="starts_at" value="{{ d['starts_at'] or '' }}" placeholder="2026-11-01 10:00">
<label>Sale ends (optional)</label><input name="ends_at" value="{{ d['ends_at'] or '' }}">
<button style="width:100%">Save changes</button></form></div>
""")

def _own_deal(did, u):
    if not u:
        return None
    rows = _q("SELECT * FROM deals WHERE id = " + PH + " AND seller_id = " + PH, (did, u["id"]), fetch=True)
    return rows[0] if rows else None


@app.route("/api/deal/<int:did>/watch", methods=["POST"])
def watch_toggle(did):
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "error": "login_required"}), 401
    rows = _q("SELECT deal_id FROM watch WHERE user_id = " + PH + " AND deal_id = " + PH, (u["id"], did), fetch=True)
    if rows:
        _q("DELETE FROM watch WHERE user_id = " + PH + " AND deal_id = " + PH, (u["id"], did), commit=True)
        return jsonify({"ok": True, "watching": False})
    exists = _q("SELECT id FROM deals WHERE id = " + PH, (did,), fetch=True)
    if not exists:
        return jsonify({"ok": False, "error": "not_found"}), 404
    _q("INSERT INTO watch (user_id, deal_id) VALUES (" + PH + ", " + PH + ")", (u["id"], did), commit=True)
    return jsonify({"ok": True, "watching": True})



GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
_ASSIST_RULES = [
    ("book", "To book a ticket: open Deals (or the Home page), tap any deal card, it opens the deal's live 3D event. Choose your seats and press Book. You must be signed in to book; if not, you'll be asked to log in first."),
    ("sign", "Sign up from the Sign up button (top right, or /signup). You can also sign in with Google or GitHub in one tap. Signing in is needed only for booking, selling and your profile; browsing is open to everyone."),
    ("sell", "To sell tickets: sign in, open Sell or Post deal from the top menu, fill in the event title, price per seat and total seats, and post. You can edit, pause or delete your deal anytime from the deal page."),
    ("watch", "Tap the heart on any deal card to save it to your watchlist. Your watchlist lives in Settings."),
    ("theme", "Change theme (Dark / Light / System) and turn the 3D background on or off in Settings, under Appearance & preferences. Press Save preferences after choosing."),
    ("payment", "Payments: bookings are recorded instantly and marked pending payment. Razorpay checkout is coming soon, so demo bookings are confirmed without payment for now."),
    ("profile", "Edit your name, phone, city and profile photo in Settings. Everything saves with the Save button at the end of each card."),
    ("cancel", "To cancel a booking or get help with a deal, mention the deal title here and I'll guide you, or open the deal page and use its controls."),
]

def _assistant_reply(msg):
    m = (msg or "").lower()
    best = None
    for k, a in _ASSIST_RULES:
        if k in m:
            best = a
            break
    if GEMINI_API_KEY:
        try:
            import json as _json, urllib.request as _ur
            ctx = ("You are the friendly in-app guide for Aurora, a ticket-booking demo app by the Kavach engine. "
                   "App pages: Home, Deals, Live Event (3D venue), Sell, Post deal, Settings. "
                   "Booking needs sign-in; browse is open. Payments are demo-mode (Razorpay coming). "
                   "Users can watch deals (heart), change theme and 3D effects in Settings. "
                   "Answer briefly (under 120 words), plain text, help with booking, selling and app questions. "
                   "If asked something outside the app, answer briefly anyway.")
            body = _json.dumps({"contents": [{"parts": [{"text": ctx + "\n\nUser: " + (msg or "")}]}],
                                "generationConfig": {"maxOutputTokens": 220, "temperature": 0.4}}).encode()
            req = _ur.Request("https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=" + GEMINI_API_KEY,
                              data=body, headers={"Content-Type": "application/json"})
            with _ur.urlopen(req, timeout=12) as r:
                data = _json.loads(r.read().decode())
            txt = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text")
            if txt:
                return txt.strip()[:1200]
        except Exception:
            pass
    return best or ("I'm Aurora's guide. I can help you book tickets (open a deal, then book in its live 3D event), "
                    "post your own deal, use the watchlist, or change theme and 3D effects in Settings. What do you need?")

@app.route("/api/assistant", methods=["POST"])
def api_assistant():
    n = session.get("_assist_n", 0)
    if n > 40:
        return jsonify({"ok": False, "error": "A lot of questions asked in a short while, try again later."})
    session["_assist_n"] = n + 1
    msg = (request.get_json(silent=True) or {}).get("message", "")
    return jsonify({"ok": True, "reply": _assistant_reply(msg)[:1200]})


@app.route("/settings", methods=["GET", "POST"])
def settings_page():
    u = _current_user()
    if not u:
        return _need_login()
    saved = perr = None
    if request.method == "POST":
        name = (request.form.get("name") or "").strip()[:60]
        phone = (request.form.get("phone") or "").strip()[:20]
        city = (request.form.get("city") or "").strip()[:60]
        f = request.files.get("photo")
        av = None
        if f and f.filename and SB_READY:
            av = _sb_upload_image(f, prefix="avatars/av")
            if not av:
                perr = "Photo skipped: use PNG/JPG/WebP under 5 MB."
        phone = phone or (u.get("phone") or "")
        city = city or (u.get("city") or "")
        theme = request.form.get("theme")
        if theme not in ("dark", "light", "auto"):
            theme = u.get("theme") or "dark"
        fx = "off" if request.form.get("fx") == "off" else "on"
        notify = "off" if request.form.get("notify") == "off" else "on"
        _q("UPDATE users SET name = " + PH + ", phone = " + PH + ", city = " + PH + ", theme = " + PH + ", fx = " + PH + ", notify = " + PH + " WHERE id = " + PH,
           (name or u["name"] or "", phone, city, theme, fx, notify, u["id"]), commit=True)
        if av:
            _q("UPDATE users SET avatar_url = " + PH + " WHERE id = " + PH, (av, u["id"]), commit=True)
        u = _current_user()
        saved = "Settings saved."
    ca = u.get("created_at")
    joined = ca.strftime("%b %d, %Y") if hasattr(ca, "strftime") else str(ca or "")[:10]
    has_pw = bool(u.get("pw_hash"))
    mybookings = _q("SELECT b.deal_id, b.seats, b.amount, b.payment_status, d.title FROM bookings b JOIN deals d ON d.id = b.deal_id WHERE b.buyer_id = " + PH + " ORDER BY b.created_at DESC LIMIT 20", (u["id"],), fetch=True) or []
    watching = _q("SELECT d.id, d.title, d.price_inr, d.seats_total, d.sold FROM watch w JOIN deals d ON d.id = w.deal_id WHERE w.user_id = " + PH + " AND d.status != 'deleted' ORDER BY w.created_at DESC", (u["id"],), fetch=True) or []
    return render_template_string(_T_SETTINGS, u=u, saved=saved, perr=perr, joined=joined, has_pw=has_pw, mybookings=mybookings, watching=watching)


@app.route("/deal/<int:did>/edit", methods=["GET", "POST"])
def deal_edit(did):
    u = _current_user()
    d = _own_deal(did, u)
    if d is None:
        return _need_login() if not u else redirect("/deals")
    err = None
    if request.method == "POST":
        try:
            price = float(request.form.get("price") or 0)
            seats = int(request.form.get("seats") or 0)
        except ValueError:
            price = seats = 0
        title = (request.form.get("title") or "").strip()[:120]
        if not title or price <= 0 or seats < d["sold"]:
            err = "Title and price are needed; seats cannot go below the %d already sold." % d["sold"]
        else:
            _q("UPDATE deals SET title=" + PH + ", description=" + PH + ", price_inr=" + PH + ", seats_total=" + PH +
               ", starts_at=" + PH + ", ends_at=" + PH + " WHERE id=" + PH,
               (title, (request.form.get("description") or "").strip()[:2000], price, seats,
                request.form.get("starts_at") or None, request.form.get("ends_at") or None, did), commit=True)
            return redirect("/deal/%d" % did)
    return render_template_string(_T_EDIT, d=d, err=err, u=u)


@app.route("/deal/<int:did>/pause", methods=["POST"])
def deal_pause(did):
    u = _current_user()
    if _own_deal(did, u) is None:
        return _need_login() if not u else redirect("/deals")
    _q("UPDATE deals SET status='paused' WHERE id=" + PH, (did,), commit=True)
    return redirect("/deal/%d" % did)


@app.route("/deal/<int:did>/resume", methods=["POST"])
def deal_resume(did):
    u = _current_user()
    if _own_deal(did, u) is None:
        return _need_login() if not u else redirect("/deals")
    _q("UPDATE deals SET status='active' WHERE id=" + PH, (did,), commit=True)
    return redirect("/deal/%d" % did)


@app.route("/deal/<int:did>/delete", methods=["POST"])
def deal_delete(did):
    u = _current_user()
    if _own_deal(did, u) is None:
        return _need_login() if not u else redirect("/deals")
    _q("UPDATE deals SET status='deleted' WHERE id=" + PH, (did,), commit=True)
    return redirect("/sell")


SEATMAP_CAP = 5000
HOLD_TTL = 120


def _ensure_deal_seats(d):
    """Backfill one row per seat for a deal; first `sold` seats are marked sold."""
    did, total, sold = d["id"], d["seats_total"], d["sold"]
    if total <= 0 or total > 50000:
        return
    row = _q("SELECT COUNT(*) AS c FROM deal_seats WHERE deal_id = " + PH, (did,), fetch=True)[0]
    have = row["c"] or 0
    if have >= total:
        return
    if PG:
        _q("INSERT INTO deal_seats (deal_id, seat_no, status) SELECT {0}, s, CASE WHEN s <= {1} THEN 'sold' ELSE 'free' END "
           "FROM generate_series(1, {2}) s "
           "WHERE NOT EXISTS (SELECT 1 FROM deal_seats x WHERE x.deal_id = {0} AND x.seat_no = s)".format(PH, PH, int(total)),
           (did, sold, did), commit=True)
    else:
        start = have + 1
        vals = [(did, k, "sold" if k <= sold else "free") for k in range(start, total + 1)]
        if vals:
            conn = _connect()
            cur = conn.cursor()
            cur.executemany("INSERT OR IGNORE INTO deal_seats (deal_id, seat_no, status) VALUES (?,?,?)", vals)
            conn.commit()
            cur.close()
            conn.close()


def _release_expired_holds(did):
    import time as _t
    _q("UPDATE deal_seats SET status = 'free', hold_token = NULL, held_until = NULL "
       "WHERE deal_id = " + PH + " AND status = 'held' AND COALESCE(held_until, 0) < {0}".format(int(_t.time()) if not PG else "%s"),
       (did, int(_t.time())) if PG else (did,), commit=True)


def _pg_stats_one(did, now_s):
    """One roundtrip: release expired holds + deal row + counts + seatmap."""
    sql = ("WITH rel AS (UPDATE deal_seats SET status='free', hold_token=NULL, held_until=NULL "
           "WHERE deal_id=%s AND status='held' AND COALESCE(held_until,0) < %s RETURNING 1) "
           "SELECT d.id, d.title, d.price_inr, d.sold, d.seats_total, d.status, d.starts_at, "
           "(SELECT COUNT(*) FROM rel) AS released, "
           "(SELECT COUNT(*) FROM deal_seats WHERE deal_id=%s) AS have_rows, "
           "(SELECT COUNT(*) FROM deal_seats WHERE deal_id=%s AND status='held') AS held_now")
    args = [did, now_s, did, did]
    if True:
        sql += ", (SELECT string_agg(CASE WHEN d.seats_total <= 5000 THEN (CASE status WHEN 'sold' THEN '2' WHEN 'held' THEN '1' ELSE '0' END) END, '' ORDER BY seat_no) FROM deal_seats WHERE deal_id=%s) AS seatmap"
        args.append(did)
    sql += " FROM deals d WHERE d.id=%s AND d.status<>'deleted'"
    args.append(did)
    rows = _q(sql, tuple(args), fetch=True, commit=True)
    if not rows:
        return None
    d = rows[0]
    if d["seats_total"] > SEATMAP_CAP:
        d["seatmap"] = None
    return d

def _pg_hold_grab(did, n, token, until):
    """One roundtrip: release expired holds, then atomically grab the first n free seats."""
    sql = ("WITH rel AS (UPDATE deal_seats SET status='free', hold_token=NULL, held_until=NULL "
           "WHERE deal_id=%s AND status='held' AND COALESCE(held_until,0) < %s RETURNING 1), "
           "grab AS (UPDATE deal_seats SET status='held', hold_token=%s, held_until=%s "
           "WHERE deal_id=%s AND status='free' AND seat_no IN ("
           "SELECT seat_no FROM deal_seats WHERE deal_id=%s AND status='free' ORDER BY seat_no LIMIT %s) "
           "RETURNING seat_no) "
           "SELECT (SELECT COUNT(*) FROM rel) AS released, "
           "(SELECT COUNT(*) FROM grab) AS grabbed, "
           "(SELECT COALESCE(array_agg(seat_no ORDER BY seat_no), ARRAY[]::int[]) FROM grab) AS seats")
    rows = _q(sql, (did, int(time.time()), token, until, did, did, n), fetch=True, commit=True)
    return rows[0] if rows else None

def _pg_book_held(did, token):
    """One roundtrip, race-safe: mark the token's held seats sold and bump the sold counter under a guard."""
    sql = ("WITH mark AS (UPDATE deal_seats SET status='sold' "
           "WHERE deal_id=%s AND status='held' AND hold_token=%s RETURNING seat_no), "
           "cnt AS (SELECT COUNT(*) AS c FROM mark), "
           "upd AS (UPDATE deals SET sold = sold + (SELECT c FROM cnt) "
           "WHERE id=%s AND status='active' AND (SELECT c FROM cnt) >= 1 AND sold + (SELECT c FROM cnt) <= seats_total "
           "RETURNING sold) "
           "SELECT (SELECT c FROM cnt) AS n, (SELECT sold FROM upd) AS new_sold, "
           "(SELECT COALESCE(array_agg(seat_no ORDER BY seat_no), ARRAY[]::int[]) FROM mark) AS seats")
    rows = _q(sql, (did, token, did), fetch=True, commit=True)
    return rows[0] if rows else None


def _deal_held_count(did):
    return _q("SELECT COUNT(*) AS c FROM deal_seats WHERE deal_id = " + PH + " AND status = 'held'", (did,), fetch=True)[0]["c"] or 0


def _deal_seatmap(did, total):
    if total <= 0 or total > SEATMAP_CAP:
        return None
    rows = _q("SELECT seat_no, status FROM deal_seats WHERE deal_id = " + PH + " ORDER BY seat_no", (did,), fetch=True)
    if len(rows) < total:
        return None
    m = {"free": "0", "held": "1", "sold": "2"}
    return "".join(m.get(r["status"], "0") for r in rows)


def _booking_ref(bid):
    return "KV-" + str(bid).zfill(6)


@app.get("/api/deal/<int:did>/stats")
def deal_stats_api(did):
    if PG:
        try:
            st = _pg_stats_one(did, int(time.time()))
        except Exception as e:
            print("pg stats batched failed:", e, file=sys.stderr)
            st = False
        if st is None:
            return jsonify({"ok": False, "status": "not_found"}), 404
        if st:
            if st["have_rows"] < st["seats_total"] and st["seats_total"] <= 50000:
                _ensure_deal_seats({"id": did, "seats_total": st["seats_total"], "sold": st["sold"]})
                st2 = _pg_stats_one(did, int(time.time()))
                st = st2 if st2 else st
            held = st["held_now"]
            payload = {"ok": True, "id": st["id"], "title": st["title"], "price": st["price_inr"],
                       "sold": st["sold"], "seats_total": st["seats_total"], "starts_at": st.get("starts_at"),
                       "held": held, "left": max(0, st["seats_total"] - st["sold"] - held), "status": st["status"]}
            if st.get("seatmap"):
                payload["seats"] = st["seatmap"]
            resp = jsonify(payload)
            resp.headers["Cache-Control"] = "no-store"
            return resp
    rows = _q("SELECT id, title, price_inr, sold, seats_total, status, starts_at FROM deals WHERE id = " + PH, (did,), fetch=True)
    if not rows or rows[0]["status"] == "deleted":
        return jsonify({"ok": False, "status": "not_found"}), 404
    d = rows[0]
    try:
        _ensure_deal_seats(d)
        _release_expired_holds(did)
    except Exception as e:
        print("deal_seats upkeep failed:", e, file=sys.stderr)
    held = _deal_held_count(did)
    payload = {"ok": True, "id": d["id"], "title": d["title"], "price": d["price_inr"],
               "sold": d["sold"], "seats_total": d["seats_total"], "starts_at": d.get("starts_at"),
               "held": held, "left": max(0, d["seats_total"] - d["sold"] - held), "status": d["status"]}
    try:
        sm = _deal_seatmap(did, d["seats_total"])
        if sm:
            payload["seats"] = sm
    except Exception as e:
        print("seatmap failed:", e, file=sys.stderr)
    resp = jsonify(payload)
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _mark_deal_seats(did, seat_nos, status, booking_id=None, token=None):
    phs = ",".join([PH] * len(seat_nos))
    sets = "status = " + PH + ", hold_token = NULL, held_until = NULL"
    args = [status]
    if booking_id is not None:
        sets += ", booking_id = " + PH
        args.append(booking_id)
    args.extend([did])
    args.extend(seat_nos)
    extra = ""
    if status == "sold":
        extra = " AND status IN ('held','free')"
    else:
        extra = " AND status = 'free'"
    _q("UPDATE deal_seats SET " + sets + " WHERE deal_id = " + PH + " AND seat_no IN (" + phs + ")" + extra,
       tuple(args), commit=True)


@app.post("/api/deal/<int:did>/hold")
def deal_hold_api(did):
    import time as _t
    from uuid import uuid4
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "status": "login_required"}), 401
    b = request.get_json(silent=True) or {}
    try:
        n = max(1, int(b.get("seats") or 1))
    except (TypeError, ValueError):
        return jsonify({"ok": False, "status": "bad_request"}), 400
    rows = _q("SELECT * FROM deals WHERE id = " + PH, (did,), fetch=True)
    if not rows or rows[0]["status"] == "deleted":
        return jsonify({"ok": False, "status": "not_found"}), 404
    d0 = rows[0]
    if d0["status"] == "paused":
        return jsonify({"ok": False, "status": "paused"}), 409
    if PG:
        token = uuid4().hex
        until = int(_t.time()) + HOLD_TTL
        try:
            got = _pg_hold_grab(did, n, token, until)
        except Exception as e:
            print("pg hold batched failed:", e, file=sys.stderr)
            got = None
        if got is not None:
            seat_nos = list(got["seats"] or [])
            if len(seat_nos) < n and d0["seats_total"] <= 50000:
                _ensure_deal_seats(d0)
                got = _pg_hold_grab(did, n, token, until)
                seat_nos = list(got["seats"] or [])
            if len(seat_nos) >= n:
                amount = round(d0["price_inr"] * n, 2)
                return jsonify({"ok": True, "hold_token": token, "seats": seat_nos, "amount": amount, "expires_in": HOLD_TTL})
            return jsonify({"ok": False, "status": "not_enough_seats"}), 409
    _ensure_deal_seats(d0)
    _release_expired_holds(did)
    free = _q("SELECT seat_no FROM deal_seats WHERE deal_id = " + PH + " AND status = 'free' ORDER BY seat_no LIMIT " + PH,
              (did, n), fetch=True)
    if not free or len(free) < n:
        return jsonify({"ok": False, "status": "not_enough_seats"}), 409
    seat_nos = [r["seat_no"] for r in free]
    token = uuid4().hex
    until = int(_t.time()) + HOLD_TTL
    _mark_deal_seats(did, seat_nos, "held", token=token)
    _q("UPDATE deal_seats SET hold_token = " + PH + ", held_until = " + PH + " WHERE deal_id = " + PH + " AND seat_no IN (" + ",".join([PH] * len(seat_nos)) + ")",
       tuple([token, until, did] + seat_nos), commit=True)
    amount = round(d0["price_inr"] * n, 2)
    return jsonify({"ok": True, "hold_token": token, "seats": seat_nos, "amount": amount, "expires_in": HOLD_TTL})


@app.post("/api/deal/<int:did>/book")
def deal_book_api(did):
    u = _current_user()
    if not u:
        return jsonify({"ok": False, "status": "login_required"}), 401
    b = request.get_json(silent=True) or {}
    token = (b.get("hold_token") or "").strip()
    rows = _q("SELECT * FROM deals WHERE id = " + PH, (did,), fetch=True)
    if not rows or rows[0]["status"] == "deleted":
        return jsonify({"ok": False, "status": "not_found"}), 404
    d0 = rows[0]
    if d0["status"] == "paused":
        return jsonify({"ok": False, "status": "paused"}), 409
    if PG and token:
        try:
            res = _pg_book_held(did, token)
        except Exception as e:
            print("pg book batched failed:", e, file=sys.stderr)
            res = None
        if res is not None:
            seat_nos = list(res["seats"] or [])
            n = len(seat_nos)
            if not seat_nos or res["new_sold"] is None:
                st = "hold_expired" if not seat_nos else "not_enough_seats"
                return jsonify({"ok": False, "status": st}), 409
            amount = round(d0["price_inr"] * n, 2)
            order_id = None
            if RAZORPAY_KEY_ID:
                order_id = _razorpay_order(amount)
            bid = _q("INSERT INTO bookings (deal_id, buyer_id, seats, amount, payment_status, razorpay_order_id)"
                     " VALUES (" + ",".join([PH] * 6) + ") RETURNING id",
                     (did, u["id"], n, amount, "pending" if order_id else "paid", order_id), fetch=True)[0]["id"]
            _q("UPDATE deal_seats SET booking_id = " + PH + " WHERE deal_id = " + PH + " AND seat_no IN (" + ",".join([PH] * len(seat_nos)) + ")",
               tuple([bid, did] + seat_nos), commit=True)
            return jsonify({"ok": True, "seats": n, "seat_nos": seat_nos, "amount": amount,
                            "booking_ref": _booking_ref(bid), "name": u.get("name") or "", "phone": u.get("phone") or "",
                            "city": u.get("city") or "", "sold": res["new_sold"], "seats_total": d0["seats_total"]})
    _ensure_deal_seats(d0)
    _release_expired_holds(did)
    if token:
        held_rows = _q("SELECT seat_no FROM deal_seats WHERE deal_id = " + PH + " AND status = 'held' AND hold_token = " + PH + " ORDER BY seat_no",
                       (did, token), fetch=True)
        if not held_rows:
            return jsonify({"ok": False, "status": "hold_expired"}), 409
        seat_nos = [r["seat_no"] for r in held_rows]
        n = len(seat_nos)
    else:
        try:
            n = max(1, int(b.get("seats") or 1))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "status": "bad_request"}), 400
        free = _q("SELECT seat_no FROM deal_seats WHERE deal_id = " + PH + " AND status = 'free' ORDER BY seat_no LIMIT " + PH,
                  (did, n), fetch=True)
        if not free or len(free) < n:
            return jsonify({"ok": False, "status": "not_enough_seats"}), 409
        seat_nos = [r["seat_no"] for r in free]
    _q("UPDATE deals SET sold = sold + {0} WHERE id = {0} AND status = 'active' AND sold + {0} <= seats_total".format(PH),
       (n, did, n), commit=True)
    after = _q("SELECT sold FROM deals WHERE id = " + PH, (did,), fetch=True)[0]["sold"]
    if after <= d0["sold"]:
        if token:
            _release_expired_holds(did)
        return jsonify({"ok": False, "status": "not_enough_seats"}), 409
    amount = round(d0["price_inr"] * n, 2)
    order_id = None
    if RAZORPAY_KEY_ID:
        order_id = _razorpay_order(amount)
    _q("INSERT INTO bookings (deal_id, buyer_id, seats, amount, payment_status, razorpay_order_id)"
       " VALUES (" + ",".join([PH] * 6) + ")",
       (did, u["id"], n, amount, "pending" if order_id else "paid", order_id), commit=True)
    bid = _q("SELECT id FROM bookings ORDER BY id DESC LIMIT " + PH, (1,), fetch=True)[0]["id"]
    _mark_deal_seats(did, seat_nos, "sold", booking_id=bid)
    d2 = _q("SELECT sold, seats_total FROM deals WHERE id = " + PH, (did,), fetch=True)[0]
    return jsonify({"ok": True, "seats": n, "seat_nos": seat_nos, "amount": amount,
                    "booking_ref": _booking_ref(bid), "name": u.get("name") or "", "phone": u.get("phone") or "",
                    "city": u.get("city") or "", "sold": d2["sold"], "seats_total": d2["seats_total"]})


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
