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
import json, os, sys, threading, time, uuid

WEBHOOK_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webhooks.jsonl")

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "..", "..", "sdk", "python"))
from flask import Flask, Response, jsonify, request, send_from_directory
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "9000")), threaded=True)
