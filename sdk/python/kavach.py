"""Kavach Python SDK: the two-line integration for the Kavach booking engine.

Quickstart (local engine, no auth):

    from kavach import Kavach
    k = Kavach("http://localhost:8080")
    print(k.book("priya", "priya-booking-1", seats=1))

Production engine (API key + HMAC signing):

    k = Kavach("https://your-kavach-host", api_key="sk_live_abc", hmac_secret=os.environ["KAVACH_HMAC_SECRET"])

No dependencies: standard library only.
"""
import hashlib
import hmac as _hmac
import json
import time
import urllib.error
import urllib.request
import uuid


class KavachError(Exception):
    """A Kavach engine answered with a non-200 status."""

    def __init__(self, status, body):
        self.status = status
        self.body = body
        super().__init__(f"Kavach returned {status}: {body}")


class Kavach:
    def __init__(self, base_url="http://127.0.0.1:8080", api_key=None, hmac_secret=None, timeout=30):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.hmac_secret = hmac_secret
        self.timeout = timeout

    # ---- transport ----

    def _headers(self, body: bytes, method: str = "POST", path: str = "") -> dict:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["Authorization"] = "Bearer " + self.api_key
            if self.hmac_secret:
                ts = str(int(time.time()))
                rid = uuid.uuid4().hex
                # the signature covers method + path + body: one signature,
                # one request, no replay against another endpoint
                mac = _hmac.new(self.hmac_secret.encode(),
                                (ts + "\n" + rid + "\n" + method + "\n" + path + "\n").encode() + body,
                                hashlib.sha256)
                h["X-Kavach-Timestamp"] = ts
                h["X-Kavach-Request-Id"] = rid
                h["X-Kavach-Signature"] = mac.hexdigest()
        return h

    def _request(self, method, path, payload=None):
        body = json.dumps(payload).encode() if payload is not None else b""
        req = urllib.request.Request(self.base_url + path, data=body if payload is not None else None,
                                     headers=self._headers(body, method, path), method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                return json.loads(resp.read() or b"{}")
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                raise KavachError(e.code, json.loads(raw) if raw else {})
            except json.JSONDecodeError:
                raise KavachError(e.code, {"raw": raw.decode("utf-8", "replace")})

    # ---- the cart flow ----

    def book(self, user_id, idempotency_key, seats=1):
        """Direct purchase path: reserve and confirm in one call."""
        return self._request("POST", "/book",
                             {"user_id": user_id, "idempotency_key": idempotency_key, "seats": seats})

    def reserve(self, user_id, idempotency_key, seats=1):
        """Hold seats for a TTL; confirm later, after payment succeeds."""
        return self._request("POST", "/reserve",
                             {"user_id": user_id, "idempotency_key": idempotency_key, "seats": seats})

    def confirm(self, reservation_id, user_id=None):
        """Turn a live hold into a confirmed booking (idempotent)."""
        payload = {"reservation_id": reservation_id}
        if user_id is not None:
            payload["user_id"] = user_id
        return self._request("POST", "/confirm", payload)

    def release(self, reservation_id):
        """Give held seats back before the TTL expires."""
        return self._request("POST", "/release", {"reservation_id": reservation_id})

    def reservation(self, reservation_id):
        """Resolve any ambiguity: the live status of one reservation."""
        return self._request("GET", "/reservation?id=" + reservation_id)

    # ---- waitlist ----

    def waitlist_join(self, user_id, idempotency_key, seats=1):
        return self._request("POST", "/waitlist",
                             {"user_id": user_id, "idempotency_key": idempotency_key, "seats": seats})

    def waitlist(self, user_id):
        return self._request("GET", "/waitlist?user_id=" + user_id)

    def waitlist_cancel(self, waitlist_id):
        return self._request("POST", "/waitlist/cancel", {"waitlist_id": waitlist_id})

    # ---- observability ----

    def seats(self):
        return self._request("GET", "/seats")

    def metrics(self):
        return self._request("GET", "/metrics")
