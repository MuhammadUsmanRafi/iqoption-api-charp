#!/usr/bin/env python3
"""Generate golden test data by running the REAL Python iqoptionapi code with a fake network.

Usage (from the repository root or anywhere):  python3 IqOptionApi.CSharp/tests/golden/generate_golden.py
Writes JSON files into IqOptionApi.CSharp/tests/IqOptionApi.Tests/Golden/.
Needs only the Python standard library: `requests` and `websocket` are replaced by stubs.
"""
import json
import os
import queue
import random
import subprocess
import sys
import threading
import time as _time
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
OUT = os.path.abspath(os.path.join(HERE, "..", "IqOptionApi.Tests", "Golden"))
T = 1791549020            # pinned clock: 2026-10-09 12:30:20 UTC
METHOD_TZ = "Asia/Karachi"
EXPIRY_ZONES = ["UTC", "Asia/Karachi", "Asia/Kolkata", "Asia/Kathmandu", "Australia/Adelaide", "America/New_York"]

HTTP_LOG = []
_now = [float(T)]
_real_sleep = _time.sleep


def pin_time(ts):
    _now[0] = float(ts)


def install_stubs():
    """Replace `requests` and `websocket` so the library imports and runs offline."""
    requests = types.ModuleType("requests")

    class FakeResponse:
        def __init__(self, status_code=200, text="{}", cookies=None):
            self.status_code = status_code
            self.text = text
            self.cookies = cookies or {}
            self.headers = {}

        def raise_for_status(self):
            pass

    class FakeCookies(dict):
        def clear_session_cookies(self):
            pass

        def get_dict(self):
            return dict(self)

    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.cookies = FakeCookies()
            self.proxies = {}
            self.auth = None
            self.verify = True
            self.trust_env = True

        def request(self, method, url, data=None, params=None, headers=None, cookies=None):
            HTTP_LOG.append({"method": method, "url": url, "data": data, "headers": headers})
            if url.endswith("/api/v2/login"):
                return FakeResponse(200, '{"code":"success"}', {"ssid": "SSID-TEST"})
            return FakeResponse(200, "{}", {})

    requests.Session = FakeSession
    requests.packages = types.SimpleNamespace(urllib3=types.SimpleNamespace(disable_warnings=lambda *a, **k: None))
    requests.utils = types.SimpleNamespace(add_dict_to_cookiejar=lambda jar, d: jar.update(d))
    auth = types.ModuleType("requests.auth")
    auth.HTTPProxyAuth = lambda user, password: (user, password)
    requests.auth = auth
    sys.modules["requests"] = requests
    sys.modules["requests.auth"] = auth

    websocket = types.ModuleType("websocket")
    websocket.WebSocketApp = FakeWebSocketApp
    sys.modules["websocket"] = websocket

    _time.time = lambda: _now[0]
    _time.sleep = lambda s: _real_sleep(min(s, 0.001))
    sys.path.insert(0, REPO)


# ---------------------------------------------------------------- rules ----
def frame_key(frame):
    msg = frame.get("msg")
    if frame["name"] in ("sendMessage", "subscribeMessage", "unsubscribeMessage") \
            and isinstance(msg, dict) and isinstance(msg.get("name"), str):
        return frame["name"] + ":" + msg["name"]
    return frame["name"]


def subst(obj, rid):
    if isinstance(obj, dict):
        return {k: subst(v, rid) for k, v in obj.items()}
    if isinstance(obj, list):
        return [subst(v, rid) for v in obj]
    if obj == "$RID":
        return rid
    return obj


def replies_for(frame, rules):
    key = frame_key(frame)
    out = []
    for rule in rules:
        if rule["on"] == key:
            out.extend(subst(r, frame.get("request_id", "")) for r in rule["reply"])
    return out


# --------------------------------------------------------- fake socket ----
DELIVERY = queue.Queue()
CONNECT_RULES = []
FRAMES = []


class FakeWebSocketApp:
    """Stands in for websocket.WebSocketApp during the `connect` part."""

    def __init__(self, url, on_message=None, on_error=None, on_close=None, on_open=None, **kwargs):
        self.url = url
        self.on_message = on_message
        self.on_open = on_open
        self.sock = None

    def run_forever(self, **kwargs):
        self.on_open(self)

    def send(self, data):
        FRAMES.append(data)
        for reply in replies_for(json.loads(data), CONNECT_RULES):
            DELIVERY.put((self, json.dumps(reply)))

    def close(self):
        pass


def _delivery_worker():
    while True:
        app, raw = DELIVERY.get()
        _real_sleep(0.002)
        app.on_message(app, raw)


# ---------------------------------------------------------------- json ----
def to_json(x):
    if isinstance(x, dict):
        return {str(k) if not isinstance(k, float) else repr(k): to_json(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [to_json(v) for v in x]
    if hasattr(x, "maxlen"):   # deque
        return [to_json(v) for v in x]
    return x


def prune(x):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            v = prune(v)
            if v == {} or v == []:
                continue
            out[k] = v
        return out
    if isinstance(x, list):
        return [prune(v) for v in x]
    return x


def write(name, data):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name), "w") as f:
        json.dump(data, f, indent=1, sort_keys=False)
        f.write("\n")
    print("wrote", name)


# -------------------------------------------------------------- expiry ----
def part_expiry(tz):
    from iqoptionapi import expiration
    from iqoptionapi.ws.chanels.strike_list import Strike_list

    rnd = random.Random(42)
    start = 1767225600    # 2026-01-01 UTC
    cases = []
    while len(cases) < 300:
        ts = start + rnd.randrange(0, 365 * 86400)
        if _time.localtime(ts - 3 * 3600).tm_gmtoff != _time.localtime(ts + 14 * 3600).tm_gmtoff:
            continue   # skip windows that contain a DST change (spec §10)
        pin_time(ts)
        row = {"ts": ts, "exp": {}, "digital": {}, "strike": {}}
        for d in (1, 2, 3, 4, 5, 15, 30, 45, 60, 120):
            row["exp"][str(d)] = list(expiration.get_expiration_time(ts, d))
        row["remaining"] = [list(x) for x in expiration.get_remaning_time(ts)]
        tsf = ts + 0.37
        pin_time(tsf)
        row["remaining_f"] = [list(x) for x in expiration.get_remaning_time(tsf)]
        pin_time(ts)
        api = types.SimpleNamespace(timesync=types.SimpleNamespace(server_timestamp=ts))
        for d in (1, 5, 15, 30, 60):
            v = expiration.get_digital_exp(ts, d)
            row["digital"][str(d)] = [v, isinstance(v, float)]
            row["strike"][str(d)] = Strike_list(api).get_digital_expiration_time(d)
        cases.append(row)
    write("expiry_" + tz.replace("/", "_") + ".json", {"tz": tz, "cases": cases})


# ---------------------------------------------------- later parts -------
def part_tables():
    import iqoptionapi.constants as OP_code
    import iqoptionapi.country_id as Country
    write("tables.json", {"actives": [[k, v] for k, v in OP_code.ACTIVES.items()],
                          "countries": [[k, v] for k, v in Country.ID.items()]})


def part_router():
    pass


def part_connect():
    pass


def part_methods():
    pass


def run_part(part, tz):
    env = dict(os.environ, TZ=tz)
    subprocess.run([sys.executable, os.path.abspath(__file__), "--part", part, "--tz", tz], env=env, check=True)


def main():
    if "--part" in sys.argv:
        part = sys.argv[sys.argv.index("--part") + 1]
        tz = sys.argv[sys.argv.index("--tz") + 1]
        _time.tzset()
        install_stubs()
        threading.Thread(target=_delivery_worker, daemon=True).start()
        {"expiry": lambda: part_expiry(tz), "tables": part_tables, "router": part_router,
         "connect": part_connect, "methods": part_methods}[part]()
        return
    for tz in EXPIRY_ZONES:
        run_part("expiry", tz)
    for part in ("tables", "router", "connect", "methods"):
        run_part(part, METHOD_TZ)


if __name__ == "__main__":
    main()
