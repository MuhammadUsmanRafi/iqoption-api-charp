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
APPS = []


class FakeWebSocketApp:
    """Stands in for websocket.WebSocketApp during the `connect` part."""

    def __init__(self, url, on_message=None, on_error=None, on_close=None, on_open=None, **kwargs):
        self.url = url
        self.on_message = on_message
        self.on_open = on_open
        self.sock = None
        APPS.append(self)

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


# ----------------------------------------------------------- fixtures ----
REAL_ID, PRACTICE_ID = 10001, 10004
PROFILE = {"name": "profile", "request_id": "$RID",
           "msg": {"user_id": 777, "name": "Test User", "currency": "USD", "balance_id": PRACTICE_ID}}
BALANCES = {"name": "balances", "request_id": "$RID", "msg": [
    {"id": REAL_ID, "type": 1, "amount": 0, "currency": "USD"},
    {"id": PRACTICE_ID, "type": 4, "amount": 10000.5, "currency": "USD"}]}
TIME_SYNC = {"name": "timeSync", "msg": T * 1000}


def fixtures():
    return {"T": T, "tz": METHOD_TZ, "real_id": REAL_ID, "practice_id": PRACTICE_ID,
            "profile": PROFILE, "balances": BALANCES, "time_sync": TIME_SYNC}


def make_client():
    """An IQ_Option whose api looks exactly like it does right after a successful connect()."""
    from iqoptionapi.stable_api import IQ_Option
    from iqoptionapi.api import IQOptionAPI
    from iqoptionapi.ws.client import WebsocketClient
    import iqoptionapi.global_value as gv
    iq = IQ_Option("user@example.com", "secret")
    api = IQOptionAPI("iqoption.com", "user@example.com", "secret")
    iq.api = api
    api.websocket_client = WebsocketClient(api)
    api.object_id = 1
    api.init_global_value(1)
    gv.get_req_id(1)                    # "1" is used by `authenticate` during a real connect
    gv.balance_id[1] = PRACTICE_ID
    gv._tmp_raw_balance_id.pop(1, None)   # change_balance cache must start empty
    gv.check_websocket_if_connect[1] = 1
    api.timesync.server_timestamp = T * 1000
    api.profile.msg = PROFILE["msg"]
    api.balances_raw = subst(BALANCES, "")
    return iq, api


ROUTER_MAXDICT = {"EURUSD": {"60": 2, "1": 2, "5": 2}}
ROUTER_MESSAGES = [
    {"name": "timeSync", "msg": T * 1000 + 250},
    {"name": "balances", "msg": [{"id": 1, "type": 1, "amount": 5}, {"id": 2, "type": 4, "amount": 9}]},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 100, "to": 160, "id": 1, "open": 1.1, "close": 1.2, "min": 1.0, "max": 1.3, "volume": 0, "at": 5}},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 160, "to": 220, "id": 2, "open": 1.2, "close": 1.25, "min": 1.1, "max": 1.3, "volume": 0, "at": 6}},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 220, "to": 280, "id": 3, "open": 1.25, "close": 1.3, "min": 1.2, "max": 1.4, "volume": 0, "at": 7}},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 220, "to": 280, "id": 3, "open": 1.25, "close": 1.31, "min": 1.2, "max": 1.4, "volume": 0, "at": 8}},
    {"name": "candles-generated", "msg": {"active_id": 1861, "at": 9, "ask": 1.4, "bid": 1.39, "value": 1.395,
                                           "candles": {"1": {"id": 11, "from": 300, "to": 301, "open": 1.3, "min": 1.3, "max": 1.4, "volume": 1},
                                                       "5": {"id": 12, "from": 300, "to": 305, "open": 1.3, "min": 1.3, "max": 1.4, "volume": 2}}}},
    {"name": "commission-changed", "msg": {"instrument_type": "turbo-option", "active_id": 1861, "commission": {"value": 18}}},
    {"name": "traders-mood-changed", "msg": {"asset_id": 1861, "value": 0.61}},
    {"name": "top-assets-updated", "msg": {"instrument_type": "forex", "data": [{"active_id": 1861}]}},
    {"name": "position-changed", "microserviceName": "portfolio", "msg": {"source": "digital-options", "status": "open", "external_id": 900, "raw_event": {"order_ids": [800]}}},
    {"name": "position-changed", "microserviceName": "portfolio", "msg": {"source": "binary-options", "status": "open", "external_id": 901, "raw_event": {}}},
    {"name": "position-changed", "microserviceName": "portfolio", "msg": {"source": "trading", "status": "closed", "external_id": 902, "raw_event": {"order_ids": [802]}}},
    {"name": "order-changed", "microserviceName": "portfolio", "msg": {"source": "digital-options", "raw_event": {"position_id": 900, "id": 800}}},
    {"name": "option-opened", "msg": {"option_id": 903, "amount": 1}},
    {"name": "option-closed", "msg": {"option_id": 903, "amount": 1, "profit_amount": 1.85}},
    {"name": "client-price-generated", "msg": {"asset_id": 1861, "prices": [
        {"strike": "SPT", "call": {"symbol": "do1861A20261009D123300T1MCSPT", "ask": 52.5, "bid": 47}, "put": {"symbol": "do1861A20261009D123300T1MPSPT", "ask": 48.0, "bid": 51}},
        {"strike": "1170000", "call": {"symbol": "C1"}, "put": {"symbol": "P1", "ask": 40}}]}},
    {"name": "live-deal-binary-option-placed", "msg": {"active_id": 1861, "option_type": "turbo", "amount": 5}},
    {"name": "live-deal-binary-option-placed", "msg": {"active_id": 1861, "option_type": "turbo", "amount": 6}},
    {"name": "live-deal-digital-option", "msg": {"instrument_active_id": 76, "expiration_type": "PT1M", "amount_enrolled": 3}},
    {"name": "live-deal", "msg": {"instrument_active_id": 1861, "instrument_type": "forex", "amount": 7}},
    {"name": "balance-changed", "msg": {"current_balance": {"id": 2, "amount": 9.5}}},
    {"name": "something-unknown", "msg": {"x": 1}},
]


def dump_state(api):
    import iqoptionapi.global_value as gv
    live = {}
    for n, d1 in api.live_deal_data.items():
        for a, d2 in d1.items():
            for t, q in d2.items():
                live["%s|%s|%s" % (n, a, t)] = list(q)
    return prune(to_json({
        "server_timestamp": api.timesync.server_timestamp,
        "balance_id": gv.balance_id[api.object_id],
        "real_time_candles": api.real_time_candles,
        "candle_generated_check": {a: {s: v for s, v in d.items() if v is True} for a, d in api.candle_generated_check.items()},
        "candle_generated_all_size_check": {a: v for a, v in api.candle_generated_all_size_check.items() if v is True},
        "traders_mood": api.traders_mood,
        "commission": api.subscribe_commission_changed_data,
        "top_assets_updated": api.top_assets_updated_data,
        "live_deal": live,
        "order_async": {k: dict(v) for k, v in api.order_async.items()},
        "option_opened": api.socket_option_opened,
        "digital_opened": api.digital_opened,
        "digital_profit": api.digital_profit,
        "quotes": api.instrument_quites_generated_data,
        "quotes_raw": api.instrument_quotes_generated_raw_data,
        "balance": api.balance,
    }))


def part_router():
    import iqoptionapi.global_value as gv
    iq, api = make_client()
    gv.balance_id[1] = None
    for asset, sizes in ROUTER_MAXDICT.items():
        for size, maxdict in sizes.items():
            api.real_time_candles_maxdict_table[asset][int(size)] = maxdict
    for m in ROUTER_MESSAGES:
        api.websocket_client.on_message(None, json.dumps(m))
    write("router.json", {"maxdict": ROUTER_MAXDICT, "messages": ROUTER_MESSAGES, "state": dump_state(api)})


def part_connect():
    global CONNECT_RULES
    from iqoptionapi.stable_api import IQ_Option
    CONNECT_RULES = [
        {"on": "authenticate", "reply": [{"name": "authenticated", "msg": True, "request_id": "$RID"}]},
        {"on": "sendMessage:get-profile", "reply": [PROFILE]},
        {"on": "sendMessage:get-balances", "reply": [BALANCES]},
    ]
    stop = threading.Event()

    def time_pusher():
        while not stop.is_set():
            for app in list(APPS):
                DELIVERY.put((app, json.dumps(TIME_SYNC)))
            _real_sleep(0.02)

    threading.Thread(target=time_pusher, daemon=True).start()
    iq = IQ_Option("user@example.com", "secret")
    ok, reason = iq.connect()
    stop.set()
    login = [h for h in HTTP_LOG if h["url"].endswith("/api/v2/login")][0]
    write("connect.json", {"fixtures": fixtures(), "ok": ok, "frames": list(FRAMES),
                           "login_url": login["url"],
                           "login_form": {k: v for k, v in login["data"].items() if v is not None}})


def amount_of(raw):
    import iqoptionapi.global_value as gv
    return next((b["amount"] for b in raw["msg"] if b["id"] == gv.balance_id[1]), None)


def actives_items():
    import iqoptionapi.constants as OP_code
    return [[k, v] for k, v in OP_code.ACTIVES.items()]


def run_case(case):
    import iqoptionapi.constants as OP_code
    saved = list(OP_code.ACTIVES.items())
    iq, api = make_client()
    frames = []
    rules = case.get("rules", [])
    delay = case.get("delay", False)

    def deliver(reply):
        raw = json.dumps(reply)
        if delay:
            threading.Timer(0.05, api.websocket_client.on_message, args=(None, raw)).start()
        else:
            api.websocket_client.on_message(None, raw)

    def fake_send(name, msg, request_id="", no_force_send=True):
        data = json.dumps(dict(name=name, msg=msg, request_id=request_id))
        frames.append(data)
        for reply in replies_for(json.loads(data), rules):
            deliver(reply)

    api.send_websocket_request = fake_send
    for m in case.get("pre", []):
        api.websocket_client.on_message(None, json.dumps(m))
    if "setup" in case:
        case["setup"](iq, api)
    result = case["call"](iq)
    OP_code.ACTIVES.clear()
    OP_code.ACTIVES.update(saved)
    return {"name": case["name"], "csharp": case["csharp"], "setup_csharp": case.get("setup_csharp"),
            "pre": case.get("pre", []), "rules": rules, "frames": frames,
            "result": to_json(result), "compare_result": case.get("compare_result", True)}


def build_cases():
    cases = []
    cases += [
        {"name": "heartbeat_reply",
         "call": lambda iq: iq.api.websocket_client.on_message(None, json.dumps({"name": "heartbeat", "msg": 1791549020123})),
         "csharp": {"method": "Heartbeat", "args": {"heartbeatTime": 1791549020123}}, "compare_result": False},
    ]
    # @@CASES@@
    return cases


def part_methods():
    write("methods.json", {"fixtures": fixtures(), "cases": [run_case(c) for c in build_cases()]})



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
