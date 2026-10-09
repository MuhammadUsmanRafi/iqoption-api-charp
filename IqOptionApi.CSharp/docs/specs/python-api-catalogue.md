# iqoptionapi (v9.4) — API catalogue for a faithful C# port

Source: `/Users/usman/Documents/iqoptionapi_pro-master/iqoptionapi` (stable_api.py, api.py, ws/client.py, ws/chanels/*, ws/objects/*, http/*, expiration.py, constants.py, country_id.py, global_value.py).
Runtime deps per pyproject: `requests`, `websocket-client==0.58`.

---

## 0. Conventions used in this document

### 0.1 Outgoing frame envelope (every websocket send)

`IQOptionAPI.send_websocket_request(name, msg, request_id="", no_force_send=True)`:

```python
data = json.dumps(dict(name=name, msg=msg, request_id=request_id))
def doing_send():
    if no_force_send==False:
        self.websocket.send(data)                       # heartbeat only: NO mutex
    else:
        global_value.ssl_Mutex[self.object_id].acquire()
        self.websocket.send(data)
        global_value.ssl_Mutex[self.object_id].release()
try:
    doing_send()
except:
    self.connect()      # IQOptionAPI.connect(): full low-level reconnect (new websocket, new object_id)
    doing_send()
```

* Top-level key order is always `name`, `msg`, `request_id`.
* `json.dumps` defaults ⇒ separators `", "` and `": "` (a space after every comma and colon), `ensure_ascii=True` (non-ASCII ⇒ `\uXXXX`), `True/False/None` ⇒ `true/false/null`, Python `float` ⇒ `repr()` (`1.0` stays `1.0`, `1e-05`, `1e+16`), `int` ⇒ plain digits. Dict key order = insertion order shown in this file.
* Byte-exact example: `{"name": "sendMessage", "msg": {"name": "get-balances", "version": "1.0"}, "request_id": ""}`
* `request_id` is ALWAYS a JSON string (`""` or a counter string), never a number.
* If `send()` throws while holding the mutex, the mutex is never released (the old one is abandoned because `connect()` creates a new object_id / mutex).

### 0.2 Abbreviations

| Abbrev | Meaning |
|---|---|
| `rid` | `global_value.get_req_id(api.object_id)` → `str(counter)`; per-websocket counter starting at `1`, incremented under `req_mutex`; reset to 1 whenever a new websocket is created (`init_global_value`). |
| `BAL` | `int(global_value.balance_id[api.object_id])` (current balance id; set to the PRACTICE balance on every new websocket — see §3). |
| `ACT(x)` | `OP_code.ACTIVES[x]` (module-level dict `constants.ACTIVES`, name → int id). KeyError if missing. |
| `SEND(outer, msg, rid)` | `send_websocket_request(outer, msg, request_id=rid)`; rid `""` when not given. |
| busy-wait | `while cond: pass` — spin loop, 100% CPU, **no timeout** unless stated. |
| nested_dict(n, T) | `defaultdict` auto-vivification n levels deep, leaf default `T()` (so an unset leaf reads as `{}` / `0` / empty deque). `== {}` is used everywhere as the "not yet received" sentinel. |
| server_ts | `api.timesync.server_timestamp` = last `timeSync` msg (ms) / 1000 → float seconds. |

Groups: **C** connection/session · **A** account/balance · **I** assets/instruments/init data · **S** candles & realtime streams · **B** binary/turbo options · **D** digital options · **M** margin (forex/cfd/crypto) · **X** extras · **U** utility.

---

## 1. Public methods of `IQ_Option` (stable_api.py, file order)

126 `def`s on the class (incl. `__init__` and `_get_balances`).

### 1. `__init__(email, password, http_proxy_host=None, http_proxy_port=None, http_proxy_auth=None, set_ssid=None, auto_logout=True)` — C
* No network. Class attr `__version__ = "9.4"`.
* Sets: `size = [1,5,10,15,30,60,120,300,600,900,1800,3600,7200,14400,28800,43200,86400,604800,2592000]`, `suspend = 0.1`, `thread = None` (unused), `subscribe_candle = []` (strings `"ACTIVE,size"`), `subscribe_candle_all_size = []`, `subscribe_mood = []`, `get_digital_spot_profit_after_sale_data = nested_dict(2, int)`, `get_realtime_strike_list_temp_data = {}`, `get_realtime_strike_list_temp_expiration = 0`, `SESSION_HEADER = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"}`, `SESSION_COOKIE = {}`, proxy fields, `set_ssid`, `auto_logout`, `_2FA_TOKEN = None`.
* Quirk: `self.api` is only created in `connect()`; every other method raises `AttributeError` before the first `connect()`.

### 2. `logout()` — C
* `self.api.logout()` → HTTP `POST https://auth.iqoption.com/api/v1.0/logout` (no body) on the api `requests.Session`. Returns `None` (response discarded, no status check).

### 3. `set_call_back_for_client(function)` — U
* `global_value.client_callback[self.api.object_id] = function`. `on_message` calls `function(message_dict)` for **every** incoming message, before routing, while `ssl_Mutex` is held.
* Quirks: keyed by `object_id` ⇒ silently lost after `connect()` or any internal re-connect (new object_id). The callback must not send (send acquires the same non-reentrant mutex ⇒ deadlock); an exception in it leaves the mutex locked forever.

### 4. `get_server_timestamp()` — U
* Returns `server_ts` (float seconds = last `timeSync` ms / 1000). No network.

### 5. `re_subscribe_stream()` — S
* For each `"ACTIVE,size"` in `subscribe_candle`: `start_candles_one_stream(sp[0], sp[1])` (size passed as a **string**); for each in `subscribe_candle_all_size`: `start_candles_all_size_stream(ac)`; for each in `subscribe_mood`: `start_mood_stream(ac)`. Each of the 3 loops is wrapped in one try/except (first failure skips the rest of that group, warning logged).
* Called by `connect()` right after a successful `api.connect()`.
* Quirks: (a) `subscribe_mood` is never populated (bug in #62) so moods are never re-subscribed. (b) After `connect()` the new `IQOptionAPI` has an empty `real_time_candles_maxdict_table`; the first incoming `candle-generated` then evaluates `len(...) < {}` → `TypeError` inside `on_message` → mutex never released → all later sends deadlock (see §2 note). (c) Each stream blocks up to 20 s.

### 6. `set_session(header, cookie)` — C
* `SESSION_HEADER = header; SESSION_COOKIE = cookie` (replaces the default UA dict entirely). Applied at next `connect()` via `api.set_session(headers=..., cookies=...)`: `session.headers.update(headers)`, `session.cookies.clear_session_cookies()`, `requests.utils.add_dict_to_cookiejar(session.cookies, cookies)`.

### 7. `get_ssid()` — C
* Returns `global_value.SSID[self.api.object_id]` (KeyError possible).

### 8. `setting_2FA_TOKEN(code)` — C
* `self._2FA_TOKEN = code`; sent as form field `token` by the next `connect()` login.

### 9. `TWO_FA(token, method=None, code=None)` — C
* HTTP `POST https://auth.iqoption.com/api/v2/verify/2fa`, extra header `{"CONTENT-TYPE": "application/json"}`, raw body `json.dumps({"method": method, "token": token, "code": code})` e.g. `{"method": "sms", "token": "abc", "code": null}`.
* Returns `json.loads(r.text)` (dict). No status check; raises on non-JSON. Needs `self.api` (i.e. after a first, failed `connect()`). Flow in §3.7.

### 10. `close()` — C
* `try: self.api.close()` (→ `WebSocketApp.close()`); `if auto_logout: self.api.logout()` (HTTP POST logout); every exception swallowed (debug log).
* Quirk: `global_value.auto_reconnect[object_id]` stays `True`, so websocket-client's teardown (0.58 semantics) calls `on_close`, which calls `wss.run_forever()` again and `on_open` re-logs-in over HTTP + re-authenticates (§3.5). `close()` therefore does **not** stop the socket. A port should set auto_reconnect=false before closing.

### 11. `connect()` → `(True, None)` | `(False, reason)` — C
Full sequence in §3. Summary:
1. `try: self.api.close()` (AttributeError on first call; swallowed). NB: closing the previous socket triggers its own auto-reconnect (see #10) — an orphan connection can survive.
2. `try: self.set_ssid = global_value.SSID[self.api.object_id]` (re-use previous SSID).
3. `self.api = IQOptionAPI("iqoption.com", email, password, http_proxy_host=..., http_proxy_port=..., http_proxy_auth=..., set_ssid=self.set_ssid, auto_logout=..., _2FA_TOKEN=self._2FA_TOKEN)`; `self.api.set_session(headers=SESSION_HEADER, cookies=SESSION_COOKIE)`; `check, reason = self.api.connect()`.
4. If check: `re_subscribe_stream()`; busy-wait `global_value.balance_id[oid] == None` (set by the `balances` message — **always the PRACTICE balance**); `balances = self.get_balances()`; for each `i in balances["msg"]`: `position_change_all("subscribeMessage", i["id"])`; for each name in `["internal-billing.balance-created","internal-billing.auth-balance-changed","internal-billing.balance-changed","internal-billing.marginal-changed","tournaments.user-registered-in-tournament","tournaments.user-tournament-position-changed"]`: `api.subscribeMessage_routingFilters_None(name)`; `order_changed_all("subscribeMessage")`; `api.setOptions("", True)`; return `(True, None)`.
5. Else return `(False, reason)`.
* Quirks: no timeout anywhere (hangs if no `balances`, or if account has no type-4 balance). Every `connect()` silently switches the active balance back to PRACTICE (balance_id reset to None per new websocket). `update_ACTIVES_OPCODE()` is NOT called (commented out).

### 12. `check_connect()` → bool — C
* `global_value.check_websocket_if_connect[oid] != 0` (None before first open ⇒ True; 1 after `on_open`; 0 after `on_close`).

### 13. `get_all_ACTIVES_OPCODE()` — I
* Returns the module-level `OP_code.ACTIVES` dict object (process-wide, mutable, shared by all instances).

### 14. `update_ACTIVES_OPCODE()` — I
* Calls only `get_ALL_Binary_ACTIVES_OPCODE()` (`get_All_Digital_ACTIVES_OPCODE` and `instruments_input_all_in_ACTIVES` are commented out). Returns None.

### 15. `get_name_by_activeId(activeId)` — I
* `info = get_financial_information(activeId)`; returns `info["msg"]["data"]["active"]["name"]`, or `None` on KeyError/TypeError.

### 16. `get_financial_information(activeId)` — I
* `api.financial_information = None`, then `SEND("sendMessage", msg, "")`:
```json
{"name": "get-financial-information", "version": "1.0", "body": {"query": "<QUERY>", "operationName": "GetAssetProfileInfo", "variables": {"activeId": <activeId raw>}}}
```
  `<QUERY>` is this exact Python string literal (the `\n` are real newlines, serialised by json as `\n`):
```
"query GetAssetProfileInfo($activeId:ActiveID!, $locale: LocaleName){\n active(id: $activeId) {\n id\n name(source: TradeRoom, locale: $locale)\n ticker\n media {\n siteBackground\n }\n charts {\n dtd {\n change\n }\n m1 {\n change\n }\n y1 {\n change\n }\n ytd {\n change\n }\n }\n index_fininfo: fininfo {\n ... on Index {\n description(locale: $locale)\n }\n }\n fininfo {\n ... on Pair {\n type\n description(locale: $locale)\n currency {\n name(locale: $locale)\n }\n base {\n name(locale: $locale)\n ... on Stock {\n company {\n country {\n nameShort\n }\n gics {\n sector\n industry\n }\n site\n domain\n }\n keyStat {\n marketCap\n peRatioHigh\n }\n }\n ... on CryptoCurrency {\n site\n domain\n coinsInCirculation\n maxCoinsQuantity\n volume24h\n marketCap\n }\n }\n }\n }\n }\n }"
```
* Waits: busy-wait on `api.financial_information` ← router `financial-information` (stores the **whole message**). No rid correlation, no timeout.
* Returns the whole message dict (`{"name":"financial-information","msg":{"data":{"active":{...}}},...}`).

### 17. `get_leader_board(country, from_position, to_position, near_traders_count, user_country_id=0, near_traders_country_count=0, top_country_count=0, top_count=0, top_type=2)` — X
* `country_id = Country.ID[country]` (country_id.py: `"Worldwide": 0`, ISO-2 codes → ids; appendix B). `api.leaderboard_deals_client = None`, then:
```json
{"name": "request-leaderboard-deals-client", "version": "1.0", "body": {"country_id": <country_id>, "user_country_id": <user_country_id>, "from_position": <from_position>, "to_position": <to_position>, "near_traders_country_count": <near_traders_country_count>, "near_traders_count": <near_traders_count>, "top_country_count": <top_country_count>, "top_count": <top_count>, "top_type": <top_type>}}
```
  outer `sendMessage`, rid `""`. All values raw (no casts).
* Waits: busy-wait `api.leaderboard_deals_client` ← `leaderboard-deals-client` (`message["msg"]`).
* Returns that msg dict.

### 18. `get_active_exposure(instrument_type, active, duration, currency="USD")` — X
* `rid`; `active_id = ACT(active)`; expiration computed inside the channel:
  * `instrument_type == "digital-option"` → `exp = get_digital_exp(int(server_ts), duration)` (int when duration==1, otherwise a **float** that keeps the current seconds — §4.4);
  * else `exp, _ = get_expiration_time(int(server_ts), duration)` (int).
```json
{"name": "get-active-exposure", "version": "1.0", "body": {"instrument_type": <instrument_type>, "active_id": <active_id>, "time": <exp>, "currency": <currency>}}
```
  outer `sendMessage`, request_id = rid.
* Waits: **after** sending, sets `api.active_exposure[rid] = None`, then busy-waits ← `active-exposure` stored at `active_exposure[message["request_id"]]` (whole message). Then `del`.
* Returns `{"call": msg["msg"]["call"], "put": msg["msg"]["put"]}`; `{}` if those keys are missing (debug log).
* Quirk: race — the None assignment happens after the send; if the reply is processed first it is overwritten ⇒ infinite wait.

### 19. `get_instruments(type, polling=0)` — I
* `time.sleep(0.1)`; `api.instruments = None`; loop while None: `SEND("sendMessage", {"name": "get-instruments", "version": "4.0", "body": {"type": <type>}})` then busy-wait ≤10 s; on exception `self.connect()` (full reconnect); `time.sleep(polling)`. Re-sends every ~10 s forever.
* `type`: `"crypto"`/`"forex"`/`"cfd"`. Waits ← `instruments` (`message["msg"]`).
* Returns msg dict (`{"instruments": [{"id":..., "active_id":..., "name":..., "schedule":[{"open":..,"close":..}], ...}], ...}`).

### 20. `instruments_input_to_ACTIVES(type)` — I
* `for ins in get_instruments(type)["instruments"]: OP_code.ACTIVES[ins["id"]] = ins["active_id"]`. Mutates the global map. Returns None.

### 21. `instruments_input_all_in_ACTIVES()` — I
* `instruments_input_to_ACTIVES` for `"crypto"`, `"forex"`, `"cfd"` (in that order).

### 22. `get_All_Digital_ACTIVES_OPCODE()` — I
* `data = get_digital_underlying_list_data()` (polling 0); `for list_data in data["underlying"]: OP_code.ACTIVES[list_data["name"]] = list_data["active_id"]`. TypeError if the 30 s wait timed out (None).

### 23. `get_ALL_Binary_ACTIVES_OPCODE()` — I
* `init_info = get_all_init_v2()`; for `dirr in ["binary", "turbo"]`, for each key `i` (string active id) of `init_info[dirr]["actives"]`: if `active["exchange"] == ""` **or** `active["provider"] == "OTC"` ⇒ `OP_code.ACTIVES[active["ticker"]] = int(i)`. TypeError if init returned None.

### 24. `get_all_init()` — I — **DUPLICATE/OLD VERSION OF `get_all_init_v2`**
* Outer `while True`: `api.api_option_init_all_result = None`; send (retrying with `self.connect(); sleep(5)` on exception) the legacy frame `{"name": "api_option_init_all", "msg": "", "request_id": ""}` (msg is the empty **string**); busy-wait ≤30 s on ← `api_option_init_all_result` (`message["msg"]`); if `result["isSuccessful"] is True` return it, else loop again (infinite retries).
* Returns `{"isSuccessful": true, "result": {"binary": {"actives": {...}}, "turbo": {"actives": {...}}}, ...}`.
* Legacy endpoint; v2 is newer (versioned `sendMessage` API, returns None on timeout instead of looping forever).

### 25. `get_all_init_v2(polling=3)` — I — **newest init**
* `api.api_option_init_all_result_v2 = None`; `SEND("sendMessage", {"name": "get-initialization-data", "version": "3.0", "body": {}})`; then `while result is None: if elapsed >= 30: return None; time.sleep(polling); re-SEND same request`.
* Waits ← `initialization-data` (`message["msg"]`).
* Returns `{"binary": {"actives": {"<id>": {...ticker, name, enabled, is_suspended, exchange, provider, option:{profit:{commission}}...}}}, "turbo": {...}, "blitz": {...}?, ...}` or `None` after 30 s.
* Quirks: sleeps `polling` before the first check (≈3 s minimum latency); re-sends the request every `polling` seconds; warning log on timeout.

### 26. `get_all_open_time(polling)` — I (`polling` is required)
* `OPEN_TIME = nested_dict(3, dict)`.
* Binary/turbo: `get_all_init_v2(polling)`; for each active in `[option]["actives"]`: `name = str(active["name"]).split(".")[1]` (e.g. `"front.EURUSD"` → `EURUSD`); `OPEN_TIME[option][name]["open"] = (not is_suspended) if enabled else enabled`.
* Digital: `get_digital_underlying_list_data(polling)["underlying"]`; `name = d["underlying"]`; open = False, then True if any `schedule` entry has `open < time.time() < close` (local clock, strict).
* `"cfd","forex","crypto"`: `get_instruments(t, polling)["instruments"]`; `name = detail["name"]`; same schedule test.
* Returns the nested defaultdict `{"binary": {name: {"open": bool}}, "turbo": ..., "digital": ..., "cfd": ..., "forex": ..., "crypto": ...}`. Crashes (TypeError) if any underlying call timed out with None.

### 27. `get_binary_option_detail()` — I (uses OLD `get_all_init`)
* `detail = nested_dict(2, dict)`; for turbo then binary actives of `get_all_init()["result"][...]["actives"]`: `name = name[name.index(".")+1:]`; `detail[name]["turbo"|"binary"] = active_dict`.
* Returns `{name: {"turbo": {...}, "binary": {...}}}`.

### 28. `get_all_profit()` — B — **DUPLICATE/OLD VERSION OF `get_all_profit_v2`**
* `get_all_init()` (v1). For turbo then binary: `name` = text after first `"."`; if `enabled == False` ⇒ `all_profit[name][type] = {}` (empty dict!) and continue; else `(100.0 - active["option"]["profit"]["commission"]) / 100.0`.
* Returns nested defaultdict `{name: {"turbo": 0.87, "binary": 0.8}}`.

### 29. `get_all_profit_v2()` — B — **newest**
* `get_all_init_v2()` (default polling 3); returns `{}` if None. For `option_type in ["turbo", "binary"]`, for each active: skip if `is_suspended`; `name = active["ticker"]` (NOT the "front." name); skip if not `enabled`; `all_profit[name][option_type] = (100.0 - commission) / 100.0`.
* Returns `defaultdict(dict)` `{ticker: {"turbo": x, "binary": y}}`. Keyed by ticker ⇒ matches `ACTIVES` keys after `update_ACTIVES_OPCODE`.

### 30. `get_all_profit_blitz_v2()` — B
* Same as #29 but only `option_type in ["blitz"]`, skipped if `"blitz" not in init_info`. Returns `{ticker: {"blitz": x}}` or `{}`.

### 31. `get_profile()` — A
* `SEND("sendMessage", {"name": "get-profile", "version": "1.0", "body": {}}, rid)`.
* Waits: busy-wait while `api.profile.msg is None` — but `profile.msg` is **not reset** before sending, so it returns the profile cached at connect immediately; the fresh `profile` reply overwrites it later. Router `profile` ⇒ `api.profile.msg = message["msg"]`.
* Returns profile msg dict.

### 32. `get_profile_ansyc()` — A — **DUPLICATE of `get_profile` without the request** (typo for "async")
* Busy-wait `profile.msg is None`; return it.

### 33. `get_currency()` — A
* `for balance in get_balances()["msg"]: if balance["id"] == balance_id: return balance["currency"]`; else None.

### 34. `get_balance_id()` — A
* Returns `global_value.balance_id[oid]`.

### 35. `get_balance()` — A — **OLD/unreliable; see `get_balance_v2` / `_get_balances`**
* `api.get_balances()` (sends `get-balances`) then `self.get_balances()` (sends it **again** and returns the *cached* `balances_raw` immediately if not None). Returns `balance["amount"]` for the current balance_id, else None.
* Quirk: returns the amount from the previous `balances` message (stale) — the reply to this call is not awaited.

### 36. `get_balance_v2()` — A — **newest**
* If `balance_id in api.balance` (filled by pushed `balance-changed` events: `api.balance[current_balance.id] = current_balance.amount`) return it; else `api.balance[id] = get_balance()` and return it. No network if cached.

### 37. `_get_balances()` — A — **correct blocking variant of `get_balances`**
* `api.balances_raw = None`; `SEND("sendMessage", {"name": "get-balances", "version": "1.0"})` (no body key); busy-wait ← `balances` (whole message). Returns whole message.

### 38. `get_balances()` — A
* Sends the same `get-balances` frame; if `api.balances_raw is not None` return it immediately (cached/stale); else poll every 0.1 s until set. Returns whole message `{"name": "balances", "msg": [{"id":..., "type":1|4|..., "amount":..., "currency":...}, ...], ...}`.

### 39. `get_balance_mode()` — A
* For balances in `get_balances()["msg"]` with `id == balance_id`: `type == 1` → `"REAL"`, `type == 4` → `"PRACTICE"`; otherwise/none → `None`.

### 40. `reset_practice_balance()` — A
* `api.training_balance_reset_request = None`; `SEND("sendMessage", {"name": "reset-training-balance", "version": "2.0"})`.
* Busy-wait ← `training-balance-reset` (`message["msg"]["isSuccessful"]`). Returns that bool.

### 41. `position_change_all(Main_Name, user_balance_id)` — U (subscription helper used by connect)
* For `ins in ["cfd","forex","crypto","digital-option","turbo-option","binary-option"]`: `api.portfolio(Main_Name=Main_Name, name="portfolio.position-changed", instrument_type=ins, user_balance_id=user_balance_id)` ⇒ outer `Main_Name` (`subscribeMessage`/`unsubscribeMessage`), rid `""`:
```json
{"name": "portfolio.position-changed", "version": "2.0", "params": {"routingFilters": {"instrument_type": "<ins>", "user_balance_id": <user_balance_id raw>}}}
```
* Fire-and-forget; returns None.

### 42. `order_changed_all(Main_Name)` — U
* Same 6 instrument types, outer `Main_Name`, rid `""`:
```json
{"name": "portfolio.order-changed", "version": "1.0", "params": {"routingFilters": {"instrument_type": "<ins>"}}}
```

### 43. `portfolio_get_positions(limit=1, offset=0, polling_time=1)` — M — **newer variant of `get_positions`**
* `api.positions = None`; `api.portfolio(Main_Name="sendMessage", name="portfolio.get-positions", instrument_type=<the Python list>, limit=limit, offset=offset)`:
```json
{"name": "portfolio.get-positions", "version": "3.0", "body": {"instrument_type": "['cfd', 'forex', 'crypto', 'digital-option', 'turbo-option', 'binary-option']", "limit": <int(limit)>, "offset": <int(offset)>}}
```
  NB: `instrument_type` is `str(list)` — the literal Python repr string above (bug, but byte-exact behaviour).
* Waits ← `positions` (whole message) polling every `polling_time` s (sleep before re-check), no timeout. Shares the `api.positions` slot with #102.
* Returns whole message.

### 44. `get_balance_id_by_mode(mode)` — A
* Iterates `get_balances()["msg"]`: last `type==1` id → real_id, last `type==4` id → practice_id. `mode=="REAL"` → real_id, `"PRACTICE"` → practice_id, else None. UnboundLocalError if that type does not exist.

### 45. `change_balance(Balance_MODE)` — A
* If `global_value._tmp_raw_balance_id[oid] == {}`: fill `["REAL"]` (type 1) and `["PRACTICE"]` (type 4) from `get_balances()["msg"]`.
* `"REAL"`/`"PRACTICE"` ⇒ `global_value.balance_id[oid] = id`; anything else ⇒ error log + `raise ValueError(f"Unknown balance mode: {Balance_MODE!r}")`.
* **Sends nothing to the server** (no set-user-settings, no portfolio re-subscribe); only changes the `user_balance_id` used by later requests. Cache is keyed by object_id ⇒ rebuilt after reconnect, and `connect()` resets the selection to PRACTICE. Missing REAL ⇒ defaultdict yields `{}` and later `int({})` fails.

### 46. `get_candles(ACTIVES, interval, count, endtime)` — S
* `api.candles.candles_data = None`; loop: `SEND("sendMessage", msg)`:
```json
{"name": "get-candles", "version": "2.0", "body": {"active_id": <int(ACT(ACTIVES))>, "size": <interval raw>, "to": <int(endtime)>, "count": <count raw>, "": <ACT(ACTIVES) raw>}}
```
  (yes, a key that is the empty string `""`, value = active id).
* Waits: `while self.check_connect and candles_data is None: pass` — `check_connect` is **not called** (method object is always truthy) ⇒ plain busy-wait, no timeout. On exception: `self.connect()` and retry. Router `candles` ⇒ `candles.candles_data = message["msg"]["candles"]`. No rid correlation (concurrent calls race).
* Returns list of candle dicts as sent by server (`id, from, at, to, open, close, min, max, volume`).

### 47. `start_candles_stream(ACTIVE, size, maxdict)` — S — public wrapper
* `size == "all"`: for every `s` in `self.size` (19 sizes): `full_realtime_get_candle(ACTIVE, s, maxdict)` then `api.real_time_candles_maxdict_table[ACTIVE][s] = maxdict`; then `start_candles_all_size_stream(ACTIVE)`.
* `size in self.size`: set `maxdict_table[ACTIVE][size] = maxdict`; `full_realtime_get_candle(...)`; `start_candles_one_stream(ACTIVE, size)`.
* else error log. Always returns None (stream bool discarded).

### 48. `stop_candles_stream(ACTIVE, size)` — S
* `"all"` → `stop_candles_all_size_stream(ACTIVE)`; valid size → `stop_candles_one_stream`; else error log. Returns None.

### 49. `get_realtime_candles(ACTIVE, size)` — S
* `"all"` → `api.real_time_candles[ACTIVE]` (`{size: {from: candle}}`); valid size → `api.real_time_candles[ACTIVE][size]` (`{from_ts: candle}`); invalid size → error log + `False`.
* Quirk: the `except KeyError: return False` branches are unreachable (nested defaultdict auto-creates) ⇒ unknown asset returns an empty dict.

### 50. `get_all_realtime_candles()` — S
* Returns `api.real_time_candles` (`nested_dict(3, dict)`: `[active_name][size][from] = candle`).

### 51. `full_realtime_get_candle(ACTIVE, size, maxdict)` — S
* `candles = get_candles(ACTIVE, size, maxdict, server_ts)`; for each: `api.real_time_candles[str(ACTIVE)][int(size)][can["from"]] = can`. (Seeds buffer; no maxdict trimming here.)

### 52. `start_candles_one_stream(ACTIVE, size)` → bool — S
* Append `ACTIVE + "," + str(size)` to `subscribe_candle` if absent; `api.candle_generated_check[str(ACTIVE)][int(size)] = {}`; loop up to 20 s: if check `is True` → return True; else `SEND("subscribeMessage", msg)` then `sleep(1)` (re-subscribes every second until the first candle arrives); on send exception → `self.connect()`.
```json
{"name": "candle-generated", "params": {"routingFilters": {"active_id": "<str(ACT(ACTIVE))>", "size": <int(size)>}}}
```
  (no `version` key; `active_id` is a **string**.)
* Returns True, or False after 20 s (error log).
* Router `candle-generated` sets the check to True and buffers the candle (§4.8). Requires `real_time_candles_maxdict_table[ACTIVE][size]` to be set first (only `start_candles_stream` does that) or the router crashes (§2 note).

### 53. `stop_candles_one_stream(ACTIVE, size)` → True — S
* Remove from `subscribe_candle`; loop: if `candle_generated_check[ACTIVE][size] == {}` → return True; else set it to `{}`, `SEND("unsubscribeMessage", msg)`, `sleep(1)`. Loops until one full second passes with no new candle.
```json
{"name": "candle-generated", "params": {"routingFilters": {"active_id": "<str(active_id)>", "size": <int(size)>}}}
```
* Quirk: if the check is already `{}` (never received data) it returns True **without** sending unsubscribe.

### 54. `start_candles_all_size_stream(ACTIVE)` → bool — S
* `candle_generated_all_size_check[str(ACTIVE)] = {}`; add to `subscribe_candle_all_size`; same 20 s / 1 s re-send loop with:
```json
{"name": "candles-generated", "params": {"routingFilters": {"active_id": "<str(ACT(ACTIVE))>"}}}
```
  outer `subscribeMessage`. True when router `candles-generated` sets the check True.

### 55. `stop_candles_all_size_stream(ACTIVE)` — S
* Same pattern as #53 with outer `unsubscribeMessage` and the `candles-generated` msg above; returns None.

### 56. `subscribe_top_assets_updated(instrument_type)` — X
* `SEND("subscribeMessage", {"name": "top-assets-updated", "params": {"routingFilters": {"instrument_type": "<str(instrument_type)>"}}, "version": "1.2"})`. Fire-and-forget.

### 57. `unsubscribe_top_assets_updated(instrument_type)` — X
* Same msg with outer `unsubscribeMessage`.

### 58. `get_top_assets_updated(instrument_type)` — X
* Returns `api.top_assets_updated_data[instrument_type]` (router `top-assets-updated`: `[str(msg.instrument_type)] = msg.data`) or None.

### 59. `subscribe_commission_changed(instrument_type)` — X
* `SEND("subscribeMessage", {"name": "commission-changed", "params": {"routingFilters": {"instrument_type": "<str(instrument_type)>"}}, "version": "1.0"})`. instrument_type: binary-option/turbo-option/digital-option/crypto/forex/cfd.

### 60. `unsubscribe_commission_changed(instrument_type)` — X
* Same with `unsubscribeMessage`.

### 61. `get_commission_change(instrument_type)` — X
* Returns `api.subscribe_commission_changed_data[instrument_type]` = `{active_name: {server_ts_at_receipt (float s): int(commission.value)}}` (auto-vivified empty dict if none).

### 62. `start_mood_stream(ACTIVES)` — X
* `if ACTIVES in self.subscribe_mood == False:` — Python chained comparison ⇒ `(ACTIVES in list) and (list == False)` ⇒ always False ⇒ **never appended** (bug).
* Loop: `SEND("subscribeMessage", msg)`; if `api.traders_mood[ACT(ACTIVES)]` exists → break, else `sleep(5)` and re-send. No timeout.
```json
{"name": "traders-mood-changed", "params": {"routingFilters": {"instrument": "turbo-option", "asset_id": <ACT(ACTIVES) raw int>}}}
```
  (no version). Returns None.

### 63. `stop_mood_stream(ACTIVES)` — X
* Same chained-comparison bug (`del` never executes; would also be wrong — `del list[str]`). `SEND("unsubscribeMessage", <same msg as #62>)`.

### 64. `get_traders_mood(ACTIVES)` — X
* Returns `api.traders_mood[ACT(ACTIVES)]` (router `traders-mood-changed`: `[msg.asset_id] = msg.value`, the "higher"/call fraction). KeyError if not yet received.

### 65. `get_all_traders_mood()` — X
* Returns `api.traders_mood` dict `{asset_id: value}`.

### 66. `raw_check_win(external_id)` — B (helper of #67)
* `position_data = get_history_positions(external_id)`; finds last row in `position_data["msg"]["positions"]` with `row["external_id"] == external_id`; returns `round(row["pnl_realized"], 2)` (Python banker's rounding on binary float) or `None`.

### 67. `check_win(external_id, polling_time=0.5)` — B/M — rewritten "v1" (portfolio-history based)
* `check, data = get_order(external_id)` (sends `get-order`, §99). Inner `get_position_id()`: if `check` (it is a margin order): loop `get_order` every `polling_time` until `data["position_id"]` not None; if `data["status"] == "canceled"` → return `"canceled"`; result cached. If not check (binary option id) → use `external_id` itself.
* Loop: `"canceled"` → return **None**; `PL = raw_check_win(id)`; non-None → return PL; else `sleep(polling_time)`.
* Returns profit/loss float rounded to 2 dp (pnl_realized; for binary a loss is `-amount`), or None if cancelled. Infinite wait otherwise. Each poll costs a `get-history-positions` round-trip.
* **Duplicate group** `check_win` / `check_win_v2` / `check_win_v3`: v2 is the legacy `api_game_betinfo` API (oldest); v3 is event-driven (no requests, uses pushed `option-closed`), and is what the docs call "great way" for binary/turbo; this `check_win` is the most recent rewrite and the most general (also handles margin orders via `get-order` → position_id). Recommend porting v3 for binary and `check_win` for margin/general.

### 68. `check_win_v2(id_number, polling_time)` — B — **DUPLICATE/OLD VERSION OF check_win_v3**
* Loop: `check, data = get_betinfo(id_number)`; `win = data["result"]["data"][str(id_number)]["win"]`; if `check and win != ""` → return `profit - deposit` from that row (KeyError/TypeError logged and loop continues); `sleep(polling_time)`.
* Quirk: if `get_betinfo` returns `(False, None)` → `None["result"]` TypeError propagates (crash).

### 69. `check_win_v3(id_number, polling_time)` — B — **newest/best for binary/turbo**
* Loop (sleep `polling_time`) until `get_async_order(id_number)["option-closed"] != {}` (router `option-closed`: `order_async[int(msg.option_id)]["option-closed"] = message`). Returns `closed["msg"]["profit_amount"] - closed["msg"]["amount"]`.
* No request sent; relies on server push (enabled by `setOptions sendResults=true` at connect). No timeout. Docs call it with one arg but `polling_time` has no default.

### 70. `get_betinfo(id_number)` → `(bool|None, dict|None)` — B (legacy)
* Loop: `api.game_betinfo.isSuccessful = None`; `SEND("api_game_betinfo", data)` (outer name is the channel name; no sendMessage wrapper), rid `""`:
  * `data = {"currency": "USD"}` then for a list: `data["id[0]"] = int(v0)`, `data["id[1]"] = ...`; scalar: `data["id[0]"] = int(id_number)`; None: error log, sends `{"currency": "USD"}` only. E.g. `{"currency": "USD", "id[0]": 123}`.
  * On send exception → `self.connect()`.
* Waits ← `api_game_betinfo_result` (`isSuccessful = msg["isSuccessful"]`, `dict = msg`). After 10 s it calls `self.connect()` + re-send + `sleep(1)` **on every subsequent iteration** (start not reset ⇒ reconnect storm).
* Returns `(True, msg)` if isSuccessful is True, else `(isSuccessful, None)` (i.e. `(False, None)`).

### 71. `get_optioninfo(limit)` — B — **DUPLICATE/OLD VERSION OF get_optioninfo_v2**
* `api.api_game_getoptions_result = None`; `SEND("api_game_getoptions", {"limit": <int(limit)>, "user_balance_id": <BAL>})`. Busy-wait ← `api_game_getoptions_result` (whole message). Returns whole message.

### 72. `get_optioninfo_v2(limit)` — B — newer
* `api.get_options_v2_data = None`; `SEND("sendMessage", {"name": "get-options", "body": {"limit": <limit raw>, "instrument_type": "binary,turbo", "user_balance_id": <BAL>}})` (no version key).
* Busy-wait ← `options` (whole message). Returns whole message.

### 73. `buy_multi(price, ACTIVES, ACTION, expirations, polling=0.1)` → list — B
* Allocates one `rid` per element (even if lengths mismatch). If `len(price)==len(ACTIVES)==len(ACTION)==len(expirations)`: for each idx `api.buyv3(price[i], ACT(ACTIVES[i]), ACTION[i], expirations[i], rid[i])` (frame in #76). Then poll every `polling` s until every rid is a key of `api.buy_multi_option` (router `option`: `buy_multi_option[str(request_id)] = msg`).
* Returns list: `None` where the msg has key `"message"` (error), else `msg["id"]`. Entries are never deleted. Mismatched lengths → error log, returns None.

### 74. `get_remaning(duration, time=None)` → int | "ERROR duration" — U/B
* `time` defaults to `server_ts` (float). Iterates `get_remaning_time(time)` (§4.3) and returns the seconds-remaining value whose label equals `duration` (labels 1..5 and 15,30,...,165). Unknown duration → error log + the **string** `"ERROR duration"`.

### 75. `buy_by_raw_expirations(price, active, direction, option, expired)` — B
* `rid`; `api.buy_multi_option[rid] = None`; active int used as-is, str → `ACT(active)`. `option`: `"turbo"`→3, `"binary"`→1, int → as-is (other strings ⇒ UnboundLocalError).
```json
{"body": {"price": <price raw>, "active_id": <active_id>, "expired": <int(expired)>, "direction": "<direction.lower()>", "option_type_id": <1|3|int>, "user_balance_id": <BAL>}, "name": "binary-options.open-option", "version": "1.0"}
```
  outer `sendMessage`, request_id = rid.
* Waits ← `option` (`buy_multi_option[str(rid)] = msg`) polling 0.1 s. The in-loop `"message"` check is dead code (value is None there).
* Returns `(True, msg)` (whole msg dict, **even when it is an error** containing `"message"`); entry deleted. Never returns False in practice.

### 76. `buy(price, ACTIVES, ACTION, expirations, polling=0.1)` → `(bool, id|message)` — B — main binary buy
* `rid`; `buy_multi_option[rid] = None`; `api.buyv3(price, ACT(ACTIVES), ACTION, expirations, rid)`:
  * `exp, idx = get_expiration_time(int(time.time()), duration)` — **local clock**, not server_ts (§4.2); `option_type_id = 3` (turbo) if `idx < 5` else `1` (binary).
```json
{"body": {"price": <price raw>, "active_id": <ACT(ACTIVES)>, "expired": <int(exp)>, "direction": "<ACTION.lower()>", "option_type_id": <3|1>, "user_balance_id": <BAL>}, "name": "binary-options.open-option", "version": "1.0"}
```
  outer `sendMessage`, request_id = rid.
* Waits ← `option` keyed by `str(request_id)`, polling `polling` s, no timeout.
* Returns `(False, msg["message"])` if the msg has `"message"`, else `(True, msg["id"])` (KeyError if neither). Entry never deleted (leak).
* `expirations` = duration in minutes (1–5 turbo; ≥15 picks a quarter-hour binary expiry).

### 77. `buy_blitz(price, ACTIVES, direction, duration=30, polling=0.1)` → `(bool, msg|message)` — B
* `rid`; `buy_multi_option[rid] = None`; `user_balance_id = global_value.balance_id[oid]` (raw, not int-cast); `active_id = ACT(ACTIVES)` (KeyError raised before the friendlier ValueError check below).
* Validation: `is_otc = ACTIVES.upper().endswith('-OTC')`; OTC: 30 ≤ duration ≤ 300, else 60 ≤ duration ≤ 900, otherwise `ValueError(...)`.
* `expired = server_ts + duration` (float; int() in channel; not minute-aligned); `profit_percent = int(get_all_profit_blitz_v2()[ACTIVES]["blitz"] * 100)` if present else 0 — NB this triggers a blocking `get-initialization-data` round-trip (≥3 s) on **every** buy.
```json
{"body": {"user_balance_id": <balance_id raw>, "active_id": <active_id>, "option_type_id": 12, "direction": "<direction.lower()>", "expired": <int(expired)>, "price": <price raw>, "profit_percent": <int>, "expiration_size": <duration>}, "name": "binary-options.open-option", "version": "2.0"}
```
  outer `sendMessage`, request_id = str(rid).
* Waits ← `option` polling. Returns `(False, msg["message"])` (entry not deleted) or `(True, msg)` (whole dict; entry deleted). NB different success shape from `buy` (dict vs id).

### 78. `sell_option(options_ids)` — B
* `rid`; non-list wrapped into a list; `SEND("sendMessage", {"name": "sell-options", "version": "2.0", "body": {"options_ids": [<ids raw>]}}, rid)`.
* Returns None immediately (fire-and-forget). Router stores `sold-options` at `sold_options_respond[message["request_id"]]` but nothing reads it.

### 79. `get_digital_underlying_list_data(polling=0)` — D/I
* `api.underlying_list_data = None`; `SEND("sendMessage", {"name": "digital-option-instruments.get-underlying-list", "version": "3.0", "body": {"type": "digital-option"}})`; wait ≤30 s sleeping `polling` (0 ⇒ busy) ← `underlying-list` (`message["msg"]`).
* Returns msg `{"underlying": [{"active_id", "name", "underlying", "schedule": [...]}, ...]}` or None on timeout.

### 80. `get_strike_list(ACTIVES, duration)` → `(raw_msg, dict|None)` — D
* `api.strike_list = None`; channel computes `exp = get_digital_expiration_time(duration)` (§4.5):
```json
{"name": "get-strike-list", "body": {"type": "digital-option", "underlying": "<ACTIVES name>", "expiration": <int(exp)*1000>, "period": <duration*60>}, "version": "4.0"}
```
  outer `sendMessage`, rid `""`.
* Busy-wait ← `strike-list` (whole message).
* Returns `(raw_message, ans)` where `ans = {"%.6f" % (float(s["value"]) * 10e-7): {"call": s["call"]["id"], "put": s["put"]["id"]}}` over `raw["msg"]["strike"]` (10e-7 == 1e-6); on parse error `(raw_message, None)`.

### 81. `subscribe_strike_list(ACTIVE, expiration_period)` — D
* `SEND("subscribeMessage", msg)`, expiration_period in minutes:
```json
{"name": "price-splitter.client-price-generated", "params": {"routingFilters": {"instrument_type": "digital-option", "expiration_period": <int(expiration_period*60)>, "asset_id": <int(ACT(ACTIVE))>}}, "version": "1.0"}
```
* Incoming name is `client-price-generated` (router §2).

### 82. `unsubscribe_strike_list(ACTIVE, expiration_period)` — D
* `SEND("unsubscribeMessage", {"name": "price-splitter.client-price-generated", "params": {"routingFilters": {"instrument_type": "digital-option", "asset_id": <int(ACT(ACTIVE))>}}, "version": "1.0"})` — **no expiration_period** (asymmetric with subscribe; parameter ignored).

### 83. `get_instrument_quites_generated_data(ACTIVE, duration)` — D
* Busy-wait while `api.instrument_quotes_generated_raw_data[ACTIVE][duration*60] == {}`; returns the raw last `client-price-generated` message for that SPT period. Infinite if not subscribed.

### 84. `get_realtime_strike_list(ACTIVE, duration)` — D — **newest strike-list accessor** (v8.5 "fixed")
* Busy-wait until `api.instrument_quites_generated_data[ACTIVE][duration*60]` is truthy, return it: `{strike_key: {"call": {"id": symbol, "profit": pct|None}, "put": {"id": symbol, "profit": pct|None}}}` (strike_key is the server's `strike` string, `"SPT"` for spot). Old caching logic is commented out.
* Group: `get_strike_list` (request/response, ids only) vs `get_realtime_strike_list` (stream, ids+profit) vs `get_instrument_quites_generated_data` (raw stream message).

### 85. `get_top_asset(instrument_type)` — X
* `rid`; `api.get_top_assets_data[rid] = None` (set before send — correct); `SEND("sendMessage", {"name": "get-top-assets", "version": "1.2", "body": {"instrument_type": <instrument_type>}}, rid)`.
* Busy-wait ← `top-assets` (`get_top_assets_data[message["request_id"]] = message["msg"]`); entry deleted. Returns msg.

### 86. `get_digital_current_profit(ACTIVE, duration)` — D
* `p = api.digital_profit[ACTIVE][duration*60]` (set by `client-price-generated` from the SPT call ask, §4.7); returns `p` if `p != {}` else `False`. Can return `None` (no ask). Requires `subscribe_strike_list`.

### 87. `old_buy_digital_spot(active, amount, action, duration)` — D — **DUPLICATE/OLD VERSION OF buy_digital_spot**
* `rid`; action `'put'→'P'`, `'call'→'C'`, else error log + return **-1** (not a tuple).
* Expiry: §4.4 (`duration==1` → `get_expiration_time(ts,1)[0]`; else the +90 s / minute%duration loop). `instrument_id = "do" + active + utc(exp).strftime("%Y%m%d%H%M") + "PT" + str(duration) + "M" + action + "SPT"` e.g. `doEURUSD201907191250PT5MPSPT` (asset **name** format).
* `digital_option_placed[rid] = None`; `SEND("sendMessage", {"name": "digital-options.place-digital-option", "version": "1.0", "body": {"user_balance_id": <BAL>, "instrument_id": "<id>", "amount": "<str(amount)>"}}, rid)`.
* Busy-wait (no sleep) ← `digital-option-placed` keyed by `message["request_id"]`; delete; `id = msg["msg"]["id"]` else return `(False, whole_message)`. Then wait (sleep 0.1) until `len(order_async[int(id)]["position-changed"]) != 0`; return `(True, pc["msg"]["external_id"])` else `(False, None)`.

### 88. `buy_digital_spot(active, amount, action, duration)` — D — **newest spot buy**
* Same as #87 except: `dateFormated = utc(exp).strftime("%Y%m%dD%H%M")`; `instrument_id = "do" + str(ACT(active)) + "A" + dateFormated + "00" + "T" + str(duration) + "M" + action + "SPT"` e.g. `do1A20210224D154900T1MCSPT` (asset **id** format); uses `place_digital_option_v2` which (because id starts with `do`) parses `asset_id = instrument_id.split("do")[1].split("A")[0]`:
```json
{"name": "digital-options.place-digital-option", "version": "2.0", "body": {"user_balance_id": <BAL>, "instrument_id": "<id>", "instrument_index": 0, "amount": "<str(amount)>", "asset_id": <int(asset_id)>}}
```
  outer `sendMessage`, request_id = rid. Wait loop sleeps 0.1 s.
* Returns `(True, position_external_id)` | `(False, digital-option-placed message)` | `(False, None)` | `-1`.
* Note `amount` is sent as Python `str(amount)` (`"1"`, `"1.5"`, `"10.0"`).

### 89. `get_digital_spot_profit_after_sale(position_id)` → float|None — D
* Busy-wait `get_async_order(position_id)["position-changed"] != {}`; `position = ...["msg"]["raw_event"]`; then needs the raw `client-price-generated` stream (busy-waits forever unless `subscribe_strike_list(underlying, duration)` was called). Algorithm in §4.7.3. Quirk: `z` (is-call flag) is always False because `str.find()` returns -1/positive (truthy) — puts and calls are computed the same way. Cached bids default to `0` (nested_dict(2,int)), so the `f != None` guard never triggers.

### 90. `buy_digital(amount, instrument_id)` → `(True, order-changed msg)` | `(False, None)` — D (buy an arbitrary strike instrument id)
* `rid`; `digital_option_placed[rid] = None`; `place_digital_option_v2(instrument_id, amount, rid)`: if `instrument_id.find("do") == 0` → v2.0 body of #88 with `asset_id = int(id.split("do")[1].split("A")[0])` (ValueError for old name-format ids like `doEURUSD...`); else v1.0 body of #87.
* Busy-wait ← `digital-option-placed`; delete. If `"id" in msg["msg"]`: busy-wait (no sleep, no timeout) until `order_async[int(id)]` has key `"order-changed"` (router stores `order-changed` only for `microserviceName=="portfolio"` and `source=="digital-options"`); return `(True, that whole message)`. Else `(False, None)` (error text discarded).

### 91. `close_digital_option(external_id)` → bool — D
* `rid`; `api.result[rid] = None`; `SEND("sendMessage", {"name": "digital-options.close-position", "version": "1.0", "body": {"position_id": <int(external_id)>}}, rid)`.
* Busy-wait ← `result` (`api.result[str(request_id)] = msg["success"]` when request_id != ""). Returns that bool.

### 92. `close_digital_option_batch(external_id: list)` → bool — D
* Same with `{"name": "digital-options.close-position-batch", "version": "1.0", "body": {"position_ids": <list raw>}}`.

### 93. `check_win_digital(buy_order_id, polling_time)` — D — **DUPLICATE/OLD VERSION OF check_win_digital_v2**
* Loop: `sleep(polling_time)`; `data = get_digital_position(buy_order_id)` (sends `digital-options.get-position` each time); if `data["msg"]["position"]["status"] == "closed"`: `close_reason == "default"` → return `pnl_realized`; `== "expired"` → return `pnl_realized - buy_amount`. Any other close_reason ⇒ loops forever.

### 94. `check_win_digital_v2(buy_order_id)` → `(bool, float|None)` — D — newer (event-driven)
* Busy-wait `get_async_order(buy_order_id)["position-changed"] != {}`; `od = ...["msg"]`; if `od["status"] == "closed"`: `"expired"` → `(True, od["close_profit"] - od["invest"])`; `"default"` → `(True, od["pnl_realized"])`; other reason → implicit `None`. Not closed → `(False, None)` **immediately** (does not wait for closing; caller must loop).

### 95. `buy_order(instrument_type, instrument_id, side, amount, leverage, type, limit_price=None, stop_price=None, stop_lose_kind=None, stop_lose_value=None, take_profit_kind=None, take_profit_value=None, use_trail_stop=False, auto_margin_call=False, use_token_for_commission=False)` → `(bool, order_id|reject_status|None)` — M
* `rid`; `api.buy_order_id[rid] = None`; `SEND("sendMessage", msg, rid)` (body key order exactly):
```json
{"name": "place-order-temp", "version": "4.0", "body": {"instrument_type": "<str>", "instrument_id": "<str>", "side": "<str: buy|sell>", "amount": <float(amount)>, "leverage": <int(leverage)>, "type": <type raw: market|limit|stop>, "limit_price": <raw|null>, "stop_price": <raw|null>, "stop_lose_kind": <raw|null>, "stop_lose_value": <raw|null>, "take_profit_kind": <raw|null>, "take_profit_value": <raw|null>, "use_trail_stop": <bool>, "auto_margin_call": <bool>, "use_token_for_commission": <bool>, "user_balance_id": <BAL>, "client_platform_id": "9"}}
```
  (`amount` is always a float ⇒ `1` is sent as `1.0`; `client_platform_id` is the **string** `"9"`.) kinds: `"percent"|"price"|"diff"|None`.
* Busy-wait ← `order-placed-temp` (`buy_order_id[message["request_id"]] = msg["id"]`), no timeout (an error reply with a different name ⇒ hang). Then `check, data = get_order(order_id)`; while `data["status"] == "pending_new"`: re-`get_order`, `sleep(1)`.
* Returns `(True, order_id)` if status != "rejected"; `(False, data["reject_status"])` if rejected; `(False, None)` if check False — but `data` is None then and `data["status"]` already raised TypeError (crash).

### 96. `change_auto_margin_call(ID_Name, ID, auto_margin_call)` → `(bool, message)` — M
* `api.auto_margin_call_changed_respond = None`; `SEND("sendMessage", {"name": "change-auto-margin-call", "version": "2.0", "body": {<ID_Name>: <ID>, "auto_margin_call": <bool>}})` (ID_Name = `"order_id"` or `"position_id"` is used as the key).
* Busy-wait ← `auto-margin-call-changed` (whole message). Returns `(status == 2000, whole message)`.

### 97. `change_order(ID_Name, order_id, stop_lose_kind, stop_lose_value, take_profit_kind, take_profit_value, use_trail_stop, auto_margin_call)` — M
* `ID_Name == "position_id"` → `check, d = get_order(order_id)`; `ID = d["position_id"]`. `"order_id"` → `ID = order_id`. Else error log (then NameError on `ID`).
* If check: `api.tpsl_changed_respond = None`; `SEND("sendMessage", msg)`:
```json
{"name": "change-tpsl", "version": "2.0", "body": {"<ID_Name>": <ID>, "stop_lose_kind": <raw>, "stop_lose_value": <raw>, "take_profit_kind": <raw>, "take_profit_value": <raw>, "use_trail_stop": <raw>, "extra": {"stop_lose_kind": <raw>, "take_profit_kind": <raw>}}}
```
  then calls `change_auto_margin_call(ID_Name, ID, auto_margin_call)` (blocking, result ignored), then busy-waits ← `tpsl-changed` (whole message).
* Returns `(True, msg["msg"])` if status 2000, else `(False, whole message)`; `(False, None)` if `get_order` failed (actually crashes first on `None["position_id"]`).

### 98. `get_async_order(buy_order_id)` — U
* Returns `api.order_async[buy_order_id]` (auto-creates). Shape: `{"position-changed": msg|{}, "order-changed": msg|{}, "option-opened": msg|{}, "option-closed": msg|{}}`. Key type must be int (router uses `int(...)`).

### 99. `get_order(buy_order_id)` → `(True, msg)` | `(False, None)` — M
* `rid`; `api.order_data[rid] = None`; `SEND("sendMessage", {"name": "get-order", "body": {"order_id": <int(buy_order_id)>}}, rid)` (no version key).
* Busy-wait ← `order` (`order_data[message["request_id"]] = whole message`); never deleted. Returns `(True, msg["msg"])` if `status == 2000` else `(False, None)`.
* msg.status values: `pending_new`, `new`, `filled`, `rejected`, `canceled`; fields `position_id`, `reject_status`, ...

### 100. `get_pending(instrument_type)` — M
* `api.deferred_orders = None`; `SEND("sendMessage", {"name": "get-deferred-orders", "version": "1.0", "body": {"user_balance_id": <BAL>, "instrument_type": <raw>}})`. Busy-wait ← `deferred-orders`. Returns `(True, msg)` if 2000 else `(False, None)`.

### 101. `get_transactions(from_t, to_t, limit, offset, types)` — A
* `rid`; `api.transactions[rid] = None`; `SEND("sendMessage", {"name": "get-transactions", "body": {"from": <from_t>, "to": <to_t>, "limit": <limit>, "offset": <offset>, "types": <list>}}, rid)` (no version). types e.g. `["airdrop","commission","deposit","emission","manual","order_purchase","order_sell","tournament_rebuy","tournament_registration","tournament_reward","transfer","withdrawal"]`.
* Busy-wait ← `transactions` (`transactions[message["request_id"]] = whole message`); deleted. Returns whole message.

### 102. `get_positions(instrument_type)` — M — **OLDER variant of portfolio_get_positions**
* `api.positions = None`; name = `"digital-options.get-positions"` if `instrument_type == "digital-option"`, `"trading-fx-option.get-positions"` if `"fx-option"`, else `"get-positions"`; `SEND("sendMessage", {"name": <name>, "body": {"instrument_type": <raw>, "user_balance_id": <BAL>}})` (no version).
* Busy-wait ← `positions` (whole message; slot shared with #43). Returns `(True, msg)` / `(False, None)`.

### 103. `get_position(buy_order_id)` — M
* `api.position = None`; `check, od = get_order(buy_order_id)`; `SEND("sendMessage", {"name": "get-position", "body": {"position_id": <od["position_id"] raw>}})` (no version). Busy-wait ← `position` (whole message; slot shared with #104/#105). Returns `(True, msg)` / `(False, None)`.

### 104. `get_digital_position_by_position_id(position_id)` — D
* `api.position = None`; `SEND("sendMessage", {"name": "digital-options.get-position", "body": {"position_id": <raw>}})`. Busy-wait ← `position`. Returns **whole message** (no tuple).

### 105. `get_digital_position(order_id)` — D
* `api.position = None`; busy-wait `get_async_order(order_id)["position-changed"] != {}`; `position_id = ...["msg"]["external_id"]`; same request as #104. Returns whole message.
* Group: #104 takes the position id directly; #105 resolves it from the pushed position-changed (works with order id or position id).

### 106. `get_position_history(instrument_type)` — M — **OLD**
* `api.position_history = None`; `SEND("sendMessage", {"name": "get-position-history", "body": {"instrument_type": <raw>, "user_balance_id": <BAL>}})` (no version). Busy-wait ← `position-history`. Returns `(True, msg)` / `(False, None)`.

### 107. `get_history_positions(external_id, polling_time=0.5)` — M/B
* `rid`; `api.history_positions[rid] = None`; `SEND("sendMessage", {"name": "get-history-positions", "body": {"external_id": <raw>}}, rid)` (no version). Poll every `polling_time` ← `history-positions` (`history_positions[str(request_id)] = whole message`); deleted. Returns whole message (`msg.positions[]` with `external_id`, `pnl_realized`, ...).

### 108. `get_position_history_v2(user_balance_id, start, end, offset, limit, instrument_type)` — M — **newest history**
* `rid`; `api.position_history_v2[rid] = {}`; `SEND("sendMessage", {"name": "get-history", "body": {"instrument_types": <instrument_type raw (list expected)>, "user_balance_id": <raw>, "start": <raw>, "end": <raw>, "offset": <raw>, "limit": <raw>}, "version": "2.0"}, rid)` (note key order: name, body, version).
* Busy-wait while `== {}` ← `history` (`position_history_v2[str(request_id)] = whole message`); deleted. Returns `(True, msg)` / `(False, None)`.
* Group: `get_position_history` (old, per-instrument) / `get_history_positions` (by external id) / `get_position_history_v2` (paged, newest).

### 109. `get_available_leverages(instrument_type, actives="")` — M
* `api.available_leverages = None`; `actives == ""` → `""` else `ACT(actives)`; `SEND("sendMessage", {"name": "get-available-leverages", "version": "2.0", "body": {"instrument_type": <raw>, "actives": [<"" | active_id>]}})` (empty case sends `[""]`).
* Busy-wait ← `available-leverages`. Returns `(True, msg)` / `(False, None)`.

### 110. `cancel_order(buy_order_id)` → bool — M
* `api.order_canceled = None`; `SEND("sendMessage", {"name": "cancel-order", "version": "1.0", "body": {"order_id": <raw>}})`. Busy-wait ← `order-canceled`. Returns `status == 2000`.

### 111. `close_position(position_id)` → bool — M
* Parameter is actually an **order id**: `check, data = get_order(position_id)`; if `data["position_id"] != None`: `api.close_position_data = None`; `SEND("sendMessage", {"name": "close-position", "version": "1.0", "body": {"position_id": <data["position_id"]>}})`; busy-wait ← `position-closed`; return `status == 2000`. Else False. Crash if get_order failed.

### 112. `close_position_v2(position_id)` — M — **broken variant of close_position**
* `while get_async_order(position_id) == None` never loops (defaultdict is never None); `get_async_order(position_id)["id"]` auto-creates `{}` ⇒ sends `{"name": "close-position", "version": "1.0", "body": {"position_id": {}}}`; does not reset `close_position_data` so it may return a stale result. Should not be ported as-is.

### 113. `get_overnight_fee(instrument_type, active)` — M
* `api.overnight_fee = None`; `SEND("sendMessage", {"name": "get-overnight-fee", "version": "1.0", "body": {"user_group_id": 1, "instrument_type": <raw>, "active_id": <ACT(active)>}})`. Busy-wait ← `overnight-fee`. Returns `(True, msg)` / `(False, None)`.

### 114. `get_option_open_by_other_pc()` — B
* Returns `api.socket_option_opened` = `{external_id: position-changed message}` for `source=="binary-options"` & `status=="open"` (filled by router; includes trades from this client too).

### 115. `del_option_open_by_other_pc(id)` — B
* `del api.socket_option_opened[id]` (KeyError ignored).

### 116. `get_digital_open_by_other_pc()` — D
* Returns `api.digital_opened` (`source=="digital-options"` & `status=="open"`).

### 117. `del_digital_open_by_other_pc(id)` — D
* `del api.digital_opened[id]` (KeyError ignored).

### 118. `opcode_to_name(opcode)` — U
* `list(ACTIVES.keys())[list(ACTIVES.values()).index(opcode)]` — first key with that value; ValueError if absent.

### 119. `subscribe_live_deal(name, active, _type, buffersize)` — X
* `active_id = ACT(active)`; `SEND("subscribeMessage", msg)`; filter key names depend on `name`:
  * `"live-deal-binary-option-placed"` → `active_id` / `option_type` (turbo|binary)
  * `"live-deal-digital-option"` → `instrument_active_id` / `expiration_type` (e.g. PT1M)
  * `"live-deal"` → `instrument_active_id` / `instrument_type`
  * other names ⇒ UnboundLocalError
```json
{"name": "<name>", "params": {"routingFilters": {"<active key>": <int(active_id)>, "<type key>": "<str(_type)>"}}, "version": "2.0"}
```
* `buffersize` is **ignored** (deque creation commented out) ⇒ buffer is an unbounded `deque()` unless `clear_live_deal` is called first.

### 120. `unscribe_live_deal(name, active, _type)` — X (typo "unscribe" is the public name)
* Same msg (`"name": str(name)`) with outer `unsubscribeMessage`.

### 121. `get_live_deal(name, active, _type)` — X
* Returns `api.live_deal_data[name][active][_type]` (deque; newest at the left via `appendleft`).

### 122. `pop_live_deal(name, active, _type)` — X
* `.pop()` ⇒ removes/returns the **oldest** (right end). IndexError if empty.

### 123. `clear_live_deal(name, active, _type, buffersize)` — X
* `live_deal_data[name][active][_type] = deque(list(), buffersize)` (bounded; appendleft then drops oldest from the right).

### 124. `get_user_profile_client(user_id)` — X
* `api.user_profile_client = None`; `SEND("sendMessage", {"name": "get-user-profile-client", "body": {"user_id": <int(user_id)>}, "version": "1.0"})`. Busy-wait ← `user-profile-client` (`msg`). Returns msg.

### 125. `request_leaderboard_userinfo_deals_client(user_id, country_id)` — X
* `api.leaderboard_userinfo_deals_client = None`; loop: if `client["isSuccessful"] is True` break; else `SEND("sendMessage", {"name": "request-leaderboard-userinfo-deals-client", "body": {"country_ids": [<country_id>], "requested_user_id": <int(user_id)>}, "version": "1.0"})`; `sleep(0.2)`. Re-sends every 0.2 s forever until success. Router `leaderboard-userinfo-deals-client` (`msg`). Returns msg.

### 126. `get_users_availability(user_id)` — X
* `api.users_availability = None`; while None: `SEND("sendMessage", {"name": "get-users-availability", "body": {"user_ids": [<user_id raw>]}, "version": "1.0"})`; `sleep(0.2)`. Router `users-availability` (`msg`). Returns msg.

---

### 1.A Duplicate / old-version groups (summary)

| Group | Members (oldest → newest) | Recommended to port as primary | Why |
|---|---|---|---|
| Binary result | `check_win_v2` (legacy `api_game_betinfo`) → `check_win_v3` (push `option-closed`) ; `check_win` (+`raw_check_win`, portfolio history) | `check_win_v3` for binary/turbo; `check_win` for margin & general | v3 sends nothing and reacts to the push; `check_win` is the latest rewrite and handles order→position resolution |
| Digital result | `check_win_digital` → `check_win_digital_v2` | v2 (but add a wait-until-closed loop) | v2 event-driven; v1 re-requests each poll and loops forever on unknown close_reason |
| Init data | `get_all_init` (legacy `api_option_init_all`) → `get_all_init_v2` (`get-initialization-data` 3.0) | v2 | versioned API; bounded timeout |
| Profit table | `get_all_profit` (v1 init, name keys) → `get_all_profit_v2` (v2 init, ticker keys) (+ `get_all_profit_blitz_v2`) | v2 | ticker keys match ACTIVES; skips suspended |
| Option list | `get_optioninfo` (legacy `api_game_getoptions`) → `get_optioninfo_v2` (`get-options`) | v2 | |
| Profile | `get_profile` / `get_profile_ansyc` | either (both return cache) | |
| Balance | `get_balance` (stale) → `get_balance_v2` (push cache); `get_balances` (cached) vs `_get_balances` (true round-trip) | `_get_balances` + `get_balance_v2` | |
| Binary buy | `buy`, `buy_multi`, `buy_by_raw_expirations`, `buy_blitz` (variants of `binary-options.open-option`); legacy `buyV2` channel unused | `buy` | |
| Digital buy | `old_buy_digital_spot` (name-format id, v1.0) → `buy_digital_spot` (asset-id format, v2.0); `buy_digital` (any strike id) | `buy_digital_spot` | current instrument id format |
| Positions | `get_positions` → `portfolio_get_positions` | `portfolio_get_positions` (fix list→str bug knowingly) | |
| History | `get_position_history` → `get_history_positions` → `get_position_history_v2` | v2 | |
| Close margin | `close_position` → `close_position_v2` (broken) | `close_position` | |
| Digital position | `get_digital_position_by_position_id` / `get_digital_position` | both | |
| Strike data | `get_strike_list` / `get_realtime_strike_list` / `get_instrument_quites_generated_data` | `get_realtime_strike_list` (stream) | |
| ACTIVES refresh | `update_ACTIVES_OPCODE` → `get_ALL_Binary_ACTIVES_OPCODE`; `get_All_Digital_ACTIVES_OPCODE`; `instruments_input_all_in_ACTIVES` | all three | |
| Candle stream | `start_candles_stream` (wrapper, sets maxdict) vs `start_candles_one_stream` / `start_candles_all_size_stream` (raw) | wrapper | raw ones crash router without maxdict |

---

## 2. Incoming message router (`WebsocketClient.on_message`)

Every message: `ssl_Mutex[oid].acquire()` → `json.loads(str(message))` → if `oid in client_callback`: `client_callback[oid](message)` → `if/elif` chain on `message["name"]` → `ssl_Mutex[oid].release()`.

> **Critical:** there is no try/finally. Any exception in a branch (or the `return` in `client-price-generated`) leaves `ssl_Mutex` held forever ⇒ every later `send_websocket_request` (except heartbeat) blocks forever, and the next `on_message` deadlocks the websocket thread. websocket-client 0.58 catches/logs callback exceptions, so this fails silently. Known triggers: reverse lookup of an `active_id` absent from `ACTIVES` (`list.index` ValueError) in `candle-generated`, `candles-generated`, `commission-changed`, `client-price-generated`, `instrument-quotes-generated`, `live-deal*`; missing maxdict (`int < {}` TypeError) in candle branches; missing `microserviceName`; `client-price-generated` without an `SPT` strike (NameError `minute`).

"ActiveName(x)" below = `list(ACTIVES.keys())[list(ACTIVES.values()).index(x)]`.

| `message["name"]` | Action (state field ← value) | Keyed by |
|---|---|---|
| `timeSync` | `api.timesync.server_timestamp = msg` (ms int; getter divides by 1000) | — |
| `candle-generated` | `Active_name = ActiveName(msg.active_id)`; `size=int(msg.size)`; `from_=int(msg["from"])`; `maxdict = real_time_candles_maxdict_table[Active_name][size]`; `dict_queue_add(real_time_candles, maxdict, str(Active_name), size, from_, msg)`; `candle_generated_check[active][size] = True` | active name, size, from |
| `options` | `api.get_options_v2_data = message` (whole) | — |
| `candles-generated` | `Active_name = ActiveName(msg.active_id)`; for `k, v in msg.candles.items()`: `v["active_id"]=msg.active_id; v["at"]=msg.at; v["ask"]=msg.ask; v["bid"]=msg.bid; v["close"]=msg.value; v["size"]=int(k)`; `dict_queue_add(real_time_candles, maxdict_table[Active_name][size], active, size, int(v["from"]), v)`; then `candle_generated_all_size_check[active] = True` | name, size, from |
| `commission-changed` | `subscribe_commission_changed_data[msg.instrument_type][ActiveName(msg.active_id)][server_ts] = int(msg.commission.value)` | instrument_type, name, receipt time (float s) |
| `heartbeat` | `try: api.heartbeat(msg)` → sends `{"name": "heartbeat", "msg": {"msg": {"heartbeatTime": int(msg), "userTime": int(server_ts*1000)}}, "request_id": ""}` **without mutex** (`no_force_send=False`); exceptions swallowed. NB double-nested `msg`. | — |
| `balances` | if `global_value.balance_id[oid] is None`: for each balance with `type == 4` set `balance_id = balance["id"]` (last practice wins); always `api.balances_raw = message` (whole) | — |
| `profile` | `api.profile.msg = msg` | — |
| `authenticated` | `api.authenticated = msg` (true/false) | — |
| `candles` | `api.candles.candles_data = msg["candles"]` (exceptions swallowed) | — |
| `buyComplete` | `api.buy_successful = msg.isSuccessful; api.buy_id = msg.result.id` (swallowed) — legacy, unused | — |
| `buyV2_result` | `api.buy_successful = msg.isSuccessful` — legacy, unused | — |
| `option` | `api.buy_multi_option[str(message["request_id"])] = msg` | request_id |
| `listInfoData` | for each item: `api.listinfodata.set(win, game_state, id)` ⇒ `listinfodata_dict[id] = {"win":..., "game_state":...}` — legacy, nothing reads it | option id |
| `api_option_init_all_result` | `api.api_option_init_all_result = msg` | — |
| `initialization-data` | `api.api_option_init_all_result_v2 = msg` | — |
| `underlying-list` | `api.underlying_list_data = msg` | — |
| `instruments` | `api.instruments = msg` | — |
| `financial-information` | `api.financial_information = message` (whole) | — |
| `order-changed` | if `message["microserviceName"] == "portfolio"` and `msg.source == "digital-options"`: `order_async[int(msg.raw_event.position_id)]["order-changed"] = message`; `order_async[int(msg.raw_event.id)]["order-changed"] = message` | position id & order id |
| `position-changed` | `api.position_changed = message`; `if microserviceName=="portfolio" and (source=="digital-options") or source=="trading":` (precedence: `(A and B) or C`) ⇒ `order_async[int(msg.raw_event.order_ids[0])]["position-changed"] = message` and `order_async[int(msg.external_id)]["position-changed"] = message`; `elif microserviceName=="portfolio" and source=="binary-options"` ⇒ `order_async[int(msg.external_id)]["position-changed"] = message`. Then (independent): `source=="binary-options" and status=="open"` ⇒ `socket_option_opened[msg.external_id] = message`; `elif source=="digital-options" and status=="open"` ⇒ `digital_opened[msg.external_id] = message` | order id, external id |
| `option-opened` / `option-closed` | `order_async[int(msg.option_id)][message["name"]] = message` | option id |
| `top-assets-updated` | `top_assets_updated_data[str(msg.instrument_type)] = msg.data` | instrument_type |
| `strike-list` | `api.strike_list = message` (whole) | — |
| `api_game_betinfo_result` | `game_betinfo.isSuccessful = msg.isSuccessful; game_betinfo.dict = msg` (swallowed) | — |
| `traders-mood-changed` | `traders_mood[msg.asset_id] = msg.value` | asset id (int) |
| `order-placed-temp` | `buy_order_id[message["request_id"]] = msg.id` | request_id |
| `order` | `order_data[message["request_id"]] = message` (whole) | request_id |
| `positions` | `api.positions = message` | — |
| `position` | `api.position = message` | — |
| `deferred-orders` | `api.deferred_orders = message` | — |
| `position-history` | `api.position_history = message` | — |
| `history` | `position_history_v2[str(message["request_id"])] = message` | request_id |
| `history-positions` | `history_positions[str(message["request_id"])] = message` | request_id |
| `available-leverages` | `api.available_leverages = message` | — |
| `order-canceled` | `api.order_canceled = message` | — |
| `position-closed` | `api.close_position_data = message` | — |
| `overnight-fee` | `api.overnight_fee = message` | — |
| `api_game_getoptions_result` | `api.api_game_getoptions_result = message` | — |
| `sold-options` | `sold_options_respond[message["request_id"]] = message` (never read) | request_id |
| `tpsl-changed` | `api.tpsl_changed_respond = message` | — |
| `auto-margin-call-changed` | `api.auto_margin_call_changed_respond = message` | — |
| `digital-option-placed` | `digital_option_placed[message["request_id"]] = message` (swallowed) | request_id |
| `result` | if `message["request_id"] != ""`: `api.result[str(request_id)] = msg["success"]` | request_id |
| `client-price-generated` | see below | asset name, period |
| `instrument-quotes-generated` | legacy (its subscription channel is unused): `Active_name = ActiveName(msg.active)`; `period = msg.expiration.period`; `ans = {symbol: ProfitPercent}` for every `quote` (ProfitPercent = None if `price.ask` is None else `((100-ask)*100)/ask`) and every symbol in `quote.symbols`; `instrument_quites_generated_timestamp[A][period] = msg.expiration.timestamp`; `instrument_quites_generated_data[A][period] = ans`; `instrument_quotes_generated_raw_data[A][period] = message` | name, period |
| `training-balance-reset` | `api.training_balance_reset_request = msg.isSuccessful` | — |
| `top-assets` | `get_top_assets_data[message["request_id"]] = msg` | request_id |
| `live-deal-binary-option-placed` | `live_deal_data[name][ActiveName(msg.active_id)][msg.option_type].appendleft(msg)` (swallowed) | name, asset, option_type |
| `live-deal-digital-option` | `live_deal_data[name][ActiveName(msg.instrument_active_id)][msg.expiration_type].appendleft(msg)` | |
| `leaderboard-deals-client` | `api.leaderboard_deals_client = msg` | — |
| `live-deal` | `live_deal_data[name][ActiveName(msg.instrument_active_id)][msg.instrument_type].appendleft(msg)` | |
| `user-profile-client` | `api.user_profile_client = msg` | — |
| `leaderboard-userinfo-deals-client` | `api.leaderboard_userinfo_deals_client = msg` | — |
| `users-availability` | `api.users_availability = msg` | — |
| `transactions` | `transactions[message["request_id"]] = message` | request_id |
| `active-exposure` | `active_exposure[message["request_id"]] = message` | request_id |
| `balance-changed` | `api.balance[msg.current_balance.id] = msg.current_balance.amount` | balance id |
| anything else | ignored | |

`client-price-generated` branch, literally:
```python
Active_name = ActiveName(message["msg"]["asset_id"])
ans = nested_dict(2, dict)
for strike in message["msg"]["prices"]:
    ProfitPercent     = None if "ask" not in strike["call"] else ((100-float(call.ask))*100)/float(call.ask)
    ProfitPercent_put = None if "ask" not in strike["put"]  else ((100-float(put.ask))*100)/float(put.ask)
    if strike["strike"] == "SPT":
        symbol = strike["call"]["symbol"]
        try:    minute = int(symbol.split("T")[1].split("M")[0])
        except: return            # <-- returns WITHOUT releasing ssl_Mutex
        api.digital_profit[Active_name][minute*60] = ProfitPercent            # call side only
        api.instrument_quotes_generated_raw_data[Active_name][minute*60] = message
    ans[strike["strike"]]["call"]["id"] = strike["call"]["symbol"]
    ans[strike["strike"]]["call"]["profit"] = ProfitPercent
    ans[strike["strike"]]["put"]["id"] = strike["put"]["symbol"]
    ans[strike["strike"]]["put"]["profit"] = ProfitPercent_put
api.instrument_quites_generated_data[Active_name][minute*60] = ans   # NameError if no SPT row
```
Symbol parse: `"do1A20210224D154900T1MCSPT".split("T")` → `["do1A20210224D154900", "1MCSP", ""]` → `"1"`; old format `"doEURUSD201907191250PT5MPSPT"` → `"5"`. The period key is `minute*60` (seconds).

### 2.1 `on_error(wss, error)` (static)
`websocket_error_reason[id(wss)] = str(error)`; `check_websocket_if_error[id(wss)] = True` (never reset for that socket).

### 2.2 `on_open(wss)` / `on_close(wss, code, msg)` — see §3.5.

### 2.3 Response names never handled (silently dropped)
Anything not in the table, e.g. server error replies for `sendMessage` requests with other names — callers waiting on a slot then hang forever.

---

## 3. Connection lifecycle

### 3.1 Process-global state (`global_value.py`), all keyed by `object_id = id(websocket_client.wss)`

| name | init per websocket (`init_global_value`) | use |
|---|---|---|
| `ssl_Mutex[oid]` | `threading.Lock()` | send mutex **and** held for the whole of `on_message`; acquired in `on_close`, released in `on_open` |
| `check_websocket_if_connect[oid]` | `None` | 1 on_open, 0 on_close |
| `SSID[oid]` | `set_ssid` if key absent; then overwritten by previous socket's SSID if any | auth token |
| `check_websocket_if_error[oid]` / `websocket_error_reason[oid]` | `False` / `None` | set by on_error |
| `balance_id[oid]` | `None` | set by first `balances` msg to the type-4 (practice) id |
| `req_mutex[oid]`, `req_id[oid]` | Lock, `1` | request id counter |
| `account[oid]` | `{"email": username, "password": password}` | used by low-level reconnect |
| `auto_reconnect[oid]` | `True` | gate for on_close reconnect |
| `happen_close[oid]` | (not set) | True after a close, False after reconnect auth |
| `low_level_reconnect_session[oid]` | (not set) | `requests.Session` used by reconnect login |
| `client_callback[oid]` | (not set) | user callback |
| `_tmp_raw_balance_id[oid]` | nested_dict | change_balance cache |

`del_init_global_value` exists but is never called (entries leak per reconnect).

### 3.2 `IQOptionAPI.__init__(host="iqoption.com", ...)`
* `https_url = "https://iqoption.com/api"`, `wss_url = "wss://iqoption.com/echo/websocket"` (comment: some countries must use `id-iqoption.com`; not configurable from `IQ_Option`).
* `ping_interval = 5`, `ping_timeout = 3` — **unused** (not passed to `run_forever`; no client pings).
* `session = requests.Session()`; proxies (§3.9); `session.verify = False`; `session.trust_env = False`; `requests.packages.urllib3.disable_warnings()` at import.
* State fields: see §6.

### 3.3 `IQOptionAPI.connect()` (called by `IQ_Option.connect`, and internally on any send failure)
1. `try: self.logout()` → HTTP `POST https://auth.iqoption.com/api/v1.0/logout` (fresh session ⇒ normally no ssid cookie ⇒ effectively a no-op; but on an internal re-connect the session already carries the `ssid` cookie, so this **invalidates the current SSID**). Errors swallowed.
2. `if auto_logout: atexit.register(self.logout)` (a new registration on every connect).
3. `try: self.close()` (→ `websocket_client.wss.close()`; AttributeError first time).
4. `start_websocket()`:
   * `websocket_client = WebsocketClient(self)` → `websocket.WebSocketApp(wss_url, on_message, on_error, on_close, on_open)` (no headers/cookies/subprotocols).
   * `prev_ssid = SSID[old_oid]` if `object_id` not None; `object_id = id(wss)`; `init_global_value(oid)`; `if prev_ssid: SSID[oid] = prev_ssid`.
   * daemon thread: `wss.run_forever(sslopt={"check_hostname": False, "cert_reqs": ssl.CERT_NONE, "ca_certs": "cacert.pem"}, http_proxy_host=..., http_proxy_port=..., http_proxy_auth=...)`.
   * Busy-wait (no timeout): error flag → `return False, global_value.websocket_error_reason` (**the whole dict**, not the string — bug); `connect == 0` → `(False, "Websocket connection closed.")`; `== 1` → `(True, None)`.
   * Handshake (websocket-client 0.58 defaults): `GET /echo/websocket`, `Host: iqoption.com`, `Upgrade: websocket`, `Connection: Upgrade`, `Sec-WebSocket-Key`, `Sec-WebSocket-Version: 13`, `Origin: http://iqoption.com` (0.58 builds `http://` + host). No cookie/UA — authentication is purely by the `authenticate` message.
5. If start failed → return `(False, reason)`.
6. If `SSID[oid] != None` (re-use path): `check = send_ssid()`; if False: `response = get_ssid()` (HTTP login); `status_code != 200` → `(False, response.text)`; `start_websocket()` **again** (old socket not explicitly closed — it is closed by the server, and its own auto-reconnect may then kick in); `SSID[oid] = response.cookies["ssid"]` (missing → `(False, response.text)`); `send_ssid()` (result ignored).
7. Else (no SSID): `response = get_ssid()`; `status_code != 200` → `(False, response.text)`; no `ssid` cookie → `global_value.auto_reconnect[oid]==False` (comparison, **no-op typo**), `self.close()`, `(False, response.text)`; else `SSID[oid] = cookie`; `send_ssid()` (result ignored).
   * `get_ssid()` returns the exception object if `login` raised ⇒ `.status_code` AttributeError (crash).
8. `requests.utils.add_dict_to_cookiejar(session.cookies, {"ssid": SSID[oid]})` (domain-less cookie ⇒ sent on all later HTTP requests of this session).
9. `timesync.server_timestamp = None`; busy-wait until a `timeSync` arrives (getter raises on None/1000 → caught).
10. `return True, None`.

### 3.4 `send_ssid()`
```python
self.profile.msg = None
req_id = global_value.get_req_id(self.object_id)      # "1" on a fresh socket
self.authenticated = None
self.ssid(global_value.SSID[self.object_id], req_id)
#  → {"name": "authenticate", "msg": {"ssid": "<SSID>", "protocol": 3}, "request_id": "1"}
while self.authenticated == None: pass               # router "authenticated" → msg (true/false); no timeout
if self.authenticated == False: return False
self.get_profile_ws("")   # {"name": "sendMessage", "msg": {"name": "get-profile", "version": "1.0", "body": {}}, "request_id": ""}
self.get_balances()       # {"name": "sendMessage", "msg": {"name": "get-balances", "version": "1.0"}, "request_id": ""}
while self.profile.msg == None: pass
return self.profile.msg != False
```
If an invalid SSID makes the server close the socket instead of answering, the wait is only released if the low-level auto-reconnect (§3.5) re-authenticates and an `authenticated` message arrives.

### 3.5 Low-level auto-reconnect (`on_close` → `run_forever` → `on_open`)
```python
@staticmethod
def on_close(wss, close_status_code, close_msg):
    ssl_Mutex[id(wss)].acquire()                 # released only by the next on_open
    check_websocket_if_connect[id(wss)] = 0
    if auto_reconnect[id(wss)] == True:
        happen_close[id(wss)] = True
        wss.sock = None
        wss.run_forever()                         # recursive; NO sslopt / proxy kwargs this time

@staticmethod
def on_open(wss):
    check_websocket_if_connect[id(wss)] = 1
    if id(wss) not in auto_reconnect: return
    if id(wss) not in happen_close: return        # first open of a socket ends here
    if auto_reconnect[id(wss)] == True and happen_close[id(wss)] == True:
        headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"}
        try: low_level_reconnect_session[id(wss)].request(method="POST", url="https://auth.iqoption.com/api/v1.0/logout", headers=headers)
        except: pass
        session = requests.Session(); low_level_reconnect_session[id(wss)] = session
        data = {"identifier": account[id(wss)]["email"], "password": account[id(wss)]["password"], "token": None}
        response = session.request(method="POST", url="https://auth.iqoption.com/api/v2/login", data=data, headers=headers)
        try: SSID[id(wss)] = response.cookies["ssid"]
        except: auto_reconnect[id(wss)] = False; ssl_Mutex[id(wss)].release(); return
        wss.send(json.dumps(dict(name="authenticate", msg={"ssid": response.cookies["ssid"], "protocol": 3})))
        #  → {"name": "authenticate", "msg": {"ssid": "<SSID>", "protocol": 3}}   (no request_id key)
        happen_close[id(wss)] = False
    ssl_Mutex[id(wss)].release()
```
Consequences to reproduce or deliberately fix:
* Same `object_id` survives ⇒ balance selection, req counter, callbacks, `api.*` state survive.
* **Nothing is re-subscribed** (portfolio position/order events, internal-billing, candle/mood/strike streams, `setOptions`), profile/balances not re-requested, the `authenticated` reply is not checked.
* Reconnect login uses a brand-new `requests.Session` with default TLS verification, `trust_env=True`, **no proxy**, no user cookies, no 2FA token (2FA accounts fail ⇒ `auto_reconnect=False`).
* `run_forever()` is re-entered without sslopt/proxy, and recursion depth grows by one per disconnect.
* If `auto_reconnect` is False, the mutex acquired in `on_close` is never released (later sends block forever instead of failing).
* If the reconnect attempt fails at TCP/TLS level, `on_close` runs again inside the still-held lock ⇒ the websocket thread self-deadlocks (non-reentrant `Lock`).
* Triggered also by `IQ_Option.close()` and by the `self.close()` inside `connect()` (orphan sockets reconnect in the background).

### 3.6 Heartbeat and time sync
* Server pushes `{"name": "timeSync", "msg": <epoch ms>}`; stored raw; `server_ts` = value/1000.
* Server pushes `{"name": "heartbeat", "msg": <heartbeatTime>}`; client answers (no mutex):
  `{"name": "heartbeat", "msg": {"msg": {"heartbeatTime": <int(heartbeatTime)>, "userTime": <int(server_ts*1000)>}}, "request_id": ""}` — note the doubled `msg` nesting and that `userTime` is derived from the last server time, not the local clock.
* No client-initiated websocket ping.

### 3.7 2FA flow (per docs/index.md)
1. `ok, r = iq.connect()` → login response has no `ssid` cookie (or non-200) ⇒ `(False, response.text)`; JSON text contains `"method"` and `"token"`.
2. `r = iq.TWO_FA(method="sms", token=r["token"])` → `POST https://auth.iqoption.com/api/v2/verify/2fa` body `{"method": "sms", "token": "<t1>", "code": null}`.
3. `r = iq.TWO_FA(code=<sms code>, token=r["token"])` → body `{"method": null, "token": "<t2>", "code": "<code>"}`.
4. If `r["code"] == "success"`: `iq.setting_2FA_TOKEN(r["token"])`; `iq.connect()` → login form now includes `token=<t3>`.
Login error example (docs): `{"code":"invalid_credentials","message":"You entered the wrong credentials. Please check that the login/password is correct."}`.

### 3.8 Logout
`POST https://auth.iqoption.com/api/v1.0/logout`, no body, api session (cookie `ssid` attached after connect). Fired by: `IQ_Option.logout()`, `IQ_Option.close()` (if auto_logout), start of every `IQOptionAPI.connect()`, `atexit` (one registration per connect, if auto_logout), and the low-level reconnect (on its own session).

### 3.9 Proxies
* `IQ_Option(..., http_proxy_host, http_proxy_port, http_proxy_auth=(user, pass))`.
* HTTP: `session.proxies = {"http": "http://" + str(host) + ":" + str(port), "https": "https://" + str(host) + ":" + str(port)}` (only if host is not None); `session.auth = HTTPProxyAuth(user, pass)` ⇒ `Proxy-Authorization: Basic base64(user:pass)` on every request (only if auth is not None).
* WebSocket: `run_forever(http_proxy_host=host, http_proxy_port=port, http_proxy_auth=auth)` (websocket-client HTTP CONNECT tunnel with basic auth). Not applied on low-level reconnect.

### 3.10 `set_session`
`IQ_Option.set_session(header, cookie)` stores; at each `connect()` → `api.set_session(cookies, headers)` (positional order is `(cookies, headers)`, called by keyword): `session.headers.update(headers)` (default UA is the Chrome/66 string above), `session.cookies.clear_session_cookies()`, `add_dict_to_cookiejar(session.cookies, cookies)`.

### 3.11 request_id generation
```python
def get_req_id(object_id):
    req_mutex[object_id].acquire()
    v = req_id[object_id]; req_id[object_id] = v + 1
    req_mutex[object_id].release()
    return str(v)
```
Starts at 1 per websocket (`init_global_value`), so a new socket restarts at `"1"` (the `authenticate` frame normally gets `"1"`). Many requests send `request_id ""` and are correlated only through a single shared slot.

### 3.12 Literal frame sequence of a fresh `IQ_Option.connect()` (no prior SSID)
1. HTTP POST logout (no-op), HTTP POST `/api/v2/login` (form).
2. `{"name": "authenticate", "msg": {"ssid": "<SSID>", "protocol": 3}, "request_id": "1"}`
3. `{"name": "sendMessage", "msg": {"name": "get-profile", "version": "1.0", "body": {}}, "request_id": ""}`
4. `{"name": "sendMessage", "msg": {"name": "get-balances", "version": "1.0"}, "request_id": ""}`
5. (wait profile, wait timeSync; candle/mood re-subscriptions if any; wait `balances` → balance_id)
6. `{"name": "sendMessage", "msg": {"name": "get-balances", "version": "1.0"}, "request_id": ""}` (from `get_balances()`)
7. For each balance id B, for ins in `cfd, forex, crypto, digital-option, turbo-option, binary-option`:
   `{"name": "subscribeMessage", "msg": {"name": "portfolio.position-changed", "version": "2.0", "params": {"routingFilters": {"instrument_type": "<ins>", "user_balance_id": <B>}}}, "request_id": ""}`
8. For N in `internal-billing.balance-created, internal-billing.auth-balance-changed, internal-billing.balance-changed, internal-billing.marginal-changed, tournaments.user-registered-in-tournament, tournaments.user-tournament-position-changed`:
   `{"name": "subscribeMessage", "msg": {"name": "<N>", "version": "1.0", "params": {"routingFilters": {}}}, "request_id": ""}`
9. For ins as in 7: `{"name": "subscribeMessage", "msg": {"name": "portfolio.order-changed", "version": "1.0", "params": {"routingFilters": {"instrument_type": "<ins>"}}}, "request_id": ""}`
10. `{"name": "setOptions", "msg": {"sendResults": true}, "request_id": ""}`

---

## 4. Helper logic that must be ported exactly

### 4.1 `TimeSync` (ws/objects/timesync.py)
* Private value initialised to `time.time()` (**seconds**), but the getter returns `value / 1000` ⇒ before the first `timeSync` the getter yields ~1.7e6 (garbage). `api.connect()` hides this by setting None and waiting for the first `timeSync`.
* Setter stores raw ms. Getter → float seconds. `server_datetime = datetime.fromtimestamp(server_ts)` (local). `expiration_time` (default 1 min), `expiration_datetime`, `expiration_timestamp = mktime(...)` — unused by the library.

### 4.2 `get_expiration_time(timestamp, duration)` → `(int exp, int idx)` (binary/turbo expiry; also digital duration 1 and active exposure)
```python
def date_to_timestamp(dt): return time.mktime(dt.timetuple())       # local tz

def get_expiration_time(timestamp, duration):
    now_date = datetime.fromtimestamp(timestamp)                     # LOCAL naive datetime
    exp_date = now_date.replace(second=0, microsecond=0)
    if (int(date_to_timestamp(exp_date+timedelta(minutes=1))) - timestamp) > 30:
        exp_date = exp_date + timedelta(minutes=1)
    else:
        exp_date = exp_date + timedelta(minutes=2)
    exp = []
    for _ in range(5):
        exp.append(date_to_timestamp(exp_date)); exp_date = exp_date + timedelta(minutes=1)
    idx = 50; index = 0
    now_date = datetime.fromtimestamp(timestamp)
    exp_date = now_date.replace(second=0, microsecond=0)
    while index < idx:
        if int(exp_date.strftime("%M")) % 15 == 0 and (int(date_to_timestamp(exp_date)) - int(timestamp)) > 60*5:
            exp.append(date_to_timestamp(exp_date)); index = index + 1
        exp_date = exp_date + timedelta(minutes=1)
    remaning = [int(t) - int(time.time()) for t in exp]              # LOCAL CLOCK, not `timestamp`
    close = [abs(x - 60*duration) for x in remaning]
    return int(exp[close.index(min(close))]), int(close.index(min(close)))
```
Epoch-arithmetic equivalent (valid when the local UTC offset is a whole multiple of 15 min — true for every current zone — and no DST change inside the window):
```
m0     = ts - ts % 60
first  = m0 + 60  if (m0 + 60) - ts > 30  else m0 + 120
cands  = [first, first+60, first+120, first+180, first+240]               # idx 0..4  ⇒ turbo
q = m0; while len(quarters) < 50: if (q/60) % 15 == 0 and q - int(ts) > 300: quarters.append(q); q += 60
cands += quarters                                                          # idx 5..54 ⇒ binary
now    = int(local_wall_clock())
i      = first index of min |(c - now) - 60*duration|                      # ties → lowest index
return (c[i], i)
```
* Callers: `Buyv3` passes `int(time.time())` (local clock!); `Buyv2` (unused) same; `Get_Active_Exposure` / digital use `int(server_ts)`.
* `Buyv3`: `option_type_id = 3` ("turbo") if `idx < 5` else `1` ("binary").
* Examples (ts at hh:mm:20): duration 1 → first boundary (+40 s), idx 0; duration 5 → +280 s, idx 4; duration 15 → nearest quarter-hour that is >5 min away, idx ≥5.
* For a faithful port use the machine's local time zone (`TimeZoneInfo.Local`) for the `fromtimestamp`/`mktime` round trips, or document the UTC equivalence above.

### 4.3 `get_remaning_time(timestamp)` → list of `(label, seconds_left)`
Same construction with **11** quarter candidates (5 + 11 = 16 entries); labels `dr = idx+1` for idx<5 (1..5), `15*(idx-4)` for idx≥5 (15, 30, …, 165); `seconds_left = int(t) - int(time.time())`. `IQ_Option.get_remaning(duration, time=server_ts)` returns the `seconds_left` whose label == duration, else the string `"ERROR duration"`. Docs: "purchase time = remaining time − 30".

### 4.4 Digital spot expiry and instrument ids
`expiration.get_digital_exp(ts, duration)` (used by active exposure) and the identical inline code in `buy_digital_spot` / `old_buy_digital_spot` (with `ts = int(server_ts)`):
```python
if duration == 1:
    exp, _ = get_expiration_time(ts, duration)           # int; always the first candidate (31–90 s ahead)
else:
    now_date = datetime.fromtimestamp(ts) + timedelta(minutes=1, seconds=30)   # LOCAL, seconds NOT truncated
    while True:
        if now_date.minute % duration == 0 and time.mktime(now_date.timetuple()) - ts > 30:   # 2nd test always true
            break
        now_date = now_date + timedelta(minutes=1)
    exp = time.mktime(now_date.timetuple())             # FLOAT, keeps ts's seconds (e.g. 12:05:47)
```
Then (buy paths only) the date is formatted in **UTC**, which drops the seconds:
* **New format** (`buy_digital_spot`): `"do" + str(ACTIVES[active]) + "A" + utcfromtimestamp(exp).strftime("%Y%m%dD%H%M") + "00" + "T" + str(duration) + "M" + ("C" for call | "P" for put) + "SPT"` → `do1A20210224D154900T1MCSPT`.
* **Old format** (`old_buy_digital_spot`): `"do" + active_name + utcfromtimestamp(exp).strftime("%Y%m%d%H%M") + "PT" + str(duration) + "M" + ("C"|"P") + "SPT"` → `doEURUSD201907191250PT5MPSPT`.
* Strike (non-spot) ids come from the server, e.g. `doUSDJPY-OTC201811111204PT1MC11350481`.
* `place_digital_option_v2`: if `instrument_id.find("do") == 0` → v2.0 body with `asset_id = int(str(instrument_id).split("do")[1].split("A")[0])` and `"instrument_index": 0`; else v1.0 body.
* Timezone note: `minute % duration` is evaluated on the **local** clock but the id uses the UTC minute; identical for durations 1/5/15 with real-world offsets, but differs for e.g. duration 30/60 in UTC+5:30, UTC+9:30, UTC+5:45 zones.
* `get_active_exposure` sends this raw `exp` as `"time"` (int for duration 1, otherwise a float with non-zero seconds, serialised like `1700000747.0`).

### 4.5 Strike-list expiration (`Strike_list.get_digital_expiration_time`; identical unused copies in subscribe/unsubscribe v1/v2 classes)
```python
exp = int(server_ts)
minute = int(datetime.fromtimestamp(exp).strftime('%M'))      # local minute
ans = exp - exp % 60                                           # drop seconds
ans = ans + (duration - minute % duration) * 60                # next boundary, always ≥ +1 min
if exp > ans - 10:
    ans = ans + duration * 60
return ans                                                     # body: "expiration": int(ans)*1000, "period": duration*60
```

### 4.6 ACTIVES opcode map
* `constants.ACTIVES`: module-level dict, 198 entries name → int id (Appendix A; values currently unique; e.g. `EURUSD` 1861, `EURUSD-OTC` 76, `GBPUSD` 1867, `USDJPY` 1865, `BTCUSD` 1916, `XAUUSD` 1912). Shared by all `IQ_Option` instances in the process.
* Forward lookup `ACTIVES[name]` (KeyError if unknown) everywhere a name is given.
* Reverse lookup (router, `opcode_to_name`): `list(keys)[list(values).index(id)]` ⇒ the **first-inserted** key with that id; ValueError if none (⇒ router mutex deadlock).
* Runtime updates (all overwrite/insert into the global dict; never called automatically):
  * `get_ALL_Binary_ACTIVES_OPCODE` / `update_ACTIVES_OPCODE`: from `get-initialization-data` (v2), for `binary` and `turbo`: if `exchange == ""` or `provider == "OTC"`: `ACTIVES[active["ticker"]] = int(active_id_key)`.
  * `get_All_Digital_ACTIVES_OPCODE`: `ACTIVES[underlying["name"]] = underlying["active_id"]`.
  * `instruments_input_to_ACTIVES(type)`: `ACTIVES[instrument["id"]] = instrument["active_id"]` for crypto/forex/cfd.
* Hazard: once two names map to the same id, stream data is filed under the first name while `real_time_candles_maxdict_table` is keyed by the name you subscribed with ⇒ maxdict lookup yields `{}` ⇒ TypeError in router ⇒ deadlock. Updating an existing key keeps its original insertion position (Python dict semantics) — a C# port needs an insertion-ordered map to reproduce reverse lookups.

### 4.7 Profit / P&L calculations
1. **Binary/turbo/blitz payout fraction** (`get_all_profit*`): `(100.0 - active["option"]["profit"]["commission"]) / 100.0` (double). v1 stores `{}` for disabled actives; v2 skips suspended/disabled.
2. **Blitz `profit_percent`**: `int(fraction * 100)` — truncation of a double: commissions 42, 43, 71 give 57, 56, 28 (one less than `100-commission`). Reproduce with double math + truncation.
3. **Digital profit %** (router): `((100 - ask) * 100) / ask` with `ask = float(...)`; None when `ask` missing (client-price-generated) or `price.ask is None` (instrument-quotes-generated). `digital_profit[asset][period]` uses only the SPT **call** side.
4. **Strike key** (`get_strike_list`): `"%.6f" % (float(value) * 10e-7)` (10e-7 == 1e-6 exactly as a double literal) ⇒ e.g. `"1.123450"`.
5. **check_win**: `round(pnl_realized, 2)` (Python round-half-even on the binary double; .NET `Math.Round(x, 2)` default ToEven is the closest analogue).
6. **check_win_v2**: `profit - deposit`; **check_win_v3**: `profit_amount - amount`.
7. **check_win_digital**: `default` → `pnl_realized`; `expired` → `pnl_realized - buy_amount`. **check_win_digital_v2**: `expired` → `close_profit - invest`; `default` → `pnl_realized`.
8. **Digital sell-back value** (`get_digital_spot_profit_after_sale`, from decompiled `Position.java`):
```
p      = order_async[position_id]["position-changed"]["msg"]["raw_event"]
z      = False                        # intended: True for call ("MCSPT"), but str.find() is always truthy ⇒ always False
amount = max(p.buy_amount, p.sell_amount)
dur    = int(p.instrument_period / 60)
count  = p.count
K = p.instrument_strike_value / 1e6
L = p.extra_data.lower_instrument_strike / 1e6
U = p.extra_data.upper_instrument_strike / 1e6
q = get_instrument_quites_generated_data(p.instrument_underlying, dur)   # raw client-price-generated msg (blocks)
bid(id) = first row in q.msg.prices with row.call.symbol==id → row.call.bid, or row.put.symbol==id → row.put.bid, else None
f  = bid(p.extra_data.lower_instrument_id)  (else cached value, default 0)   — cached per position_id
f2 = bid(p.extra_data.upper_instrument_id)  (else cached value, default 0)
if L != K and f is not None and f2 is not None:
    if L > K or K > U:
        if z: K = (U - K) / abs(U - L); f = abs(f2 - f)
        else: K = (K - U) / abs(U - L); f = abs(f2 - f)
    elif z: f += ((K - L) / (U - L)) * (f2 - f)          # K NOT normalised in this branch (original bug)
    else:  K = (U - K) / (U - L); f -= f2
    f = f2 + K * f
return (f / p.currency_rate) * count - amount   if f is not None else None
```

### 4.8 Real-time candle buffering
* Storage: `api.real_time_candles = nested_dict(3, dict)` → `[active_name:str][size:int][from:int] = candle`; `api.real_time_candles_maxdict_table = nested_dict(2, dict)` → `[active_name][size] = maxdict` (set only by `start_candles_stream`); `candle_generated_check = nested_dict(2, dict)` (`[name][size]` = `{}` | `True`); `candle_generated_all_size_check = nested_dict(1, dict)`.
* Seeding: `start_candles_stream` first calls `get_candles(ACTIVE, size, count=maxdict, endtime=server_ts)` and stores each by `from`.
* `dict_queue_add` (exact):
```python
def dict_queue_add(self, dict, maxdict, key1, key2, key3, value):
    if key3 in dict[key1][key2]:
        dict[key1][key2][key3] = value                       # same candle (same 'from') → replace
    else:
        while True:
            try: dic_size = len(dict[key1][key2])
            except: dic_size = 0
            if dic_size < maxdict:
                dict[key1][key2][key3] = value
                break
            else:
                del dict[key1][key2][sorted(dict[key1][key2].keys(), reverse=False)[0]]   # evict smallest 'from'
```
  Edge cases: `maxdict` missing ⇒ `{}` ⇒ `int < dict` TypeError; `maxdict == 0` ⇒ IndexError on empty dict.
* `candle-generated` stores the message `msg` as-is; `candles-generated` enriches every per-size candle with `active_id, at, ask, bid, close (= msg.value), size (= int(key))` before storing.
* Stream "started" detection = first candle sets the check to True (subscribe re-sent every 1 s up to 20 s). "Stopped" = 1 s without a new candle after unsubscribe.

### 4.9 Live-deal buffers
`api.live_deal_data = nested_dict(3, deque)` → `[name][active_name][type]`; router `appendleft(msg)` (newest left). Default deque is unbounded; `clear_live_deal(..., buffersize)` installs `deque(maxlen=buffersize)`. `pop_live_deal` pops the right end (oldest).

### 4.10 Time / timezone handling summary
| Where | Clock | TZ |
|---|---|---|
| `server_ts` | `timeSync` ms / 1000 | UTC epoch |
| `buy` / `buy_multi` (`Buyv3`) expiry base | `int(time.time())` (local machine clock) | local datetime arithmetic |
| `get_expiration_time` remaining-time comparison | `time.time()` | — |
| `get_remaning` | `server_ts` for candidates, `time.time()` for remaining | local |
| `get_active_exposure`, digital buys | `int(server_ts)` | minute test local, id formatted UTC |
| strike list | `int(server_ts)` | minute test local |
| `buy_blitz` | `server_ts + duration` | — |
| `get_all_open_time` schedule test | `time.time()` | epoch compare |
| heartbeat `userTime` | `int(server_ts*1000)` | — |
| commission-changed key | `server_ts` at receipt (float) | — |

### 4.11 Busy-wait / sentinel semantics to preserve (or consciously replace)
* `None` sentinel slots (single global per message type, no correlation) vs request_id-keyed dicts (only for: `option`, `order`, `order-placed-temp`, `digital-option-placed`, `result`, `top-assets`, `transactions`, `active-exposure`, `history`, `history-positions`, `sold-options`).
* `{}` sentinel in auto-vivifying dicts (`order_async`, strike/quote data, candle checks, `position_history_v2`).
* Almost all waits have no timeout; exceptions: `get_instruments` (10 s resend), `get_all_init` (30 s retry loop), `get_all_init_v2` / `get_digital_underlying_list_data` (30 s → None), `get_betinfo` (10 s → reconnect), candle start (20 s → False).

### 4.12 JSON byte-equivalence notes for C#
* Use `", "` / `": "` separators and ASCII escaping (`\uXXXX`, lower-case hex as Python emits), preserve insertion order, never emit numbers for `request_id`.
* Python float formatting: `float(amount)` in `place-order-temp` always shows a fraction (`1.0`); `repr` exponent style `1e-05` / `1e+16`; integers that are Python `int` print without `.0`. Values passed through "raw" keep the caller's int/float type (e.g. `price` in buys).
* `str(amount)` for digital `amount` follows Python `str(float)` (`"1"` for int 1, `"1.0"` for float 1.0, shortest round-trip repr otherwise).
* `str(list)` in `portfolio.get-positions` = `"['cfd', 'forex', 'crypto', 'digital-option', 'turbo-option', 'binary-option']"`.
* Server-side key order is irrelevant to IQ Option, but these rules are required for byte-for-byte fidelity.

---

## 5. HTTP endpoints

All go through the IQOptionAPI `requests.Session` (verify=False, trust_env=False, proxies/proxy-auth if configured, headers = requests defaults `Accept: */*`, `Accept-Encoding: gzip, deflate`, `Connection: keep-alive` + `SESSION_HEADER` (default UA `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36`), cookies = `SESSION_COOKIE` + `ssid` after connect) unless stated.

| # | Method & URL | Payload | Headers (extra) | Called by | Notes |
|---|---|---|---|---|---|
| 1 | `POST https://auth.iqoption.com/api/v2/login` | form-urlencoded `identifier=<email>&password=<password>[&token=<2FA token>]` (requests drops the `token` field when None) | `Content-Type: application/x-www-form-urlencoded` | `IQOptionAPI.get_ssid()` ← `connect()` | success = HTTP 200 **and** `Set-Cookie: ssid=...`; `send_http_request_v2` (no raise_for_status) |
| 2 | `POST https://auth.iqoption.com/api/v2/verify/2fa` | raw JSON `{"method": <m>, "token": <t>, "code": <c>}` (json.dumps, nulls kept) | `CONTENT-TYPE: application/json` | `IQ_Option.TWO_FA` | returns JSON |
| 3 | `POST https://auth.iqoption.com/api/v1.0/logout` | none | — | `logout`, `close`, start of `api.connect`, atexit | response ignored |
| 4 | `POST https://auth.iqoption.com/api/v2/login` (reconnect) | form `identifier`, `password` (token None ⇒ omitted) | `User-Agent` (Chrome/66) only, **separate fresh `requests.Session`** (TLS verify on, env proxies honoured, no configured proxy) | `on_open` after a close | |
| 5 | `POST https://auth.iqoption.com/api/v1.0/logout` (reconnect) | none | UA only, previous reconnect session | `on_open` | errors swallowed |
| 6 | `<method> https://event.iqoption.com/api/v1/events` | `data` as given (form) | — | `api.events(method, data)` | not used by IQ_Option |
| 7 | `GET https://iqoption.com/api/appinit` | — | — | `api.appinit()` | `send_http_request` (raise_for_status); unused |
| 8 | `GET https://iqoption.com/api/getprofile` | — | — | `api.getprofile()` | only referenced in commented-out code |
| 9 | `GET https://iqoption.com/api/auth/token` | — | — | `api.token()` | unused |
| 10 | `POST https://iqoption.com/api/billing/buyback` | form `option_id=<id>` (from `{"option_id": [id]}`) | — | `api.buyback(option_id)` | unused |
| 11 | `POST https://iqoption.com/api/profile/changebalance` | form `balance_id=<id>` | — | `api.changebalance(balance_id)` | unused (change_balance is local only) |
| 12 | `GET https://iqoption.com/api/register/getregdata` | — | — | class `Getprofile` in http/getregdata.py | not wired into `IQOptionAPI` |
| — | `Auth` (`auth`), `Billing` (`billing`), `Profile` (`profile`), `Register` (`register`) | | | | URL-prefix holders only; `Loginv2` (url `/v2`) overrides nothing ⇒ identical to #1 |

WebSocket: `wss://iqoption.com/echo/websocket` (sslopt `check_hostname=False`, `cert_reqs=CERT_NONE`, `ca_certs="cacert.pem"`).

---

## 6. `IQOptionAPI` state fields (initial values)

| field | init | written by router message |
|---|---|---|
| `socket_option_opened`, `digital_opened` | `{}` | position-changed (open) |
| `get_top_assets_data` | `{}` | top-assets |
| `timesync` | `TimeSync()` | timeSync |
| `profile` | `Profile()` (`.msg=None`; skey/balance/balance_id/balances unused; `balance_type` getter raises AttributeError if never set) | profile |
| `candles` | `Candles()` (`.candles_data=None`; `first_candle/second_candle/current_candle` → `Candle(list)` helpers index `[0..4]` = time/open/close/high/low; `candle_type` green/red/None — legacy, unused) | candles |
| `listinfodata` | `ListInfoData()` | listInfoData |
| `api_option_init_all_result` | `[]` | api_option_init_all_result |
| `api_option_init_all_result_v2` | `[]` | initialization-data |
| `underlying_list_data` | None | underlying-list |
| `position_changed` | None | position-changed (last) |
| `instrument_quites_generated_data` | nested_dict(2, dict) | client-price-generated / instrument-quotes-generated |
| `digital_profit` | nested_dict(2, dict) | client-price-generated |
| `instrument_quotes_generated_raw_data` | nested_dict(2, dict) | same |
| `instrument_quites_generated_timestamp` | nested_dict(2, dict) | instrument-quotes-generated |
| `strike_list` | None | strike-list |
| `leaderboard_deals_client` | None | leaderboard-deals-client |
| `order_async` | nested_dict(2, dict) | position-changed, order-changed, option-opened/closed |
| `game_betinfo` | `Game_betinfo_data()` (`isSuccessful=None`, `dict={}`) | api_game_betinfo_result |
| `instruments`, `financial_information` | None | instruments, financial-information |
| `buy_id`, `buy_successful` | None | buyComplete / buyV2_result (legacy) |
| `buy_order_id`, `order_data` | `{}` | order-placed-temp, order |
| `traders_mood` | `{}` | traders-mood-changed |
| `positions`, `position`, `deferred_orders`, `position_history` | None | positions, position, deferred-orders, position-history |
| `position_history_v2` | nested_dict(2, dict) | history |
| `available_leverages`, `order_canceled`, `close_position_data`, `overnight_fee` | None | resp. names |
| `authenticated` | None | authenticated |
| `balance` | `{}` | balance-changed |
| `digital_option_placed` | `{}` | digital-option-placed |
| `live_deal_data` | nested_dict(3, deque) | live-deal* |
| `history_positions`, `transactions` | `{}` | history-positions, transactions |
| `subscribe_commission_changed_data` | nested_dict(2, dict) | commission-changed |
| `real_time_candles` | nested_dict(3, dict) | candle(s)-generated |
| `real_time_candles_maxdict_table`, `candle_generated_check` | nested_dict(2, dict) | (check) candle-generated |
| `candle_generated_all_size_check` | nested_dict(1, dict) | candles-generated |
| `api_game_getoptions_result` | None | api_game_getoptions_result |
| `sold_options_respond` | `{}` | sold-options |
| `tpsl_changed_respond`, `auto_margin_call_changed_respond` | None | tpsl-changed, auto-margin-call-changed |
| `top_assets_updated_data` | `{}` | top-assets-updated |
| `get_options_v2_data` | None | options |
| `buy_multi_result` | None | (unused) |
| `buy_multi_option`, `active_exposure`, `result` | `{}` | option, active-exposure, result |
| `training_balance_reset_request`, `balances_raw`, `user_profile_client`, `leaderboard_userinfo_deals_client`, `users_availability` | None | resp. names |
| `Buyv3`, `BlitzBuy` | channel instances | — |

---

## 7. Channels / api methods present but not used by `IQ_Option`

| Channel / method | Frame |
|---|---|
| `api.buy` → `Buyv2` | outer `buyV2`, msg `{"price": p, "act": active, "exp": int(exp), "type": <"turbo" or "binary">, "direction": dir.lower(), "user_balance_id": BAL, "time": server_ts}` (exp via `get_expiration_time(int(time.time()), duration)`, turbo if idx<5); sets `api.buy_successful = None` on access |
| `api.setactives` → `SetActives` | outer `setActives`, msg `{"actives": actives}` |
| ws `Changebalance` (not imported) | outer `api_profile_changebalance`, msg `{"balance_id": id}` |
| ws `Buyback` | `__call__` is `pass` |
| `Subscribe_Instrument_Quites_Generated` (v1) | outer `subscribeMessage`, msg `{"name": "instrument-quotes-generated", "params": {"routingFilters": {"active": int(ACT(a)), "expiration_period": int(p*60), "kind": "digital-option"}}, "version": "1.0"}` |
| `Unsubscribe_Instrument_Quites_Generated` (v1) | same with `unsubscribeMessage` |
| `api.set_user_settings(balanceId, request_id="")` | `sendMessage`, `{"name": "set-user-settings", "version": "1.0", "body": {"name": "traderoom_gl_common", "version": 3, "config": {"balanceId": balanceId}}}`, request_id `str(request_id)` |
| `api.subscribe_position_changed(name, instrument_type, request_id)` | `subscribeMessage`, `{"name": name, "version": "1.0", "params": {"routingFilters": {"instrument_type": str(instrument_type)}}}`, request_id str — only in a commented block in `connect()` |
| `api.websocket_alive()` | `websocket_thread.is_alive()` |
| `api.events`, `appinit`, `token`, `getprofile`, `buyback`, `changebalance`, `loginv2`, `auth`, `billing` | HTTP, §5 |
| `api.portfolio(..., name="portfolio.get-positions")` with outer `unsubscribeMessage` etc. | generic helper; only the three names shown in §1 are built (any other `name` ⇒ UnboundLocalError on `msg`) |

---

## 8. Top quirks a port must decide on (reproduce vs fix)

1. **Router holds the send mutex with no try/finally** — any exception inside `on_message` (unknown active id in a reverse lookup, missing candle maxdict after reconnect, `client-price-generated` without SPT, missing `microserviceName`, user callback error) or the early `return` leaves `ssl_Mutex` locked ⇒ every later send deadlocks silently.
2. **Uncorrelated busy-waits** — ~40 methods spin on a single shared slot (`while x is None: pass`) with no timeout and no request_id; concurrent calls race; a disconnect or an error reply under another message name hangs forever. Races also exist where the slot is reset after sending (`get_active_exposure`) or never reset (`get_profile`, `get_balances`, `close_position_v2`).
3. **Expiration math uses mixed clocks/time zones** — binary `buy` derives expiry from the local `time.time()`, `get_expiration_time` compares with `time.time()`, digital uses local `minute % duration` but UTC-formatted ids; `get_active_exposure` may send a float time with seconds.
4. **Reconnect semantics** — `close()` and every `connect()` trigger the low-level auto-reconnect of the old socket; low-level reconnect re-authenticates but re-subscribes nothing, ignores proxies/TLS options/2FA, recurses `run_forever`; `connect()` always resets the active balance to PRACTICE and loses the client callback.
5. **Byte-level oddities in outgoing JSON** — heartbeat double `msg` nesting; `portfolio.get-positions` sends a Python list repr string; `get-candles` body has an empty-string key; digital `amount` is `str(amount)`; `place-order-temp` `amount` is always a float and `client_platform_id` the string `"9"`; several `sendMessage` bodies omit `version`; Python `json.dumps` separators `", "`/`": "`.
6. Logic bugs: `start_mood_stream`/`stop_mood_stream` chained comparison (never tracks moods); `get_digital_spot_profit_after_sale` call/put flag always False; `buy_by_raw_expirations` never reports failure; `close_position_v2` sends `{}` as position id; `get_candles` never calls `check_connect`; `auto_reconnect[...]==False` comparison typo; `start_websocket` returns the whole error dict; `buy_blitz` truncation (`int(0.29*100) == 28`) and a blocking init round-trip per buy; `buy_digital_spot` returns `-1` (not a tuple) for a bad action.

---

## Appendix A. constants.ACTIVES (198 entries, source order)

Copy verbatim; key order matters for reverse lookups (first key wins).

```json
{"LINKUSD-OTC": 2143, "EURAUD-OTC": 2120, "EURCHF-OTC": 2131, "ATOMUSD-OTC": 2150, "USDPLN-OTC": 2128, "AS51": 2048, "GBPCAD-OTC": 2114, "XRPUSD": 2107, "USDZAR-OTC": 1380, "GBPAUD-OTC": 2116, "SATSUSD-OTC": 2159, "AMZN/EBAY-OTC": 2083, "USDHKD-OTC": 1382, "MCDON": 2103, "USDJPY": 1865, "TIAUSD-OTC": 2162, "US30/JP225-OTC": 2079, "USDNOK-OTC": 2121, "BIDU-OTC": 2097, "USDSGD-OTC": 1381, "NFLX/AMZN-OTC": 2090, "US100/JP225-OTC": 2080, "HMSTR-OTC": 2192, "INTEL/IBM-OTC": 2089, "CADJPY-OTC": 2136, "USNDAQ 100 (NDX) Spot Index": 1972, "PYTHUSD-OTC": 2160, "RONINUSD-OTC": 2161, "NIKE": 2105, "DOTUSD-OTC": 2149, "WIFUSD-OTC": 2144, "AUDJPY-OTC": 2113, "JPM": 2102, "USDMXN-OTC-L": 1548, "NOTCOIN-OTC": 2096, "DAX": 2046, "USDTHB-OTC": 2182, "NVDA/AMD-OTC": 2084, "AUDCAD": 1868, "STXUSD-OTC": 2166, "USDCHF-OTC": 78, "LTCUSD-OTC": 2126, "BONKUSD-OTC": 2142, "EURTHB-OTC": 2181, "JUPUSD-OTC": 2141, "CHFJPY-OTC": 2118, "AUDJPY": 1869, "S&P 500": 1971, "USDCAD-OTC": 2112, "NZDUSD": 1896, "TON/USD-OTC": 2091, "INTEL": 2098, "ICPUSD-OTC": 2139, "IMXUSD-OTC": 2140, "SNAP-OTC": 2125, "MSFT/AAPL-OTC": 2088, "SEIUSD-OTC": 2152, "EURNZD-OTC": 2122, "MORSTAN": 2104, "GS": 2110, "IOTAUSD-OTC": 2153, "AUDCAD-OTC": 86, "EURGBP-OTC": 77, "NZDJPY-OTC": 2138, "GER30/UK100-OTC": 2093, "GALAUSD-OTC": 2147, "INJUSD-OTC": 2151, "GBPCHF-OTC": 2115, "BCHUSD-OTC": 2148, "AUDUSD": 1870, "Dollar Index": 2062, "NZDCAD-OTC": 2137, "GRTUSD-OTC": 2165, "US2000 Spot Index": 2108, "JPYTHB-OTC": 2183, "AMZN/ALIBABA-OTC": 2082, "WLDUSD-OTC": 2157, "ALIBABA": 2106, "US500/JP225-OTC": 2081, "CHFNOK-OTC": 2200, "NEARUSD-OTC": 2168, "Yen_Index": 2063, "COKE": 2101, "AUDUSD-OTC": 2111, "EOSUSD-OTC": 2127, "NKY": 2051, "EURGBP": 1862, "PEPEUSD-OTC": 2145, "GBPNZD-OTC": 2132, "SANDUSD-OTC": 2164, "DASHUSD-OTC": 2155, "CAC": 2045, "IBEX": 2044, "XAU/XAG-OTC": 2086, "BEAMUSD-OTC": 2154, "CADCHF-OTC": 2119, "AUDCHF-OTC": 2129, "EURCAD-OTC": 2117, "EURJPY": 1864, "AIG": 2109, "MANAUSD-OTC": 2163, "SX5E": 2050, "GBPUSD": 1867, "NOKJPY-OTC": 2201, "CITI": 2100, "FLOKIUSD-OTC": 2146, "GOOGLE/MSFT-OTC": 2085, "HSI": 2049, "ARBUSD-OTC": 2156, "MATICUSD-OTC": 2167, "INDU": 1973, "NZDCHF-OTC": 2202, "EURUSD": 1861, "GBPUSD-OTC": 81, "UKX": 2047, "MSFT": 2099, "USDTRY-OTC": 2124, "USDSEK-OTC": 2123, "ORDIUSD-OTC": 2158, "TESLA/FORD-OTC": 2087, "GBPJPY": 1866, "META/GOOGLE-OTC": 2094, "EURUSD-OTC": 76, "XAUUSD": 1912, "USDJPY-OTC": 85, "CADCHF": 1871, "TESLA": 1936, "MS": 1907, "KO": 1922, "FB": 1937, "INTC": 1928, "GOOGL": 1933, "GBPJPY-OTC": 84, "AMZN": 1935, "NZDUSD-OTC": 80, "EURJPY-OTC": 79, "C": 1903, "MCD": 1906, "AAPL": 1938, "USDCAD": 1878, "BIDU": 1911, "AUDNZD-OTC": 2130, "BABA": 1908, "EURCHF": 1876, "USDMXN-OTC": 2300, "AUDNZD": 1900, "EURNZD": 1901, "TrumpCoin OTC": 2265, "FARTCOINUSD-OTC": 2279, "OTC": 2287, "ONDOUSD-OTC": 2276, "USDINR-OTC": 1383, "XAGUSD-OTC": 1858, "USOUSD-OTC": 1859, "LABUBUUSD-OTC": 2304, "DYDXUSD-OTC": 2277, "MUSKvsTRUMP-OTC": 2296, "ETHUSD": 1918, "TESLA/FORD": 2072, "PENGUUSD-OTC": 2280, "XAU/XAG": 2071, "META/GOOGLE": 2076, "US30/JP225": 2064, "MSFT/AAPL": 2073, "NZDJPY": 1882, "CISCO": 1902, "ONYXCOINUSD-OTC": 2278, "USDCOP-OTC": 2299, "EURAUD": 1874, "NZDCAD": 1881, "AMZN/ALIBABA": 2067, "AUDCHF": 1884, "USDBRL-OTC": 2298, "GBPCAD": 1897, "EURCAD": 1875, "GBPNZD": 1880, "XNGUSD-OTC": 1863, "GBPAUD": 1877, "UKOUSD-OTC": 1931, "NFLX/AMZN": 2075, "PENUSD-OTC": 2301, "AMZN/EBAY": 2068, "MelaniaCoin OTC": 2267, "NVDA/AMD": 2069, "BTCUSD": 1916, "XAUUSD-OTC": 1857, "US100/JP225": 2065, "INTEL/IBM": 2074, "US500/JP225": 2066, "GER30/UK100": 2078, "SNAP": 1904, "GBPCHF": 1898, "GOOGLE/MSFT": 2070, "BTCUSD-OTC-op": 2270, "SOLUSD": 1978, "ADAUSD": 1974, "TRON-OTC": 1976, "ETHUSD-OTC": 1941}
```

## Appendix B. country_id.ID (190 entries, source order) — used by get_leader_board

```json
{"Worldwide": 0, "AF": 1, "AL": 2, "DZ": 3, "AD": 5, "AO": 6, "AI": 7, "AG": 9, "AR": 10, "AM": 11, "AW": 12, "AT": 14, "AZ": 15, "BS": 16, "BH": 17, "BD": 18, "BB": 19, "BY": 20, "BZ": 22, "BJ": 23, "BM": 24, "BO": 26, "BA": 27, "BW": 28, "BV": 29, "BR": 30, "BN": 31, "BG": 32, "BF": 33, "BI": 34, "KH": 35, "CM": 36, "CV": 38, "KY": 39, "TD": 41, "CL": 42, "CN": 43, "CC": 45, "CO": 46, "KM": 47, "CG": 48, "CK": 49, "CR": 50, "CI": 51, "HR": 52, "CU": 53, "CY": 54, "CZ": 55, "DK": 56, "DJ": 57, "DM": 58, "DO": 59, "TL": 60, "EC": 61, "EG": 62, "SV": 63, "EE": 66, "ET": 67, "FO": 69, "FJ": 70, "FI": 71, "FR": 72, "GF": 73, "PF": 74, "GA": 75, "GM": 76, "GE": 77, "DE": 78, "GH": 79, "GR": 81, "GD": 83, "GP": 84, "GT": 86, "GN": 87, "GY": 88, "HT": 89, "HN": 90, "HK": 91, "HU": 92, "IS": 93, "ID": 94, "IQ": 95, "IE": 96, "IT": 97, "JM": 98, "JO": 100, "KZ": 101, "KE": 102, "KI": 103, "KW": 104, "KG": 105, "LA": 106, "LV": 107, "LB": 108, "LS": 109, "LR": 110, "LY": 111, "LT": 113, "LU": 114, "MO": 115, "MK": 116, "MG": 117, "MW": 118, "MY": 119, "MV": 120, "ML": 121, "MT": 122, "MQ": 124, "MR": 125, "MU": 126, "MX": 128, "FM": 129, "MD": 130, "MC": 131, "MN": 132, "MA": 134, "MZ": 135, "MM": 136, "NA": 137, "NP": 139, "NL": 140, "AN": 141, "NC": 142, "NZ": 143, "NI": 144, "NE": 145, "NG": 146, "NO": 149, "OM": 150, "PK": 151, "PW": 152, "PA": 153, "PG": 154, "PY": 155, "PE": 156, "PH": 157, "PL": 159, "PT": 160, "QA": 162, "RE": 163, "RO": 164, "RW": 166, "KN": 167, "LC": 168, "SA": 171, "SN": 172, "SC": 173, "SG": 175, "SK": 176, "SI": 177, "SO": 179, "ZA": 180, "KR": 181, "ES": 182, "LK": 183, "SH": 184, "SR": 186, "SZ": 187, "SE": 188, "CH": 189, "TW": 191, "TJ": 192, "TZ": 193, "TH": 194, "TG": 195, "TT": 198, "TN": 199, "TR": 200, "TM": 201, "UG": 203, "UA": 204, "AE": 205, "GB": 206, "UY": 207, "UZ": 208, "VE": 211, "VN": 212, "VG": 213, "YE": 216, "ZM": 218, "ZW": 219, "RS": 220, "ME": 221, "IN": 225, "TC": 234, "CD": 235, "GG": 236, "IM": 237, "JE": 239, "CW": 246}
```
