# IqOptionApi for C# — Design Spec

**Date:** 2026-10-09
**Status:** Approved in conversation (parts 1–4), awaiting review of this written spec
**Source of truth for behaviour:** the Python library in this repository (`iqoptionapi/`, v9.4).
**Message-level reference:** [`python-api-catalogue.md`](python-api-catalogue.md) (every Python method, frame, router branch and helper, with section numbers referenced below as "Cat §x").

---

## 1. Intent

| | |
|---|---|
| **What the user said** | Convert the whole Python `iqoptionapi` library to C#. Keep it simple: a few big files, not 80+ small ones. It must work exactly like the Python version. It is a library to import into the user's **own trading bot**. It lives in the new `IqOptionApi.CSharp/` folder. |
| **Agreed in conversation** | .NET 9, no third-party packages in the library, async/await API, mixed return types (typed for hot paths, raw JSON for big server payloads), "request matching + events" engine, the file layout below, merged duplicates, the copy/fix lists in §6, SSL verification on, the testing plan in §9, git for history. |
| **Success** | (1) Builds with zero warnings. (2) All tests pass. (3) Every outgoing frame is byte-identical to what the Python code sends for the same inputs (golden tests). (4) The example bot works on the user's PRACTICE account. |

**Non-goals:** no UI, no trading strategy, no NuGet publishing, no porting of legacy endpoints that the Python library never uses (Cat §7), no porting of old duplicate methods as separate methods (their replacements are named in §5, the leftovers in §5.10).

---

## 2. Technology

- **Target:** `net9.0` (the only runtime installed on this machine; runs on Windows, macOS and Linux).
- **Library dependencies:** none. Only built-ins: `HttpClient`, `ClientWebSocket`, `System.Text.Json` (`JsonNode`/`JsonObject`/`JsonArray`), `System.Collections.Generic.OrderedDictionary<TKey,TValue>` (.NET 9).
- **Project settings:** `<Nullable>enable</Nullable>`, `<ImplicitUsings>enable</ImplicitUsings>`, `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>`, `<GenerateDocumentationFile>true</GenerateDocumentationFile>` (every public member has an XML doc comment, so IntelliSense explains it).
- **Test-only dependencies:** xUnit (`xunit`, `xunit.runner.visualstudio`, `Microsoft.NET.Test.Sdk`) and the ASP.NET Core shared framework (`<FrameworkReference Include="Microsoft.AspNetCore.App" />`, already installed) for the fake server.

---

## 3. Folder and file layout

```
IqOptionApi.CSharp/
├── IqOptionApi.sln
├── .gitignore                         bin/ obj/ TestResults/ .vs/ *.user
├── README.md                          usage guide + Python→C# method table
├── src/IqOptionApi/
│   ├── IqOptionApi.csproj
│   ├── IqOption.cs                    public class IqOption — every public method, one #region per group (§5)
│   ├── Connection.cs                  internal: HTTP login/2FA/logout, WebSocket, send lock, request matching,
│   │                                  heartbeat, auto-reconnect
│   ├── MessageRouter.cs               internal: incoming message → stores + events (port of ws/client.py)
│   ├── Models.cs                      public: IqOptionOptions, enums, Candle, StrikeQuote, InstrumentTypes, …
│   ├── Actives.cs                     internal: asset table (198 entries, Cat App. A) + country table (Cat App. B)
│   └── Helpers.cs                     internal: expiry maths, digital ids, profit formulas, Python-style JSON writer
├── tests/
│   ├── golden/generate_golden.py      runs the real Python code with a fake network → golden JSON files
│   └── IqOptionApi.Tests/             xUnit: golden, router, maths, fake-server flow tests
│       └── Golden/*.json              committed output of generate_golden.py
├── examples/SimpleBot/                console example (practice account only by default)
│   ├── SimpleBot.csproj
│   └── Program.cs
└── docs/specs/                        this spec + python-api-catalogue.md
```

The bot sees one class, `IqOption`, plus the small public types in `Models.cs`. Everything else is `internal` (tests reach internals via `InternalsVisibleTo("IqOptionApi.Tests")`).

**Using it from a bot:** `dotnet add reference <path>/src/IqOptionApi/IqOptionApi.csproj`, or copy `IqOptionApi.dll`, or `dotnet pack` and install the package locally. Then `using IqOptionApi;`.

---

## 4. Engine

### 4.1 Options

```csharp
public sealed class IqOptionOptions
{
    public string Host { get; set; } = "iqoption.com";              // wss://{Host}/echo/websocket
    public string? WebSocketUrl { get; set; }                        // null = wss://{Host}/echo/websocket; overridable (tests)
    public string AuthUrl { get; set; } = "https://auth.iqoption.com"; // /api/v2/login, /api/v2/verify/2fa, /api/v1.0/logout
    public string? ProxyHost { get; set; }
    public int? ProxyPort { get; set; }
    public string? ProxyUser { get; set; }
    public string? ProxyPassword { get; set; }
    public string? Ssid { get; set; }                                // Python set_ssid
    public bool AutoLogout { get; set; } = true;                     // logout on Close/Dispose
    public bool AutoReconnect { get; set; } = true;
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
    public IReadOnlyList<TimeSpan>? ReconnectDelays { get; set; }    // null = 1, 2, 5, 10, 30 s (last repeats)
    public bool IgnoreCertificateErrors { get; set; } = false;      // Python always ignored them; C# checks by default
    public TimeZoneInfo? TimeZone { get; set; }                      // null = TimeZoneInfo.Local (Python uses local time)
    public Action<string>? Log { get; set; }
    public bool LogFrames { get; set; } = false;                     // also log raw frames (contain the SSID)
}
```

Proxy settings apply to HTTP **and** WebSocket, on first connect **and** on every reconnect (Python's reconnect dropped them).

### 4.2 Connecting (`ConnectAsync`)

Same steps and the same frames, in the same order, as Python `IQ_Option.connect()` (Cat §3.3, §3.4, §3.12):

1. Close any previous connection **without** triggering auto-reconnect.
2. If an SSID is known (options or previous session), open the socket and authenticate with it; if authentication fails or the server closes the socket, fall back to step 3.
3. HTTP `POST {AuthUrl}/api/v2/login`, form `identifier`, `password`, plus `token` only if a 2FA token was set. HTTP status ≠ 200 or no `ssid` cookie → return `(false, responseBody)` (this is how the 2FA "method/token" JSON reaches the caller, as in Python).
4. Open the WebSocket (`Origin: http://{Host}` header, as websocket-client 0.58 sends) and send `{"name": "authenticate", "msg": {"ssid": "<SSID>", "protocol": 3}, "request_id": "1"}` (request-id counter restarts at 1 on every new socket). Wait for `authenticated`; `false` → `(false, "authentication failed")`.
5. Send `get-profile` (request_id `""`) and `get-balances` (request_id `""`), wait for `profile` (≠ false) and the first `timeSync`.
6. Re-subscribe tracked streams (empty on first connect).
7. Wait for `balances`; select the **PRACTICE** balance (`type == 4`, last one wins), exactly like Python.
8. Send `get-balances` again, then for every balance id × `cfd, forex, crypto, digital-option, turbo-option, binary-option` a `portfolio.position-changed` subscription; the six `internal-billing.*` / `tournaments.*` subscriptions; `portfolio.order-changed` × the six instrument types; `setOptions {"sendResults": true}`.
9. Return `(true, null)`.

Every wait in this sequence uses `RequestTimeout`; on timeout the method returns `(false, "timeout: <step>")`.

### 4.3 Sending

- One `SemaphoreSlim` serialises all `SendAsync` calls (ClientWebSocket allows one concurrent send). Heartbeat replies go through the same lock.
- Frames are serialised by the Python-style JSON writer (§4.9), so they are byte-identical to Python `json.dumps`.
- If the socket is down when a request is about to be sent, the call waits for the reconnect (bounded by `RequestTimeout`) and then sends **once** (Python reconnects and re-sends once).

### 4.4 Request matching

Every method sends the **same `request_id` value Python sends** (a counter string, or `""`). Replies are matched in one of three ways, chosen per method to mirror how Python finds its answer:

| Kind | Python behaviour | C# behaviour |
|---|---|---|
| **By request_id** | dict keyed by `request_id` (`option`, `order`, `order-placed-temp`, `digital-option-placed`, `result`, `top-assets`, `transactions`, `active-exposure`, `history`, `history-positions`, `sold-options`) | `TaskCompletionSource` registered under (reply name, request_id) **before** sending; completed by the router. Fully concurrent. |
| **By reply name** | one shared slot per reply name (`positions`, `position`, `available-leverages`, `candles`, `profile`, …), reset to `None` then busy-waited | One gate (`SemaphoreSlim(1)`) per reply name: the method takes the gate, registers a waiter for the next message with that name, sends, awaits, releases. Requests that share a reply name (e.g. `get-positions` and `portfolio.get-positions` → `positions`; `get-position` and `digital-options.get-position` → `position`) share the gate, so answers can never be swapped. |
| **By pushed event** | busy-wait until an auto-vivified store entry appears (`order_async[id]["position-changed"]`, `option-closed`, strike/quote data, candle checks) | `WaitForAsync(predicate)` on the store: completes when the router stores a matching entry (or immediately if it is already there). |

Waits use `RequestTimeout` unless stated otherwise. Methods that wait for a **trade to finish** (`CheckWinAsync`, `CheckWinDigitalAsync`, `CheckWinMarginAsync`) have no fixed timeout (trades can last hours); they take a `CancellationToken`.

**Failure shapes** (instead of hanging):
- Methods that return a Python-style success tuple return their failure shape: `(false, …, "timeout")` / `(false, …, "connection lost")`, or `(false, null)` when the tuple has no error slot.
- Methods that return `bool` return `false`.
- Methods for which Python itself returns `None` after its own timeout (`GetAllInitAsync`, `GetDigitalUnderlyingListAsync`) return `null`.
- Methods that return data (JSON, candles, lists) throw `TimeoutException`, or `IqOptionException` for "connection lost" / "not connected".
- Calling any server method before `ConnectAsync` succeeded throws `InvalidOperationException("Call ConnectAsync first")`.

**Retry loops Python uses on purpose are kept** (same frames, same interval), bounded by `RequestTimeout` or by Python's own limit:

| Method | Python loop | C# |
|---|---|---|
| `GetInstrumentsAsync` | re-send every 10 s | same, until `RequestTimeout` |
| `GetAllInitAsync` | re-send every 3 s, give up after 30 s → `None` | same (no 3 s delay before the first check) |
| `StartCandlesStreamAsync` | re-subscribe every 1 s for up to 20 s | same → `bool` |
| `StopCandlesStreamAsync` | unsubscribe every 1 s until 1 s without candles | same, bounded by `RequestTimeout` |
| `StartMoodStreamAsync` | re-subscribe every 5 s until data | same, bounded by `RequestTimeout` → `bool` |
| `RequestLeaderboardUserinfoDealsClientAsync` | re-send every 0.2 s until `isSuccessful` | same, bounded |
| `GetUsersAvailabilityAsync` | re-send every 0.2 s until reply | same, bounded |
| `BuyOrderAsync` | `get-order` every 1 s while `pending_new` | same, bounded |
| `CheckWinMarginAsync` | `get-order` / `get-history-positions` every 0.5 s | same, no timeout (CancellationToken) |

### 4.5 Message router

- Port of every branch in Cat §2 that feeds a ported method, with the same storage keys and transformations (candle enrichment for `candles-generated`, `client-price-generated` profit maths and SPT period parsing, `position-changed` routing including Python's operator-precedence behaviour `(portfolio && digital) || trading`, `balances` → practice id, `balance-changed`, live deals `appendleft`, etc.).
- Branches for legacy messages whose request methods are not ported (`buyComplete`, `buyV2_result`, `listInfoData`, `api_option_init_all_result`, `api_game_betinfo_result`, `api_game_getoptions_result`, `instrument-quotes-generated`) are not stored; they are still visible through `MessageReceived`.
- **Each message is handled inside its own try/catch.** A message that cannot be handled (unknown asset id, missing field, no SPT row, …) is logged and skipped; it never blocks sending or later messages (Python deadlocks here, Cat §2 note).
- Reverse asset lookup (id → name) returns the first name in table order (Python semantics); an unknown id skips that store write.
- `MessageReceived` fires for every message before routing; exceptions thrown by the bot's handler are caught and logged.

### 4.6 Stores and events

Thread-safe stores (`ConcurrentDictionary` + locks where order matters) mirror the Python state fields that are read by public methods: real-time candles (`[asset][size][from]`, `dict_queue_add` eviction exactly as Cat §4.8), candle stream checks, digital profit / strike data / raw quotes (`[asset][period]`), traders' mood, commission changes, top-assets-updated, live deals (bounded deque, newest first), order-async (`[id]["position-changed" | "order-changed" | "option-opened" | "option-closed"]`), open-by-other-pc maps, balance cache.

Events (plain `event Action<…>`):

| Event | Raised when |
|---|---|
| `MessageReceived(JsonObject message)` | every incoming message (replaces `set_call_back_for_client`) |
| `CandleReceived(string asset, int size, Candle candle)` | a streamed candle is stored |
| `PositionChanged(JsonObject message)` | `position-changed` |
| `BalanceChanged(long balanceId, double amount)` | `balance-changed` |
| `Disconnected(string reason)` | the socket drops or reconnect gives up |
| `Reconnected()` | auto-reconnect finished re-subscribing |

### 4.7 Heartbeat and time

- `timeSync` → `ServerTimestamp = ms / 1000.0` (`GetServerTimestamp()`).
- `heartbeat` → reply `{"name": "heartbeat", "msg": {"msg": {"heartbeatTime": <int>, "userTime": <int(server_ts*1000)>}}, "request_id": ""}` (double `msg` kept).
- **All "now" values use the server clock** (fix 4, §6.2). Local-time steps in the expiry maths (Python `datetime.fromtimestamp` / `mktime`) use `options.TimeZone ?? TimeZoneInfo.Local`.

### 4.8 Auto-reconnect

Triggered when the socket closes or errors while the user has not called `CloseAsync` and `AutoReconnect` is true.

1. Raise `Disconnected(reason)`. Fail every in-flight request with "connection lost". **Requests already sent are never re-sent** (no double trades).
2. Retry with `ReconnectDelays`: re-run the §4.2 sequence (re-use SSID, else fresh login with the same proxy/TLS settings), but **keep the currently selected balance** (Python's low-level reconnect keeps it; only an explicit `ConnectAsync` resets to PRACTICE).
3. Re-subscribe everything that was active: candle streams (one size and all sizes, with their `maxDict`), mood streams, strike-list subscriptions, top-assets and commission subscriptions, live deals; plus the portfolio / billing subscriptions and `setOptions` from §4.2 step 8.
4. Recover trade results that may have been pushed while disconnected: each pending `CheckWinAsync` does one `get-history-positions` lookup (as Python `check_win` does), each pending `CheckWinDigitalAsync` one `digital-options.get-position` lookup.
5. Raise `Reconnected()`.
6. If login is rejected (wrong credentials, 2FA required), stop retrying and raise `Disconnected("login failed: …")`.

### 4.9 Python-compatible JSON writer

Outgoing frames are written by a small custom writer (in `Helpers.cs`) so they are byte-identical to Python `json.dumps` (Cat §0.1, §4.12):

- separators `", "` and `": "`, key insertion order, ASCII-only output with lowercase `\uXXXX` escapes;
- `true/false/null`; `request_id` always a JSON string;
- numbers: integers as digits; values Python passes through `float(...)` always print as floats (`1.0`, `1e-05`, `1e+16`, shortest round-trip repr); values Python passes through unchanged (`price` etc.) print as integers when they have no fractional part (matches the normal Python call `buy(1, …)`), otherwise as Python float repr;
- the odd-but-working payloads are reproduced: empty-string key in `get-candles`, digital `amount` as `str(amount)`, `client_platform_id` `"9"`, the Python-list text in `portfolio.get-positions`, bodies without `version` where Python omits it, key order `name, body, version` where Python uses it.

---

## 5. Public API

Conventions: PascalCase; methods that talk to the server end in `Async` and accept an optional `CancellationToken ct = default` as the last parameter (omitted below). Plain methods read local stores instantly. Assets are passed by name (`"EURUSD"`). Raw server data is `JsonObject`/`JsonNode`. Each row names the Python method(s) it replaces; frames are in the catalogue section listed.

### 5.1 Public types (`Models.cs`)

- `enum Direction { Call, Put }` → serialised `"call"` / `"put"`; digital ids use `C` / `P`.
- `enum BalanceMode { Real, Practice }` (`type` 1 / 4).
- `enum TradeResult { Win, Loss, Equal }` (sign of the profit).
- `record Candle` — `Id, From, To, At, Open, Close, Min, Max, Volume` (+ stream extras `Ask, Bid, Size, ActiveId`, nullable) and `Raw` (`JsonObject`). Prices are `double` (Python `float`).
- `record StrikeQuote(string CallId, double? CallProfit, string PutId, double? PutProfit)`.
- `static class InstrumentTypes` — string constants `Cfd = "cfd"`, `Forex = "forex"`, `Crypto = "crypto"`, `DigitalOption = "digital-option"`, `TurboOption = "turbo-option"`, `BinaryOption = "binary-option"`, `FxOption = "fx-option"`.
- `class IqOptionException : Exception`.
- `IqOptionOptions` (§4.1).
- `static IReadOnlyList<int> IqOption.CandleSizes` = 1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 604800, 2592000.

### 5.2 Connection & session

| C# | Python | Notes |
|---|---|---|
| `new IqOption(string email, string password, IqOptionOptions? options = null)` | `__init__` | no network |
| `Task<(bool Success, string? Reason)> ConnectAsync()` | `connect` | §4.2 |
| `Task<JsonNode?> TwoFactorAsync(string token, string? method = null, string? code = null)` | `TWO_FA` | Cat §3.7 |
| `void Set2FAToken(string token)` | `setting_2FA_TOKEN` | used by next login |
| `void SetSession(IDictionary<string,string> headers, IDictionary<string,string> cookies)` | `set_session` | default UA = Python's Chrome/66 string |
| `Task LogoutAsync()` | `logout` | |
| `Task CloseAsync()` / `ValueTask DisposeAsync()` | `close`, `atexit` logout | really stops (fix 9) |
| `bool IsConnected` | `check_connect` | |
| `string? Ssid` | `get_ssid` | |
| `double GetServerTimestamp()` | `get_server_timestamp` | |
| events (§4.6) | `set_call_back_for_client` | |
| *(automatic)* | `re_subscribe_stream`, `position_change_all`, `order_changed_all` | inside connect/reconnect |

### 5.3 Assets & market info

| C# | Python |
|---|---|
| `IReadOnlyDictionary<string,int> GetAllActives()` | `get_all_ACTIVES_OPCODE` (per instance; starts from the 198-entry table) |
| `Task UpdateActivesAsync()` | `update_ACTIVES_OPCODE`, `get_ALL_Binary_ACTIVES_OPCODE` |
| `Task UpdateDigitalActivesAsync()` | `get_All_Digital_ACTIVES_OPCODE` |
| `Task UpdateMarginActivesAsync(string? instrumentType = null)` | `instruments_input_to_ACTIVES` (type given), `instruments_input_all_in_ACTIVES` (null → crypto, forex, cfd) |
| `string? OpcodeToName(int activeId)` | `opcode_to_name` (null instead of ValueError) |
| `Task<string?> GetNameByActiveIdAsync(int activeId)` | `get_name_by_activeId` |
| `Task<JsonObject> GetFinancialInformationAsync(int activeId)` | `get_financial_information` |
| `Task<JsonObject> GetInstrumentsAsync(string instrumentType)` | `get_instruments` |
| `Task<JsonObject?> GetAllInitAsync()` | `get_all_init`, `get_all_init_v2` (uses v2 frame) |
| `Task<Dictionary<string, Dictionary<string,bool>>> GetAllOpenTimeAsync()` | `get_all_open_time` (`[type][asset] = open`) |
| `Task<Dictionary<string, Dictionary<string,JsonObject>>> GetBinaryOptionDetailAsync()` | `get_binary_option_detail` (data from v2 init) |
| `Task<Dictionary<string, Dictionary<string,double>>> GetAllProfitAsync()` | `get_all_profit`, `get_all_profit_v2` (v2 rules: ticker keys, skip suspended/disabled) |
| `Task<Dictionary<string, Dictionary<string,double>>> GetAllProfitBlitzAsync()` | `get_all_profit_blitz_v2` |
| `Task<JsonObject?> GetDigitalUnderlyingListAsync()` | `get_digital_underlying_list_data` |
| `Task<JsonNode> GetTopAssetAsync(string instrumentType)` | `get_top_asset` |

### 5.4 Account & balance

| C# | Python |
|---|---|
| `Task<JsonObject> GetProfileAsync()` | `get_profile`, `get_profile_ansyc` (fresh reply, fix 7) |
| `Task<JsonObject> GetBalancesAsync()` | `get_balances`, `_get_balances` (fresh reply) |
| `Task<double?> GetBalanceAsync()` | `get_balance`, `get_balance_v2` (pushed `balance-changed` cache, else fresh `get-balances`) |
| `long? GetBalanceId()` | `get_balance_id` |
| `Task<BalanceMode?> GetBalanceModeAsync()` | `get_balance_mode` |
| `Task<long?> GetBalanceIdByModeAsync(BalanceMode mode)` | `get_balance_id_by_mode` |
| `Task ChangeBalanceAsync(BalanceMode mode)` | `change_balance` (local selection only, sends nothing — same as Python) |
| `Task<string?> GetCurrencyAsync()` | `get_currency` |
| `Task<bool> ResetPracticeBalanceAsync()` | `reset_practice_balance` |
| `Task<JsonObject> GetTransactionsAsync(long from, long to, int limit, int offset, IEnumerable<string> types)` | `get_transactions` |

### 5.5 Candles & live prices

| C# | Python |
|---|---|
| `Task<List<Candle>> GetCandlesAsync(string asset, int intervalSeconds, int count, double endTime)` | `get_candles` |
| `Task<bool> StartCandlesStreamAsync(string asset, int size, int maxDict)` | `start_candles_stream(size)`, `start_candles_one_stream`, `full_realtime_get_candle` |
| `Task<bool> StartAllCandlesStreamsAsync(string asset, int maxDict)` | `start_candles_stream("all")`, `start_candles_all_size_stream` |
| `Task StopCandlesStreamAsync(string asset, int size)` | `stop_candles_stream(size)`, `stop_candles_one_stream` |
| `Task StopAllCandlesStreamsAsync(string asset)` | `stop_candles_stream("all")`, `stop_candles_all_size_stream` |
| `IReadOnlyDictionary<long,Candle> GetRealtimeCandles(string asset, int size)` | `get_realtime_candles(asset, size)` |
| `IReadOnlyDictionary<int, IReadOnlyDictionary<long,Candle>> GetRealtimeCandles(string asset)` | `get_realtime_candles(asset, "all")` |
| `IReadOnlyDictionary<string, IReadOnlyDictionary<int, IReadOnlyDictionary<long,Candle>>> GetAllRealtimeCandles()` | `get_all_realtime_candles` |
| event `CandleReceived` | — |

Invalid `size` (not in `CandleSizes`) → `ArgumentOutOfRangeException` (Python logs and returns `False`).

### 5.6 Binary / Turbo / Blitz

| C# | Python |
|---|---|
| `Task<(bool Success, long Id, string? Error)> BuyAsync(double amount, string asset, Direction direction, int minutes)` | `buy` (Buyv3; turbo if expiry index < 5 else binary) |
| `Task<List<long?>> BuyMultiAsync(IList<double> amounts, IList<string> assets, IList<Direction> directions, IList<int> minutes)` | `buy_multi` (length mismatch → `ArgumentException`) |
| `Task<(bool Success, JsonObject? Result, string? Error)> BuyByRawExpirationAsync(double amount, string asset, Direction direction, string optionType, long expired)` (+ overloads taking `int activeId` / `int optionTypeId`) | `buy_by_raw_expirations` (reports failures, fix 8) |
| `Task<(bool Success, JsonObject? Result, string? Error)> BuyBlitzAsync(double amount, string asset, Direction direction, int durationSeconds = 30)` | `buy_blitz` (same duration validation → `ArgumentOutOfRangeException`) |
| `Task<JsonObject?> SellOptionAsync(IEnumerable<long> optionIds)` | `sell_option` (returns the `sold-options` reply if it arrives within the timeout, else null) |
| `Task<(TradeResult Result, double Profit)> CheckWinAsync(long optionId)` | `check_win_v3` (+ `check_win_v2`, `get_betinfo`): waits for `option-closed`, profit = `profit_amount − amount` |
| `Task<JsonObject> GetOptionInfoAsync(int limit)` | `get_optioninfo`, `get_optioninfo_v2` (v2 frame) |
| `int GetRemainingTime(int minutes, double? timestamp = null)` | `get_remaning` (invalid duration → `ArgumentOutOfRangeException`) |
| `IReadOnlyDictionary<long,JsonObject> GetOptionOpenByOtherPc()` / `void DelOptionOpenByOtherPc(long id)` | same names |

### 5.7 Digital

| C# | Python |
|---|---|
| `Task<(bool Success, long Id, string? Error)> BuyDigitalSpotAsync(string asset, double amount, Direction direction, int minutes)` | `buy_digital_spot`, `old_buy_digital_spot` (new id format `do{id}A{yyyyMMdd}D{HHmm}00T{n}M{C|P}SPT`, v2.0 frame) |
| `Task<(bool Success, JsonObject? OrderChanged, string? Error)> BuyDigitalAsync(double amount, string instrumentId)` | `buy_digital` |
| `Task<bool> CloseDigitalOptionAsync(long positionId)` | `close_digital_option` |
| `Task<bool> CloseDigitalOptionBatchAsync(IEnumerable<long> positionIds)` | `close_digital_option_batch` |
| `Task<(TradeResult Result, double Profit)> CheckWinDigitalAsync(long orderId)` | `check_win_digital`, `check_win_digital_v2`: waits until closed; `expired` → `close_profit − invest`; `default` (and any other reason, logged) → `pnl_realized` |
| `Task<(JsonObject Raw, Dictionary<string,(string Call, string Put)>? Strikes)> GetStrikeListAsync(string asset, int minutes)` | `get_strike_list` |
| `Task SubscribeStrikeListAsync(string asset, int minutes)` / `Task UnsubscribeStrikeListAsync(string asset, int minutes)` | `subscribe_strike_list` / `unsubscribe_strike_list` |
| `Task<Dictionary<string,StrikeQuote>> GetRealtimeStrikeListAsync(string asset, int minutes)` | `get_realtime_strike_list` |
| `Task<JsonObject> GetInstrumentQuotesGeneratedDataAsync(string asset, int minutes)` | `get_instrument_quites_generated_data` |
| `double? GetDigitalCurrentProfit(string asset, int minutes)` | `get_digital_current_profit` (null instead of `False`) |
| `Task<double?> GetDigitalSpotProfitAfterSaleAsync(long positionId)` | `get_digital_spot_profit_after_sale` (Cat §4.7.8, quirk kept) |
| `Task<JsonObject> GetDigitalPositionAsync(long orderId)` | `get_digital_position` |
| `Task<JsonObject> GetDigitalPositionByPositionIdAsync(long positionId)` | `get_digital_position_by_position_id` |
| `IReadOnlyDictionary<long,JsonObject> GetDigitalOpenByOtherPc()` / `void DelDigitalOpenByOtherPc(long id)` | same names |

### 5.8 Margin (Forex / CFD / Crypto)

| C# | Python |
|---|---|
| `Task<(bool Success, long? OrderId, string? Error)> BuyOrderAsync(string instrumentType, string instrumentId, string side, double amount, int leverage, string type, double? limitPrice = null, double? stopPrice = null, string? stopLoseKind = null, double? stopLoseValue = null, string? takeProfitKind = null, double? takeProfitValue = null, bool useTrailStop = false, bool autoMarginCall = false, bool useTokenForCommission = false)` | `buy_order` (`Error` = `reject_status` when rejected) |
| `Task<(bool Success, JsonNode? Result)> ChangeOrderAsync(string idName, long id, string? stopLoseKind, double? stopLoseValue, string? takeProfitKind, double? takeProfitValue, bool useTrailStop, bool autoMarginCall)` | `change_order` (`idName` is `"order_id"` or `"position_id"`, else `ArgumentException`) |
| `Task<(bool Success, JsonObject? Message)> ChangeAutoMarginCallAsync(string idName, long id, bool autoMarginCall)` | `change_auto_margin_call` (same `idName` values) |
| `Task<(bool Success, JsonObject? Order)> GetOrderAsync(long orderId)` | `get_order` |
| `JsonObject GetAsyncOrder(long id)` | `get_async_order` (only keys received so far) |
| `Task<(bool Success, JsonObject? Message)> GetPendingAsync(string instrumentType)` | `get_pending` |
| `Task<(bool Success, JsonObject? Message)> GetPositionsAsync(string instrumentType)` | `get_positions` |
| `Task<JsonObject> GetPortfolioPositionsAsync(int limit = 1, int offset = 0)` | `portfolio_get_positions` |
| `Task<(bool Success, JsonObject? Message)> GetPositionAsync(long orderId)` | `get_position` |
| `Task<(bool Success, JsonObject? Message)> GetPositionHistoryAsync(long userBalanceId, long start, long end, int offset, int limit, IEnumerable<string> instrumentTypes)` | `get_position_history_v2` (+ old `get_position_history`) |
| `Task<JsonObject> GetHistoryPositionsAsync(long externalId)` | `get_history_positions` |
| `Task<(bool Success, JsonObject? Message)> GetAvailableLeveragesAsync(string instrumentType, string asset = "")` | `get_available_leverages` |
| `Task<bool> CancelOrderAsync(long orderId)` | `cancel_order` |
| `Task<bool> ClosePositionAsync(long orderId)` | `close_position` (`close_position_v2` dropped: broken) |
| `Task<(bool Success, JsonObject? Message)> GetOvernightFeeAsync(string instrumentType, string asset)` | `get_overnight_fee` |
| `Task<double?> CheckWinMarginAsync(long id, double pollingSeconds = 0.5)` | `check_win` (+ `raw_check_win`); null when the order was cancelled |

### 5.9 Extras

| C# | Python |
|---|---|
| `Task<JsonObject> GetLeaderBoardAsync(string country, int fromPosition, int toPosition, int nearTradersCount, int userCountryId = 0, int nearTradersCountryCount = 0, int topCountryCount = 0, int topCount = 0, int topType = 2)` | `get_leader_board` |
| `Task<JsonObject> GetActiveExposureAsync(string instrumentType, string asset, int durationMinutes, string currency = "USD")` | `get_active_exposure` (`{call, put}`) |
| `Task<bool> StartMoodStreamAsync(string asset)` / `Task StopMoodStreamAsync(string asset)` | `start_mood_stream` / `stop_mood_stream` (tracked for reconnect, fix 5) |
| `double? GetTradersMood(string asset)` / `IReadOnlyDictionary<int,double> GetAllTradersMood()` | `get_traders_mood` / `get_all_traders_mood` |
| `Task SubscribeTopAssetsUpdatedAsync(string instrumentType)` / `Task UnsubscribeTopAssetsUpdatedAsync(…)` / `JsonNode? GetTopAssetsUpdated(…)` | `subscribe_top_assets_updated` / `unsubscribe_…` / `get_top_assets_updated` |
| `Task SubscribeCommissionChangedAsync(string instrumentType)` / `Task UnsubscribeCommissionChangedAsync(…)` / `IReadOnlyDictionary<string, IReadOnlyDictionary<double,int>> GetCommissionChange(…)` | `subscribe_commission_changed` / `unsubscribe_…` / `get_commission_change` |
| `Task SubscribeLiveDealAsync(string name, string asset, string type, int bufferSize)` / `Task UnsubscribeLiveDealAsync(string name, string asset, string type)` | `subscribe_live_deal` (buffer size applied, fix 6) / `unscribe_live_deal` |
| `IReadOnlyList<JsonObject> GetLiveDeal(…)` / `JsonObject? PopLiveDeal(…)` / `void ClearLiveDeal(…, int bufferSize)` | `get_live_deal` (newest first) / `pop_live_deal` (oldest; null if empty) / `clear_live_deal` |
| `Task<JsonNode> GetUserProfileClientAsync(long userId)` | `get_user_profile_client` |
| `Task<JsonNode> RequestLeaderboardUserinfoDealsClientAsync(long userId, int countryId)` | `request_leaderboard_userinfo_deals_client` |
| `Task<JsonNode> GetUsersAvailabilityAsync(long userId)` | `get_users_availability` |

### 5.10 Not ported

`close_position_v2` (sends an empty id — broken), legacy frames used only by merged-away methods (`api_option_init_all`, `api_game_betinfo`, `api_game_getoptions`, old `place-digital-option` v1.0 spot path, old `get-position-history`), Python channels/HTTP endpoints never called by `IQ_Option` (Cat §7, §5 rows 6–12). `BuyDigitalAsync` still sends the v1.0 body for instrument ids that do not start with `do`, exactly like Python's `place_digital_option_v2`.

---

## 6. Copy exactly vs. fix

### 6.1 Copied exactly
- All outgoing frames byte-for-byte (§4.9), including `request_id` values and Python's odd-but-working payloads.
- Expiry maths: `get_expiration_time` (Cat §4.2), `get_remaning_time` (§4.3), digital spot expiry and instrument-id formatting (§4.4, local-time minute test + UTC id), strike-list expiry (§4.5).
- Formulas: payout `(100 − commission) / 100`; blitz `profit_percent = (int)(fraction * 100)` (double truncation); digital profit `((100 − ask) * 100) / ask`; strike key `"%.6f" % (value * 10e-7)`; `check_win` rounding = `Math.Round(x, 2, MidpointRounding.ToEven)`; digital sell-back value (Cat §4.7.8) including the always-false call flag.
- Candle buffer eviction (`dict_queue_add`), live-deal deque semantics, insertion-ordered asset table with first-name-wins reverse lookup.
- Login / 2FA / logout HTTP requests and the default User-Agent.
- `ConnectAsync` starts on PRACTICE; `ChangeBalanceAsync` changes the local selection only.

### 6.2 Fixed (each would freeze or break a bot)
1. Router errors are logged and skipped — never a silent deadlock.
2. Every request→reply wait has a timeout.
3. Reconnect re-subscribes everything and recovers trade results pushed while offline.
4. One clock: IQ Option's server time everywhere (Python's `buy` used the PC clock).
5. Mood streams are tracked and re-subscribed (Python's chained-comparison bug).
6. Live-deal `bufferSize` is applied.
7. `GetProfileAsync` / `GetBalancesAsync` / `GetBalanceAsync` return fresh data, not stale cache.
8. `BuyByRawExpirationAsync` reports failures.
9. `CloseAsync` / `DisposeAsync` really stop (no hidden auto-reconnect of the closed socket); proxy/TLS settings are kept on reconnect.
10. SSL certificates are verified (opt-out: `IgnoreCertificateErrors`).

### 6.3 Known quirk kept on purpose
`GetDigitalSpotProfitAfterSaleAsync` computes calls with the put formula, as Python does (Cat §4.7.8). Without IQ Option reference numbers we cannot prove which is correct; documented in the README.

---

## 7. Logging and errors
- `options.Log` receives one line per notable event (connect steps, reconnects, skipped messages, timeouts). Raw frames are logged only at a "verbose" flag (`IqOptionOptions.LogFrames`, default false) because they contain the SSID.
- Passwords and SSIDs are never written to logs.

## 8. Example bot (`examples/SimpleBot`)
- Reads `IQ_EMAIL` / `IQ_PASSWORD` from environment variables (never from files).
- Connects, prints balance mode and amount, fetches 10 EURUSD 60 s candles, streams 1-minute candles for 30 s, places a **$1** turbo `BuyAsync` and a **$1** `BuyDigitalSpotAsync` (1 minute), waits for both results, prints them, disposes.
- **Safety lock:** exits if the selected balance is not PRACTICE unless started with `--allow-real`.

## 9. Testing
1. **Golden tests (byte-for-byte vs Python).** `tests/golden/generate_golden.py` runs with the system `python3` (3.9), stubbing `requests` and `websocket` so no packages or network are needed. With `time.time` and the server timestamp pinned and `TZ` fixed (e.g. `Asia/Karachi`, `UTC`, `America/New_York`), it:
   - calls every ported Python method with fixed inputs through a fake API that records each outgoing frame and feeds canned replies to the real `on_message`;
   - records the connect frame sequence (Cat §3.12);
   - records expiry maths for thousands of timestamp × duration combinations, payout formulas, strike-key formatting, candle-buffer eviction and router state for canned incoming messages.
   Output is committed to `tests/IqOptionApi.Tests/Golden/`. The C# tests replay the same inputs and require exact equality.
2. **Fake IQ Option server tests.** An in-process ASP.NET Core server (HTTP login + WebSocket) scripts conversations: connect (SSID reuse, fresh login, wrong password, 2FA), buy → check win for binary/digital/margin, concurrent requests sharing a reply name, timeouts, malformed/unknown messages (no freeze), connection drop mid-trade (reconnect, re-subscribe, no re-send of the buy, result recovery), `CloseAsync` stops reconnecting.
3. **Unit tests** for the JSON writer (Python float repr cases), asset table order and lookups, enums/serialisation.
4. **Live check** by the user with `examples/SimpleBot` on PRACTICE.

**Definition of done:** zero build warnings; all tests green; 100% golden match; user's practice run succeeds.

## 10. Risks
- IQ Option can change its private protocol at any time; the golden tests prove parity with the Python v9.4 code, not with the live server. The live check covers the main flows only.
- Reply types without `request_id` are processed one request at a time per reply name (safe, slightly less parallel).
- Reconnect logs in again when the SSID is rejected; accounts that require 2FA on every login cannot auto-reconnect (raised via `Disconnected`).
- Automated access may conflict with IQ Option's terms of service; using it is the account owner's decision.
