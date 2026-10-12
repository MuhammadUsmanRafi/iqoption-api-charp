# IqOptionApi for C#

A C# (.NET 9) port of the Python **iqoptionapi** library (v9.4) in this repository.

It is a **library**: there's no screen and no trading strategy. Your own bot or app references it and uses one class, `IqOption`, to log in, read prices and trade on IQ Option. Every message it sends is byte-for-byte what the Python library sends (plus a few subscriptions and the perpetual-crypto methods Python is missing), and this is checked by tests generated from the Python code itself.

> **Note.** IQ Option has no official public API. This library uses the same private protocol as their web app, so it can stop working when IQ Option changes that protocol. Automated trading may also conflict with IQ Option's terms of service. Using it is your decision.

## Requirements

- .NET 9 SDK (`dotnet --version` shows 9.x)

## Add it to your bot

Choose one:

1. **Project reference** (simplest):
   ```bash
   dotnet add <YourBot>.csproj reference <path>/IqOptionApi.CSharp/src/IqOptionApi/IqOptionApi.csproj
   ```
2. **Copy the DLL:** `dotnet build -c Release src/IqOptionApi`, then reference `src/IqOptionApi/bin/Release/net9.0/IqOptionApi.dll`.
3. **Local NuGet package:** `dotnet pack -c Release src/IqOptionApi -o ./packages`, then `dotnet add package IqOptionApi --source ./packages`.

## Quick start

```csharp
using IqOptionApi;

await using var api = new IqOption("you@example.com", "password");
var (ok, reason) = await api.ConnectAsync();          // starts on the PRACTICE balance
if (!ok) { Console.WriteLine(reason); return; }

var candles = await api.GetCandlesAsync("EURUSD", 60, 10, api.GetServerTimestamp());
Console.WriteLine($"last close: {candles[^1].Close}");

var (bought, id, error) = await api.BuyAsync(1, "EURUSD", Direction.Call, 1);   // $1, 1 minute
if (bought)
{
    var (result, profit) = await api.CheckWinAsync(id);                         // waits until it closes
    Console.WriteLine($"{result}: {profit}");
}
```

Live data is also available through events: `CandleReceived`, `PositionChanged`, `BalanceChanged`, `Disconnected`, `Reconnected`, and `MessageReceived` for every raw message.

## Running the example bot

```bash
export IQ_EMAIL=you@example.com
export IQ_PASSWORD='your password'
dotnet run --project examples/SimpleBot -- --asset EURUSD-OTC
```

It connects, prints your balance, shows 10 candles, streams live candles for 30 seconds, then buys one **$1 turbo** and one **$1 digital** option and prints both results.
**Safety lock:** it refuses to run unless the selected balance is PRACTICE. Pass `--allow-real` to override.
Use an `-OTC` asset at weekends, when the normal markets are closed.

### MT5 tick trader (two programs, shared memory)

`BtcTickTrader/` (next to this folder, at the top of the repo) has an MT5 Bitcoin price feeder and a bot that trades IQ Option blitz/turbo when the price moves past a threshold. They talk through shared memory. See [../BtcTickTrader/README.md](../BtcTickTrader/README.md).

### Bitcoin bot (all four products)

```bash
dotnet run --project examples/BitcoinBot -- status
dotnet run --project examples/BitcoinBot -- blitz call --amount 6 --seconds 120
dotnet run --project examples/BitcoinBot -- binary put --amount 1 --minutes 1
dotnet run --project examples/BitcoinBot -- digital call --amount 1 --minutes 5
dotnet run --project examples/BitcoinBot -- perp open buy --isolated --margin 1 --leverage 200
dotnet run --project examples/BitcoinBot -- perp open sell --cross --count 0.01 --leverage 200 --close-after 30
dotnet run --project examples/BitcoinBot -- perp close 100105469448
```

`status` shows which Bitcoin products are open now. Bitcoin digital has 5- and 15-minute expiries only (no 1 minute). Blitz, binary and digital wait for the result and print it. `perp open` prints the position id and leaves the position open (or closes it after `--close-after` seconds); `perp close <id>` closes it and prints the profit. It uses the PRACTICE balance unless you add `--real`.

## Options

`new IqOption(email, password, new IqOptionOptions { ... })`: `Host`, `ProxyHost`/`ProxyPort`/`ProxyUser`/`ProxyPassword`, `Ssid` (reuse a session), `AutoLogout` (true), `AutoReconnect` (true), `RequestTimeout` (30 s), `ReceiveTimeout` (60 s of silence = dead connection), `ReconnectDelays`, `IgnoreCertificateErrors` (false), `TimeZone` (computer's zone), `Log`, `LogFrames` (false; frames contain your session id).

## How it behaves

- Every request has a timeout. Trade methods return `(false, …, "timeout")` and data methods throw `TimeoutException`; nothing hangs forever. The exception is the `CheckWin*` methods, which wait until the trade closes; cancel them with a `CancellationToken`.
- If the connection drops, the library reconnects, subscribes to all your streams again, and recovers trade results that arrived while it was offline. A buy that was already sent is **never sent twice**: it returns `(false, …, "connection lost")`.
- A connection that goes silent, for example after laptop sleep or a Wi-Fi change, counts as dropped after `ReceiveTimeout` and is reconnected. A drop *during* a reconnect starts another reconnect.
- A request made while the library is reconnecting waits for the new connection and is then sent once.
- Once the server has created an order, the result always carries its id, even when a later step fails. For example, `BuyOrderAsync` can return `(false, orderId, reason)`, so you can still track or close the order.
- Methods can be called from many tasks at once, and each call gets its own reply.
- Event handlers run on the library's receive loop. Keep them short, never block in them (`.Result`, `.Wait()`), and catch your own exceptions in `async` handlers.

## Live check (9 Oct 2026, practice account)

| What was checked | Result |
|---|---|
| Login, balance, historic and live candles | ✅ |
| Turbo buy + `CheckWinAsync` | ✅ |
| Digital spot buy + `CheckWinDigitalAsync` | ✅ |
| Blitz buy + `CheckWinAsync` | ✅ (with the `blitz-option` subscription, fix 11) |
| Bitcoin perpetual, cross and isolated: open, close, `CheckWinPerpetualAsync` | ✅ (not in Python, see below) |

What the live server does differently from the Python code's assumptions:

- **Closed binary/turbo options.** The server no longer sends `option-closed`, so Python's `check_win_v3` would wait forever. A closed option now arrives as a closed `position-changed`, and `CheckWinAsync` understands both.
- **Blitz options.** Their `position-changed` messages (`instrument_type` `blitz-option`) are only pushed to clients that subscribe to `blitz-option`. Python never does, so a blitz result never arrives there. `ConnectAsync` adds this one subscription per balance; every other connect frame is still Python's.
- **Perpetual crypto (e.g. Bitcoin-PerpFuture).** These are traded through `marginal-crypto` messages, a newer system than Python's `place-order-temp` / `crypto` orders (`BuyOrderAsync`). The messages were copied from the official web traderoom. Their updates are only pushed to clients that subscribe to `marginal-crypto`, which `ConnectAsync` now does. The API session receives `raw_event` flat while the web traderoom receives it wrapped (`{"marginal_crypto_position_changed1": {…}}`); both are understood.
- **The `get-instruments` list (forex/CFD/crypto).** The server did not answer this request, so `GetInstrumentsAsync`, `UpdateMarginActivesAsync` and `GetAllOpenTimeAsync` time out. Python sends the same request and would hang.
- **Asset availability changes.** Assets are suspended from time to time ("active is suspended"), and some digital names now end in `-op` (e.g. `EURUSD-op`). Use `GetAllInitAsync()` / `GetDigitalUnderlyingListAsync()` to see what is open, and `UpdateDigitalActivesAsync()` to learn new names.

## Python → C# methods

### Public types (`Models.cs`)

- `enum Direction { Call, Put }` → serialised `"call"` / `"put"`; digital ids use `C` / `P`.
- `enum BalanceMode { Real, Practice }` (`type` 1 / 4).
- `enum TradeResult { Win, Loss, Equal }` (sign of the profit).
- `record Candle` — `Id, From, To, At, Open, Close, Min, Max, Volume` (+ stream extras `Ask, Bid, Size, ActiveId`, nullable) and `Raw` (`JsonObject`). Prices are `double` (Python `float`).
- `record StrikeQuote(string CallId, double? CallProfit, string PutId, double? PutProfit)`.
- `static class InstrumentTypes` — string constants `Cfd = "cfd"`, `Forex = "forex"`, `Crypto = "crypto"`, `DigitalOption = "digital-option"`, `TurboOption = "turbo-option"`, `BinaryOption = "binary-option"`, `FxOption = "fx-option"`.
- `class IqOptionException : Exception`.
- `IqOptionOptions` (see Options above).
- `static IReadOnlyList<int> IqOption.CandleSizes` = 1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 604800, 2592000.

### Connection & session

| C# | Python | Notes |
|---|---|---|
| `new IqOption(string email, string password, IqOptionOptions? options = null)` | `__init__` | no network |
| `Task<(bool Success, string? Reason)> ConnectAsync()` | `connect` | starts on PRACTICE |
| `Task<JsonNode?> TwoFactorAsync(string token, string? method = null, string? code = null)` | `TWO_FA` | 2-step login |
| `void Set2FAToken(string token)` | `setting_2FA_TOKEN` | used by next login |
| `void SetSession(IDictionary<string,string> headers, IDictionary<string,string> cookies)` | `set_session` | default UA = Python's Chrome/66 string |
| `Task LogoutAsync()` | `logout` | |
| `Task CloseAsync()` / `ValueTask DisposeAsync()` | `close`, `atexit` logout | really stops (fix 9) |
| `bool IsConnected` | `check_connect` | |
| `string? Ssid` | `get_ssid` | |
| `double GetServerTimestamp()` | `get_server_timestamp` | |
| events | `set_call_back_for_client` | |
| *(automatic)* | `re_subscribe_stream`, `position_change_all`, `order_changed_all` | inside connect/reconnect |

### Assets & market info

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

### Account & balance

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

### Candles & live prices

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

### Binary / Turbo / Blitz

| C# | Python |
|---|---|
| `Task<(bool Success, long Id, string? Error)> BuyAsync(double amount, string asset, Direction direction, int minutes)` | `buy` (Buyv3; turbo if expiry index < 5 else binary) |
| `Task<List<long?>> BuyMultiAsync(IList<double> amounts, IList<string> assets, IList<Direction> directions, IList<int> minutes)` | `buy_multi` (length mismatch → `ArgumentException`) |
| `Task<(bool Success, JsonObject? Result, string? Error)> BuyByRawExpirationAsync(double amount, string asset, Direction direction, string optionType, long expired)` (+ overloads taking `int activeId` / `int optionTypeId`) | `buy_by_raw_expirations` (reports failures, fix 8) |
| `Task<(bool Success, JsonObject? Result, string? Error)> BuyBlitzAsync(double amount, string asset, Direction direction, int durationSeconds = 30)` | `buy_blitz` (same duration validation → `ArgumentOutOfRangeException`) |
| `BuyBlitzAsync(amount, asset, direction, durationSeconds, int profitPercent)` | — (C# only) same frame, but uses the payout you pass instead of requesting it first; one round trip less |
| `Task<int?> GetBlitzProfitPercentAsync(string asset)` | — (C# only) blitz payout percent as the buy sends it, or null when closed |
| `Task<JsonObject?> SellOptionAsync(IEnumerable<long> optionIds)` | `sell_option` (returns the `sold-options` reply if it arrives within the timeout, else null) |
| `Task<(TradeResult Result, double Profit)> CheckWinAsync(long optionId)` | `check_win_v3` (+ `check_win_v2`, `get_betinfo`): waits for `option-closed` or a closed `position-changed`, profit = payout − stake; works for binary, turbo and blitz |
| `Task<JsonObject> GetOptionInfoAsync(int limit)` | `get_optioninfo`, `get_optioninfo_v2` (v2 frame) |
| `int GetRemainingTime(int minutes, double? timestamp = null)` | `get_remaning` (invalid duration → `ArgumentOutOfRangeException`) |
| `IReadOnlyDictionary<long,JsonObject> GetOptionOpenByOtherPc()` / `void DelOptionOpenByOtherPc(long id)` | same names |

### Digital

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
| `Task<double?> GetDigitalSpotProfitAfterSaleAsync(long positionId)` | `get_digital_spot_profit_after_sale` (Python's formula kept as-is) |
| `Task<JsonObject> GetDigitalPositionAsync(long orderId)` | `get_digital_position` |
| `Task<JsonObject> GetDigitalPositionByPositionIdAsync(long positionId)` | `get_digital_position_by_position_id` |
| `IReadOnlyDictionary<long,JsonObject> GetDigitalOpenByOtherPc()` / `void DelDigitalOpenByOtherPc(long id)` | same names |

### Margin (Forex / CFD / Crypto)

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

### Perpetual crypto (not in Python)

Bitcoin-PerpFuture is active id `1979`. In the web traderoom, Settings → Trading → Trading mode chooses the default: "Simple mode" = isolated, "Advanced mode" = cross. The API sends the mode with every order, whatever that setting is.

| C# | What it does |
|---|---|
| `Task<(bool Success, long? OrderId, long? PositionId, string? Error)> OpenPerpetualIsolatedAsync(int activeId, string side, double margin, int leverage)` | Isolated margin, sized by the amount invested (at most `margin` can be lost). `side` is `"buy"` or `"sell"`. |
| `Task<(bool Success, long? OrderId, long? PositionId, string? Error)> OpenPerpetualAsync(int activeId, string side, double count, int leverage)` | Cross margin, sized by coin quantity (e.g. `0.01` BTC); the whole balance backs it. |
| `Task<bool> ClosePerpetualAsync(long positionId)` | Closes at market. |
| `Task<(TradeResult Result, double Profit)> CheckWinPerpetualAsync(long positionId)` | Waits until the position closes (by you, TP/SL or the server); profit = `pnl_net`. A close that happens while the connection is down is not recovered. |

Both open methods wait until the position exists and return its id. Once the server has created the order, its id is always returned, even when a later step fails; `Error` is the server's reason (e.g. `reject_status`). Not supported yet: TP/SL, limit/stop orders, other perpetual coins' active ids (pass them yourself), closing several positions at once.

### Extras

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

### Not ported

`close_position_v2` (sends an empty id — broken), legacy frames used only by merged-away methods (`api_option_init_all`, `api_game_betinfo`, `api_game_getoptions`, old `place-digital-option` v1.0 spot path, old `get-position-history`), Python channels/HTTP endpoints never called by `IQ_Option`. `BuyDigitalAsync` still sends the v1.0 body for instrument ids that do not start with `do`, exactly like Python's `place_digital_option_v2`.

---

## Differences from Python

### Copied exactly
- All outgoing frames byte-for-byte (apart from the extra blitz and `marginal-crypto` subscriptions, fixes 11–12, and the perpetual-crypto methods), including `request_id` values and Python's odd-but-working payloads.
- Expiry maths: `get_expiration_time` , `get_remaning_time` , digital spot expiry and instrument-id formatting , strike-list expiry .
- Formulas: payout `(100 − commission) / 100`; blitz `profit_percent = (int)(fraction * 100)` (double truncation); digital profit `((100 − ask) * 100) / ask`; strike key `"%.6f" % (value * 10e-7)`; `check_win` rounding = `Math.Round(x, 2, MidpointRounding.ToEven)`; digital sell-back value including the always-false call flag.
- Candle buffer eviction (`dict_queue_add`), live-deal deque semantics, insertion-ordered asset table with first-name-wins reverse lookup.
- Login / 2FA / logout HTTP requests and the default User-Agent.
- `ConnectAsync` starts on PRACTICE; `ChangeBalanceAsync` changes the local selection only.

### Fixed (each would freeze or break a bot)
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
11. Blitz results arrive: `ConnectAsync` also subscribes `portfolio.position-changed` for `blitz-option`.
12. Perpetual crypto updates arrive: `ConnectAsync` also subscribes `portfolio.position-changed` (per balance) and `portfolio.order-changed` for `marginal-crypto`.

### Known quirk kept on purpose
`GetDigitalSpotProfitAfterSaleAsync` computes calls with the put formula, as Python does. Without IQ Option reference numbers we cannot prove which is correct; documented in the README.

---


Also fixed: Python's `buy_by_raw_expirations` always crashes with `NameError` (its `api.py` never imports `Buyv3_by_raw_expired`). `BuyByRawExpirationAsync` works.

## Tests

```bash
dotnet test IqOptionApi.sln
```

- **Golden tests** compare every outgoing message, the expiry maths (6 time zones) and the message router against output recorded from the real Python code. To regenerate that data (Python 3, standard library only, no network): `python3 tests/golden/generate_golden.py`. The asset table is generated with `python3 tests/golden/gen_actives_cs.py`. Both outputs are committed.
- **Fake-server tests** run a local IQ Option look-alike (HTTP login + WebSocket) to test connecting, buying, disconnects, reconnects and closing.

Design spec: `docs/specs/2026-10-09-csharp-port-design.md` · Python reference: `docs/specs/python-api-catalogue.md`
