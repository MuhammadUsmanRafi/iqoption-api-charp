using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IqOptionApi;

/// <summary>
/// IQ Option client: log in, read market data and trade. C# port of the Python <c>IQ_Option</c> class (v9.4).
/// Methods ending in <c>Async</c> talk to the server; the others read live data that is already stored.
/// </summary>
public sealed class IqOption : IAsyncDisposable
{
    private static readonly string[] PortfolioTypes = { "cfd", "forex", "crypto", "digital-option", "turbo-option", "binary-option" };

    private static readonly string[] BillingSubscriptions =
    {
        "internal-billing.balance-created", "internal-billing.auth-balance-changed", "internal-billing.balance-changed",
        "internal-billing.marginal-changed", "tournaments.user-registered-in-tournament", "tournaments.user-tournament-position-changed",
    };

    private static readonly TimeSpan[] DefaultReconnectDelays =
    {
        TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(30),
    };

    private readonly string _email;
    private readonly string _password;
    private readonly IqOptionOptions _o;
    private readonly Connection _conn;
    private readonly State _s = new();
    private readonly ActivesTable _actives = new();
    private readonly MessageRouter _router;
    private readonly TimeZoneInfo _tz;
    private readonly object _track = new();
    private readonly List<(string Asset, int Size)> _candleStreams = new();
    private readonly List<string> _allSizeStreams = new();
    private readonly List<string> _moodStreams = new();
    private readonly List<(string Asset, int Minutes)> _strikeSubscriptions = new();
    private readonly List<string> _topAssetsSubscriptions = new();
    private readonly List<string> _commissionSubscriptions = new();
    private readonly List<(string Name, string Asset, string Type)> _liveDealSubscriptions = new();
    private readonly Dictionary<BalanceMode, long> _balanceIds = new();
    private readonly HashSet<long> _pendingBinary = new();
    private readonly HashSet<long> _pendingDigital = new();
    private readonly Dictionary<long, double> _recovered = new();
    private readonly Dictionary<long, double> _recoveredDigital = new();
    private readonly Dictionary<long, (double F, double F2)> _afterSale = new();
    private readonly SemaphoreSlim _reconnecting = new(1, 1);
    private string? _ssid;
    private string? _twoFaToken;
    private volatile bool _connected;
    private volatile bool _closing;
    private bool _loggedIn;

    /// <summary>Creates a client. Nothing is sent until <see cref="ConnectAsync"/>.</summary>
    /// <param name="email">Account e-mail.</param>
    /// <param name="password">Account password.</param>
    /// <param name="options">Optional settings.</param>
    public IqOption(string email, string password, IqOptionOptions? options = null)
        : this(email, password, options ?? new IqOptionOptions(), null, null)
    {
    }

    internal IqOption(string email, string password, IqOptionOptions options, Func<ISocket>? socketFactory, HttpMessageHandler? httpHandler)
    {
        _email = email;
        _password = password;
        _o = options;
        _ssid = options.Ssid;
        _tz = options.TimeZone ?? TimeZoneInfo.Local;
        _router = new MessageRouter(_s, _actives, Log) { HeartbeatReceived = t => _ = ReplyHeartbeatAsync(t) };
        _conn = new Connection(options, socketFactory, httpHandler, Log);
        _conn.MessageReceived += _router.Handle;
        _conn.Closed += OnSocketClosed;
    }

    /// <summary>Every message from the server, before it is processed (Python <c>set_call_back_for_client</c>).</summary>
    public event Action<JsonObject>? MessageReceived
    {
        add => _router.MessageReceived += value;
        remove => _router.MessageReceived -= value;
    }

    /// <summary>A streamed candle was stored: asset name, size in seconds, candle.</summary>
    public event Action<string, int, Candle>? CandleReceived
    {
        add => _router.CandleReceived += value;
        remove => _router.CandleReceived -= value;
    }

    /// <summary>A <c>position-changed</c> message arrived.</summary>
    public event Action<JsonObject>? PositionChanged
    {
        add => _router.PositionChanged += value;
        remove => _router.PositionChanged -= value;
    }

    /// <summary>A balance changed: balance id, new amount.</summary>
    public event Action<long, double>? BalanceChanged
    {
        add => _router.BalanceChanged += value;
        remove => _router.BalanceChanged -= value;
    }

    /// <summary>The connection dropped (or reconnecting gave up); the text says why.</summary>
    public event Action<string>? Disconnected;

    /// <summary>Automatic reconnect finished and every stream was subscribed again.</summary>
#pragma warning disable CS0067 // raised by the reconnect loop (Task 15)
    public event Action? Reconnected;
#pragma warning restore CS0067

    /// <summary>Valid candle sizes in seconds.</summary>
    public static IReadOnlyList<int> CandleSizes { get; } =
        new[] { 1, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 43200, 86400, 604800, 2592000 };

    /// <summary>True while connected (Python <c>check_connect</c>).</summary>
    public bool IsConnected => _connected && _conn.IsOpen && _conn.Ready;

    /// <summary>Current session id (Python <c>get_ssid</c>).</summary>
    public string? Ssid => _ssid;

    internal double TimeScale { get; set; } = 1.0;

    internal Connection Conn => _conn;

    internal State StateForTests => _s;

    private TimeSpan T => _o.RequestTimeout;

    private double Now
    {
        get
        {
            lock (_s.Lock) return _s.ServerTimestamp;
        }
    }

    private long Bal
    {
        get
        {
            lock (_s.Lock) return _s.BalanceId ?? throw new IqOptionException("No balance is selected yet.");
        }
    }

    /// <summary>Server time in Unix seconds from the last <c>timeSync</c> (Python <c>get_server_timestamp</c>).</summary>
    public double GetServerTimestamp() => Now;

    // ================================================================ connection

    /// <summary>Logs in and opens the connection. Starts on the PRACTICE balance, like Python.</summary>
    /// <returns><c>(true, null)</c>, or <c>(false, reason)</c>; for 2FA accounts the reason is the server's JSON text.</returns>
    public async Task<(bool Success, string? Reason)> ConnectAsync(CancellationToken ct = default)
    {
        _closing = false;
        _connected = false;
        var r = await ConnectCoreAsync(resetBalance: true, ct).ConfigureAwait(false);
        _connected = r.Success;
        return (r.Success, r.Reason);
    }

    private async Task<(bool Success, string? Reason, bool LoginRejected)> ConnectCoreAsync(bool resetBalance, CancellationToken ct)
    {
        _conn.Ready = false;
        lock (_s.Lock)
        {
            _s.TimeSynced = false;
            if (resetBalance) _s.BalanceId = null;
        }
        if (resetBalance) _balanceIds.Clear();

        bool authenticated = false;
        if (_ssid != null)
        {
            await _conn.OpenAsync(ct).ConfigureAwait(false);
            authenticated = await AuthenticateAsync(ct).ConfigureAwait(false);
        }
        if (!authenticated)
        {
            var (status, body, ssid) = await _conn.LoginAsync(_email, _password, _twoFaToken, ct).ConfigureAwait(false);
            if (status != 200 || ssid == null)
            {
                await _conn.CloseSocketAsync().ConfigureAwait(false);
                return (false, body, true);
            }
            _ssid = ssid;
            _loggedIn = true;
            lock (_s.Lock) _s.TimeSynced = false;
            await _conn.OpenAsync(ct).ConfigureAwait(false);
            if (!await AuthenticateAsync(ct).ConfigureAwait(false)) return (false, "authentication failed", false);
        }
        _loggedIn = true;

        try
        {
            using var profile = _conn.AddWaiter("profile", null);
            using var balances = _conn.AddWaiter("balances", null);
            await SendRaw("sendMessage", new Obj { ["name"] = "get-profile", ["version"] = "1.0", ["body"] = new Obj() }, ct).ConfigureAwait(false);
            await SendRaw("sendMessage", new Obj { ["name"] = "get-balances", ["version"] = "1.0" }, ct).ConfigureAwait(false);
            var p = await profile.Task.WaitAsync(T, ct).ConfigureAwait(false);
            if (p["msg"] is JsonValue pv && pv.GetValueKind() == JsonValueKind.False) return (false, "the server refused the profile request", false);
            await _conn.WaitUntilAsync(() => { lock (_s.Lock) return _s.TimeSynced; }, T, ct).ConfigureAwait(false);
            await ResubscribeStreamsAsync(ct).ConfigureAwait(false);
            await balances.Task.WaitAsync(T, ct).ConfigureAwait(false);

            JsonObject raw;
            lock (_s.Lock)
            {
                if (_s.BalanceId == null) return (false, "the account has no practice balance", false);
                raw = _s.BalancesRaw!;
            }
            await SendRaw("sendMessage", new Obj { ["name"] = "get-balances", ["version"] = "1.0" }, ct).ConfigureAwait(false);
            foreach (var b in J.Arr(raw["msg"]))
            {
                foreach (string ins in PortfolioTypes)
                {
                    await SendRaw("subscribeMessage", new Obj
                    {
                        ["name"] = "portfolio.position-changed",
                        ["version"] = "2.0",
                        ["params"] = new Obj { ["routingFilters"] = new Obj { ["instrument_type"] = ins, ["user_balance_id"] = b!["id"]!.DeepClone() } },
                    }, ct).ConfigureAwait(false);
                }
            }
            foreach (string name in BillingSubscriptions)
            {
                await SendRaw("subscribeMessage", new Obj { ["name"] = name, ["version"] = "1.0", ["params"] = new Obj { ["routingFilters"] = new Obj() } }, ct)
                    .ConfigureAwait(false);
            }
            foreach (string ins in PortfolioTypes)
            {
                await SendRaw("subscribeMessage", new Obj
                {
                    ["name"] = "portfolio.order-changed",
                    ["version"] = "1.0",
                    ["params"] = new Obj { ["routingFilters"] = new Obj { ["instrument_type"] = ins } },
                }, ct).ConfigureAwait(false);
            }
            await SendRaw("setOptions", new Obj { ["sendResults"] = true }, ct).ConfigureAwait(false);
            _conn.Ready = true;
            return (true, null, false);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, "timeout: " + ex.Message, false);
        }
    }

    private async Task<bool> AuthenticateAsync(CancellationToken ct)
    {
        try
        {
            using var w = _conn.AddWaiter("authenticated", null);
            await _conn.SendAsync("authenticate", new Obj { ["ssid"] = _ssid, ["protocol"] = 3 }, _conn.NextRequestId(), ct, requireReady: false)
                .ConfigureAwait(false);
            var m = await w.Task.WaitAsync(T, ct).ConfigureAwait(false);
            return J.IsTrue(m["msg"]);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return false;
        }
    }

    private Task SendRaw(string outer, object msg, CancellationToken ct) => _conn.SendAsync(outer, msg, "", ct, requireReady: false);

    private async Task ReplyHeartbeatAsync(long heartbeatTime)
    {
        try
        {
            await _conn.SendAsync("heartbeat", new Obj
            {
                ["msg"] = new Obj { ["heartbeatTime"] = heartbeatTime, ["userTime"] = (long)(Now * 1000) },
            }, "", CancellationToken.None, requireReady: false).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            Log("heartbeat reply failed: " + ex.Message);
        }
    }

    private void OnSocketClosed(string reason)
    {
        if (_closing || !_connected) return;
        Log("connection lost: " + reason);
        RaiseDisconnected(reason);
        if (!_o.AutoReconnect) _connected = false;
        // Task 15 starts the reconnect loop here.
    }

    private void RaiseDisconnected(string reason)
    {
        try
        {
            Disconnected?.Invoke(reason);
        }
        catch (Exception ex)
        {
            Log("Disconnected handler threw: " + ex.Message);
        }
    }

    /// <summary>Verifies a 2FA step (Python <c>TWO_FA</c>): first with <paramref name="method"/> = "sms", then with the received <paramref name="code"/>.</summary>
    /// <param name="token">Token from the previous step.</param>
    /// <param name="method">Delivery method, e.g. "sms".</param>
    /// <param name="code">The code you received.</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>The server's JSON answer.</returns>
    public async Task<JsonNode?> TwoFactorAsync(string token, string? method = null, string? code = null, CancellationToken ct = default) =>
        JsonNode.Parse(await _conn.TwoFactorAsync(token, method, code, ct).ConfigureAwait(false));

    /// <summary>Sets the 2FA token sent with the next login (Python <c>setting_2FA_TOKEN</c>).</summary>
    /// <param name="token">Token returned by the last <see cref="TwoFactorAsync"/> step.</param>
    public void Set2FAToken(string token) => _twoFaToken = token;

    /// <summary>Replaces the HTTP headers and cookies used for login/logout (Python <c>set_session</c>).</summary>
    /// <param name="headers">Headers, e.g. a User-Agent.</param>
    /// <param name="cookies">Cookies.</param>
    public void SetSession(IDictionary<string, string> headers, IDictionary<string, string> cookies)
    {
        _conn.Headers.Clear();
        foreach (var (k, v) in headers) _conn.Headers[k] = v;
        _conn.Cookies.Clear();
        foreach (var (k, v) in cookies) _conn.Cookies[k] = v;
    }

    /// <summary>Logs out over HTTP (Python <c>logout</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public Task LogoutAsync(CancellationToken ct = default) => _conn.LogoutAsync(ct);

    /// <summary>Closes the connection for good (no reconnect) and logs out if <see cref="IqOptionOptions.AutoLogout"/> is on.</summary>
    public async Task CloseAsync()
    {
        _closing = true;
        _connected = false;
        await _conn.CloseSocketAsync().ConfigureAwait(false);
        if (_o.AutoLogout && _loggedIn)
        {
            try
            {
                await _conn.LogoutAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Log("logout failed: " + ex.Message);
            }
        }
    }

    /// <summary>Same as <see cref="CloseAsync"/>, then frees resources.</summary>
    public async ValueTask DisposeAsync()
    {
        await CloseAsync().ConfigureAwait(false);
        _conn.Dispose();
    }

    // =================================================================== helpers

    private void Log(string line) => _o.Log?.Invoke(line);

    private TimeSpan Scaled(double seconds) => TimeSpan.FromSeconds(seconds * TimeScale);

    private void EnsureConnected()
    {
        if (!_connected) throw new InvalidOperationException("Call ConnectAsync first");
    }

    private int Act(string asset) => _actives.Get(asset);

    private string NextRid() => _conn.NextRequestId();

    private Task Send(string outer, object msg, string rid, CancellationToken ct) => _conn.SendAsync(outer, msg, rid, ct);

    private Task<JsonObject> ById(string reply, object msg, string rid, CancellationToken ct) =>
        _conn.RequestAsync("sendMessage", msg, rid, reply, true, T, ct);

    private Task<JsonObject> ByName(string reply, object msg, string rid, CancellationToken ct) =>
        _conn.RequestAsync("sendMessage", msg, rid, reply, false, T, ct);

    private async Task<bool> WaitCondAsync(Func<bool> condition, TimeSpan timeout, CancellationToken ct)
    {
        try
        {
            await _conn.WaitUntilAsync(condition, timeout, ct).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    private static bool IsFailure(Exception ex) => ex is TimeoutException or IqOptionException;

    private static string Reason(Exception ex) => ex is TimeoutException ? "timeout" : ex.Message;

    private static bool Status2000(JsonObject m) => J.Long(m["status"]) == 2000;

    // ========================================================= subscription frames

    private Obj CandleStreamMsg(string asset, int size) => new()
    {
        ["name"] = "candle-generated",
        ["params"] = new Obj { ["routingFilters"] = new Obj { ["active_id"] = Act(asset).ToString(CultureInfo.InvariantCulture), ["size"] = size } },
    };

    private Obj AllSizesMsg(string asset) => new()
    {
        ["name"] = "candles-generated",
        ["params"] = new Obj { ["routingFilters"] = new Obj { ["active_id"] = Act(asset).ToString(CultureInfo.InvariantCulture) } },
    };

    private Obj MoodMsg(string asset) => new()
    {
        ["name"] = "traders-mood-changed",
        ["params"] = new Obj { ["routingFilters"] = new Obj { ["instrument"] = "turbo-option", ["asset_id"] = Act(asset) } },
    };

    private Obj StrikeSubscribeMsg(string asset, int minutes) => new()
    {
        ["name"] = "price-splitter.client-price-generated",
        ["params"] = new Obj
        {
            ["routingFilters"] = new Obj { ["instrument_type"] = "digital-option", ["expiration_period"] = minutes * 60, ["asset_id"] = Act(asset) },
        },
        ["version"] = "1.0",
    };

    private Obj StrikeUnsubscribeMsg(string asset) => new()
    {
        ["name"] = "price-splitter.client-price-generated",
        ["params"] = new Obj { ["routingFilters"] = new Obj { ["instrument_type"] = "digital-option", ["asset_id"] = Act(asset) } },
        ["version"] = "1.0",
    };

    private static Obj TopAssetsMsg(string instrumentType) => new()
    {
        ["name"] = "top-assets-updated",
        ["params"] = new Obj { ["routingFilters"] = new Obj { ["instrument_type"] = instrumentType } },
        ["version"] = "1.2",
    };

    private static Obj CommissionMsg(string instrumentType) => new()
    {
        ["name"] = "commission-changed",
        ["params"] = new Obj { ["routingFilters"] = new Obj { ["instrument_type"] = instrumentType } },
        ["version"] = "1.0",
    };

    private Obj LiveDealMsg(string name, string asset, string type)
    {
        var (activeKey, typeKey) = name switch
        {
            "live-deal-binary-option-placed" => ("active_id", "option_type"),
            "live-deal-digital-option" => ("instrument_active_id", "expiration_type"),
            "live-deal" => ("instrument_active_id", "instrument_type"),
            _ => throw new ArgumentException("name must be live-deal-binary-option-placed, live-deal-digital-option or live-deal", nameof(name)),
        };
        return new Obj
        {
            ["name"] = name,
            ["params"] = new Obj { ["routingFilters"] = new Obj { [activeKey] = Act(asset), [typeKey] = type } },
            ["version"] = "2.0",
        };
    }

    private async Task ResubscribeStreamsAsync(CancellationToken ct)
    {
        List<object> frames = new();
        lock (_track)
        {
            frames.AddRange(_candleStreams.Select(x => (object)CandleStreamMsg(x.Asset, x.Size)));
            frames.AddRange(_allSizeStreams.Select(a => (object)AllSizesMsg(a)));
            frames.AddRange(_moodStreams.Select(a => (object)MoodMsg(a)));
            frames.AddRange(_strikeSubscriptions.Select(x => (object)StrikeSubscribeMsg(x.Asset, x.Minutes)));
            frames.AddRange(_topAssetsSubscriptions.Select(t => (object)TopAssetsMsg(t)));
            frames.AddRange(_commissionSubscriptions.Select(t => (object)CommissionMsg(t)));
            frames.AddRange(_liveDealSubscriptions.Select(x => (object)LiveDealMsg(x.Name, x.Asset, x.Type)));
        }
        foreach (var msg in frames) await SendRaw("subscribeMessage", msg, ct).ConfigureAwait(false);
    }

    #region Account & balance

    /// <summary>Profile of the logged-in user (Python <c>get_profile</c>; always fresh).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetProfileAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        var r = await ByName("profile", new Obj { ["name"] = "get-profile", ["version"] = "1.0", ["body"] = new Obj() }, NextRid(), ct)
            .ConfigureAwait(false);
        return J.Obj(r["msg"]);
    }

    /// <summary>All balances: the raw <c>balances</c> message (Python <c>get_balances</c>; always fresh).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetBalancesAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        return await ByName("balances", new Obj { ["name"] = "get-balances", ["version"] = "1.0" }, "", ct).ConfigureAwait(false);
    }

    /// <summary>Amount of the selected balance (Python <c>get_balance_v2</c>): pushed value if known, else asks the server.</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<double?> GetBalanceAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        long id = Bal;
        lock (_s.Lock)
        {
            if (_s.Balance.TryGetValue(id, out double cached)) return cached;
        }
        foreach (var b in J.Arr((await GetBalancesAsync(ct).ConfigureAwait(false))["msg"]))
        {
            if (J.Long(b!["id"]) != id) continue;
            double amount = J.Dbl(b["amount"]);
            lock (_s.Lock) _s.Balance[id] = amount;
            return amount;
        }
        return null;
    }

    /// <summary>Id of the selected balance (Python <c>get_balance_id</c>).</summary>
    public long? GetBalanceId()
    {
        lock (_s.Lock) return _s.BalanceId;
    }

    /// <summary>Whether the selected balance is real or practice (Python <c>get_balance_mode</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<BalanceMode?> GetBalanceModeAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        long? id = GetBalanceId();
        foreach (var b in J.Arr((await GetBalancesAsync(ct).ConfigureAwait(false))["msg"]))
        {
            if (J.Long(b!["id"]) == id)
                return J.Long(b["type"]) switch { 1 => BalanceMode.Real, 4 => BalanceMode.Practice, _ => null };
        }
        return null;
    }

    /// <summary>Balance id of a mode (Python <c>get_balance_id_by_mode</c>); null if the account has none.</summary>
    /// <param name="mode">Real or practice.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<long?> GetBalanceIdByModeAsync(BalanceMode mode, CancellationToken ct = default)
    {
        EnsureConnected();
        long? real = null, practice = null;
        foreach (var b in J.Arr((await GetBalancesAsync(ct).ConfigureAwait(false))["msg"]))
        {
            long type = J.Long(b!["type"]);
            if (type == 1) real = J.Long(b["id"]);
            if (type == 4) practice = J.Long(b["id"]);
        }
        return mode == BalanceMode.Real ? real : practice;
    }

    /// <summary>Selects the balance later trades use (Python <c>change_balance</c>; sends nothing to the server, like Python).</summary>
    /// <param name="mode">Real or practice.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task ChangeBalanceAsync(BalanceMode mode, CancellationToken ct = default)
    {
        EnsureConnected();
        if (_balanceIds.Count == 0)
        {
            foreach (var b in J.Arr((await GetBalancesAsync(ct).ConfigureAwait(false))["msg"]))
            {
                long type = J.Long(b!["type"]);
                if (type == 1) _balanceIds[BalanceMode.Real] = J.Long(b["id"]);
                if (type == 4) _balanceIds[BalanceMode.Practice] = J.Long(b["id"]);
            }
        }
        if (!_balanceIds.TryGetValue(mode, out long id)) throw new IqOptionException($"This account has no {mode} balance.");
        lock (_s.Lock) _s.BalanceId = id;
    }

    /// <summary>Currency of the selected balance (Python <c>get_currency</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<string?> GetCurrencyAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        long? id = GetBalanceId();
        foreach (var b in J.Arr((await GetBalancesAsync(ct).ConfigureAwait(false))["msg"]))
        {
            if (J.Long(b!["id"]) == id) return J.Str(b["currency"]);
        }
        return null;
    }

    /// <summary>Resets the practice balance (Python <c>reset_practice_balance</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<bool> ResetPracticeBalanceAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        try
        {
            var r = await ByName("training-balance-reset", new Obj { ["name"] = "reset-training-balance", ["version"] = "2.0" }, "", ct)
                .ConfigureAwait(false);
            return J.IsTrue((r["msg"] as JsonObject)?["isSuccessful"]);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return false;
        }
    }

    /// <summary>Account transactions (Python <c>get_transactions</c>); returns the whole message.</summary>
    /// <param name="from">Start (Unix seconds).</param>
    /// <param name="to">End (Unix seconds).</param>
    /// <param name="limit">Page size.</param>
    /// <param name="offset">Page offset.</param>
    /// <param name="types">e.g. "deposit", "order_sell", "withdrawal".</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetTransactionsAsync(long from, long to, int limit, int offset, IEnumerable<string> types, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        return await ById("transactions", new Obj
        {
            ["name"] = "get-transactions",
            ["body"] = new Obj { ["from"] = from, ["to"] = to, ["limit"] = limit, ["offset"] = offset, ["types"] = types.ToList() },
        }, rid, ct).ConfigureAwait(false);
    }

    #endregion

    #region Assets & market info

    private const string FinancialQuery =
        "query GetAssetProfileInfo($activeId:ActiveID!, $locale: LocaleName){\n active(id: $activeId) {\n id\n name(source: TradeRoom, locale: $locale)\n ticker\n media {\n siteBackground\n }\n charts {\n dtd {\n change\n }\n m1 {\n change\n }\n y1 {\n change\n }\n ytd {\n change\n }\n }\n index_fininfo: fininfo {\n ... on Index {\n description(locale: $locale)\n }\n }\n fininfo {\n ... on Pair {\n type\n description(locale: $locale)\n currency {\n name(locale: $locale)\n }\n base {\n name(locale: $locale)\n ... on Stock {\n company {\n country {\n nameShort\n }\n gics {\n sector\n industry\n }\n site\n domain\n }\n keyStat {\n marketCap\n peRatioHigh\n }\n }\n ... on CryptoCurrency {\n site\n domain\n coinsInCirculation\n maxCoinsQuantity\n volume24h\n marketCap\n }\n }\n }\n }\n }\n }";

    private static JsonNode? Path(JsonNode? node, params string[] keys)
    {
        foreach (string key in keys) node = (node as JsonObject)?[key];
        return node;
    }

    /// <summary>Sends <paramref name="msg"/> and re-sends it every <paramref name="interval"/> until a <paramref name="reply"/> arrives (Python retry loops).</summary>
    private async Task<JsonObject> ResendUntilAsync(string reply, object msg, TimeSpan interval, TimeSpan total, CancellationToken ct)
    {
        var gate = _conn.Gate(reply);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            using var w = _conn.AddWaiter(reply, null);
            var clock = Stopwatch.StartNew();
            while (true)
            {
                await Send("sendMessage", msg, "", ct).ConfigureAwait(false);
                var left = total - clock.Elapsed;
                if (left <= TimeSpan.Zero) throw new TimeoutException($"no '{reply}' reply");
                try
                {
                    return await w.Task.WaitAsync(interval < left ? interval : left, ct).ConfigureAwait(false);
                }
                catch (TimeoutException) when (clock.Elapsed < total)
                {
                    // no answer yet: send again, like Python
                }
            }
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Asset name → id table used by every method (Python <c>get_all_ACTIVES_OPCODE</c>).</summary>
    public IReadOnlyDictionary<string, int> GetAllActives() => _actives.Snapshot();

    /// <summary>Adds/updates binary and turbo assets from the init data (Python <c>update_ACTIVES_OPCODE</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task UpdateActivesAsync(CancellationToken ct = default)
    {
        var init = await GetAllInitAsync(ct).ConfigureAwait(false) ?? throw new TimeoutException("get-initialization-data did not answer");
        foreach (string group in new[] { "binary", "turbo" })
        {
            foreach (var (key, node) in J.Obj(J.Obj(init[group])["actives"]))
            {
                var a = J.Obj(node);
                if (J.Str(a["exchange"]) == "" || J.Str(a["provider"]) == "OTC")
                    _actives.Set(J.Str(a["ticker"])!, int.Parse(key, CultureInfo.InvariantCulture));
            }
        }
    }

    /// <summary>Adds/updates digital assets (Python <c>get_All_Digital_ACTIVES_OPCODE</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task UpdateDigitalActivesAsync(CancellationToken ct = default)
    {
        var data = await GetDigitalUnderlyingListAsync(ct).ConfigureAwait(false) ?? throw new TimeoutException("underlying list did not answer");
        foreach (var n in J.Arr(data["underlying"])) _actives.Set(J.Str(n!["name"])!, (int)J.Long(n["active_id"]));
    }

    /// <summary>Adds/updates forex/cfd/crypto assets (Python <c>instruments_input_to_ACTIVES</c>; null = crypto, forex, cfd).</summary>
    /// <param name="instrumentType">"crypto", "forex", "cfd" or null for all three.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task UpdateMarginActivesAsync(string? instrumentType = null, CancellationToken ct = default)
    {
        foreach (string type in instrumentType != null ? new[] { instrumentType } : new[] { "crypto", "forex", "cfd" })
        {
            foreach (var n in J.Arr((await GetInstrumentsAsync(type, ct).ConfigureAwait(false))["instruments"]))
                _actives.Set(J.Str(n!["id"])!, (int)J.Long(n["active_id"]));
        }
    }

    /// <summary>Asset name for an id (Python <c>opcode_to_name</c>); null if unknown.</summary>
    /// <param name="activeId">Asset id.</param>
    public string? OpcodeToName(int activeId) => _actives.NameOf(activeId);

    /// <summary>Display name of an asset (Python <c>get_name_by_activeId</c>).</summary>
    /// <param name="activeId">Asset id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<string?> GetNameByActiveIdAsync(int activeId, CancellationToken ct = default) =>
        J.Str(Path(await GetFinancialInformationAsync(activeId, ct).ConfigureAwait(false), "msg", "data", "active", "name"));

    /// <summary>Asset profile (Python <c>get_financial_information</c>); returns the whole message.</summary>
    /// <param name="activeId">Asset id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetFinancialInformationAsync(int activeId, CancellationToken ct = default)
    {
        EnsureConnected();
        return await ByName("financial-information", new Obj
        {
            ["name"] = "get-financial-information",
            ["version"] = "1.0",
            ["body"] = new Obj { ["query"] = FinancialQuery, ["operationName"] = "GetAssetProfileInfo", ["variables"] = new Obj { ["activeId"] = activeId } },
        }, "", ct).ConfigureAwait(false);
    }

    /// <summary>Instruments of a type (Python <c>get_instruments</c>; re-asks every 10 s until answered).</summary>
    /// <param name="instrumentType">"crypto", "forex" or "cfd".</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetInstrumentsAsync(string instrumentType, CancellationToken ct = default)
    {
        EnsureConnected();
        var r = await ResendUntilAsync("instruments", new Obj
        {
            ["name"] = "get-instruments", ["version"] = "4.0", ["body"] = new Obj { ["type"] = instrumentType },
        }, Scaled(10), T, ct).ConfigureAwait(false);
        return J.Obj(r["msg"]);
    }

    /// <summary>Binary/turbo/blitz init data (Python <c>get_all_init_v2</c>); null after 30 s without answer.</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject?> GetAllInitAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        try
        {
            var r = await ResendUntilAsync("initialization-data", new Obj
            {
                ["name"] = "get-initialization-data", ["version"] = "3.0", ["body"] = new Obj(),
            }, Scaled(3), TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
            return J.Obj(r["msg"]);
        }
        catch (TimeoutException)
        {
            Log("get-initialization-data: no answer after 30 s");
            return null;
        }
    }

    /// <summary>Which assets are open right now, per type: binary, turbo, digital, cfd, forex, crypto (Python <c>get_all_open_time</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<Dictionary<string, Dictionary<string, bool>>> GetAllOpenTimeAsync(CancellationToken ct = default)
    {
        var result = new Dictionary<string, Dictionary<string, bool>>();
        var init = await GetAllInitAsync(ct).ConfigureAwait(false) ?? throw new TimeoutException("get-initialization-data did not answer");
        foreach (string option in new[] { "binary", "turbo" })
        {
            foreach (var (_, node) in J.Obj(J.Obj(init[option])["actives"]))
            {
                var a = J.Obj(node);
                string name = J.Str(a["name"])!.Split('.')[1];
                State.GetOrAdd(result, option)[name] = J.IsTrue(a["enabled"]) && !J.IsTrue(a["is_suspended"]);
            }
        }
        double now = Now;
        var digital = await GetDigitalUnderlyingListAsync(ct).ConfigureAwait(false) ?? throw new TimeoutException("underlying list did not answer");
        foreach (var d in J.Arr(digital["underlying"]))
            State.GetOrAdd(result, "digital")[J.Str(d!["underlying"])!] = IsOpenNow(d["schedule"], now);
        foreach (string type in new[] { "cfd", "forex", "crypto" })
        {
            foreach (var d in J.Arr((await GetInstrumentsAsync(type, ct).ConfigureAwait(false))["instruments"]))
                State.GetOrAdd(result, type)[J.Str(d!["name"])!] = IsOpenNow(d["schedule"], now);
        }
        return result;
    }

    private static bool IsOpenNow(JsonNode? schedule, double now)
    {
        bool open = false;
        foreach (var s in J.Arr(schedule))
        {
            if (J.Dbl(s!["open"]) < now && now < J.Dbl(s["close"])) open = true;
        }
        return open;
    }

    /// <summary>Turbo and binary details per asset name (Python <c>get_binary_option_detail</c>, data from the v2 init).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<Dictionary<string, Dictionary<string, JsonObject>>> GetBinaryOptionDetailAsync(CancellationToken ct = default)
    {
        var init = await GetAllInitAsync(ct).ConfigureAwait(false) ?? throw new TimeoutException("get-initialization-data did not answer");
        var detail = new Dictionary<string, Dictionary<string, JsonObject>>();
        foreach (string type in new[] { "turbo", "binary" })
        {
            foreach (var (_, node) in J.Obj(J.Obj(init[type])["actives"]))
            {
                var a = J.Obj(node);
                string name = J.Str(a["name"])!;
                int dot = name.IndexOf('.');
                if (dot < 0) throw new FormatException($"unexpected active name '{name}'");
                State.GetOrAdd(detail, name[(dot + 1)..])[type] = a;
            }
        }
        return detail;
    }

    /// <summary>Payout fraction per ticker for turbo and binary, e.g. 0.87 (Python <c>get_all_profit_v2</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<Dictionary<string, Dictionary<string, double>>> GetAllProfitAsync(CancellationToken ct = default) =>
        ProfitTable(await GetAllInitAsync(ct).ConfigureAwait(false), new[] { "turbo", "binary" });

    /// <summary>Blitz payout fraction per ticker (Python <c>get_all_profit_blitz_v2</c>).</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<Dictionary<string, Dictionary<string, double>>> GetAllProfitBlitzAsync(CancellationToken ct = default) =>
        ProfitTable(await GetAllInitAsync(ct).ConfigureAwait(false), new[] { "blitz" });

    private static Dictionary<string, Dictionary<string, double>> ProfitTable(JsonObject? init, string[] types)
    {
        var all = new Dictionary<string, Dictionary<string, double>>();
        if (init == null) return all;
        foreach (string type in types)
        {
            if (init[type] is not JsonObject group) continue;
            foreach (var (_, node) in J.Obj(group["actives"]))
            {
                var a = J.Obj(node);
                if (J.Truthy(a["is_suspended"]) || !J.Truthy(a["enabled"])) continue;
                State.GetOrAdd(all, J.Str(a["ticker"])!)[type] = Helpers.PayoutFraction(J.Dbl(Path(a, "option", "profit", "commission")));
            }
        }
        return all;
    }

    /// <summary>Digital assets with schedules (Python <c>get_digital_underlying_list_data</c>); null after 30 s.</summary>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject?> GetDigitalUnderlyingListAsync(CancellationToken ct = default)
    {
        EnsureConnected();
        try
        {
            var r = await _conn.RequestAsync("sendMessage", new Obj
            {
                ["name"] = "digital-option-instruments.get-underlying-list", ["version"] = "3.0", ["body"] = new Obj { ["type"] = "digital-option" },
            }, "", "underlying-list", false, TimeSpan.FromSeconds(30), ct).ConfigureAwait(false);
            return J.Obj(r["msg"]);
        }
        catch (TimeoutException)
        {
            return null;
        }
    }

    /// <summary>Top assets of a type (Python <c>get_top_asset</c>).</summary>
    /// <param name="instrumentType">e.g. "forex", "digital-option".</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonNode?> GetTopAssetAsync(string instrumentType, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        var r = await ById("top-assets", new Obj
        {
            ["name"] = "get-top-assets", ["version"] = "1.2", ["body"] = new Obj { ["instrument_type"] = instrumentType },
        }, rid, ct).ConfigureAwait(false);
        return r["msg"];
    }

    #endregion

    #region Candles & live prices

    private static void CheckSize(int size)
    {
        if (!CandleSizes.Contains(size)) throw new ArgumentOutOfRangeException(nameof(size), size, "Use one of IqOption.CandleSizes.");
    }

    /// <summary>Historic candles ending at <paramref name="endTime"/> (Python <c>get_candles</c>).</summary>
    /// <param name="asset">Asset name, e.g. "EURUSD".</param>
    /// <param name="intervalSeconds">Candle size in seconds.</param>
    /// <param name="count">How many candles.</param>
    /// <param name="endTime">End time (Unix seconds), e.g. <see cref="GetServerTimestamp"/>.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<List<Candle>> GetCandlesAsync(string asset, int intervalSeconds, int count, double endTime, CancellationToken ct = default)
    {
        EnsureConnected();
        int id = Act(asset);
        var r = await ByName("candles", new Obj
        {
            ["name"] = "get-candles",
            ["version"] = "2.0",
            ["body"] = new Obj { ["active_id"] = id, ["size"] = intervalSeconds, ["to"] = (long)endTime, ["count"] = count, [""] = id },
        }, "", ct).ConfigureAwait(false);
        return J.Arr(Path(r, "msg", "candles")).Select(n => Candle.FromJson(J.Obj(n))).ToList();
    }

    /// <summary>Loads <paramref name="maxDict"/> candles and streams new ones, keeping the newest <paramref name="maxDict"/> (Python <c>start_candles_stream</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="size">Candle size, one of <see cref="CandleSizes"/>.</param>
    /// <param name="maxDict">How many candles to keep.</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns>True when the first live candle arrived within 20 s.</returns>
    public async Task<bool> StartCandlesStreamAsync(string asset, int size, int maxDict, CancellationToken ct = default)
    {
        EnsureConnected();
        CheckSize(size);
        lock (_s.Lock) State.GetOrAdd(_s.MaxDict, asset)[size] = maxDict;
        await SeedCandlesAsync(asset, size, maxDict, ct).ConfigureAwait(false);
        lock (_track)
        {
            if (!_candleStreams.Contains((asset, size))) _candleStreams.Add((asset, size));
        }
        lock (_s.Lock) State.GetOrAdd(_s.CandleCheck, asset)[size] = false;
        return await SubscribeUntilDataAsync(CandleStreamMsg(asset, size), () => CandleCheck(asset, size), ct).ConfigureAwait(false);
    }

    /// <summary>Same as <see cref="StartCandlesStreamAsync"/> for all sizes at once (Python <c>start_candles_stream(asset, "all", maxdict)</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="maxDict">How many candles to keep per size.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<bool> StartAllCandlesStreamsAsync(string asset, int maxDict, CancellationToken ct = default)
    {
        EnsureConnected();
        foreach (int size in CandleSizes)
        {
            await SeedCandlesAsync(asset, size, maxDict, ct).ConfigureAwait(false);
            lock (_s.Lock) State.GetOrAdd(_s.MaxDict, asset)[size] = maxDict;
        }
        lock (_s.Lock) _s.AllSizeCheck[asset] = false;
        lock (_track)
        {
            if (!_allSizeStreams.Contains(asset)) _allSizeStreams.Add(asset);
        }
        return await SubscribeUntilDataAsync(AllSizesMsg(asset), () => AllSizeCheck(asset), ct).ConfigureAwait(false);
    }

    private async Task SeedCandlesAsync(string asset, int size, int maxDict, CancellationToken ct)
    {
        var candles = await GetCandlesAsync(asset, size, maxDict, Now, ct).ConfigureAwait(false);
        lock (_s.Lock)
        {
            var buffer = _s.CandleBuffer(asset, size);
            foreach (var c in candles) buffer[c.From] = c;
        }
    }

    private bool CandleCheck(string asset, int size)
    {
        lock (_s.Lock) return _s.CandleCheck.TryGetValue(asset, out var d) && d.TryGetValue(size, out bool v) && v;
    }

    private bool AllSizeCheck(string asset)
    {
        lock (_s.Lock) return _s.AllSizeCheck.TryGetValue(asset, out bool v) && v;
    }

    /// <summary>Python stream start: subscribe again every second until data arrives, for up to 20 s.</summary>
    private async Task<bool> SubscribeUntilDataAsync(object msg, Func<bool> hasData, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        while (true)
        {
            if (clock.Elapsed > TimeSpan.FromSeconds(20))
            {
                Log("stream did not start within 20 s");
                return false;
            }
            if (hasData()) return true;
            await Send("subscribeMessage", msg, "", ct).ConfigureAwait(false);
            await WaitCondAsync(hasData, Scaled(1), ct).ConfigureAwait(false);
        }
    }

    /// <summary>Python stream stop: unsubscribe again until one second passes without data.</summary>
    private async Task UnsubscribeUntilQuietAsync(object msg, Func<bool> gotData, Action reset, CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        while (gotData() && clock.Elapsed < T)
        {
            reset();
            await Send("unsubscribeMessage", msg, "", ct).ConfigureAwait(false);
            await Task.Delay(Scaled(1), ct).ConfigureAwait(false);
        }
    }

    /// <summary>Stops one candle stream (Python <c>stop_candles_stream(asset, size)</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="size">Candle size.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task StopCandlesStreamAsync(string asset, int size, CancellationToken ct = default)
    {
        EnsureConnected();
        CheckSize(size);
        lock (_track) _candleStreams.Remove((asset, size));
        await UnsubscribeUntilQuietAsync(CandleStreamMsg(asset, size), () => CandleCheck(asset, size),
            () => { lock (_s.Lock) State.GetOrAdd(_s.CandleCheck, asset)[size] = false; }, ct).ConfigureAwait(false);
    }

    /// <summary>Stops the all-sizes stream (Python <c>stop_candles_stream(asset, "all")</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task StopAllCandlesStreamsAsync(string asset, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track) _allSizeStreams.Remove(asset);
        await UnsubscribeUntilQuietAsync(AllSizesMsg(asset), () => AllSizeCheck(asset),
            () => { lock (_s.Lock) _s.AllSizeCheck[asset] = false; }, ct).ConfigureAwait(false);
    }

    /// <summary>Stored live candles of one size, keyed by start time (Python <c>get_realtime_candles</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="size">Candle size.</param>
    public IReadOnlyDictionary<long, Candle> GetRealtimeCandles(string asset, int size)
    {
        CheckSize(size);
        lock (_s.Lock)
        {
            return _s.Candles.TryGetValue(asset, out var bySize) && bySize.TryGetValue(size, out var buffer)
                ? new SortedDictionary<long, Candle>(buffer)
                : new SortedDictionary<long, Candle>();
        }
    }

    /// <summary>Stored live candles of every size (Python <c>get_realtime_candles(asset, "all")</c>).</summary>
    /// <param name="asset">Asset name.</param>
    public IReadOnlyDictionary<int, IReadOnlyDictionary<long, Candle>> GetRealtimeCandles(string asset)
    {
        lock (_s.Lock)
        {
            var result = new SortedDictionary<int, IReadOnlyDictionary<long, Candle>>();
            if (_s.Candles.TryGetValue(asset, out var bySize))
            {
                foreach (var (size, buffer) in bySize) result[size] = new SortedDictionary<long, Candle>(buffer);
            }
            return result;
        }
    }

    /// <summary>All stored live candles (Python <c>get_all_realtime_candles</c>).</summary>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<int, IReadOnlyDictionary<long, Candle>>> GetAllRealtimeCandles()
    {
        string[] assets;
        lock (_s.Lock) assets = _s.Candles.Keys.ToArray();
        return assets.ToDictionary(a => a, a => GetRealtimeCandles(a));
    }

    #endregion

    #region Binary / Turbo / Blitz

    private Obj BuyFrame(double amount, int activeId, Direction direction, int minutes)
    {
        double now = Now;
        var (exp, index) = Helpers.GetExpirationTime((long)now, minutes, now, _tz);
        return new Obj
        {
            ["body"] = new Obj
            {
                ["price"] = amount,
                ["active_id"] = activeId,
                ["expired"] = exp,
                ["direction"] = Helpers.DirectionText(direction),
                ["option_type_id"] = index < 5 ? 3 : 1,
                ["user_balance_id"] = Bal,
            },
            ["name"] = "binary-options.open-option",
            ["version"] = "1.0",
        };
    }

    private static (bool, JsonObject?, string?) OptionReply(JsonObject reply)
    {
        var m = J.Obj(reply["msg"]);
        return m.ContainsKey("message") ? (false, null, J.Str(m["message"])) : (true, m, null);
    }

    /// <summary>Buys a turbo (1–5 min) or binary (15+ min) option (Python <c>buy</c>).</summary>
    /// <param name="amount">Stake.</param>
    /// <param name="asset">Asset name, e.g. "EURUSD".</param>
    /// <param name="direction">Call or put.</param>
    /// <param name="minutes">Expiry in minutes.</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns><c>(true, optionId, null)</c> or <c>(false, 0, error)</c>.</returns>
    public async Task<(bool Success, long Id, string? Error)> BuyAsync(double amount, string asset, Direction direction, int minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        int activeId = Act(asset);
        string rid = NextRid();
        try
        {
            var (ok, m, error) = OptionReply(await ById("option", BuyFrame(amount, activeId, direction, minutes), rid, ct).ConfigureAwait(false));
            return ok ? (true, J.Long(m!["id"]), null) : (false, 0, error);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, 0, Reason(ex));
        }
    }

    /// <summary>Buys several options at once (Python <c>buy_multi</c>); null where a buy failed.</summary>
    /// <param name="amounts">Stakes.</param>
    /// <param name="assets">Asset names.</param>
    /// <param name="directions">Directions.</param>
    /// <param name="minutes">Expiries in minutes.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<List<long?>> BuyMultiAsync(IList<double> amounts, IList<string> assets, IList<Direction> directions, IList<int> minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        if (amounts.Count != assets.Count || assets.Count != directions.Count || directions.Count != minutes.Count)
            throw new ArgumentException("All lists must have the same length.");
        var activeIds = assets.Select(Act).ToList();
        var rids = amounts.Select(_ => NextRid()).ToList();
        var waiters = rids.Select(r => _conn.AddWaiter("option", r)).ToList();
        try
        {
            for (int i = 0; i < rids.Count; i++)
                await Send("sendMessage", BuyFrame(amounts[i], activeIds[i], directions[i], minutes[i]), rids[i], ct).ConfigureAwait(false);
            var ids = new List<long?>();
            foreach (var w in waiters)
            {
                try
                {
                    var (ok, m, _) = OptionReply(await w.Task.WaitAsync(T, ct).ConfigureAwait(false));
                    ids.Add(ok ? J.Long(m!["id"]) : null);
                }
                catch (Exception ex) when (IsFailure(ex))
                {
                    ids.Add(null);
                }
            }
            return ids;
        }
        finally
        {
            foreach (var w in waiters) w.Dispose();
        }
    }

    /// <summary>Buys with an exact expiry timestamp (Python <c>buy_by_raw_expirations</c>); <paramref name="optionType"/> is "turbo" or "binary".</summary>
    /// <param name="amount">Stake.</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="direction">Call or put.</param>
    /// <param name="optionType">"turbo" or "binary".</param>
    /// <param name="expired">Expiry (Unix seconds).</param>
    /// <param name="ct">Cancellation.</param>
    public Task<(bool Success, JsonObject? Result, string? Error)> BuyByRawExpirationAsync(double amount, string asset, Direction direction, string optionType, long expired, CancellationToken ct = default) =>
        BuyByRawExpirationAsync(amount, Act(asset), direction, optionType switch
        {
            "turbo" => 3,
            "binary" => 1,
            _ => throw new ArgumentException("optionType must be \"turbo\" or \"binary\"", nameof(optionType)),
        }, expired, ct);

    /// <summary>Buys with an exact expiry and raw ids (Python <c>buy_by_raw_expirations</c> with an int active and option).</summary>
    /// <param name="amount">Stake.</param>
    /// <param name="activeId">Asset id.</param>
    /// <param name="direction">Call or put.</param>
    /// <param name="optionTypeId">3 = turbo, 1 = binary.</param>
    /// <param name="expired">Expiry (Unix seconds).</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonObject? Result, string? Error)> BuyByRawExpirationAsync(double amount, int activeId, Direction direction, int optionTypeId, long expired, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        try
        {
            return OptionReply(await ById("option", new Obj
            {
                ["body"] = new Obj
                {
                    ["price"] = amount,
                    ["active_id"] = activeId,
                    ["expired"] = expired,
                    ["direction"] = Helpers.DirectionText(direction),
                    ["option_type_id"] = optionTypeId,
                    ["user_balance_id"] = Bal,
                },
                ["name"] = "binary-options.open-option",
                ["version"] = "1.0",
            }, rid, ct).ConfigureAwait(false));
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null, Reason(ex));
        }
    }

    /// <summary>Buys a blitz option (Python <c>buy_blitz</c>): OTC 30–300 s, others 60–900 s.</summary>
    /// <param name="amount">Stake.</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="direction">Call or put.</param>
    /// <param name="durationSeconds">Expiry in seconds.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonObject? Result, string? Error)> BuyBlitzAsync(double amount, string asset, Direction direction, int durationSeconds = 30, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        long? balance = GetBalanceId();
        int activeId = Act(asset);
        bool otc = asset.ToUpperInvariant().EndsWith("-OTC", StringComparison.Ordinal);
        var (min, max) = otc ? (30, 300) : (60, 900);
        if (durationSeconds < min || durationSeconds > max)
            throw new ArgumentOutOfRangeException(nameof(durationSeconds), $"Blitz expiry for {(otc ? "OTC" : "normal")} pairs must be between {min}s and {max}s. You gave: {durationSeconds}");
        double expired = Now + durationSeconds;
        int profitPercent = 0;
        var blitz = await GetAllProfitBlitzAsync(ct).ConfigureAwait(false);
        if (blitz.TryGetValue(asset, out var p) && p.TryGetValue("blitz", out double fraction)) profitPercent = Helpers.BlitzPercent(fraction);
        try
        {
            return OptionReply(await ById("option", new Obj
            {
                ["body"] = new Obj
                {
                    ["user_balance_id"] = balance,
                    ["active_id"] = activeId,
                    ["option_type_id"] = 12,
                    ["direction"] = Helpers.DirectionText(direction),
                    ["expired"] = (long)expired,
                    ["price"] = amount,
                    ["profit_percent"] = profitPercent,
                    ["expiration_size"] = durationSeconds,
                },
                ["name"] = "binary-options.open-option",
                ["version"] = "2.0",
            }, rid, ct).ConfigureAwait(false));
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null, Reason(ex));
        }
    }

    /// <summary>Sells options early (Python <c>sell_option</c>); returns the server's reply, or null if none came.</summary>
    /// <param name="optionIds">Option ids.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject?> SellOptionAsync(IEnumerable<long> optionIds, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        try
        {
            return await ById("sold-options", new Obj
            {
                ["name"] = "sell-options", ["version"] = "2.0", ["body"] = new Obj { ["options_ids"] = optionIds.ToList() },
            }, rid, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return null;
        }
    }

    /// <summary>Waits until a binary/turbo option closes (Python <c>check_win_v3</c>); profit = payout − stake. No timeout: cancel with <paramref name="ct"/>.</summary>
    /// <param name="optionId">Id returned by <see cref="BuyAsync"/>.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(TradeResult Result, double Profit)> CheckWinAsync(long optionId, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track) _pendingBinary.Add(optionId);
        try
        {
            double? profit = null;
            await _conn.WaitUntilAsync(() => (profit = ClosedProfit(optionId)) != null, null, ct).ConfigureAwait(false);
            return (Helpers.ResultOf(profit!.Value), profit.Value);
        }
        finally
        {
            lock (_track) _pendingBinary.Remove(optionId);
        }
    }

    private double? ClosedProfit(long optionId)
    {
        lock (_s.Lock)
        {
            if (_s.OrderAsync.TryGetValue(optionId, out var d) && d.TryGetValue("option-closed", out var closed))
            {
                var m = J.Obj(closed["msg"]);
                return J.Dbl(m["profit_amount"]) - J.Dbl(m["amount"]);
            }
        }
        lock (_track) return _recovered.TryGetValue(optionId, out double p) ? p : null;
    }

    /// <summary>Recent binary/turbo options (Python <c>get_optioninfo_v2</c>); the whole message.</summary>
    /// <param name="limit">How many.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetOptionInfoAsync(int limit, CancellationToken ct = default)
    {
        EnsureConnected();
        return await ByName("options", new Obj
        {
            ["name"] = "get-options",
            ["body"] = new Obj { ["limit"] = limit, ["instrument_type"] = "binary,turbo", ["user_balance_id"] = Bal },
        }, "", ct).ConfigureAwait(false);
    }

    /// <summary>Seconds left to buy with this expiry (Python <c>get_remaning</c>): 1–5, 15, 30, … 165 minutes. Buy before (remaining − 30).</summary>
    /// <param name="minutes">Expiry label.</param>
    /// <param name="timestamp">Time to calculate from; null = server time.</param>
    public int GetRemainingTime(int minutes, double? timestamp = null)
    {
        double now = Now;
        foreach (var (label, seconds) in Helpers.GetRemainingTime(timestamp ?? now, now, _tz))
        {
            if (label == minutes) return (int)seconds;
        }
        throw new ArgumentOutOfRangeException(nameof(minutes), minutes, "Valid values: 1-5, 15, 30, ..., 165.");
    }

    /// <summary>Binary options opened on any device, by id (Python <c>get_option_open_by_other_pc</c>).</summary>
    public IReadOnlyDictionary<long, JsonObject> GetOptionOpenByOtherPc()
    {
        lock (_s.Lock) return new Dictionary<long, JsonObject>(_s.OptionOpened);
    }

    /// <summary>Forgets one entry of <see cref="GetOptionOpenByOtherPc"/>.</summary>
    /// <param name="id">Option id.</param>
    public void DelOptionOpenByOtherPc(long id)
    {
        lock (_s.Lock) _s.OptionOpened.Remove(id);
    }

    #endregion

    // @@GROUPS@@
}
