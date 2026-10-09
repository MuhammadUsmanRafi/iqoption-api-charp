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

    // @@GROUPS@@
}
