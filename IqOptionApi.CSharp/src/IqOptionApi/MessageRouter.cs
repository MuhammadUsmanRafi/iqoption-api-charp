using System.Text.Json.Nodes;

namespace IqOptionApi;

/// <summary>Live data filled by the router. Every field is read and written under <see cref="Lock"/>.</summary>
internal sealed class State
{
    public readonly object Lock = new();
    public double ServerTimestamp;
    public bool TimeSynced;
    public long? BalanceId;
    public JsonObject? BalancesRaw;
    public readonly Dictionary<long, double> Balance = new();
    public readonly Dictionary<string, Dictionary<int, SortedDictionary<long, Candle>>> Candles = new();
    public readonly Dictionary<string, Dictionary<int, int>> MaxDict = new();
    public readonly Dictionary<string, Dictionary<int, bool>> CandleCheck = new();
    public readonly Dictionary<string, bool> AllSizeCheck = new();
    public readonly Dictionary<string, Dictionary<int, double?>> DigitalProfit = new();
    public readonly Dictionary<string, Dictionary<int, JsonObject>> QuotesRaw = new();
    public readonly Dictionary<string, Dictionary<int, Dictionary<string, StrikeQuote>>> Quotes = new();
    public readonly Dictionary<int, double> TradersMood = new();
    public readonly Dictionary<string, Dictionary<string, Dictionary<double, int>>> Commission = new();
    public readonly Dictionary<string, JsonNode?> TopAssetsUpdated = new();
    public readonly Dictionary<(string Name, string Asset, string Type), LiveDealBuffer> LiveDeals = new();
    public readonly Dictionary<long, Dictionary<string, JsonObject>> OrderAsync = new();
    public readonly Dictionary<long, JsonObject> OptionOpened = new();
    public readonly Dictionary<long, JsonObject> DigitalOpened = new();

    public static TV GetOrAdd<TK, TV>(Dictionary<TK, TV> d, TK key) where TK : notnull where TV : new()
    {
        if (!d.TryGetValue(key, out var v)) d[key] = v = new TV();
        return v;
    }

    public SortedDictionary<long, Candle> CandleBuffer(string asset, int size) => GetOrAdd(GetOrAdd(Candles, asset), size);

    public bool HasAsync(long id, string name) => OrderAsync.TryGetValue(id, out var d) && d.ContainsKey(name);
}

/// <summary>Python <c>deque</c> with <c>appendleft</c>: newest first, optional max length dropping the oldest.</summary>
internal sealed class LiveDealBuffer
{
    private readonly LinkedList<JsonObject> _items = new();

    public int? MaxLength { get; set; }

    public void AddNewest(JsonObject m)
    {
        _items.AddFirst(m);
        Trim();
    }

    public void Trim()
    {
        while (MaxLength is int max && _items.Count > max) _items.RemoveLast();
    }

    public JsonObject? PopOldest()
    {
        if (_items.Last is null) return null;
        var v = _items.Last.Value;
        _items.RemoveLast();
        return v;
    }

    public List<JsonObject> NewestFirst() => _items.ToList();
}

/// <summary>Port of ws/client.py <c>on_message</c>: stores what public methods read and raises events. Never throws.</summary>
internal sealed class MessageRouter
{
    private readonly State _s;
    private readonly ActivesTable _actives;
    private readonly Action<string> _log;

    public MessageRouter(State state, ActivesTable actives, Action<string> log)
    {
        _s = state;
        _actives = actives;
        _log = log;
    }

    public event Action<JsonObject>? MessageReceived;

    public event Action<string, int, Candle>? CandleReceived;

    public event Action<JsonObject>? PositionChanged;

    public event Action<long, double>? BalanceChanged;

    public Action<long>? HeartbeatReceived { get; set; }

    public void Handle(JsonObject m)
    {
        Raise(() => MessageReceived?.Invoke(m), "MessageReceived");
        string? name = J.Str(m["name"]);
        var after = new List<Action>();
        try
        {
            lock (_s.Lock) Route(name, m, after);
        }
        catch (Exception ex)
        {
            _log($"skipped '{name}' message: {ex.Message}");
        }
        foreach (var a in after) Raise(a, name ?? "?");
    }

    private void Raise(Action a, string what)
    {
        try
        {
            a();
        }
        catch (Exception ex)
        {
            _log($"event handler for '{what}' threw: {ex.Message}");
        }
    }

    private string AssetName(JsonNode? activeId) =>
        _actives.NameOf((int)J.Long(activeId)) ?? throw new KeyNotFoundException($"unknown active id {activeId?.ToJsonString()}");

    private void SetAsync(long id, string name, JsonObject m) => State.GetOrAdd(_s.OrderAsync, id)[name] = m;

    /// <summary>The payload of a marginal-* raw event. The API session receives it flat (<c>{"id":…,"order_ids":[…]}</c>); the web traderoom
    /// receives it wrapped in one named object (<c>{"marginal_crypto_position_changed1": {…}}</c>). Both are accepted.</summary>
    internal static JsonObject? Inner(JsonNode? rawEvent) => rawEvent is not JsonObject o ? null
        : o.ContainsKey("id") ? o
        : o.Select(p => p.Value).OfType<JsonObject>().FirstOrDefault();

    private void Route(string? name, JsonObject m, List<Action> after)
    {
        JsonNode? msg = m["msg"];
        switch (name)
        {
            case "timeSync":
                _s.ServerTimestamp = J.Dbl(msg) / 1000.0;
                _s.TimeSynced = true;
                break;

            case "heartbeat":
            {
                long t = J.Long(msg);
                var reply = HeartbeatReceived;
                after.Add(() => reply?.Invoke(t));
                break;
            }

            case "balances":
                if (_s.BalanceId == null)
                {
                    foreach (var b in J.Arr(msg))
                    {
                        if (J.Long(b!["type"]) == 4) _s.BalanceId = J.Long(b["id"]);
                    }
                }
                _s.BalancesRaw = m;
                break;

            case "candle-generated":
            {
                var o = J.Obj(msg);
                string asset = AssetName(o["active_id"]);
                int size = (int)J.Long(o["size"]);
                long from = J.Long(o["from"]);
                int maxDict = _s.MaxDict[asset][size];
                var candle = Candle.FromJson(o);
                Helpers.DictQueueAdd(_s.CandleBuffer(asset, size), maxDict, from, candle);
                State.GetOrAdd(_s.CandleCheck, asset)[size] = true;
                after.Add(() => CandleReceived?.Invoke(asset, size, candle));
                break;
            }

            case "candles-generated":
            {
                var o = J.Obj(msg);
                string asset = AssetName(o["active_id"]);
                foreach (var (key, node) in J.Obj(o["candles"]))
                {
                    var v = J.Obj(node!.DeepClone());
                    int size = int.Parse(key, System.Globalization.CultureInfo.InvariantCulture);
                    v["active_id"] = o["active_id"]?.DeepClone();
                    v["at"] = o["at"]?.DeepClone();
                    v["ask"] = o["ask"]?.DeepClone();
                    v["bid"] = o["bid"]?.DeepClone();
                    v["close"] = o["value"]?.DeepClone();
                    v["size"] = size;
                    int maxDict = _s.MaxDict[asset][size];
                    var candle = Candle.FromJson(v);
                    Helpers.DictQueueAdd(_s.CandleBuffer(asset, size), maxDict, J.Long(v["from"]), candle);
                    after.Add(() => CandleReceived?.Invoke(asset, size, candle));
                }
                _s.AllSizeCheck[asset] = true;
                break;
            }

            case "commission-changed":
            {
                var o = J.Obj(msg);
                string type = J.Str(o["instrument_type"])!;
                string asset = AssetName(o["active_id"]);
                int value = (int)J.Dbl(o["commission"]!["value"]);
                State.GetOrAdd(State.GetOrAdd(_s.Commission, type), asset)[_s.ServerTimestamp] = value;
                break;
            }

            case "position-changed":
            {
                var o = J.Obj(msg);
                string? service = J.Str(m["microserviceName"]);
                string? source = J.Str(o["source"]);
                if ((service == "portfolio" && source == "digital-options") || source == "trading")
                {
                    SetAsync(J.Long(o["raw_event"]!["order_ids"]![0]), name, m);
                    SetAsync(J.Long(o["external_id"]), name, m);
                }
                else if (service == "portfolio" && source == "binary-options")
                {
                    SetAsync(J.Long(o["external_id"]), name, m);
                }
                else if (service == "portfolio" && source == "marginal-crypto")
                {
                    // Not in Python: perpetual crypto. Stored under the position id and the opening order id.
                    SetAsync(J.Long(o["external_id"]), name, m);
                    if (Inner(o["raw_event"])?["order_ids"] is JsonArray ids && ids.Count > 0) SetAsync(J.Long(ids[0]), name, m);
                }
                string? status = J.Str(o["status"]);
                if (source == "binary-options" && status == "open") _s.OptionOpened[J.Long(o["external_id"])] = m;
                else if (source == "digital-options" && status == "open") _s.DigitalOpened[J.Long(o["external_id"])] = m;
                after.Add(() => PositionChanged?.Invoke(m));
                break;
            }

            case "order-changed":
                if (J.Str(m["microserviceName"]) == "portfolio" && J.Str(J.Obj(msg)["source"]) == "digital-options")
                {
                    var raw = msg!["raw_event"]!;
                    SetAsync(J.Long(raw["position_id"]), name, m);
                    SetAsync(J.Long(raw["id"]), name, m);
                }
                else if (J.Str(m["microserviceName"]) == "portfolio" && J.Str(J.Obj(msg)["source"]) == "marginal-crypto"
                    && Inner(msg!["raw_event"]) is JsonObject order)
                {
                    SetAsync(J.Long(order["id"]), name, m);
                    if (!J.IsNull(order["position_id"])) SetAsync(J.Long(order["position_id"]), name, m);
                }
                break;

            case "option-opened":
            case "option-closed":
                SetAsync(J.Long(msg!["option_id"]), name, m);
                break;

            case "top-assets-updated":
                _s.TopAssetsUpdated[J.Str(msg!["instrument_type"])!] = msg["data"];
                break;

            case "traders-mood-changed":
                _s.TradersMood[(int)J.Long(msg!["asset_id"])] = J.Dbl(msg["value"]);
                break;

            case "client-price-generated":
            {
                var o = J.Obj(msg);
                string asset = AssetName(o["asset_id"]);
                int? minute = null;
                var ans = new Dictionary<string, StrikeQuote>();
                foreach (var row in J.Arr(o["prices"]))
                {
                    var call = J.Obj(row!["call"]);
                    var put = J.Obj(row["put"]);
                    double? callProfit = call.ContainsKey("ask") ? Helpers.DigitalProfitPercent(J.Dbl(call["ask"])) : null;
                    double? putProfit = put.ContainsKey("ask") ? Helpers.DigitalProfitPercent(J.Dbl(put["ask"])) : null;
                    string strike = J.Str(row["strike"])!;
                    if (strike == "SPT")
                    {
                        if (!Helpers.TryParseSpotMinutes(J.Str(call["symbol"]) ?? "", out int minutes)) return;
                        minute = minutes;
                        State.GetOrAdd(_s.DigitalProfit, asset)[minutes * 60] = callProfit;
                        State.GetOrAdd(_s.QuotesRaw, asset)[minutes * 60] = m;
                    }
                    ans[strike] = new StrikeQuote(J.Str(call["symbol"])!, callProfit, J.Str(put["symbol"])!, putProfit);
                }
                if (minute is not int period) throw new FormatException("client-price-generated without an SPT row");
                State.GetOrAdd(_s.Quotes, asset)[period * 60] = ans;
                break;
            }

            case "live-deal-binary-option-placed":
                AddLiveDeal(name, msg, "active_id", "option_type");
                break;

            case "live-deal-digital-option":
                AddLiveDeal(name, msg, "instrument_active_id", "expiration_type");
                break;

            case "live-deal":
                AddLiveDeal(name, msg, "instrument_active_id", "instrument_type");
                break;

            case "balance-changed":
            {
                var current = msg!["current_balance"]!;
                long id = J.Long(current["id"]);
                double amount = J.Dbl(current["amount"]);
                _s.Balance[id] = amount;
                after.Add(() => BalanceChanged?.Invoke(id, amount));
                break;
            }
        }
    }

    private void AddLiveDeal(string name, JsonNode? msg, string idKey, string typeKey)
    {
        var o = J.Obj(msg);
        var key = (name, AssetName(o[idKey]), J.Str(o[typeKey])!);
        if (!_s.LiveDeals.TryGetValue(key, out var buffer)) _s.LiveDeals[key] = buffer = new LiveDealBuffer();
        buffer.AddNewest(o);
    }
}
