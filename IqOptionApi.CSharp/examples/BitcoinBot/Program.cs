using System.Globalization;
using IqOptionApi;

// Trades Bitcoin in all four IQ Option products. PRACTICE balance unless --real is given.
// Credentials: export IQ_EMAIL=... ; export IQ_PASSWORD='...'
const string Usage = """
Usage (from IqOptionApi.CSharp):
  dotnet run --project examples/BitcoinBot -- status
  dotnet run --project examples/BitcoinBot -- blitz   call|put --amount 1 --seconds 60
  dotnet run --project examples/BitcoinBot -- binary  call|put --amount 1 --minutes 1
  dotnet run --project examples/BitcoinBot -- digital call|put --amount 1 --minutes 5
  dotnet run --project examples/BitcoinBot -- perp open buy|sell --isolated --margin 1 --leverage 200 [--close-after 30]
  dotnet run --project examples/BitcoinBot -- perp open buy|sell --cross --count 0.01 --leverage 200 [--close-after 30]
  dotnet run --project examples/BitcoinBot -- perp close <positionId>

  call = HIGHER, put = LOWER. binary: 1-5 minutes is turbo, longer is binary. digital: Bitcoin offers 5 or 15 minutes (forex also 1).
  --asset NAME changes the Bitcoin name for blitz/binary/digital (default BTCUSD); --active ID for perp (default 1979).
  --real trades with REAL money; without it the PRACTICE balance is used.
""";

string? email = Environment.GetEnvironmentVariable("IQ_EMAIL");
string? password = Environment.GetEnvironmentVariable("IQ_PASSWORD");
if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
{
    Console.WriteLine(Usage);
    return 0;
}
string command = args[0].ToLowerInvariant() + (args.Length > 1 && args[0].Equals("perp", StringComparison.OrdinalIgnoreCase) ? " " + args[1].ToLowerInvariant() : "");
if (command is not ("status" or "blitz" or "binary" or "digital" or "perp open" or "perp close"))
{
    Console.WriteLine("Unknown command.\n\n" + Usage);
    return 2;
}
if (string.IsNullOrEmpty(email) || string.IsNullOrEmpty(password))
{
    Console.WriteLine("Set IQ_EMAIL and IQ_PASSWORD first (they are never stored in a file).");
    return 1;
}

string Opt(string name, string fallback)
{
    int i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
}
double Num(string name, string fallback) => double.Parse(Opt(name, fallback), CultureInfo.InvariantCulture);
int Int(string name, string fallback) => int.Parse(Opt(name, fallback), CultureInfo.InvariantCulture);
bool Has(string name) => args.Contains(name);
string Arg(int index) => index < args.Length ? args[index].ToLowerInvariant() : "";

string asset = Opt("--asset", "BTCUSD");
int perpActive = Int("--active", "1979");

await using var api = new IqOption(email, password, new IqOptionOptions { Log = line => Console.WriteLine("  [log] " + line) });
api.Disconnected += reason => Console.WriteLine("Disconnected: " + reason);
api.Reconnected += () => Console.WriteLine("Reconnected.");

Console.WriteLine("Connecting...");
var (ok, reason) = await api.ConnectAsync();
if (!ok)
{
    Console.WriteLine("Login failed: " + reason);
    return 1;
}
if (Has("--real"))
{
    await api.ChangeBalanceAsync(BalanceMode.Real);
    Console.WriteLine("*** REAL MONEY ***");
}
Console.WriteLine($"Balance: {await api.GetBalanceModeAsync()}  {await api.GetBalanceAsync()} {await api.GetCurrencyAsync()}\n");

try
{
    return Arg(0) switch
    {
        "status" => await Status(),
        "blitz" => await Blitz(),
        "binary" => await Binary(),
        "digital" => await Digital(),
        "perp" when Arg(1) == "open" => await PerpOpen(),
        "perp" when Arg(1) == "close" => await PerpClose(),
        _ => Fail("Unknown command.\n\n" + Usage),
    };
}
catch (Exception ex) when (ex is ArgumentException or KeyNotFoundException or FormatException)
{
    return Fail(ex.Message);
}

static int Fail(string message)
{
    Console.WriteLine(message);
    return 2;
}

Direction Side(int index) => Arg(index) switch
{
    "call" or "higher" or "up" => Direction.Call,
    "put" or "lower" or "down" => Direction.Put,
    _ => throw new ArgumentException($"Say call (HIGHER) or put (LOWER), not '{Arg(index)}'."),
};

async Task<int> Status()
{
    var blitz = await api.GetAllProfitBlitzAsync();
    var options = await api.GetAllProfitAsync();
    var digital = await api.GetDigitalUnderlyingListAsync();
    string Payout(Dictionary<string, Dictionary<string, double>> table, string type) =>
        table.TryGetValue(asset, out var row) && row.TryGetValue(type, out double f) ? $"open, payout {f * 100:0}%" : "closed or not offered now";
    // Digital lists its own names (some end in "-op"); buying only needs the active id, so match on that.
    var underlying = (digital?["underlying"] as System.Text.Json.Nodes.JsonArray)?.OfType<System.Text.Json.Nodes.JsonObject>().ToList() ?? new();
    int? activeId = api.GetAllActives().TryGetValue(asset, out int id) ? id : null;
    var match = underlying.FirstOrDefault(u => long.TryParse(u["active_id"]?.ToString(), out long v) && v == activeId);
    string digitalText = digital == null ? "unknown (no answer)"
        : match != null ? $"listed as {match["underlying"] ?? match["name"]} (active {activeId})"
        : "not listed; Bitcoin-like names: " + string.Join(", ", underlying
            .Where(u => (u["underlying"] ?? u["name"])?.ToString().Contains("BTC", StringComparison.OrdinalIgnoreCase) == true)
            .Select(u => $"{u["underlying"] ?? u["name"]} (active {u["active_id"]})").DefaultIfEmpty("none"));
    Console.WriteLine($"Bitcoin ({asset}):");
    Console.WriteLine($"  Blitz    {Payout(blitz, "blitz")}");
    Console.WriteLine($"  Turbo    {Payout(options, "turbo")}   (binary command, 1-5 min)");
    Console.WriteLine($"  Binary   {Payout(options, "binary")}   (binary command, longer expiries)");
    Console.WriteLine($"  Digital  {digitalText}");
    Console.WriteLine($"  Perp     active {perpActive} (Bitcoin-PerpFuture); open/close with the perp command");
    return 0;
}

async Task<int> Blitz()
{
    var direction = Side(1);
    double amount = Num("--amount", "1");
    int seconds = Int("--seconds", "60");
    Console.WriteLine($"Blitz {direction} ${amount} on {asset} for {seconds}s");
    var (bought, result, error) = await api.BuyBlitzAsync(amount, asset, direction, seconds);
    if (!bought) return Fail("Buy failed: " + error);
    long id = result!["id"]!.GetValue<long>();
    Console.WriteLine($"Bought, id {id}. Waiting for the result...");
    return await OptionResult(api.CheckWinAsync, id, seconds);
}

async Task<int> Binary()
{
    var direction = Side(1);
    double amount = Num("--amount", "1");
    int minutes = Int("--minutes", "1");
    Console.WriteLine($"{(minutes <= 5 ? "Turbo" : "Binary")} {direction} ${amount} on {asset}, {minutes} min");
    var (bought, id, error) = await api.BuyAsync(amount, asset, direction, minutes);
    if (!bought) return Fail("Buy failed: " + error);
    Console.WriteLine($"Bought, id {id}. Waiting for the result...");
    return await OptionResult(api.CheckWinAsync, id, minutes * 60);
}

async Task<int> Digital()
{
    var direction = Side(1);
    double amount = Num("--amount", "1");
    int minutes = Int("--minutes", "5");
    if (minutes is not (1 or 5 or 15)) return Fail("Digital minutes must be 1, 5 or 15 (Bitcoin: 5 or 15).");
    Console.WriteLine($"Digital {direction} ${amount} on {asset}, {minutes} min");
    var (bought, id, error) = await api.BuyDigitalSpotAsync(asset, amount, direction, minutes);
    if (!bought)
        return Fail("Buy failed: " + error + (id != 0 ? $" (order {id} was placed)" : "")
            + (error == "invalid instrument" ? $"\nThere is no {minutes}-minute digital for {asset} right now. Bitcoin digital offers 5 and 15 minutes." : ""));
    Console.WriteLine($"Bought, position {id}. Waiting for the result...");
    return await OptionResult(api.CheckWinDigitalAsync, id, minutes * 60);
}

async Task<int> OptionResult(Func<long, CancellationToken, Task<(TradeResult Result, double Profit)>> check, long id, int durationSeconds)
{
    // Expiry can be up to one period after the buy, plus time for the server to settle.
    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(durationSeconds * 2 + 90));
    try
    {
        var (result, profit) = await check(id, cts.Token);
        Console.WriteLine($"Result: {result} {profit:+0.00;-0.00}");
        return 0;
    }
    catch (OperationCanceledException)
    {
        return Fail($"No result yet for {id}; check the trading history in the traderoom.");
    }
}

async Task<int> PerpOpen()
{
    string side = Arg(2) switch
    {
        "buy" or "long" => "buy",
        "sell" or "short" => "sell",
        _ => throw new ArgumentException($"Say buy or sell, not '{Arg(2)}'."),
    };
    if (Has("--isolated") == Has("--cross")) return Fail("Choose one: --isolated (sized by --margin $) or --cross (sized by --count coins).");
    int leverage = Int("--leverage", "200");
    (bool Success, long? OrderId, long? PositionId, string? Error) r;
    if (Has("--isolated"))
    {
        double margin = Num("--margin", "1");
        Console.WriteLine($"Perp ISOLATED {side} ${margin} at x{leverage} (active {perpActive})");
        r = await api.OpenPerpetualIsolatedAsync(perpActive, side, margin, leverage);
    }
    else
    {
        double count = Num("--count", "0.01");
        Console.WriteLine($"Perp CROSS {side} {count} coins at x{leverage} (active {perpActive})");
        r = await api.OpenPerpetualAsync(perpActive, side, count, leverage);
    }
    if (!r.Success)
        return Fail("Open failed: " + r.Error + (r.OrderId is long o ? $" (order {o} was placed; check open positions in the traderoom)" : ""));
    long positionId = r.PositionId!.Value;
    Console.WriteLine($"Opened. Position id: {positionId}");

    int closeAfter = Int("--close-after", "0");
    if (closeAfter <= 0)
    {
        Console.WriteLine($"It stays open. Close it with:  dotnet run --project examples/BitcoinBot -- perp close {positionId}");
        return 0;
    }
    Console.WriteLine($"Closing after {closeAfter}s...");
    await Task.Delay(TimeSpan.FromSeconds(closeAfter));
    return await CloseAndReport(positionId);
}

async Task<int> PerpClose()
{
    if (!long.TryParse(Arg(2), out long positionId)) return Fail("Give the position id: perp close <positionId>");
    return await CloseAndReport(positionId);
}

async Task<int> CloseAndReport(long positionId)
{
    var result = api.CheckWinPerpetualAsync(positionId);
    if (!await api.ClosePerpetualAsync(positionId))
        return Fail($"Close refused for position {positionId} (wrong id, already closed, or not your balance?).");
    Console.WriteLine("Closed.");
    try
    {
        var (outcome, profit) = await result.WaitAsync(TimeSpan.FromSeconds(30));
        Console.WriteLine($"Result: {outcome} {profit:+0.00;-0.00}");
    }
    catch (TimeoutException)
    {
        Console.WriteLine("Closed, but the final profit did not arrive within 30s; see the trading history.");
    }
    return 0;
}
