using System.Diagnostics;
using System.Globalization;
using BtcTickTrader;
using mtapi.mt5;

// Streams Bitcoin prices from MetaTrader 5 into a shared-memory ring that BitcoinTickBot reads.
// It connects to MT5 the same way the TradoWix price feeder does (mtapi.mt5: MT5API with account, password and broker
// server, Connect, Subscribe, OnQuote) and only delivers prices: every quote MT5 sends for the symbol is published as
// it is. The bot does all the maths (differences, spikes) and the trading.
const string Usage = """
Usage (from the BtcTickTrader folder):
  put the MT5 account in BtcTickTrader/.env (MT5Settings__AccountNumber, MT5Settings__Password, MT5Settings__Server)
  dotnet run -c Release --project PriceFeeder -- [options]

  --symbol BTCUSD           MT5 symbol, as the broker names it
  --price bid               bid (what the TradoWix feeder uses) | mid (bid/ask middle)
  --ring PATH               shared-memory file (default: /dev/shm or temp folder, btc-ticks.ring)
  --print                   print every quote with its arrival time (ms) as it comes in
""";

if (args.Contains("-h") || args.Contains("--help"))
{
    Console.WriteLine(Usage);
    return 0;
}
string Opt(string name, string fallback)
{
    int i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
}

string? envFile = DotEnv.Load();
string symbol = Opt("--symbol", "BTCUSD");
string priceKind = Opt("--price", "bid").ToLowerInvariant();
if (priceKind is not ("bid" or "mid"))
{
    Console.WriteLine("Unknown --price.\n\n" + Usage);
    return 2;
}
string ringPath = Opt("--ring", TickRing.DefaultPath);
bool print = args.Contains("--print");

// Same settings (and environment variable names) as the TradoWix feeder's MT5Settings.
string accountText = Environment.GetEnvironmentVariable("MT5Settings__AccountNumber") ?? "";
string password = Environment.GetEnvironmentVariable("MT5Settings__Password") ?? "";
string server = Environment.GetEnvironmentVariable("MT5Settings__Server") ?? "";
if (!ulong.TryParse(accountText, NumberStyles.None, CultureInfo.InvariantCulture, out ulong account) || account == 0
    || string.IsNullOrWhiteSpace(password) || string.IsNullOrWhiteSpace(server))
{
    Console.WriteLine((envFile == null ? "No .env found. " : $"The MT5 account in {envFile} is incomplete. ") +
        "Set MT5Settings__AccountNumber, MT5Settings__Password and MT5Settings__Server (see .env.example).");
    return 1;
}

using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    stop.Cancel();
};
using var sigterm = System.Runtime.InteropServices.PosixSignalRegistration.Create(System.Runtime.InteropServices.PosixSignal.SIGTERM, ctx =>
{
    ctx.Cancel = true;
    stop.Cancel();
});

TickRing ring;
try
{
    ring = TickRing.CreateWriter(ringPath, symbol);
}
catch (IOException ex)
{
    Console.WriteLine(ex.Message);
    return 1;
}
using var ownedRing = ring;
Console.WriteLine($"PriceFeeder: MT5 account {account} on {server}, {symbol} {priceKind}");
Console.WriteLine($"Ring: {ringPath}  (session {ring.SessionId}, {TickRing.Capacity} slots). Ctrl+C to stop.");

var stats = new FeedStats();
using var feed = new Mt5Feed(account, password, server, symbol, priceKind == "mid", ring, stats, print);
var statsTask = PrintStatsAsync(stats, ring, stop.Token);
TimeSpan[] backoff = { TimeSpan.Zero, TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(30) };
int attempt = 0;

while (!stop.IsCancellationRequested)
{
    try
    {
        await Task.Delay(backoff[Math.Min(attempt, backoff.Length - 1)], stop.Token);
        attempt++;
        Console.WriteLine($"{Now()} connecting to MT5...");
        // MT5API.Connect blocks, so it runs on the thread pool (as in the TradoWix feeder).
        string? error = await Task.Run(feed.Connect, stop.Token);
        if (error != null)
        {
            Console.WriteLine($"{Now()} MT5 connect failed: {error}; retrying in {backoff[Math.Min(attempt, backoff.Length - 1)].TotalSeconds:0} s");
            feed.Disconnect();
            continue;
        }
        attempt = 0;
        Console.WriteLine($"{Now()} connected, subscribed to {feed.Symbol}");

        // Health check: MT5API.Connected turns false when the socket closes or the server goes quiet past its timeout.
        bool staleWarned = false;
        while (!stop.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(5), stop.Token);
            if (!feed.IsConnected)
            {
                Console.WriteLine($"{Now()} MT5 connection lost; reconnecting");
                break;
            }
            long last = ring.LastMessageTimestamp;
            bool stale = last != 0 && Stopwatch.GetElapsedTime(last) > TimeSpan.FromSeconds(120);
            if (stale && !staleWarned) Console.WriteLine($"{Now()} no {feed.Symbol} quotes for 120 s (market may be closed)");
            staleWarned = stale;
        }
        feed.Disconnect();
    }
    catch (OperationCanceledException) when (stop.IsCancellationRequested)
    {
        break;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"{Now()} MT5 error: {ex.Message}; reconnecting");
        feed.Disconnect();
    }
}

feed.Disconnect();
await statsTask;
Console.WriteLine("Stopped.");
return 0;

static string Now() => DateTime.Now.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);

static async Task PrintStatsAsync(FeedStats stats, TickRing ring, CancellationToken ct)
{
    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
    try
    {
        while (await timer.WaitForNextTickAsync(ct))
        {
            var s = stats.TakeSnapshot();
            Console.WriteLine($"{Now()} BTC {s.LastPrice:0.00}  bid {s.LastBid:0.00} ask {s.LastAsk:0.00}  {s.Prices} quotes/s  published #{ring.Head}");
        }
    }
    catch (OperationCanceledException)
    {
    }
}

/// <summary>
/// One MT5 connection that publishes the symbol's quotes into the ring. Connect, subscribe and teardown follow the
/// TradoWix feeder's MT5QuoteService.
/// </summary>
internal sealed class Mt5Feed : IDisposable
{
    private readonly ulong _account;
    private readonly string _password;
    private readonly string _server;
    private readonly string _wanted;
    private readonly bool _mid;
    private readonly TickRing _ring;
    private readonly FeedStats _stats;
    private readonly bool _print;
    private readonly object _apiLock = new();
    private readonly object _publishLock = new(); // the ring has one writer; a late quote from an old connection must not race the new one
    private volatile MT5API? _api;
    private volatile string? _symbol;
    private bool _firstAfterConnect;
    private long _quoteNumber;

    public Mt5Feed(ulong account, string password, string server, string symbol, bool mid, TickRing ring, FeedStats stats, bool print)
    {
        _account = account;
        _password = password;
        _server = server;
        _wanted = symbol;
        _mid = mid;
        _ring = ring;
        _stats = stats;
        _print = print;
    }

    /// <summary>The symbol as the broker spells it, once connected.</summary>
    public string Symbol => _symbol ?? _wanted;

    /// <summary>True while MT5 reports the connection alive.</summary>
    public bool IsConnected
    {
        get
        {
            lock (_apiLock) return _api?.Connected ?? false;
        }
    }

    /// <summary>Connects and subscribes (blocking). Returns null on success, otherwise why it failed.</summary>
    public string? Connect()
    {
        lock (_apiLock)
        {
            var api = new MT5API(_account, _password, _server);
            _api = api;
            try
            {
                api.Connect();
            }
            catch (Exception ex)
            {
                return ex.Message;
            }
            if (!api.Connected) return "connection failed - unknown reason";

            var available = api.Symbols?.Infos?.Keys?.ToList() ?? new List<string>();
            string? actual = available.FirstOrDefault(s => s.Equals(_wanted, StringComparison.OrdinalIgnoreCase));
            if (actual == null)
            {
                var similar = available.Where(s => s.Contains("BTC", StringComparison.OrdinalIgnoreCase)).Take(10).ToList();
                return $"symbol {_wanted} is not offered by {_server}" + (similar.Count > 0 ? $"; try --symbol with one of: {string.Join(", ", similar)}" : "");
            }

            lock (_publishLock)
            {
                _symbol = actual;
                _firstAfterConnect = true; // tells the bot that prices before this one are from before an outage
                _quoteNumber = 0;
            }
            api.OnQuote += OnQuote;
            if (!api.Subscriptions().Contains(actual)) api.Subscribe(actual);
            return null;
        }
    }

    /// <summary>Closes the connection. Always calls MT5API.Disconnect, which is the only teardown of its socket and thread.</summary>
    public void Disconnect()
    {
        lock (_apiLock)
        {
            var api = _api;
            if (api == null) return;
            _api = null;
            api.OnQuote -= OnQuote;
            try
            {
                api.Disconnect();
            }
            catch (NullReferenceException)
            {
                // mtapi throws this for a client whose Connect never completed: nothing to release.
            }
            catch (Exception ex)
            {
                Console.WriteLine("MT5 disconnect error: " + ex.Message);
            }
        }
    }

    public void Dispose() => Disconnect();

    // Runs on mtapi's thread for every quote of every subscribed symbol.
    private void OnQuote(MT5API api, Quote quote)
    {
        long received = Stopwatch.GetTimestamp();
        if (!ReferenceEquals(api, _api) || quote.Bid <= 0 || !string.Equals(quote.Symbol, _symbol, StringComparison.Ordinal)) return;
        double price = _mid && quote.Ask > 0 ? (quote.Bid + quote.Ask) / 2 : quote.Bid;
        long n;
        bool gap;
        lock (_publishLock)
        {
            gap = _firstAfterConnect;
            _ring.MarkMessage(received);
            n = _ring.Publish(new Tick
            {
                Price = price,
                ExchangeTimeMs = 0,
                ReceivedTimestamp = received,
                SourceId = ++_quoteNumber,
                Quantity = quote.Volume,
                Flags = gap ? TickFlags.Gap : TickFlags.None,
            });
            _firstAfterConnect = false;
        }
        _stats.Record(price, quote.Bid, quote.Ask);
        if (_print) Console.WriteLine($"{DateTime.Now.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture)} #{n} {price:0.00}  bid {quote.Bid:0.00} ask {quote.Ask:0.00}{(gap ? " (first after connect)" : "")}");
    }
}

/// <summary>Counters the quote handler updates and the stats loop drains every second.</summary>
internal sealed class FeedStats
{
    private readonly object _lock = new();
    private Snapshot _s;

    /// <summary>Counts one published price.</summary>
    public void Record(double price, double bid, double ask)
    {
        lock (_lock)
        {
            _s.Prices++;
            _s.LastPrice = price;
            _s.LastBid = bid;
            _s.LastAsk = ask;
        }
    }

    /// <summary>Returns the counters since the last call and starts again (the last prices are kept).</summary>
    public Snapshot TakeSnapshot()
    {
        lock (_lock)
        {
            var s = _s;
            _s = new Snapshot { LastPrice = s.LastPrice, LastBid = s.LastBid, LastAsk = s.LastAsk };
            return s;
        }
    }

    /// <summary>Counters for one stats period.</summary>
    public struct Snapshot
    {
        public long Prices;
        public double LastPrice;
        public double LastBid;
        public double LastAsk;
    }
}
