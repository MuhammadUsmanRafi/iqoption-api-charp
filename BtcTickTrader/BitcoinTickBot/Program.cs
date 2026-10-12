using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading.Channels;
using BtcTickTrader;
using IqOptionApi;

// Reads MT5 Bitcoin prices from PriceFeeder's shared-memory ring, works out whether the newest price is a spike
// (SpikeDetector) and, if it is, buys a blitz or turbo option on IQ Option. Logs in with an SSID. PRACTICE balance unless --real.
const string Usage = """
Usage (from the BtcTickTrader folder; start PriceFeeder first):
  put IQ_SSID=your-ssid in BtcTickTrader/.env (copy .env.example)
  dotnet run -c Release --project BitcoinTickBot -- [options]

  --threshold 5       a spike is a move of at least this many USD
  --window-ms 0       0 = compare with the previous price (tick-to-tick difference);
                      N = compare with the lowest/highest price of the last N ms (catches fast multi-step moves)
  --mode blitz        blitz | turbo
  --seconds 120       blitz expiry in seconds; must be one IQ Option offers for the asset
                      (BTCUSD in Oct 2026: 120, 180, 300, 600, 900; the bot prints the list if not)
  --minutes 1         turbo expiry, 1-5
  --amount 1          stake per trade
  --reverse           trade against the move (default: price up = CALL, down = PUT)
  --cooldown 5        seconds between trades
  --max-open 1        open trades at the same time
  --max-trades 0      stop trading after this many trades (0 = no limit)
  --max-loss 0        stop trading when the session loss reaches this (0 = off)
  --max-age-ms 250    ignore a signal whose tick is older than this when the bot sees it
  --asset BTCUSD      IQ Option asset name
  --ssid SSID         instead of IQ_SSID from .env or the environment (shows up in shell history)
  --real              trade with REAL money (default PRACTICE)
  --dry-run           only print signals; no login, no trades
  --low-cpu           sleep 1 ms when idle instead of spinning on a core (adds up to ~1 ms)
  --ring PATH         shared-memory file (must match PriceFeeder's)
  --verbose           also print the IQ Option library's log
""";

if (args.Contains("-h") || args.Contains("--help"))
{
    Console.WriteLine(Usage);
    return 0;
}

string? envFile = DotEnv.Load();
Settings s;
try
{
    s = Settings.Parse(args);
}
catch (Exception ex) when (ex is ArgumentException or FormatException)
{
    Console.WriteLine(ex.Message + "\n\n" + Usage);
    return 2;
}

var log = new ConsoleLog();
using var stop = new CancellationTokenSource();
Console.CancelKeyPress += (sender, e) =>
{
    if (stop.IsCancellationRequested) return; // second Ctrl+C: quit now
    e.Cancel = true;
    stop.Cancel();
};
using var sigterm = System.Runtime.InteropServices.PosixSignalRegistration.Create(System.Runtime.InteropServices.PosixSignal.SIGTERM, ctx =>
{
    ctx.Cancel = true;
    stop.Cancel();
});

IqOption? api = null;
try
{
    if (!s.DryRun)
    {
        if (string.IsNullOrWhiteSpace(s.Ssid))
        {
            Console.WriteLine(envFile == null
                ? "No SSID: copy .env.example to .env in the BtcTickTrader folder and put your ssid in IQ_SSID (or use --dry-run)."
                : $"IQ_SSID is empty in {envFile}; put your ssid there (or use --dry-run).");
            return 1;
        }
        // No e-mail or password: the SSID is the login. AutoReconnect reuses it after a drop.
        api = new IqOption("", "", new IqOptionOptions { Ssid = s.Ssid, Log = s.Verbose ? line => log.Write("[iq] " + line) : null });
        api.Disconnected += reason => log.Write("IQ Option disconnected: " + reason);
        api.Reconnected += () => log.Write("IQ Option reconnected.");
        log.Write($"Connecting to IQ Option with the SSID from {s.SsidSource(envFile)}...");
        var (ok, reason) = await api.ConnectAsync(stop.Token);
        if (!ok)
        {
            log.Write("The SSID was not accepted (expired or wrong). Log in on iqoption.com and copy a fresh 'ssid' cookie.");
            if (s.Verbose) log.Write("Server said: " + reason);
            await log.CompleteAsync();
            return 1;
        }
        if (s.Real)
        {
            await api.ChangeBalanceAsync(BalanceMode.Real, stop.Token);
            log.Write("*** REAL MONEY ***");
        }
        log.Write($"Balance: {await api.GetBalanceModeAsync(stop.Token)} {await api.GetBalanceAsync(stop.Token)} {await api.GetCurrencyAsync(stop.Token)}");
    }

    var stats = new BotStats();
    var trader = new Trader(api, s, stats, log);
    await trader.RefreshPayoutAsync(stop.Token);
    log.Write(s.Describe());

    var reader = new Thread(() => TickReader.Run(s, trader, stats, log, stop.Token))
    {
        IsBackground = true,
        Name = "tick-reader",
        Priority = ThreadPriority.Highest,
    };
    reader.Start();
    var background = Task.WhenAll(trader.RefreshPayoutLoopAsync(stop.Token), PrintStatsAsync(stats, trader, log, stop.Token));

    try
    {
        await Task.Delay(Timeout.Infinite, stop.Token);
    }
    catch (OperationCanceledException)
    {
    }
    reader.Join();
    await background;
    if (trader.OpenTrades > 0)
    {
        log.Write($"Stopping: waiting for {trader.OpenTrades} open trade(s) to finish. Ctrl+C again to quit now.");
        await trader.WhenIdleAsync();
    }
    log.Write("Stopped. " + stats.Summary(trader));
}
catch (OperationCanceledException) when (stop.IsCancellationRequested)
{
    log.Write("Stopped.");
}
finally
{
    if (api != null) await api.DisposeAsync();
    await log.CompleteAsync();
}
return 0;

static async Task PrintStatsAsync(BotStats stats, Trader trader, ConsoleLog log, CancellationToken ct)
{
    using var timer = new PeriodicTimer(TimeSpan.FromSeconds(10));
    try
    {
        while (await timer.WaitForNextTickAsync(ct)) log.Write(stats.TakePeriod() + " | " + stats.Summary(trader));
    }
    catch (OperationCanceledException)
    {
    }
}

/// <summary>Command-line settings.</summary>
internal sealed record Settings(
    double Threshold, int WindowMs, bool Blitz, int Seconds, int Minutes, double Amount, bool Reverse,
    TimeSpan Cooldown, int MaxOpen, int MaxTrades, double MaxLoss, TimeSpan MaxAge, string Asset,
    string? Ssid, bool Real, bool DryRun, bool LowCpu, string RingPath, bool Verbose)
{
    /// <summary>Where the SSID came from, for the log (never the SSID itself).</summary>
    public string SsidSource(string? envFile) =>
        Environment.GetCommandLineArgs().Contains("--ssid") ? "--ssid" : DotEnv.FromFile.Contains("IQ_SSID") ? envFile! : "the IQ_SSID environment variable";

    /// <summary>Expiry of one trade.</summary>
    public TimeSpan Expiry => Blitz ? TimeSpan.FromSeconds(Seconds) : TimeSpan.FromMinutes(Minutes);

    /// <summary>Parses and checks the arguments; throws <see cref="ArgumentException"/> on bad input.</summary>
    public static Settings Parse(string[] args)
    {
        string Opt(string name, string fallback)
        {
            int i = Array.IndexOf(args, name);
            return i >= 0 && i + 1 < args.Length ? args[i + 1] : fallback;
        }
        double Num(string name, string fallback) => double.Parse(Opt(name, fallback), CultureInfo.InvariantCulture);
        int Int(string name, string fallback) => int.Parse(Opt(name, fallback), CultureInfo.InvariantCulture);

        string mode = Opt("--mode", "blitz").ToLowerInvariant();
        var s = new Settings(
            Threshold: Num("--threshold", "5"),
            WindowMs: Int("--window-ms", "0"),
            Blitz: mode == "blitz",
            Seconds: Int("--seconds", "120"),
            Minutes: Int("--minutes", "1"),
            Amount: Num("--amount", "1"),
            Reverse: args.Contains("--reverse"),
            Cooldown: TimeSpan.FromSeconds(Num("--cooldown", "5")),
            MaxOpen: Int("--max-open", "1"),
            MaxTrades: Int("--max-trades", "0"),
            MaxLoss: Num("--max-loss", "0"),
            MaxAge: TimeSpan.FromMilliseconds(Num("--max-age-ms", "250")),
            Asset: Opt("--asset", "BTCUSD"),
            Ssid: Opt("--ssid", Environment.GetEnvironmentVariable("IQ_SSID") ?? ""),
            Real: args.Contains("--real"),
            DryRun: args.Contains("--dry-run"),
            LowCpu: args.Contains("--low-cpu"),
            RingPath: Opt("--ring", TickRing.DefaultPath),
            Verbose: args.Contains("--verbose"));

        if (mode is not ("blitz" or "turbo")) throw new ArgumentException("--mode must be blitz or turbo.");
        if (s.Threshold <= 0) throw new ArgumentException("--threshold must be above 0.");
        if (s.WindowMs is < 0 or > 60_000) throw new ArgumentException("--window-ms must be 0-60000.");
        if (s.Blitz && s.Seconds is < 60 or > 900) throw new ArgumentException("--seconds must be 60-900 for Bitcoin blitz.");
        if (!s.Blitz && s.Minutes is < 1 or > 5) throw new ArgumentException("--minutes must be 1-5 for turbo.");
        if (s.Amount <= 0) throw new ArgumentException("--amount must be above 0.");
        if (s.MaxOpen < 1) throw new ArgumentException("--max-open must be at least 1.");
        return s;
    }

    /// <summary>One line describing the strategy.</summary>
    public string Describe() =>
        $"Strategy: {Asset} {(Blitz ? $"blitz {Seconds}s" : $"turbo {Minutes}m")}, ${Amount} per trade on a spike of >= {Threshold} USD " +
        (WindowMs == 0 ? "from the previous price" : $"within {WindowMs} ms") + $", {(Reverse ? "against" : "with")} the move; cooldown {Cooldown.TotalSeconds:0.#}s, max open {MaxOpen}" +
        (MaxTrades > 0 ? $", max trades {MaxTrades}" : "") + (MaxLoss > 0 ? $", stop at -{MaxLoss}" : "") +
        (DryRun ? "  [DRY RUN: no trades]" : Real ? "  [REAL]" : "  [PRACTICE]");
}

/// <summary>
/// The hot loop. It runs on its own thread and polls the shared-memory ring, so a new tick is seen within
/// microseconds of the feeder writing it, without a context switch or a wake-up from the OS.
/// </summary>
internal static class TickReader
{
    public static void Run(Settings s, Trader trader, BotStats stats, ConsoleLog log, CancellationToken ct)
    {
        TickRing? ring = null;
        var spikes = new SpikeDetector(s.WindowMs);
        long next = 0, session = 0, idleSince = Stopwatch.GetTimestamp();
        bool warnedWaiting = false, warnedSilent = false;
        var spinner = new SpinWait();
        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (ring == null || !ring.IsValid)
                {
                    ring?.Dispose();
                    ring = TickRing.TryOpenReader(s.RingPath);
                    if (ring == null)
                    {
                        if (!warnedWaiting) log.Write($"Waiting for PriceFeeder ({s.RingPath})...");
                        warnedWaiting = true;
                        ct.WaitHandle.WaitOne(200);
                        continue;
                    }
                }
                if (ring.SessionId != session)
                {
                    // New feeder run (or first attach): start at the newest tick; never trade on old ones.
                    session = ring.SessionId;
                    next = ring.Head + 1;
                    spikes.Reset();
                    warnedWaiting = false;
                    log.Write($"Attached to PriceFeeder pid {ring.WriterPid} ({ring.Symbol}), starting after tick #{next - 1}.");
                }

                var status = ring.TryRead(next, out var tick);
                if (status == ReadStatus.NotYet)
                {
                    if (s.LowCpu) Thread.Sleep(1);
                    else spinner.SpinOnce(sleep1Threshold: -1);
                    if (Stopwatch.GetElapsedTime(idleSince) > TimeSpan.FromSeconds(2))
                    {
                        idleSince = Stopwatch.GetTimestamp();
                        long last = ring.LastMessageTimestamp;
                        if (last != 0 && Stopwatch.GetElapsedTime(last) > TimeSpan.FromSeconds(15))
                        {
                            // Feeder stopped or lost MT5. Reopen in case the ring file was replaced.
                            if (!warnedSilent) log.Write("PriceFeeder has had no data for 15 s; is it running?");
                            warnedSilent = true;
                            ring.Dispose();
                            ring = null;
                        }
                    }
                    continue;
                }
                if (status == ReadStatus.Lost)
                {
                    long resume = Math.Max(next + 1, ring.Head - TickRing.Capacity + 1);
                    stats.Lost(resume - next);
                    next = resume;
                    spikes.Reset();
                    continue;
                }

                spinner.Reset();
                idleSince = Stopwatch.GetTimestamp();
                warnedSilent = false;
                next++;
                long ageTicks = idleSince - tick.ReceivedTimestamp;

                if ((tick.Flags & TickFlags.Gap) != 0) spikes.Reset(); // the feeder reconnected: don't measure across the outage
                double move = spikes.Update(tick.Price, tick.ReceivedTimestamp);
                stats.Tick(ageTicks, move);

                if (Math.Abs(move) < s.Threshold) continue;
                if (ageTicks > s.MaxAge.TotalSeconds * Stopwatch.Frequency)
                {
                    stats.Stale();
                    continue;
                }
                var direction = (move > 0) != s.Reverse ? Direction.Call : Direction.Put;
                trader.OnSignal(direction, tick.Price, move, tick.ReceivedTimestamp);
            }
        }
        catch (Exception ex)
        {
            log.Write("Tick reader stopped by an error: " + ex);
        }
        finally
        {
            ring?.Dispose();
        }
    }
}

/// <summary>
/// All of the price maths: how far the newest price moved. The feeder only delivers prices; this decides what a
/// move is. Positive = up (CALL), negative = down (PUT), 0 = no move or not enough history yet.
/// </summary>
internal sealed class SpikeDetector
{
    private const int Capacity = 1 << 16; // power of two
    private readonly long _window;          // Stopwatch ticks; 0 = previous price only
    private readonly long[] _times = new long[Capacity];
    private readonly double[] _prices = new double[Capacity];
    private int _oldest, _count;
    private double _previous = double.NaN;

    public SpikeDetector(int windowMs) => _window = windowMs * Stopwatch.Frequency / 1000;

    /// <summary>Forgets every price, e.g. after an outage, so no move is measured across it.</summary>
    public void Reset()
    {
        _count = 0;
        _previous = double.NaN;
    }

    /// <summary>Adds a price seen at <paramref name="timestamp"/> (Stopwatch) and returns its move.</summary>
    public double Update(double price, long timestamp)
    {
        if (_window == 0)
        {
            // Tick-to-tick: the difference from the previous price.
            double diff = double.IsNaN(_previous) ? 0 : price - _previous;
            _previous = price;
            return diff;
        }

        // Window: the move from the lowest (up) or highest (down) price of the last window.
        while (_count > 0 && timestamp - _times[_oldest] > _window)
        {
            _oldest = (_oldest + 1) & (Capacity - 1);
            _count--;
        }
        double min = price, max = price;
        for (int i = 0, j = _oldest; i < _count; i++, j = (j + 1) & (Capacity - 1))
        {
            min = Math.Min(min, _prices[j]);
            max = Math.Max(max, _prices[j]);
        }
        int newest = (_oldest + _count - 1) & (Capacity - 1);
        if (_count > 0 && _prices[newest] == price)
        {
            _times[newest] = timestamp; // most messages repeat the price; keep one entry with the latest time
        }
        else
        {
            if (_count == Capacity)
            {
                _oldest = (_oldest + 1) & (Capacity - 1);
                _count--;
            }
            int slot = (_oldest + _count) & (Capacity - 1);
            _times[slot] = timestamp;
            _prices[slot] = price;
            _count++;
        }
        double up = price - min, down = price - max;
        return up >= -down ? up : down;
    }
}

/// <summary>Decides whether a signal becomes a trade and runs each trade until its result.</summary>
internal sealed class Trader
{
    private readonly IqOption? _api;
    private readonly Settings _s;
    private readonly BotStats _stats;
    private readonly ConsoleLog _log;
    private readonly ConcurrentDictionary<int, Task> _running = new();
    private readonly Dictionary<string, long> _lastSkipLog = new();
    private long _lastTrade;
    private long _pauseUntil; // Stopwatch timestamp; buys wait until then after a failed buy
    private int _attempts;
    private int _placed;
    private int _open;
    private int _payout = -1; // blitz payout percent, turbo payout percent; 0 = closed, -1 = unknown

    public Trader(IqOption? api, Settings s, BotStats stats, ConsoleLog log)
    {
        _api = api;
        _s = s;
        _stats = stats;
        _log = log;
    }

    /// <summary>Trades not finished yet.</summary>
    public int OpenTrades => Volatile.Read(ref _open);

    /// <summary>Trades placed so far.</summary>
    public int Placed => Volatile.Read(ref _placed);

    /// <summary>
    /// Called on the tick-reader thread. The buy starts right here, on this thread, and runs until the frame is on its
    /// way to IQ Option; only then does the rest of the trade continue on the thread pool.
    /// </summary>
    public void OnSignal(Direction direction, double price, double move, long tickTimestamp)
    {
        _stats.Signal();
        if (Stopwatch.GetElapsedTime(_lastTrade) < _s.Cooldown) return; // the same move fires on many ticks; one trade is enough
        string? skip =
            _s.MaxTrades > 0 && Placed >= _s.MaxTrades ? $"--max-trades {_s.MaxTrades} reached" :
            _s.MaxLoss > 0 && _stats.Profit <= -_s.MaxLoss ? $"--max-loss {_s.MaxLoss} reached" :
            OpenTrades >= _s.MaxOpen ? $"{OpenTrades} trade(s) still open (--max-open {_s.MaxOpen})" :
            Stopwatch.GetTimestamp() < Volatile.Read(ref _pauseUntil) ? "the last buy failed; waiting 30 s before trying again" :
            _api != null && !_api.IsConnected ? "IQ Option is not connected" :
            _api != null && Volatile.Read(ref _payout) <= 0 ? $"{(_s.Blitz ? "blitz" : "turbo")} {_s.Asset} is closed{(_s.Blitz ? $" or not offered with a {_s.Seconds}s expiry" : "")} (see above)" :
            null;
        if (skip != null)
        {
            LogSkip(skip, $"Signal {direction} (BTC {move:+0.00;-0.00}) skipped: {skip}");
            return;
        }
        _lastTrade = Stopwatch.GetTimestamp();
        int number = Interlocked.Increment(ref _attempts);
        Interlocked.Increment(ref _placed); // reserved now so --max-trades holds; given back if IQ Option refuses the buy
        Interlocked.Increment(ref _open);
        var trade = TradeAsync(number, direction, price, move, tickTimestamp);
        if (trade.IsCompleted) return;
        _running[number] = trade;
        if (trade.IsCompleted) _running.TryRemove(number, out _); // it finished between the check and the add
    }

    private async Task TradeAsync(int number, Direction direction, double price, double move, long tickTimestamp)
    {
        try
        {
            long start = Stopwatch.GetTimestamp();
            string what = $"#{number} {direction.ToString().ToUpperInvariant()} ${_s.Amount} {(_s.Blitz ? $"blitz {_s.Seconds}s" : $"turbo {_s.Minutes}m")} | BTC {price:0.00} moved {move:+0.00;-0.00}";
            if (_api == null)
            {
                _log.Write($"{what} | DRY RUN, tick->decision {Micros(start - tickTimestamp):0} us");
                return;
            }
            long id;
            string? error;
            bool ok;
            if (_s.Blitz)
            {
                var r = await _api.BuyBlitzAsync(_s.Amount, _s.Asset, direction, _s.Seconds, Volatile.Read(ref _payout)).ConfigureAwait(false);
                (ok, error) = (r.Success, r.Error);
                id = r.Result?["id"]?.GetValue<long>() ?? 0;
            }
            else
            {
                (ok, id, error) = await _api.BuyAsync(_s.Amount, _s.Asset, direction, _s.Minutes).ConfigureAwait(false);
            }
            double ackMs = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
            if (!ok)
            {
                // A refused buy is not a trade. Wait before the next one, so a product that refuses every buy is not hammered.
                Interlocked.Decrement(ref _placed);
                _stats.Failed();
                Volatile.Write(ref _pauseUntil, Stopwatch.GetTimestamp() + 30 * Stopwatch.Frequency);
                _log.Write($"{what} | buy FAILED: {error} ({ackMs:0} ms); not counted as a trade, next buy in 30 s at the earliest");
                return;
            }
            _log.Write($"{what} | id {id} | tick->send {Micros(start - tickTimestamp):0} us, IQ accepted in {ackMs:0} ms");

            // Expiry can be up to a period away (turbo rounds to the minute), plus time for the server to settle.
            using var wait = new CancellationTokenSource(_s.Expiry * 2 + TimeSpan.FromSeconds(90));
            var (result, profit) = await _api.CheckWinAsync(id, wait.Token).ConfigureAwait(false);
            _stats.Result(result, profit);
            _log.Write($"#{number} {result} {profit:+0.00;-0.00;0.00} | {_stats.Summary(this)}");
        }
        catch (OperationCanceledException)
        {
            _log.Write($"#{number}: no result in time; check the traderoom history.");
        }
        catch (Exception ex)
        {
            _log.Write($"#{number}: error {ex.GetType().Name}: {ex.Message}");
        }
        finally
        {
            Interlocked.Decrement(ref _open);
            _running.TryRemove(number, out _);
        }
    }

    /// <summary>Completes when every running trade has its result.</summary>
    public Task WhenIdleAsync() => Task.WhenAll(_running.Values);

    /// <summary>Reads the current payout once, so the blitz buy can skip that request at trade time.</summary>
    public async Task RefreshPayoutAsync(CancellationToken ct)
    {
        if (_api == null) return;
        try
        {
            int now;
            int[] expiries = Array.Empty<int>();
            if (_s.Blitz)
            {
                now = await _api.GetBlitzProfitPercentAsync(_s.Asset, ct).ConfigureAwait(false) ?? 0;
                // IQ Option refuses a blitz expiry it does not offer right now, with the misleading "active is suspended".
                expiries = await BlitzExpiriesAsync(ct).ConfigureAwait(false);
                if (now > 0 && expiries.Length > 0 && !expiries.Contains(_s.Seconds)) now = 0;
            }
            else
            {
                var table = await _api.GetAllProfitAsync(ct).ConfigureAwait(false);
                now = table.TryGetValue(_s.Asset, out var row) && row.TryGetValue("turbo", out double f) ? (int)Math.Round(f * 100) : 0;
            }
            int before = Interlocked.Exchange(ref _payout, now);
            if (before == now) return;
            string product = _s.Blitz ? "Blitz" : "Turbo";
            if (now > 0)
            {
                _log.Write($"{product} {_s.Asset} payout {now}%" + (before == 0 ? " (it is open again; trading resumes)" : ""));
                return;
            }
            if (expiries.Length > 0 && !expiries.Contains(_s.Seconds))
            {
                _log.Write($"Blitz {_s.Asset} is open, but not with a {_s.Seconds}s expiry. IQ Option offers {string.Join(", ", expiries)} s right now. " +
                    $"Restart with --seconds {expiries[0]} (or another of those). Signals are skipped until {_s.Seconds}s is offered; the bot checks every 30 s.");
                return;
            }
            // Say what IQ Option does offer for this asset now, so a closed product is not a mystery.
            var options = await _api.GetAllProfitAsync(ct).ConfigureAwait(false);
            var blitz = await _api.GetAllProfitBlitzAsync(ct).ConfigureAwait(false);
            var open = new List<string>();
            if (blitz.TryGetValue(_s.Asset, out var b) && b.ContainsKey("blitz")) open.Add("blitz (--mode blitz)");
            if (options.TryGetValue(_s.Asset, out var o) && o.ContainsKey("turbo")) open.Add("turbo (--mode turbo)");
            if (options.TryGetValue(_s.Asset, out o) && o.TryGetValue("binary", out double bin)) open.Add($"binary 15+ min only, {bin * 100:0}% (not used by this bot)");
            _log.Write($"{product} is switched off by IQ Option for {_s.Asset} right now, so signals are skipped. " +
                $"Open for {_s.Asset} now: {(open.Count > 0 ? string.Join(", ", open) : "nothing")}. The bot checks again every 30 s and trades as soon as it reopens.");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _log.Write("Payout check failed (keeping the last one): " + ex.Message);
        }
    }

    /// <summary>Blitz expiries in seconds IQ Option offers for the asset right now (e.g. 120, 180, 300); empty when unknown.</summary>
    private async Task<int[]> BlitzExpiriesAsync(CancellationToken ct)
    {
        var init = await _api!.GetAllInitAsync(ct).ConfigureAwait(false);
        if (init?["blitz"]?["actives"] is not JsonObject actives) return Array.Empty<int>();
        foreach (var (_, node) in actives)
        {
            if (node?["ticker"]?.GetValue<string>() == _s.Asset && node["option"]?["expiration_times"] is JsonArray times)
                return times.Select(t => (int)t!.GetValue<double>()).Order().ToArray();
        }
        return Array.Empty<int>();
    }

    /// <summary>Refreshes the payout every 30 s; it changes during the day and blitz sends it with every buy.</summary>
    public async Task RefreshPayoutLoopAsync(CancellationToken ct)
    {
        if (_api == null) return;
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(30));
        try
        {
            while (await timer.WaitForNextTickAsync(ct).ConfigureAwait(false)) await RefreshPayoutAsync(ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    // A skip reason is printed at most every 10 s, so a fast market does not flood the console.
    private void LogSkip(string reason, string line)
    {
        long now = Stopwatch.GetTimestamp();
        if (_lastSkipLog.TryGetValue(reason, out long last) && Stopwatch.GetElapsedTime(last, now) < TimeSpan.FromSeconds(10)) return;
        _lastSkipLog[reason] = now;
        _log.Write(line);
    }

    private static double Micros(long stopwatchTicks) => stopwatchTicks * 1_000_000.0 / Stopwatch.Frequency;
}

/// <summary>Counters shared by the tick reader, the trades and the stats printer.</summary>
internal sealed class BotStats
{
    private readonly object _lock = new();
    private long _ticks, _lost, _stale, _signals, _ageSum, _ageMax;
    private double _maxMove;
    private int _wins, _losses, _ties, _failed;
    private double _profit;

    /// <summary>Session profit so far.</summary>
    public double Profit
    {
        get
        {
            lock (_lock) return _profit;
        }
    }

    public void Tick(long ageTicks, double move)
    {
        lock (_lock)
        {
            _ticks++;
            _ageSum += ageTicks;
            _ageMax = Math.Max(_ageMax, ageTicks);
            _maxMove = Math.Max(_maxMove, Math.Abs(move));
        }
    }

    public void Lost(long count)
    {
        lock (_lock) _lost += count;
    }

    public void Stale()
    {
        lock (_lock) _stale++;
    }

    public void Signal()
    {
        lock (_lock) _signals++;
    }

    /// <summary>Counts a buy IQ Option refused.</summary>
    public void Failed()
    {
        lock (_lock) _failed++;
    }

    public void Result(TradeResult result, double profit)
    {
        lock (_lock)
        {
            _profit += profit;
            if (result == TradeResult.Win) _wins++;
            else if (result == TradeResult.Loss) _losses++;
            else _ties++;
        }
    }

    /// <summary>Tick counters since the last call, then resets them.</summary>
    public string TakePeriod()
    {
        lock (_lock)
        {
            string latency = _ticks > 0
                ? $"feeder->bot {_ageSum * 1e6 / Stopwatch.Frequency / _ticks:0.0} us avg, {_ageMax * 1e6 / Stopwatch.Frequency:0.0} us max"
                : "no ticks";
            string text = $"ticks {_ticks}{(_lost > 0 ? $" (lost {_lost})" : "")}, biggest move {_maxMove:0.00}, {latency}, signals {_signals}" +
                (_stale > 0 ? $" (stale {_stale})" : "");
            _ticks = _lost = _stale = _signals = _ageSum = _ageMax = 0;
            _maxMove = 0;
            return text;
        }
    }

    /// <summary>Session totals.</summary>
    public string Summary(Trader trader)
    {
        lock (_lock) return $"trades {trader.Placed}{(_failed > 0 ? $" (+{_failed} refused)" : "")}, open {trader.OpenTrades}, W/L/T {_wins}/{_losses}/{_ties}, P&L {_profit:+0.00;-0.00;0.00}";
    }
}

/// <summary>Console output through a queue, so the tick reader never waits on the terminal.</summary>
internal sealed class ConsoleLog
{
    private readonly Channel<string> _lines = Channel.CreateUnbounded<string>(new UnboundedChannelOptions { SingleReader = true });
    private readonly Task _pump;

    public ConsoleLog() => _pump = PumpAsync();

    public void Write(string line) => _lines.Writer.TryWrite(DateTime.Now.ToString("HH:mm:ss.fff ", CultureInfo.InvariantCulture) + line);

    /// <summary>Prints what is queued and stops.</summary>
    public Task CompleteAsync()
    {
        _lines.Writer.TryComplete();
        return _pump;
    }

    private async Task PumpAsync()
    {
        await foreach (string line in _lines.Reader.ReadAllAsync().ConfigureAwait(false)) Console.WriteLine(line);
    }
}
