using IqOptionApi;

// Example bot. PRACTICE account only unless started with --allow-real.
// Usage:  export IQ_EMAIL=you@example.com ; export IQ_PASSWORD='secret'
//         dotnet run --project examples/SimpleBot [-- --asset EURUSD-OTC] [--allow-real]
string? email = Environment.GetEnvironmentVariable("IQ_EMAIL");
string? password = Environment.GetEnvironmentVariable("IQ_PASSWORD");
bool allowReal = args.Contains("--allow-real");
int assetArg = Array.IndexOf(args, "--asset");
string asset = assetArg >= 0 && assetArg + 1 < args.Length ? args[assetArg + 1] : "EURUSD";

if (string.IsNullOrEmpty(email) || string.IsNullOrEmpty(password))
{
    Console.WriteLine("Set IQ_EMAIL and IQ_PASSWORD first (they are never stored in a file).");
    return 1;
}

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

var mode = await api.GetBalanceModeAsync();
Console.WriteLine($"Balance: {mode}  {await api.GetBalanceAsync()} {await api.GetCurrencyAsync()}");
if (mode != BalanceMode.Practice && !allowReal)
{
    Console.WriteLine("Safety lock: the selected balance is not PRACTICE. Start with --allow-real to use real money.");
    return 2;
}

Console.WriteLine($"Last 10 one-minute candles of {asset}:");
foreach (var c in await api.GetCandlesAsync(asset, 60, 10, api.GetServerTimestamp()))
    Console.WriteLine($"  {DateTimeOffset.FromUnixTimeSeconds(c.From).ToLocalTime():HH:mm}  open {c.Open}  close {c.Close}");

Console.WriteLine("Streaming live candles for 30 seconds...");
api.CandleReceived += (name, size, candle) => Console.WriteLine($"  live {name} {size}s close {candle.Close}");
await api.StartCandlesStreamAsync(asset, 60, 10);
await Task.Delay(TimeSpan.FromSeconds(30));
await api.StopCandlesStreamAsync(asset, 60);

var (bought, optionId, error) = await api.BuyAsync(1, asset, Direction.Call, 1);
Console.WriteLine(bought ? $"Turbo option bought: {optionId}" : "Turbo buy failed: " + error);
var (digitalOk, positionId, digitalError) = await api.BuyDigitalSpotAsync(asset, 1, Direction.Put, 1);
Console.WriteLine(digitalOk ? $"Digital option bought: {positionId}" : "Digital buy failed: " + digitalError);

using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5));
if (bought)
{
    var (result, profit) = await api.CheckWinAsync(optionId, cts.Token);
    Console.WriteLine($"Turbo result: {result} {profit:+0.00;-0.00}");
}
if (digitalOk)
{
    var (result, profit) = await api.CheckWinDigitalAsync(positionId, cts.Token);
    Console.WriteLine($"Digital result: {result} {profit:+0.00;-0.00}");
}
return 0;
