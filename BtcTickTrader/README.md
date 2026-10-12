# BtcTickTrader

Two C# programs that work as one bot:

| Folder | What it does |
|---|---|
| `PriceFeeder/` | **Only provides the price.** It streams Bitcoin quotes from **MetaTrader 5** (the same `mt5api.dll` connection as the TradoWix price feeder) and writes every quote into shared memory as it is. It does no maths. |
| `BitcoinTickBot/` | **Does the maths and the trading.** It reads the prices from shared memory and works out the difference to decide whether it's a spike. On a spike it buys an IQ Option **blitz** or **turbo** option (up = CALL, down = PUT). It logs in with the **SSID from `.env`**. |

```
MT5 ──────mt5api────▶ PriceFeeder ──shared memory ring──▶ BitcoinTickBot ──WebSocket──▶ IQ Option
                       price only      (same RAM pages)     difference, spike?, buy
```

## Setup (once)

1. Copy `.env.example` to `.env` in this folder.
2. Paste your IQ Option ssid after `IQ_SSID=` and the MT5 account the feeder logs in with. These are the same keys the TradoWix price feeder uses:
   ```
   IQ_SSID=abc123yourssid
   MT5Settings__AccountNumber=12345678
   MT5Settings__Password=your_mt5_password
   MT5Settings__Server=VantageMarkets-Demo
   ```
   `.env` is in `.gitignore`, so nothing in it is ever committed or pushed. If the feeder prints `INVALID_ACCOUNT`, the MT5 account is wrong or has expired (demo accounts expire). Put a working one in `.env`.

To get the ssid: log in on iqoption.com in your browser, press F12, open **Application** (Chrome) or **Storage** (Firefox), then **Cookies**, then `iqoption.com`, and copy the value of `ssid`. An ssid stops working when you log out or after a while. The bot then says *"The SSID was not accepted"*, and you paste a fresh one into `.env`.

The bot finds `.env` in the folder you run it from or any folder above it. An `IQ_SSID` already set in your shell, or `--ssid`, takes priority over the file. The bot prints where the ssid came from, never the ssid itself.

## Run it

Two terminals, both in this `BtcTickTrader` folder. Start the feeder first:

```bash
# terminal 1: MT5 price feeder
dotnet run -c Release --project PriceFeeder

# terminal 2: try the bot without trading first
dotnet run -c Release --project BitcoinTickBot -- --dry-run --threshold 2

# then trade on the PRACTICE balance (ssid from .env)
dotnet run -c Release --project BitcoinTickBot -- --threshold 5 --mode blitz --seconds 120 --amount 1
dotnet run -c Release --project BitcoinTickBot -- --threshold 5 --mode turbo --minutes 1 --amount 1
```

The feeder logs in to MT5 with `MT5API(account, password, server)`, subscribes to `BTCUSD` and publishes every `OnQuote`. By default the price is the **bid**, as in the TradoWix feeder; `--price mid` uses the bid/ask middle. Every 5 s it checks `MT5API.Connected`, and when the connection drops it reconnects by itself (after 0, 2, 5, 10, then 30 s).

Ctrl+C stops the bot. It waits for trades that are still open and prints the session result. Press Ctrl+C again to quit at once. The bot uses the **PRACTICE** balance unless you add `--real`.

`BtcTickTrader.sln` opens both projects in Visual Studio or Rider. The bot uses the IQ Option library from `../IqOptionApi.CSharp`.

## How the signal works

The bot's `SpikeDetector` (in `BitcoinTickBot/Program.cs`) holds all the price maths:

- **`--window-ms 0` (default), tick-to-tick:** `move = price - previous price`.
- **`--window-ms N`, spike window:** `move` = the price minus the lowest price of the last N ms (up), or minus the highest (down), whichever is bigger. This catches a fast move made of several small steps, such as +$1, +$1, +$1.5 within a second.

It's a **spike** when `|move| >= --threshold`. An up spike buys CALL and a down spike buys PUT, or the reverse with `--reverse`. After a feeder reconnect the bot forgets the old prices, so it never measures a move across an outage.

| Option | Default | Meaning |
|---|---|---|
| `--threshold` | 5 | USD move that triggers a trade |
| `--window-ms` | 0 | 0 = previous price; N = lowest/highest of the last N ms |
| `--mode` | blitz | `blitz` (`--seconds`, default 120: one of the expiries IQ Option offers for the asset, BTCUSD in Oct 2026: 120, 180, 300, 600, 900) or `turbo` (`--minutes` 1-5) |
| `--amount` | 1 | Stake per trade |
| `--cooldown` | 5 | Seconds between trades. One move fires on several ticks, so this keeps it to one trade. |
| `--max-open` | 1 | Trades open at the same time. With 1, signals are skipped until the open trade has its result. |
| `--max-trades` / `--max-loss` | off | Stop trading after N trades, or once the session loss reaches this amount |
| `--ssid` | from `.env` | Override the ssid (it then shows up in your shell history) |
| `--max-age-ms` | 250 | Skip a signal whose tick is older than this |
| `--low-cpu` | off | Sleep when idle instead of spinning (see below) |

Feeder options: `--symbol BTCUSD` (as the broker names it; if it is missing, the feeder lists the broker's BTC symbols), `--price bid|mid`, `--ring`, `--print`.

**What the MT5 feed looks like.** There's no rate limit. The feeder publishes each quote the moment MT5's `OnQuote` fires, and the broker's server sends a quote only when the bid or ask changes. Measured on VantageMarkets-Demo over 40 s: BTCUSD quotes arrived **100 ms to 5.5 s apart** (0.7/s on average). The quotes come in bursts every 100-150 ms during a fast move and pause for seconds when the price is flat. The broker sends no order book (depth) for BTCUSD. Moves between quotes were $0.01-7, and the spread was about $17. A single quote rarely moves $5. Watch the bot's **biggest move** line, and use `--window-ms` (e.g. 2000-5000) to add up several quotes.

The feeder's status line shows `quotes/s`, the price it publishes, and the bid and ask. The bot prints the biggest move every 10 s; watch it for a few minutes before you choose a threshold.

## Latency: where the time goes

- **Feeder to bot: about 3 µs average, about 12 µs max** (measured on this Mac, Release build). The ring is a memory-mapped file that both processes map, so a tick goes from one process to the other through the same RAM pages. There's no socket, pipe or copy in between. Each 64-byte slot is a lock-free seqlock with one writer and any number of readers. The bot polls it on a dedicated high-priority thread, so it never waits for the OS to wake it.
  - This is shared memory, not hardware DMA. It's the fastest way two processes on one machine can share data.
  - Linux uses `/dev/shm` (RAM). macOS uses the temp folder, and the OS writes that file to disk in the background without slowing the feeder.
- **Tick to trade decision: about 5-25 µs once warm** (measured with `--dry-run`). The buy then starts on the same reader thread. For blitz, the payout is kept in memory and refreshed every 30 s, so the buy is a single message. The library's `BuyBlitzAsync(…, profitPercent)` overload exists for this.
- **The real cost is the network.** The quote travels from the broker's MT5 server to this machine, and the IQ Option buy confirmation adds another ~100+ ms. No code change can remove this. To get faster, run both programs on a server close to the MT5 broker's server.
- **CPU use.** Spinning keeps one CPU core busy. `--low-cpu` frees the core but adds up to ~1 ms.

## Things to know

- The MT5 broker's price is not IQ Option's quote. IQ settles blitz/turbo on its own feed, so a move at the broker doesn't guarantee the same move on IQ. Test on PRACTICE before trusting any threshold.
- Turbo expiries are rounded to the minute by IQ Option, as in the web traderoom: a 1-minute turbo bought 30 s or less before a minute ends expires at the end of the *next* minute, so it can stay open for up to 90 s. Blitz expires exactly `--seconds` after the buy.
- Blitz only accepts the expiries IQ Option offers for the asset at that moment. Any other expiry is refused with the misleading error *"active is suspended"*. The bot checks the list every 30 s and says which expiries are offered.
- A buy IQ Option refuses is not counted as a trade (it shows as *refused*), and the bot waits 30 s before the next buy.
- Only one feeder can write a ring (a second one is refused). After a feeder restart the bot resyncs by itself. The first tick after any reconnect is marked as a gap, so no trade is based on a move that spans an outage.
- `BitcoinTickBot` compiles `PriceFeeder/TickRing.cs` and `PriceFeeder/DotEnv.cs` (linked in its `.csproj`), so both programs always share one memory layout and read the same `.env`.
- `PriceFeeder/BinaryF/mt5api.dll` is the MT5 client library (mtapi.mt5) copied from the TradoWix price feeder. It's a commercial library, so check its license before you push it to a public repo.
