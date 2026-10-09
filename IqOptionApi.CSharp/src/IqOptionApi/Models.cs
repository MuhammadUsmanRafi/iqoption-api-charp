using System.Text.Json.Nodes;

namespace IqOptionApi;

/// <summary>Direction of an option trade.</summary>
public enum Direction
{
    /// <summary>The price will go up (sent as <c>"call"</c>).</summary>
    Call,

    /// <summary>The price will go down (sent as <c>"put"</c>).</summary>
    Put,
}

/// <summary>Kind of balance.</summary>
public enum BalanceMode
{
    /// <summary>Real money (balance type 1).</summary>
    Real,

    /// <summary>Practice money (balance type 4).</summary>
    Practice,
}

/// <summary>Outcome of a closed trade, taken from the sign of its profit.</summary>
public enum TradeResult
{
    /// <summary>Profit above zero.</summary>
    Win,

    /// <summary>Profit below zero.</summary>
    Loss,

    /// <summary>Profit exactly zero.</summary>
    Equal,
}

/// <summary>One price candle. Prices are <see cref="double"/> like Python floats.</summary>
public sealed record Candle
{
    /// <summary>Candle id.</summary>
    public long Id { get; init; }

    /// <summary>Start time (Unix seconds).</summary>
    public long From { get; init; }

    /// <summary>End time (Unix seconds).</summary>
    public long To { get; init; }

    /// <summary>Server timestamp of this update (nanoseconds), 0 if absent.</summary>
    public long At { get; init; }

    /// <summary>Opening price.</summary>
    public double Open { get; init; }

    /// <summary>Closing (latest) price.</summary>
    public double Close { get; init; }

    /// <summary>Lowest price.</summary>
    public double Min { get; init; }

    /// <summary>Highest price.</summary>
    public double Max { get; init; }

    /// <summary>Volume.</summary>
    public double Volume { get; init; }

    /// <summary>Ask price (live candles only).</summary>
    public double? Ask { get; init; }

    /// <summary>Bid price (live candles only).</summary>
    public double? Bid { get; init; }

    /// <summary>Candle size in seconds (live candles only).</summary>
    public int? Size { get; init; }

    /// <summary>Asset id (live candles only).</summary>
    public int? ActiveId { get; init; }

    /// <summary>The candle exactly as the server sent it.</summary>
    public JsonObject Raw { get; init; } = new();

    internal static Candle FromJson(JsonObject o) => new()
    {
        Id = J.LongOr(o["id"]),
        From = J.LongOr(o["from"]),
        To = J.LongOr(o["to"]),
        At = J.LongOr(o["at"]),
        Open = J.DblOr(o["open"]),
        Close = J.DblOr(o["close"]),
        Min = J.DblOr(o["min"]),
        Max = J.DblOr(o["max"]),
        Volume = J.DblOr(o["volume"]),
        Ask = J.DblOrNull(o["ask"]),
        Bid = J.DblOrNull(o["bid"]),
        Size = J.IsNull(o["size"]) ? null : (int)J.Long(o["size"]),
        ActiveId = J.IsNull(o["active_id"]) ? null : (int)J.Long(o["active_id"]),
        Raw = o,
    };
}

/// <summary>Call and put instrument ids of one digital strike, with their payout % (null when the server sent no ask price).</summary>
/// <param name="CallId">Instrument id of the call side.</param>
/// <param name="CallProfit">Payout percent of the call side.</param>
/// <param name="PutId">Instrument id of the put side.</param>
/// <param name="PutProfit">Payout percent of the put side.</param>
public sealed record StrikeQuote(string CallId, double? CallProfit, string PutId, double? PutProfit);

/// <summary>Instrument type names used by IQ Option.</summary>
public static class InstrumentTypes
{
    /// <summary>Stocks / indices (CFD).</summary>
    public const string Cfd = "cfd";

    /// <summary>Forex.</summary>
    public const string Forex = "forex";

    /// <summary>Crypto.</summary>
    public const string Crypto = "crypto";

    /// <summary>Digital options.</summary>
    public const string DigitalOption = "digital-option";

    /// <summary>Turbo (short binary) options.</summary>
    public const string TurboOption = "turbo-option";

    /// <summary>Binary options.</summary>
    public const string BinaryOption = "binary-option";

    /// <summary>FX options.</summary>
    public const string FxOption = "fx-option";
}

/// <summary>Raised when the server connection is lost or not available.</summary>
public class IqOptionException : Exception
{
    /// <summary>Creates the exception.</summary>
    /// <param name="message">What went wrong.</param>
    public IqOptionException(string message) : base(message)
    {
    }
}

/// <summary>Settings for the <c>IqOption</c> client.</summary>
public sealed class IqOptionOptions
{
    /// <summary>Server host; the WebSocket is <c>wss://{Host}/echo/websocket</c>.</summary>
    public string Host { get; set; } = "iqoption.com";

    /// <summary>Full WebSocket URL; null = derived from <see cref="Host"/>.</summary>
    public string? WebSocketUrl { get; set; }

    /// <summary>Base URL of the login service.</summary>
    public string AuthUrl { get; set; } = "https://auth.iqoption.com";

    /// <summary>HTTP proxy host (used for login and WebSocket).</summary>
    public string? ProxyHost { get; set; }

    /// <summary>HTTP proxy port.</summary>
    public int? ProxyPort { get; set; }

    /// <summary>Proxy user name.</summary>
    public string? ProxyUser { get; set; }

    /// <summary>Proxy password.</summary>
    public string? ProxyPassword { get; set; }

    /// <summary>Existing session id to try before logging in (Python <c>set_ssid</c>).</summary>
    public string? Ssid { get; set; }

    /// <summary>Log out when the client is closed or disposed.</summary>
    public bool AutoLogout { get; set; } = true;

    /// <summary>Reconnect automatically when the connection drops.</summary>
    public bool AutoReconnect { get; set; } = true;

    /// <summary>How long a request waits for its reply.</summary>
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Treat the connection as dropped when nothing arrives for this long (the server normally pushes time updates every second).</summary>
    public TimeSpan ReceiveTimeout { get; set; } = TimeSpan.FromSeconds(60);

    /// <summary>Waits between reconnect attempts (last one repeats); null = 1, 2, 5, 10, 30 seconds.</summary>
    public IReadOnlyList<TimeSpan>? ReconnectDelays { get; set; }

    /// <summary>Skip TLS certificate checks (Python always did; only use with a proxy that needs it).</summary>
    public bool IgnoreCertificateErrors { get; set; }

    /// <summary>Time zone for the expiry maths; null = the computer's zone, like Python.</summary>
    public TimeZoneInfo? TimeZone { get; set; }

    /// <summary>Receives one line per notable event.</summary>
    public Action<string>? Log { get; set; }

    /// <summary>Also log every raw frame (frames contain the session id).</summary>
    public bool LogFrames { get; set; }
}
