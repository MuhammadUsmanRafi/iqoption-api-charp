using System.Collections;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IqOptionApi;

/// <summary>Insertion-ordered JSON object used to build outgoing frames: <c>new Obj { ["name"] = "x" }</c>.</summary>
internal sealed class Obj : IEnumerable<KeyValuePair<string, object?>>
{
    private readonly List<KeyValuePair<string, object?>> _items = new();

    public object? this[string key]
    {
        get => _items.FirstOrDefault(p => p.Key == key).Value;
        set
        {
            int i = _items.FindIndex(p => p.Key == key);
            if (i >= 0) _items[i] = new(key, value);
            else _items.Add(new(key, value));
        }
    }

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() => _items.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>A number that Python passes through <c>float(...)</c>, so it always prints with a fraction or exponent.</summary>
internal readonly record struct PyFloat(double Value);

/// <summary>Writes JSON exactly like Python's <c>json.dumps</c> with default settings.</summary>
internal static class PyJson
{
    public static string Frame(string name, object? msg, string requestId) =>
        Dumps(new Obj { ["name"] = name, ["msg"] = msg, ["request_id"] = requestId });

    public static string Dumps(object? value)
    {
        var sb = new StringBuilder();
        Write(sb, value);
        return sb.ToString();
    }

    /// <summary>Python <c>str(x)</c> / raw number: integral values print without ".0".</summary>
    public static string Str(double value) => Num(value);

    public static string Num(double value) =>
        double.IsFinite(value) && Math.Abs(value) < 1e16 && value == Math.Floor(value)
            ? ((long)value).ToString(CultureInfo.InvariantCulture)
            : FloatRepr(value);

    /// <summary>Python <c>repr(float)</c>.</summary>
    public static string FloatRepr(double d)
    {
        if (double.IsNaN(d)) return "NaN";
        if (double.IsPositiveInfinity(d)) return "Infinity";
        if (double.IsNegativeInfinity(d)) return "-Infinity";
        if (d == 0) return double.IsNegative(d) ? "-0.0" : "0.0";

        string s = d.ToString("R", CultureInfo.InvariantCulture);
        bool neg = s.StartsWith('-');
        if (neg) s = s[1..];
        int exp = 0;
        int e = s.IndexOfAny(new[] { 'E', 'e' });
        if (e >= 0)
        {
            exp = int.Parse(s[(e + 1)..], NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);
            s = s[..e];
        }
        int dot = s.IndexOf('.');
        string intPart = dot >= 0 ? s[..dot] : s;
        string digits = intPart + (dot >= 0 ? s[(dot + 1)..] : "");
        int point = intPart.Length + exp;
        int lead = 0;
        while (lead < digits.Length - 1 && digits[lead] == '0') lead++;
        digits = digits[lead..];
        point -= lead;
        digits = digits.TrimEnd('0');
        if (digits.Length == 0) digits = "0";
        int sci = point - 1;

        string body;
        if (sci < -4 || sci >= 16)
        {
            string mantissa = digits.Length > 1 ? digits[0] + "." + digits[1..] : digits;
            body = mantissa + "e" + (sci < 0 ? "-" : "+") + Math.Abs(sci).ToString("00", CultureInfo.InvariantCulture);
        }
        else if (point <= 0) body = "0." + new string('0', -point) + digits;
        else if (point >= digits.Length) body = digits + new string('0', point - digits.Length) + ".0";
        else body = digits[..point] + "." + digits[point..];
        return (neg ? "-" : "") + body;
    }

    private static void Write(StringBuilder sb, object? v)
    {
        switch (v)
        {
            case null: sb.Append("null"); break;
            case bool b: sb.Append(b ? "true" : "false"); break;
            case string s: WriteString(sb, s); break;
            case PyFloat f: sb.Append(FloatRepr(f.Value)); break;
            case double d: sb.Append(Num(d)); break;
            case float f: sb.Append(Num(f)); break;
            case decimal m: sb.Append(Num((double)m)); break;
            case int or long or short or byte or uint or ulong or ushort or sbyte:
                sb.Append(Convert.ToString(v, CultureInfo.InvariantCulture));
                break;
            case JsonNode n: WriteNode(sb, n); break;
            case Obj o: WritePairs(sb, o); break;
            case IEnumerable<KeyValuePair<string, object?>> pairs: WritePairs(sb, pairs); break;
            case IEnumerable list:
                sb.Append('[');
                bool first = true;
                foreach (var item in list)
                {
                    if (!first) sb.Append(", ");
                    first = false;
                    Write(sb, item);
                }
                sb.Append(']');
                break;
            default: throw new ArgumentException($"Cannot write {v.GetType().Name} as JSON");
        }
    }

    private static void WritePairs(StringBuilder sb, IEnumerable<KeyValuePair<string, object?>> pairs)
    {
        sb.Append('{');
        bool first = true;
        foreach (var (key, value) in pairs)
        {
            if (!first) sb.Append(", ");
            first = false;
            WriteString(sb, key);
            sb.Append(": ");
            Write(sb, value);
        }
        sb.Append('}');
    }

    private static void WriteNode(StringBuilder sb, JsonNode n)
    {
        switch (n)
        {
            case JsonObject o:
                WritePairs(sb, o.Select(p => new KeyValuePair<string, object?>(p.Key, p.Value)));
                break;
            case JsonArray a:
                Write(sb, a.Select(x => (object?)x).ToList());
                break;
            case JsonValue val:
                switch (val.GetValueKind())
                {
                    case JsonValueKind.String: WriteString(sb, val.GetValue<string>()); break;
                    case JsonValueKind.True: sb.Append("true"); break;
                    case JsonValueKind.False: sb.Append("false"); break;
                    case JsonValueKind.Number:
                        string raw = val.ToJsonString();
                        sb.Append(raw.IndexOfAny(new[] { '.', 'e', 'E' }) >= 0
                            ? FloatRepr(double.Parse(raw, NumberStyles.Float, CultureInfo.InvariantCulture))
                            : raw);
                        break;
                    default: sb.Append("null"); break;
                }
                break;
        }
    }

    private static void WriteString(StringBuilder sb, string s)
    {
        sb.Append('"');
        foreach (char c in s)
        {
            switch (c)
            {
                case '"': sb.Append("\\\""); break;
                case '\\': sb.Append("\\\\"); break;
                case '\n': sb.Append("\\n"); break;
                case '\r': sb.Append("\\r"); break;
                case '\t': sb.Append("\\t"); break;
                case '\b': sb.Append("\\b"); break;
                case '\f': sb.Append("\\f"); break;
                default:
                    if (c < 0x20 || c > 0x7e) sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    else sb.Append(c);
                    break;
            }
        }
        sb.Append('"');
    }
}

/// <summary>Small, forgiving readers for incoming JSON (Python-style conversions).</summary>
internal static class J
{
    public static string? Str(JsonNode? n) => n switch
    {
        null => null,
        JsonValue v when v.TryGetValue(out string? s) => s,
        _ => n.ToJsonString(),
    };

    public static double Dbl(JsonNode? n)
    {
        switch (n)
        {
            case JsonValue v when v.TryGetValue(out double d): return d;
            case JsonValue v when v.TryGetValue(out long l): return l;
            case JsonValue v when v.TryGetValue(out int i): return i;
            case JsonValue v when v.TryGetValue(out string? s):
                return double.Parse(s, NumberStyles.Float, CultureInfo.InvariantCulture);
            case JsonValue v when v.TryGetValue(out bool b): return b ? 1 : 0;
            default: throw new FormatException($"Expected a number but got {n?.ToJsonString() ?? "nothing"}");
        }
    }

    public static double DblOr(JsonNode? n, double fallback = 0) => IsNull(n) ? fallback : Dbl(n);

    public static double? DblOrNull(JsonNode? n) => IsNull(n) ? null : Dbl(n);

    /// <summary>Python <c>int(x)</c>: integers as-is, floats truncated, numeric strings parsed.</summary>
    public static long Long(JsonNode? n)
    {
        switch (n)
        {
            case JsonValue v when v.TryGetValue(out long l): return l;
            case JsonValue v when v.TryGetValue(out int i): return i;
            case JsonValue v when v.TryGetValue(out string? s)
                && long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out long ls): return ls;
            default: return (long)Dbl(n);
        }
    }

    public static long LongOr(JsonNode? n, long fallback = 0) => IsNull(n) ? fallback : Long(n);

    public static bool IsNull(JsonNode? n) => n is null || n.GetValueKind() == JsonValueKind.Null;

    public static bool IsTrue(JsonNode? n) => n is JsonValue v && v.TryGetValue(out bool b) && b;

    /// <summary>Python truthiness.</summary>
    public static bool Truthy(JsonNode? n) => n switch
    {
        null => false,
        JsonObject o => o.Count > 0,
        JsonArray a => a.Count > 0,
        JsonValue v => v.GetValueKind() switch
        {
            JsonValueKind.True => true,
            JsonValueKind.String => v.GetValue<string>().Length > 0,
            JsonValueKind.Number => Dbl(v) != 0,
            _ => false,
        },
        _ => false,
    };

    public static JsonArray Arr(JsonNode? n) => n as JsonArray ?? throw new FormatException("Expected a JSON array");

    public static JsonObject Obj(JsonNode? n) => n as JsonObject ?? throw new FormatException("Expected a JSON object");
}

internal static class Marker
{
}
