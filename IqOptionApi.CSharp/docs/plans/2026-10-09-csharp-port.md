# IqOptionApi C# Port — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `IqOptionApi.CSharp/`, a .NET 9 library that does everything the Python `iqoptionapi` (v9.4) does — same server messages byte-for-byte, async/await API, simpler internals — plus golden tests generated from the real Python code, fake-server tests and an example bot.

**Architecture:** One public class `IqOption` (all ~100 methods) on top of three internal parts: `Connection` (HTTP login + WebSocket + request matching), `MessageRouter` (incoming messages → thread-safe `State` + events) and `Helpers` (Python-identical JSON writer, expiry maths, formulas). Replies are matched by `request_id`, by reply name (one request at a time per name) or by waiting for pushed state — never by busy loops.

**Tech Stack:** C# 13 / .NET 9 (`HttpClient`, `ClientWebSocket`, `System.Text.Json.Nodes`, `OrderedDictionary<TKey,TValue>`), xUnit, ASP.NET Core (tests only), Python 3.9 (golden generator, standard library only).

**Spec:** `IqOptionApi.CSharp/docs/specs/2026-10-09-csharp-port-design.md` — reference for every frame: `IqOptionApi.CSharp/docs/specs/python-api-catalogue.md` ("Cat §n").

## Global Constraints

- Target `net9.0`; the library has **zero** NuGet dependencies.
- Library csproj: `Nullable` enable, `ImplicitUsings` enable, `TreatWarningsAsErrors` true, `GenerateDocumentationFile` true (every public member has an XML doc comment).
- Namespace `IqOptionApi`; tests `IqOptionApi.Tests`; only `IqOption` and the types in `Models.cs` are public.
- Library files (exactly): `IqOption.cs`, `Connection.cs`, `MessageRouter.cs`, `Models.cs`, `Actives.cs`, `Helpers.cs`.
- Outgoing frames are byte-identical to Python `json.dumps(dict(name=..., msg=..., request_id=...))` for the same inputs; `request_id` values identical to Python per method.
- Server methods end in `Async` and take `CancellationToken ct = default` last.
- Default `RequestTimeout` 30 s; trade-result waits (`CheckWin*`) have no timeout.
- All "now" values use the server clock (`timeSync`); local-time steps of the expiry maths use `options.TimeZone ?? TimeZoneInfo.Local`.
- Passwords/SSIDs are never logged; raw frames only when `LogFrames` is true.
- Run every command from `IqOptionApi.CSharp/` unless stated. Build/test command: `dotnet test IqOptionApi.sln`.
- Commit after every task with the trailer `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Server answers with an error under a different message name** (e.g. a rejected buy) → the call returns its failure shape after `RequestTimeout`, never hangs. Pinned in Task 5 (`RequestAsync_TimesOut`) and Task 11 (`Buy_TimesOutWithFailureTuple`).
2. **Unknown asset name** (`"EURUSDD"`) → `KeyNotFoundException` with a helpful message *before* anything is sent. Pinned in Task 4 (`Get_UnknownName_Throws`) and Task 11 (`Buy_UnknownAsset_SendsNothing`).
3. **Two concurrent calls sharing a reply name** (`GetPositionsAsync` + `GetPortfolioPositionsAsync`, both answered by `positions`) → each gets its own answer. Pinned in Task 5 (`RequestByName_SerialisesSameReplyName`).
4. **Connection drops while a buy is waiting** → `(false, 0, "connection lost")`, the buy is **not** re-sent after reconnect. Pinned in Task 15 (`Drop_DuringBuy_FailsAndDoesNotResend`).
5. **The bot's own event handler throws** → library keeps routing and answering. Pinned in Task 6 (`ThrowingHandler_DoesNotStopRouting`).

## File map

| File | Responsibility | Task |
|---|---|---|
| `.gitignore`, `IqOptionApi.sln`, csproj files | build setup | 1 |
| `src/IqOptionApi/Helpers.cs` | `Obj`, `PyFloat`, `PyJson`, `J`, `Expiry`/formulas (`Helpers`) | 2, 3 |
| `src/IqOptionApi/Models.cs` | public enums/records/options/exception | 4 |
| `src/IqOptionApi/Actives.cs` | generated asset + country tables, `ActivesTable` | 4 |
| `src/IqOptionApi/Connection.cs` | `ISocket`, `ClientWebSocketAdapter`, `AsyncPulse`, `Waiter`, `Connection` | 5 |
| `src/IqOptionApi/MessageRouter.cs` | `State`, `LiveDealBuffer`, `MessageRouter` | 6 |
| `src/IqOptionApi/IqOption.cs` | public API; one `#region` per group | 7–15 |
| `tests/golden/generate_golden.py` | runs the real Python code with fake network → `Golden/*.json` | 3, 4, 6–14 |
| `tests/golden/gen_actives_cs.py` | writes `Actives.cs` from the Python tables | 4 |
| `tests/IqOptionApi.Tests/Support/*.cs` | `FakeSocket`, `Rule`, `FakeHttpHandler`, `Golden`, `Harness`, `JsonAssert`, `Calls`, `StateDump`, `FakeIqServer` | 3, 5–7, 15 |
| `tests/IqOptionApi.Tests/*Tests.cs` | unit, golden and fake-server tests | all |
| `examples/SimpleBot/*` | example bot | 16 |
| `README.md` | usage guide + Python→C# table | 16 |

**Insertion markers.** `IqOption.cs` ends with the line `    // @@GROUPS@@` just before the class's closing brace; each group task inserts its region **above** that line. `tests/golden/generate_golden.py` has `# @@CASES@@` inside `build_cases()`; `tests/IqOptionApi.Tests/Support/Calls.cs` has `// @@CALLS@@` inside its switch. Group tasks insert above those markers.

---

### Task 1: Solution scaffold

**Files:**
- Create: `IqOptionApi.CSharp/.gitignore`, `IqOptionApi.sln`, `src/IqOptionApi/IqOptionApi.csproj`, `tests/IqOptionApi.Tests/IqOptionApi.Tests.csproj`, `tests/IqOptionApi.Tests/ScaffoldTests.cs`

**Interfaces:** Produces the projects every later task builds on. Tests can see internals (`InternalsVisibleTo`).

- [ ] **Step 1: Create projects**

```bash
cd IqOptionApi.CSharp
dotnet new sln -n IqOptionApi
dotnet new classlib -o src/IqOptionApi -f net9.0
dotnet new xunit -o tests/IqOptionApi.Tests -f net9.0
rm src/IqOptionApi/Class1.cs tests/IqOptionApi.Tests/UnitTest1.cs
dotnet sln add src/IqOptionApi/IqOptionApi.csproj tests/IqOptionApi.Tests/IqOptionApi.Tests.csproj
dotnet add tests/IqOptionApi.Tests reference src/IqOptionApi
```

- [ ] **Step 2: Write `.gitignore`**

```gitignore
bin/
obj/
TestResults/
.vs/
*.user
```

- [ ] **Step 3: Replace `src/IqOptionApi/IqOptionApi.csproj`**

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net9.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
    <GenerateDocumentationFile>true</GenerateDocumentationFile>
    <RootNamespace>IqOptionApi</RootNamespace>
    <AssemblyName>IqOptionApi</AssemblyName>
    <Version>9.4.0</Version>
    <Description>C# port of the Python iqoptionapi library (v9.4).</Description>
  </PropertyGroup>
  <ItemGroup>
    <InternalsVisibleTo Include="IqOptionApi.Tests" />
  </ItemGroup>
</Project>
```

- [ ] **Step 4: Edit `tests/IqOptionApi.Tests/IqOptionApi.Tests.csproj`** — keep the `PackageReference` lines the template generated; make the rest:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net9.0</TargetFramework>
    <Nullable>enable</Nullable>
    <ImplicitUsings>enable</ImplicitUsings>
    <IsPackable>false</IsPackable>
    <TreatWarningsAsErrors>true</TreatWarningsAsErrors>
  </PropertyGroup>
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
  </ItemGroup>
  <ItemGroup>
    <!-- template PackageReference lines stay here (coverlet, Microsoft.NET.Test.Sdk, xunit, xunit.runner.visualstudio) -->
  </ItemGroup>
  <ItemGroup>
    <Using Include="Xunit" />
  </ItemGroup>
  <ItemGroup>
    <Content Include="Golden\**\*.json" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\..\src\IqOptionApi\IqOptionApi.csproj" />
  </ItemGroup>
</Project>
```

- [ ] **Step 5: Write `tests/IqOptionApi.Tests/ScaffoldTests.cs`**

```csharp
namespace IqOptionApi.Tests;

public class ScaffoldTests
{
    [Fact]
    public void LibraryAssemblyLoads() =>
        Assert.Equal("IqOptionApi", typeof(IqOptionApi.Marker).Assembly.GetName().Name);
}
```

Run: `dotnet test IqOptionApi.sln` — Expected: build FAILS (`Marker` does not exist).

- [ ] **Step 6: Add `src/IqOptionApi/Helpers.cs` with a temporary marker**

```csharp
namespace IqOptionApi;

internal static class Marker
{
}
```

Run: `dotnet test IqOptionApi.sln` — Expected: 1 passed, 0 warnings.

- [ ] **Step 7: Commit**

```bash
git add IqOptionApi.CSharp
git commit -m "Scaffold C# solution, library and test projects"
```

---

### Task 2: Python-identical JSON writer and JSON helpers

**Files:**
- Modify: `src/IqOptionApi/Helpers.cs` (replace whole file)
- Test: `tests/IqOptionApi.Tests/PyJsonTests.cs`

**Interfaces:**
- Produces: `internal sealed class Obj` (ordered object builder: `new Obj { ["name"] = "x" }`), `internal readonly record struct PyFloat(double Value)` (always printed as a Python float), `internal static class PyJson { string Dumps(object?); string Frame(string name, object? msg, string requestId); string FloatRepr(double); string Num(double); string Str(double) }`, `internal static class J { Str, Dbl, DblOr, DblOrNull, Long, LongOr, IsTrue, Truthy, Arr, Obj }`.
- Value rules: `double` → integer text when integral and |v| < 1e16 (Python `int` passed raw), else Python float repr; `PyFloat` → always float repr (`1.0`); `int/long` → digits; `string` → ASCII-escaped like `ensure_ascii`; `IEnumerable` → list; `JsonNode` → copied (numbers keep int/float form).

- [ ] **Step 1: Write the failing tests** `tests/IqOptionApi.Tests/PyJsonTests.cs`

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class PyJsonTests
{
    [Fact]
    public void Frame_UsesPythonSeparatorsAndKeyOrder() =>
        Assert.Equal("{\"name\": \"sendMessage\", \"msg\": {\"name\": \"get-balances\", \"version\": \"1.0\"}, \"request_id\": \"\"}",
            PyJson.Frame("sendMessage", new Obj { ["name"] = "get-balances", ["version"] = "1.0" }, ""));

    [Theory]
    [InlineData(1.0, "1.0")]
    [InlineData(0.1, "0.1")]
    [InlineData(1e-05, "1e-05")]
    [InlineData(1e16, "1e+16")]
    [InlineData(1e15, "1000000000000000.0")]
    [InlineData(123.456, "123.456")]
    [InlineData(-0.001, "-0.001")]
    [InlineData(1700000747.0, "1700000747.0")]
    [InlineData(2.5e-300, "2.5e-300")]
    public void FloatRepr_MatchesPythonRepr(double value, string expected) =>
        Assert.Equal(expected, PyJson.FloatRepr(value));

    [Theory]
    [InlineData(1.0, "1")]
    [InlineData(1.5, "1.5")]
    [InlineData(10.0, "10")]
    public void RawNumber_IntegralPrintsLikePythonInt(double value, string expected) =>
        Assert.Equal(expected, PyJson.Dumps(value));

    [Fact]
    public void PyFloat_AlwaysHasFraction() => Assert.Equal("1.0", PyJson.Dumps(new PyFloat(1)));

    [Fact]
    public void Strings_AreAsciiEscapedLowercase() =>
        Assert.Equal("\"a\\\"b\\\\c\\n\\u00e9\\u007f\\u20ac\"", PyJson.Dumps("a\"b\\c\né\u007f€"));

    [Fact]
    public void Lists_NullsBools_AndEmptyKey() =>
        Assert.Equal("{\"a\": [1, \"x\", null, true, false], \"\": 2, \"b\": {}}",
            PyJson.Dumps(new Obj { ["a"] = new object?[] { 1, "x", null, true, false }, [""] = 2, ["b"] = new Obj() }));

    [Fact]
    public void JsonNode_KeepsIntAndFloatForms() =>
        Assert.Equal("{\"i\": 5, \"f\": 2.0, \"s\": \"t\"}", PyJson.Dumps(JsonNode.Parse("{\"i\":5,\"f\":2.0,\"s\":\"t\"}")));

    [Fact]
    public void J_ReadsNumbersStringsAndTruncatesLikePythonInt()
    {
        var o = JsonNode.Parse("{\"a\":\"2.5\",\"b\":7,\"c\":3.9,\"d\":true,\"e\":null}")!.AsObject();
        Assert.Equal(2.5, J.Dbl(o["a"]));
        Assert.Equal(7, J.Long(o["b"]));
        Assert.Equal(3, J.Long(o["c"]));
        Assert.True(J.IsTrue(o["d"]));
        Assert.Null(J.DblOrNull(o["e"]));
        Assert.Equal("7", J.Str(o["b"]));
    }
}
```

Run: `dotnet test IqOptionApi.sln --filter PyJsonTests` — Expected: build FAILS (`Obj`, `PyJson`, `J` missing).

- [ ] **Step 2: Replace `src/IqOptionApi/Helpers.cs`**

```csharp
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
```

- [ ] **Step 3: Run tests** — `dotnet test IqOptionApi.sln --filter PyJsonTests` — Expected: all PASS. If `FloatRepr(2.5e-300)` fails, check the `R` output parsing (exponent sign) before changing anything else.

- [ ] **Step 4: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add Python-identical JSON writer and JSON readers"
```

---

### Task 3: Golden generator (infrastructure + expiry data) and expiry maths

**Files:**
- Create: `tests/golden/generate_golden.py`
- Modify: `src/IqOptionApi/Helpers.cs` (add `Helpers` class)
- Create (generated): `tests/IqOptionApi.Tests/Golden/expiry_*.json`
- Test: `tests/IqOptionApi.Tests/ExpiryGoldenTests.cs`, `tests/IqOptionApi.Tests/Support/Golden.cs`

**Interfaces:**
- Produces (C#): `internal static class Helpers` with
  `(long Exp, int Index) GetExpirationTime(double timestamp, int duration, double now, TimeZoneInfo tz)`,
  `List<(int Label, long Seconds)> GetRemainingTime(double timestamp, double now, TimeZoneInfo tz)`,
  `(double Value, bool IsFloat) DigitalExp(long timestamp, int duration, double now, TimeZoneInfo tz)`,
  `long StrikeListExpiration(long timestamp, int duration, TimeZoneInfo tz)`,
  `string DigitalSpotInstrumentId(int activeId, double exp, int duration, Direction direction)` (added in Task 4 once `Direction` exists),
  `double PayoutFraction(double commission)`, `int BlitzPercent(double fraction)`, `double DigitalProfitPercent(double ask)`, `string StrikeKey(double value)`, `double PyRound2(double x)`, `TradeResult ResultOf(double profit)` (Task 4), `bool TryParseSpotMinutes(string symbol, out int minutes)`, `void DictQueueAdd(SortedDictionary<long, Candle>, int, long, Candle)` (Task 6), `double SpotProfitAfterSale(...)`.
- Produces (Python): `generate_golden.py` with `install_stubs()`, `pin_time(ts)`, `to_json(x)`, `replies_for(frame, rules)`, parts `expiry`, `tables`, `router`, `connect`, `methods` (later tasks fill `router`/`connect`/`methods`).
- Produces (tests): `Golden.Load(string file) → JsonObject`, `Golden.Zone(string ianaId) → TimeZoneInfo`.

- [ ] **Step 1: Write `tests/golden/generate_golden.py`**

```python
#!/usr/bin/env python3
"""Generate golden test data by running the REAL Python iqoptionapi code with a fake network.

Usage (from the repository root or anywhere):  python3 IqOptionApi.CSharp/tests/golden/generate_golden.py
Writes JSON files into IqOptionApi.CSharp/tests/IqOptionApi.Tests/Golden/.
Needs only the Python standard library: `requests` and `websocket` are replaced by stubs.
"""
import json
import os
import queue
import random
import subprocess
import sys
import threading
import time as _time
import types

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
OUT = os.path.abspath(os.path.join(HERE, "..", "IqOptionApi.Tests", "Golden"))
T = 1791549020            # pinned clock: 2026-10-09 12:30:20 UTC
METHOD_TZ = "Asia/Karachi"
EXPIRY_ZONES = ["UTC", "Asia/Karachi", "Asia/Kolkata", "Asia/Kathmandu", "Australia/Adelaide", "America/New_York"]

HTTP_LOG = []
_now = [float(T)]
_real_sleep = _time.sleep


def pin_time(ts):
    _now[0] = float(ts)


def install_stubs():
    """Replace `requests` and `websocket` so the library imports and runs offline."""
    requests = types.ModuleType("requests")

    class FakeResponse:
        def __init__(self, status_code=200, text="{}", cookies=None):
            self.status_code = status_code
            self.text = text
            self.cookies = cookies or {}
            self.headers = {}

        def raise_for_status(self):
            pass

    class FakeCookies(dict):
        def clear_session_cookies(self):
            pass

        def get_dict(self):
            return dict(self)

    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.cookies = FakeCookies()
            self.proxies = {}
            self.auth = None
            self.verify = True
            self.trust_env = True

        def request(self, method, url, data=None, params=None, headers=None, cookies=None):
            HTTP_LOG.append({"method": method, "url": url, "data": data, "headers": headers})
            if url.endswith("/api/v2/login"):
                return FakeResponse(200, '{"code":"success"}', {"ssid": "SSID-TEST"})
            return FakeResponse(200, "{}", {})

    requests.Session = FakeSession
    requests.packages = types.SimpleNamespace(urllib3=types.SimpleNamespace(disable_warnings=lambda *a, **k: None))
    requests.utils = types.SimpleNamespace(add_dict_to_cookiejar=lambda jar, d: jar.update(d))
    auth = types.ModuleType("requests.auth")
    auth.HTTPProxyAuth = lambda user, password: (user, password)
    requests.auth = auth
    sys.modules["requests"] = requests
    sys.modules["requests.auth"] = auth

    websocket = types.ModuleType("websocket")
    websocket.WebSocketApp = FakeWebSocketApp
    sys.modules["websocket"] = websocket

    _time.time = lambda: _now[0]
    _time.sleep = lambda s: _real_sleep(min(s, 0.001))
    sys.path.insert(0, REPO)


# ---------------------------------------------------------------- rules ----
def frame_key(frame):
    msg = frame.get("msg")
    if frame["name"] in ("sendMessage", "subscribeMessage", "unsubscribeMessage") \
            and isinstance(msg, dict) and isinstance(msg.get("name"), str):
        return frame["name"] + ":" + msg["name"]
    return frame["name"]


def subst(obj, rid):
    if isinstance(obj, dict):
        return {k: subst(v, rid) for k, v in obj.items()}
    if isinstance(obj, list):
        return [subst(v, rid) for v in obj]
    if obj == "$RID":
        return rid
    return obj


def replies_for(frame, rules):
    key = frame_key(frame)
    out = []
    for rule in rules:
        if rule["on"] == key:
            out.extend(subst(r, frame.get("request_id", "")) for r in rule["reply"])
    return out


# --------------------------------------------------------- fake socket ----
DELIVERY = queue.Queue()
CONNECT_RULES = []
FRAMES = []


class FakeWebSocketApp:
    """Stands in for websocket.WebSocketApp during the `connect` part."""

    def __init__(self, url, on_message=None, on_error=None, on_close=None, on_open=None, **kwargs):
        self.url = url
        self.on_message = on_message
        self.on_open = on_open
        self.sock = None

    def run_forever(self, **kwargs):
        self.on_open(self)

    def send(self, data):
        FRAMES.append(data)
        for reply in replies_for(json.loads(data), CONNECT_RULES):
            DELIVERY.put((self, json.dumps(reply)))

    def close(self):
        pass


def _delivery_worker():
    while True:
        app, raw = DELIVERY.get()
        _real_sleep(0.002)
        app.on_message(app, raw)


# ---------------------------------------------------------------- json ----
def to_json(x):
    if isinstance(x, dict):
        return {str(k) if not isinstance(k, float) else repr(k): to_json(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [to_json(v) for v in x]
    if hasattr(x, "maxlen"):   # deque
        return [to_json(v) for v in x]
    return x


def prune(x):
    if isinstance(x, dict):
        out = {}
        for k, v in x.items():
            v = prune(v)
            if v == {} or v == []:
                continue
            out[k] = v
        return out
    if isinstance(x, list):
        return [prune(v) for v in x]
    return x


def write(name, data):
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, name), "w") as f:
        json.dump(data, f, indent=1, sort_keys=False)
        f.write("\n")
    print("wrote", name)


# -------------------------------------------------------------- expiry ----
def part_expiry(tz):
    from iqoptionapi import expiration
    from iqoptionapi.ws.chanels.strike_list import Strike_list

    rnd = random.Random(42)
    start = 1767225600    # 2026-01-01 UTC
    cases = []
    while len(cases) < 300:
        ts = start + rnd.randrange(0, 365 * 86400)
        if _time.localtime(ts - 3 * 3600).tm_gmtoff != _time.localtime(ts + 14 * 3600).tm_gmtoff:
            continue   # skip windows that contain a DST change (spec §10)
        pin_time(ts)
        row = {"ts": ts, "exp": {}, "digital": {}, "strike": {}}
        for d in (1, 2, 3, 4, 5, 15, 30, 45, 60, 120):
            row["exp"][str(d)] = list(expiration.get_expiration_time(ts, d))
        row["remaining"] = [list(x) for x in expiration.get_remaning_time(ts)]
        tsf = ts + 0.37
        pin_time(tsf)
        row["remaining_f"] = [list(x) for x in expiration.get_remaning_time(tsf)]
        pin_time(ts)
        api = types.SimpleNamespace(timesync=types.SimpleNamespace(server_timestamp=ts))
        for d in (1, 5, 15, 30, 60):
            v = expiration.get_digital_exp(ts, d)
            row["digital"][str(d)] = [v, isinstance(v, float)]
            row["strike"][str(d)] = Strike_list(api).get_digital_expiration_time(d)
        cases.append(row)
    write("expiry_" + tz.replace("/", "_") + ".json", {"tz": tz, "cases": cases})


# ---------------------------------------------------- later parts -------
def part_tables():
    pass


def part_router():
    pass


def part_connect():
    pass


def part_methods():
    pass


def run_part(part, tz):
    env = dict(os.environ, TZ=tz)
    subprocess.run([sys.executable, os.path.abspath(__file__), "--part", part, "--tz", tz], env=env, check=True)


def main():
    if "--part" in sys.argv:
        part = sys.argv[sys.argv.index("--part") + 1]
        tz = sys.argv[sys.argv.index("--tz") + 1]
        _time.tzset()
        install_stubs()
        threading.Thread(target=_delivery_worker, daemon=True).start()
        {"expiry": lambda: part_expiry(tz), "tables": part_tables, "router": part_router,
         "connect": part_connect, "methods": part_methods}[part]()
        return
    for tz in EXPIRY_ZONES:
        run_part("expiry", tz)
    for part in ("tables", "router", "connect", "methods"):
        run_part(part, METHOD_TZ)


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Generate the expiry golden files**

Run: `python3 tests/golden/generate_golden.py`
Expected: six lines `wrote expiry_<zone>.json`; files exist under `tests/IqOptionApi.Tests/Golden/`.

- [ ] **Step 3: Write test support `tests/IqOptionApi.Tests/Support/Golden.cs`**

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

internal static class Golden
{
    public static JsonObject Load(string file) =>
        JsonNode.Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Golden", file)))!.AsObject();

    public static TimeZoneInfo Zone(string ianaId) => TimeZoneInfo.FindSystemTimeZoneById(ianaId);
}
```

- [ ] **Step 4: Write the failing test `tests/IqOptionApi.Tests/ExpiryGoldenTests.cs`**

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class ExpiryGoldenTests
{
    public static IEnumerable<object[]> Zones() =>
        new[] { "UTC", "Asia/Karachi", "Asia/Kolkata", "Asia/Kathmandu", "Australia/Adelaide", "America/New_York" }
            .Select(z => new object[] { z });

    [Theory, MemberData(nameof(Zones))]
    public void MatchesPython(string zone)
    {
        var data = Golden.Load("expiry_" + zone.Replace("/", "_") + ".json");
        var tz = Golden.Zone(zone);
        foreach (var row in data["cases"]!.AsArray().Select(r => r!.AsObject()))
        {
            long ts = (long)row["ts"]!;
            foreach (var (d, expected) in row["exp"]!.AsObject())
            {
                var (exp, idx) = Helpers.GetExpirationTime(ts, int.Parse(d), ts, tz);
                Assert.True((long)expected![0]! == exp && (int)expected[1]! == idx, $"exp ts={ts} d={d}: {exp},{idx} vs {expected.ToJsonString()}");
            }
            AssertRemaining(row["remaining"]!.AsArray(), Helpers.GetRemainingTime(ts, ts, tz), ts);
            AssertRemaining(row["remaining_f"]!.AsArray(), Helpers.GetRemainingTime(ts + 0.37, ts + 0.37, tz), ts);
            foreach (var (d, expected) in row["digital"]!.AsObject())
            {
                var (value, isFloat) = Helpers.DigitalExp(ts, int.Parse(d), ts, tz);
                Assert.True((double)expected![0]! == value && (bool)expected[1]! == isFloat, $"digital ts={ts} d={d}");
            }
            foreach (var (d, expected) in row["strike"]!.AsObject())
                Assert.Equal((long)expected!, Helpers.StrikeListExpiration(ts, int.Parse(d), tz));
        }
    }

    private static void AssertRemaining(JsonArray expected, List<(int Label, long Seconds)> actual, long ts)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < actual.Count; i++)
            Assert.True((int)expected[i]![0]! == actual[i].Label && (long)expected[i]![1]! == actual[i].Seconds, $"remaining ts={ts} i={i}");
    }

    [Theory]
    [InlineData(2.675, 2.67)]
    [InlineData(0.125, 0.12)]
    [InlineData(0.375, 0.38)]
    [InlineData(-1.005, -1.0)]
    [InlineData(12.344999, 12.34)]
    public void PyRound2_MatchesPythonRound(double x, double expected) => Assert.Equal(expected, Helpers.PyRound2(x));

    [Fact]
    public void Formulas_MatchPython()
    {
        Assert.Equal(0.8, Helpers.PayoutFraction(20));
        Assert.Equal(56, Helpers.BlitzPercent(Helpers.PayoutFraction(43)));
        Assert.Equal(57, Helpers.BlitzPercent(Helpers.PayoutFraction(42)));
        Assert.Equal("1.123450", Helpers.StrikeKey(1123450));
        Assert.Equal(((100 - 52.5) * 100) / 52.5, Helpers.DigitalProfitPercent(52.5));
        Assert.True(Helpers.TryParseSpotMinutes("do1A20210224D154900T1MCSPT", out int a) && a == 1);
        Assert.True(Helpers.TryParseSpotMinutes("doEURUSD201907191250PT5MPSPT", out int b) && b == 5);
        Assert.False(Helpers.TryParseSpotMinutes("nonsense", out _));
    }
}
```

Note: Python `int(0.58*100)` is 57 (`0.58*100 == 57.99999999999999`) and `int(0.57*100)` is 56 — the second assertion pins that truncation; adjust only if a Python one-liner (`python3 -c "print(int((100.0-42)/100.0*100))"`) disagrees, and then use the Python value.

Run: `dotnet test IqOptionApi.sln --filter "ExpiryGoldenTests"` — Expected: build FAILS (`Helpers` missing).

- [ ] **Step 5: Add the `Helpers` class to `src/IqOptionApi/Helpers.cs`** (append above `internal static class Marker`)

```csharp
/// <summary>Expiry maths and formulas ported 1:1 from the Python library (Cat §4).</summary>
internal static partial class Helpers
{
    /// <summary>Python <c>datetime.fromtimestamp(ts)</c> (naive local time).</summary>
    internal static DateTime LocalFromEpoch(double ts, TimeZoneInfo tz)
    {
        long micros = (long)Math.Round(ts * 1_000_000d, MidpointRounding.ToEven);
        var utc = DateTime.SpecifyKind(DateTime.UnixEpoch.AddTicks(micros * 10), DateTimeKind.Utc);
        return DateTime.SpecifyKind(TimeZoneInfo.ConvertTimeFromUtc(utc, tz), DateTimeKind.Unspecified);
    }

    /// <summary>Python <c>time.mktime(dt.timetuple())</c> (drops microseconds).</summary>
    internal static double ToEpoch(DateTime local, TimeZoneInfo tz)
    {
        var t = new DateTime(local.Year, local.Month, local.Day, local.Hour, local.Minute, local.Second, DateTimeKind.Unspecified);
        if (tz.IsInvalidTime(t)) t = t.AddHours(1);
        return (TimeZoneInfo.ConvertTimeToUtc(t, tz) - DateTime.UnixEpoch).TotalSeconds;
    }

    private static DateTime TruncMinute(DateTime d) => new(d.Year, d.Month, d.Day, d.Hour, d.Minute, 0, DateTimeKind.Unspecified);

    private static List<double> Candidates(double timestamp, int quarterCount, TimeZoneInfo tz)
    {
        var expDate = TruncMinute(LocalFromEpoch(timestamp, tz));
        expDate = (double)(long)ToEpoch(expDate.AddMinutes(1), tz) - timestamp > 30 ? expDate.AddMinutes(1) : expDate.AddMinutes(2);
        var exp = new List<double>();
        for (int i = 0; i < 5; i++)
        {
            exp.Add(ToEpoch(expDate, tz));
            expDate = expDate.AddMinutes(1);
        }
        int index = 0;
        expDate = TruncMinute(LocalFromEpoch(timestamp, tz));
        while (index < quarterCount)
        {
            if (expDate.Minute % 15 == 0 && (long)ToEpoch(expDate, tz) - (long)timestamp > 60 * 5)
            {
                exp.Add(ToEpoch(expDate, tz));
                index++;
            }
            expDate = expDate.AddMinutes(1);
        }
        return exp;
    }

    /// <summary>Python <c>expiration.get_expiration_time</c>; <paramref name="now"/> replaces <c>time.time()</c>.</summary>
    internal static (long Exp, int Index) GetExpirationTime(double timestamp, int duration, double now, TimeZoneInfo tz)
    {
        var exp = Candidates(timestamp, 50, tz);
        int best = 0;
        long bestDistance = long.MaxValue;
        for (int i = 0; i < exp.Count; i++)
        {
            long distance = Math.Abs(((long)exp[i] - (long)now) - 60L * duration);
            if (distance < bestDistance)
            {
                bestDistance = distance;
                best = i;
            }
        }
        return ((long)exp[best], best);
    }

    /// <summary>Python <c>expiration.get_remaning_time</c>.</summary>
    internal static List<(int Label, long Seconds)> GetRemainingTime(double timestamp, double now, TimeZoneInfo tz)
    {
        var exp = Candidates(timestamp, 11, tz);
        var result = new List<(int, long)>();
        for (int i = 0; i < exp.Count; i++)
            result.Add((i >= 5 ? 15 * (i - 4) : i + 1, (long)exp[i] - (long)now));
        return result;
    }

    /// <summary>Python <c>expiration.get_digital_exp</c>: int for duration 1, otherwise a float.</summary>
    internal static (double Value, bool IsFloat) DigitalExp(long timestamp, int duration, double now, TimeZoneInfo tz)
    {
        if (duration == 1) return (GetExpirationTime(timestamp, 1, now, tz).Exp, false);
        var d = LocalFromEpoch(timestamp, tz).AddMinutes(1).AddSeconds(30);
        while (!(d.Minute % duration == 0 && ToEpoch(d, tz) - timestamp > 30))
            d = d.AddMinutes(1);
        return (ToEpoch(d, tz), true);
    }

    /// <summary>Python <c>Strike_list.get_digital_expiration_time</c>.</summary>
    internal static long StrikeListExpiration(long timestamp, int duration, TimeZoneInfo tz)
    {
        int minute = LocalFromEpoch(timestamp, tz).Minute;
        long ans = timestamp - timestamp % 60;
        ans += (duration - minute % duration) * 60L;
        if (timestamp > ans - 10) ans += duration * 60L;
        return ans;
    }

    internal static double PayoutFraction(double commission) => (100.0 - commission) / 100.0;

    internal static int BlitzPercent(double fraction) => (int)(fraction * 100);

    internal static double DigitalProfitPercent(double ask) => ((100 - ask) * 100) / ask;

    internal static string StrikeKey(double value) => (value * 10e-7).ToString("F6", CultureInfo.InvariantCulture);

    internal static bool TryParseSpotMinutes(string symbol, out int minutes)
    {
        minutes = 0;
        string[] parts = symbol.Split('T');
        return parts.Length >= 2
            && int.TryParse(parts[1].Split('M')[0], NumberStyles.Integer, CultureInfo.InvariantCulture, out minutes);
    }

    /// <summary>Python <c>round(x, 2)</c>: half-even on the exact binary value.</summary>
    internal static double PyRound2(double x)
    {
        if (!double.IsFinite(x) || x == 0) return x;
        long bits = BitConverter.DoubleToInt64Bits(x);
        bool neg = bits < 0;
        int exp = (int)((bits >> 52) & 0x7FF);
        long mantissa = bits & 0xFFFFFFFFFFFFFL;
        if (exp == 0) exp = 1;
        else mantissa |= 1L << 52;
        exp -= 1075;
        BigInteger num = new BigInteger(mantissa) * 100, den = BigInteger.One;
        if (exp > 0) num <<= exp;
        else den <<= -exp;
        BigInteger q = BigInteger.DivRem(num, den, out BigInteger r);
        int cmp = (r * 2).CompareTo(den);
        if (cmp > 0 || (cmp == 0 && !q.IsEven)) q += 1;
        double result = (double)q / 100.0;
        return neg ? -result : result;
    }

    /// <summary>Digital sell-back value (Cat §4.7.8, Python bug with the call flag kept by callers).</summary>
    internal static double SpotProfitAfterSale(double strike, double lower, double upper, double f, double f2,
        bool isCall, double rate, double count, double amount)
    {
        double k = strike;
        if (lower != k)
        {
            if (lower > k || k > upper)
            {
                k = isCall ? (upper - k) / Math.Abs(upper - lower) : (k - upper) / Math.Abs(upper - lower);
                f = Math.Abs(f2 - f);
            }
            else if (isCall)
            {
                f += ((k - lower) / (upper - lower)) * (f2 - f);
            }
            else
            {
                k = (upper - k) / (upper - lower);
                f -= f2;
            }
            f = f2 + (k * f);
        }
        return (f / rate) * count - amount;
    }
}
```

- [ ] **Step 6: Run tests** — `dotnet test IqOptionApi.sln --filter "ExpiryGoldenTests"` — Expected: all PASS for all six zones. A mismatch means a porting error in `Candidates`/`DigitalExp`; compare the failing `ts` by running the Python function directly (`TZ=<zone> python3 -c ...`).

- [ ] **Step 7: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add golden generator with expiry data and port expiry maths"
```

---

### Task 4: Public types and asset/country tables

**Files:**
- Create: `src/IqOptionApi/Models.cs`, `tests/golden/gen_actives_cs.py`, `src/IqOptionApi/Actives.cs` (generated)
- Modify: `src/IqOptionApi/Helpers.cs` (add `DigitalSpotInstrumentId`, `ResultOf`, `DirectionText`; delete `Marker`), `tests/IqOptionApi.Tests/ScaffoldTests.cs` (point at `IqOption`… see Step 7)
- Test: `tests/IqOptionApi.Tests/TablesTests.cs`

**Interfaces:**
- Produces (public): `enum Direction { Call, Put }`, `enum BalanceMode { Real, Practice }`, `enum TradeResult { Win, Loss, Equal }`, `sealed record Candle` (`Id, From, To, At, Open, Close, Min, Max, Volume, Ask?, Bid?, Size?, ActiveId?, Raw`; `internal static Candle FromJson(JsonObject)`), `sealed record StrikeQuote(string CallId, double? CallProfit, string PutId, double? PutProfit)`, `static class InstrumentTypes`, `class IqOptionException(string)`, `class IqOptionOptions` (spec §4.1).
- Produces (internal): `sealed class ActivesTable { int Get(string); bool TryGet(string, out int); void Set(string, int); string? NameOf(int); IReadOnlyDictionary<string,int> Snapshot(); IReadOnlyList<KeyValuePair<string,int>> Items(); }`, `static class Countries { IReadOnlyDictionary<string,int> Ids }`.
- Produces (Helpers): `string DigitalSpotInstrumentId(int activeId, double exp, int duration, Direction d)`, `TradeResult ResultOf(double profit)`, `string DirectionText(Direction d)` (`"call"`/`"put"`).

- [ ] **Step 1: Write `src/IqOptionApi/Models.cs`**

```csharp
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

/// <summary>Settings for <see cref="IqOption"/>.</summary>
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
```

- [ ] **Step 2: Write `tests/golden/gen_actives_cs.py`** (generates `Actives.cs` from the Python tables, keeping order)

```python
#!/usr/bin/env python3
"""Write src/IqOptionApi/Actives.cs from iqoptionapi/constants.py and country_id.py (order preserved)."""
import importlib.util
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
TARGET = os.path.abspath(os.path.join(HERE, "..", "..", "src", "IqOptionApi", "Actives.cs"))


def load(name):
    spec = importlib.util.spec_from_file_location(name, os.path.join(REPO, "iqoptionapi", name + ".py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


actives = load("constants").ACTIVES
countries = load("country_id").ID
lines = []
lines.append("// <auto-generated> by tests/golden/gen_actives_cs.py from iqoptionapi/constants.py and country_id.py. Do not edit. </auto-generated>")
lines.append("namespace IqOptionApi;")
lines.append("")
lines.append("/// <summary>Asset name ↔ id table (insertion-ordered; reverse lookups return the first name, like Python).</summary>")
lines.append("internal sealed class ActivesTable")
lines.append("{")
lines.append("    internal static readonly (string Name, int Id)[] Defaults =")
lines.append("    {")
for name, value in actives.items():
    lines.append("        (%s, %d)," % (json.dumps(name), value))
lines.append("    };")
lines.append("")
lines.append("    private readonly OrderedDictionary<string, int> _map = new();")
lines.append("    private readonly object _lock = new();")
lines.append("")
lines.append("    public ActivesTable()")
lines.append("    {")
lines.append("        foreach (var (name, id) in Defaults) _map[name] = id;")
lines.append("    }")
lines.append("")
lines.append("    public int Get(string name)")
lines.append("    {")
lines.append("        lock (_lock)")
lines.append("        {")
lines.append("            if (_map.TryGetValue(name, out int id)) return id;")
lines.append("        }")
lines.append("        throw new KeyNotFoundException($\"Unknown asset '{name}'. Check the name or call UpdateActivesAsync() to load the latest list.\");")
lines.append("    }")
lines.append("")
lines.append("    public bool TryGet(string name, out int id)")
lines.append("    {")
lines.append("        lock (_lock) return _map.TryGetValue(name, out id);")
lines.append("    }")
lines.append("")
lines.append("    public void Set(string name, int id)")
lines.append("    {")
lines.append("        lock (_lock) _map[name] = id;")
lines.append("    }")
lines.append("")
lines.append("    public string? NameOf(int id)")
lines.append("    {")
lines.append("        lock (_lock)")
lines.append("        {")
lines.append("            foreach (var pair in _map)")
lines.append("            {")
lines.append("                if (pair.Value == id) return pair.Key;")
lines.append("            }")
lines.append("            return null;")
lines.append("        }")
lines.append("    }")
lines.append("")
lines.append("    public IReadOnlyList<KeyValuePair<string, int>> Items()")
lines.append("    {")
lines.append("        lock (_lock) return _map.ToList();")
lines.append("    }")
lines.append("")
lines.append("    public IReadOnlyDictionary<string, int> Snapshot()")
lines.append("    {")
lines.append("        var copy = new OrderedDictionary<string, int>();")
lines.append("        foreach (var pair in Items()) copy[pair.Key] = pair.Value;")
lines.append("        return copy;")
lines.append("    }")
lines.append("}")
lines.append("")
lines.append("/// <summary>Country code → IQ Option country id (leaderboard).</summary>")
lines.append("internal static class Countries")
lines.append("{")
lines.append("    public static readonly IReadOnlyDictionary<string, int> Ids = new Dictionary<string, int>")
lines.append("    {")
for name, value in countries.items():
    lines.append("        [%s] = %d," % (json.dumps(name), value))
lines.append("    };")
lines.append("}")
with open(TARGET, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")
print("wrote", TARGET, len(actives), "assets,", len(countries), "countries")
```

Run: `python3 tests/golden/gen_actives_cs.py` — Expected: `wrote …/Actives.cs 198 assets, 190 countries`.

- [ ] **Step 3: Fill `part_tables` in `generate_golden.py`** (replace the `def part_tables(): pass` stub)

```python
def part_tables():
    import iqoptionapi.constants as OP_code
    import iqoptionapi.country_id as Country
    write("tables.json", {"actives": [[k, v] for k, v in OP_code.ACTIVES.items()],
                          "countries": [[k, v] for k, v in Country.ID.items()]})
```

Run: `python3 tests/golden/generate_golden.py` — Expected: also `wrote tables.json`.

- [ ] **Step 4: Write the failing test `tests/IqOptionApi.Tests/TablesTests.cs`**

```csharp
namespace IqOptionApi.Tests;

public class TablesTests
{
    [Fact]
    public void ActivesMatchPythonInOrder()
    {
        var expected = Golden.Load("tables.json")["actives"]!.AsArray();
        var actual = new ActivesTable().Items();
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < actual.Count; i++)
        {
            Assert.Equal((string)expected[i]![0]!, actual[i].Key);
            Assert.Equal((int)expected[i]![1]!, actual[i].Value);
        }
    }

    [Fact]
    public void CountriesMatchPython()
    {
        foreach (var row in Golden.Load("tables.json")["countries"]!.AsArray())
            Assert.Equal((int)row![1]!, Countries.Ids[(string)row[0]!]);
    }

    [Fact]
    public void Get_UnknownName_Throws()
    {
        var ex = Assert.Throws<KeyNotFoundException>(() => new ActivesTable().Get("EURUSDD"));
        Assert.Contains("EURUSDD", ex.Message);
    }

    [Fact]
    public void Set_ExistingKeepsPosition_AndReverseLookupReturnsFirstName()
    {
        var t = new ActivesTable();
        int index = t.Items().ToList().FindIndex(p => p.Key == "EURUSD");
        t.Set("EURUSD", 76);
        Assert.Equal(index, t.Items().ToList().FindIndex(p => p.Key == "EURUSD"));
        Assert.Equal(t.Items().First(p => p.Value == 76).Key, t.NameOf(76));
        t.Set("NEWASSET", 9999);
        Assert.Equal("NEWASSET", t.Items()[^1].Key);
    }

    [Fact]
    public void DigitalSpotInstrumentId_UsesUtcAndNewFormat() =>
        Assert.Equal("do1A20210224D154900T1MCSPT", Helpers.DigitalSpotInstrumentId(1, 1614181740, 1, Direction.Call));

    [Fact]
    public void ResultOf_UsesSign()
    {
        Assert.Equal(TradeResult.Win, Helpers.ResultOf(0.8));
        Assert.Equal(TradeResult.Loss, Helpers.ResultOf(-1));
        Assert.Equal(TradeResult.Equal, Helpers.ResultOf(0));
    }
}
```

(`1614181740` = 2021-02-24 15:49:00 UTC.)

Run: `dotnet test IqOptionApi.sln --filter TablesTests` — Expected: build FAILS (`DigitalSpotInstrumentId`, `ResultOf` missing).

- [ ] **Step 5: Add to `Helpers` (in `Helpers.cs`) and delete the `Marker` class**

```csharp
internal static partial class Helpers
{
    internal static string DirectionText(Direction d) => d == Direction.Call ? "call" : "put";

    internal static TradeResult ResultOf(double profit) =>
        profit > 0 ? TradeResult.Win : profit < 0 ? TradeResult.Loss : TradeResult.Equal;

    /// <summary>New-format spot id, e.g. <c>do1A20210224D154900T1MCSPT</c> (date in UTC, like Python <c>utcfromtimestamp</c>).</summary>
    internal static string DigitalSpotInstrumentId(int activeId, double exp, int duration, Direction d)
    {
        var utc = DateTime.UnixEpoch.AddSeconds(Math.Floor(exp));
        return "do" + activeId.ToString(CultureInfo.InvariantCulture) + "A"
            + utc.ToString("yyyyMMdd", CultureInfo.InvariantCulture) + "D" + utc.ToString("HHmm", CultureInfo.InvariantCulture)
            + "00T" + duration.ToString(CultureInfo.InvariantCulture) + "M" + (d == Direction.Call ? "C" : "P") + "SPT";
    }
}
```

- [ ] **Step 6: Point the scaffold test at a real type** — in `ScaffoldTests.cs` replace `typeof(IqOptionApi.Marker)` with `typeof(IqOptionApi.Candle)`.

- [ ] **Step 7: Run all tests** — `dotnet test IqOptionApi.sln` — Expected: all PASS, 0 warnings.

- [ ] **Step 8: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add public models and generated asset/country tables"
```

---

### Task 5: Connection core — socket, sending, request matching

**Files:**
- Create: `src/IqOptionApi/Connection.cs`
- Create: `tests/IqOptionApi.Tests/Support/FakeSocket.cs`, `tests/IqOptionApi.Tests/Support/FakeHttpHandler.cs`
- Test: `tests/IqOptionApi.Tests/ConnectionTests.cs`

**Interfaces:**
- Consumes: `PyJson.Frame`, `J.Str`, `IqOptionOptions`, `IqOptionException`.
- Produces:
  - `internal interface ISocket : IAsyncDisposable { Task ConnectAsync(Uri, CancellationToken); Task SendAsync(string, CancellationToken); Task<string?> ReceiveAsync(CancellationToken); }`
  - `internal sealed class AsyncPulse { Task Next { get; } void Pulse(); }`
  - `internal sealed class Waiter : IDisposable { string Name; string? RequestId; Task<JsonObject> Task; }`
  - `internal sealed class Connection : IDisposable` with: `Connection(IqOptionOptions, Func<ISocket>?, HttpMessageHandler?, Action<string> log)`, `AsyncPulse Pulse`, `bool IsOpen`, `bool Ready {get;set;}` (setter pulses), `Dictionary<string,string> Headers`, `Dictionary<string,string> Cookies`, `const string DefaultUserAgent`, `event Action<JsonObject>? MessageReceived`, `event Action<string>? Closed`, `string NextRequestId()`, `Task OpenAsync(CancellationToken)`, `Task CloseSocketAsync()`, `void Dispatch(string)`, `Waiter AddWaiter(string name, string? requestId)`, `SemaphoreSlim Gate(string replyName)`, `Task SendAsync(string outer, object? msg, string requestId, CancellationToken ct, bool requireReady = true)`, `Task<JsonObject> RequestAsync(string outer, object? msg, string requestId, string replyName, bool byRequestId, TimeSpan timeout, CancellationToken ct, bool requireReady = true)`, `Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout, CancellationToken ct)`, `Task<(int Status, string Body, string? Ssid)> LoginAsync(string email, string password, string? token, CancellationToken ct)`, `Task<string> TwoFactorAsync(string token, string? method, string? code, CancellationToken ct)`, `Task LogoutAsync(CancellationToken ct)`.
  - Test support: `FakeSocket` (records `Frames`, answers by `Rules`, `Push(string)`, `Drop()`), `Rule` (`On`, `Replies`, `static List<Rule> Parse(JsonNode?)`, `static IEnumerable<string> RepliesFor(JsonObject frame, IEnumerable<Rule>)`), `FakeHttpHandler` (records `Requests`; login answers 200 + `Set-Cookie: ssid=SSID-TEST`).

- [ ] **Step 1: Write `tests/IqOptionApi.Tests/Support/FakeSocket.cs`**

```csharp
using System.Text.Json.Nodes;
using System.Threading.Channels;

namespace IqOptionApi.Tests;

/// <summary>Reply rule shared with generate_golden.py: when a frame with key <see cref="On"/> is sent, push <see cref="Replies"/> ("$RID" → the frame's request_id).</summary>
internal sealed record Rule(string On, List<JsonObject> Replies)
{
    public static string KeyOf(JsonObject frame)
    {
        string outer = (string)frame["name"]!;
        if (outer is "sendMessage" or "subscribeMessage" or "unsubscribeMessage"
            && frame["msg"] is JsonObject m && m["name"] is JsonValue v && v.TryGetValue(out string? inner))
            return outer + ":" + inner;
        return outer;
    }

    public static List<Rule> Parse(JsonNode? rules) =>
        rules?.AsArray().Select(r => new Rule((string)r!["on"]!, r["reply"]!.AsArray().Select(x => x!.AsObject()).ToList())).ToList()
        ?? new List<Rule>();

    public static IEnumerable<string> RepliesFor(JsonObject frame, IEnumerable<Rule> rules)
    {
        string key = KeyOf(frame);
        string rid = frame["request_id"]?.GetValue<string>() ?? "";
        foreach (var rule in rules.Where(r => r.On == key))
            foreach (var reply in rule.Replies)
                yield return Subst(reply, rid)!.ToJsonString();
    }

    private static JsonNode? Subst(JsonNode? n, string rid) => n switch
    {
        JsonObject o => new JsonObject(o.Select(p => KeyValuePair.Create(p.Key, Subst(p.Value, rid)))),
        JsonArray a => new JsonArray(a.Select(x => Subst(x, rid)).ToArray()),
        JsonValue v when v.TryGetValue(out string? s) && s == "$RID" => JsonValue.Create(rid),
        _ => n?.DeepClone(),
    };
}

internal sealed class FakeSocket : ISocket
{
    private readonly Channel<string?> _incoming = Channel.CreateUnbounded<string?>();

    public List<string> Frames { get; } = new();

    public List<Rule> Rules { get; set; } = new();

    public Uri? ConnectedTo { get; private set; }

    public string[] TakeFrames()
    {
        lock (Frames)
        {
            var copy = Frames.ToArray();
            Frames.Clear();
            return copy;
        }
    }

    public Task ConnectAsync(Uri uri, CancellationToken ct)
    {
        ConnectedTo = uri;
        return Task.CompletedTask;
    }

    public Task SendAsync(string text, CancellationToken ct)
    {
        lock (Frames) Frames.Add(text);
        foreach (var reply in Rule.RepliesFor(JsonNode.Parse(text)!.AsObject(), Rules)) Push(reply);
        return Task.CompletedTask;
    }

    public void Push(string json) => _incoming.Writer.TryWrite(json);

    public void Drop() => _incoming.Writer.TryWrite(null);

    public async Task<string?> ReceiveAsync(CancellationToken ct) => await _incoming.Reader.ReadAsync(ct);

    public ValueTask DisposeAsync()
    {
        _incoming.Writer.TryComplete();
        return ValueTask.CompletedTask;
    }
}
```

- [ ] **Step 2: Write `tests/IqOptionApi.Tests/Support/FakeHttpHandler.cs`**

```csharp
using System.Net;

namespace IqOptionApi.Tests;

internal sealed class FakeHttpHandler : HttpMessageHandler
{
    public List<(string Method, string Url, string Body, string Headers)> Requests { get; } = new();

    public Func<HttpRequestMessage, string, HttpResponseMessage>? Respond { get; set; }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
    {
        string body = request.Content == null ? "" : await request.Content.ReadAsStringAsync(ct);
        lock (Requests) Requests.Add((request.Method.Method, request.RequestUri!.ToString(), body, request.Headers.ToString()));
        if (Respond != null) return Respond(request, body);
        var resp = new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"code\":\"success\"}") };
        if (request.RequestUri!.AbsolutePath.EndsWith("/api/v2/login", StringComparison.Ordinal))
            resp.Headers.Add("Set-Cookie", "ssid=SSID-TEST; Path=/; HttpOnly");
        return resp;
    }
}
```

- [ ] **Step 3: Write the failing tests `tests/IqOptionApi.Tests/ConnectionTests.cs`**

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class ConnectionTests
{
    private static (Connection Conn, FakeSocket Socket) Open(TimeSpan? timeout = null)
    {
        var socket = new FakeSocket();
        var options = new IqOptionOptions { RequestTimeout = timeout ?? TimeSpan.FromSeconds(2) };
        var conn = new Connection(options, () => socket, new FakeHttpHandler(), _ => { });
        conn.OpenAsync(default).GetAwaiter().GetResult();
        conn.Ready = true;
        return (conn, socket);
    }

    [Fact]
    public void RequestIds_StartAtOneAndResetOnOpen()
    {
        var (conn, _) = Open();
        Assert.Equal("1", conn.NextRequestId());
        Assert.Equal("2", conn.NextRequestId());
        conn.OpenAsync(default).GetAwaiter().GetResult();
        Assert.Equal("1", conn.NextRequestId());
    }

    [Fact]
    public async Task RequestById_MatchesOnlyItsOwnRequestId()
    {
        var (conn, socket) = Open();
        var a = conn.RequestAsync("sendMessage", new Obj { ["name"] = "x" }, "7", "option", true, TimeSpan.FromSeconds(2), default);
        var b = conn.RequestAsync("sendMessage", new Obj { ["name"] = "x" }, "8", "option", true, TimeSpan.FromSeconds(2), default);
        socket.Push("{\"name\":\"option\",\"request_id\":\"8\",\"msg\":{\"id\":2}}");
        socket.Push("{\"name\":\"option\",\"request_id\":\"7\",\"msg\":{\"id\":1}}");
        Assert.Equal(1, (int)(await a)["msg"]!["id"]!);
        Assert.Equal(2, (int)(await b)["msg"]!["id"]!);
        Assert.Equal("{\"name\": \"sendMessage\", \"msg\": {\"name\": \"x\"}, \"request_id\": \"7\"}", socket.Frames[0]);
    }

    [Fact]
    public async Task RequestByName_SerialisesSameReplyName()
    {
        var (conn, socket) = Open();
        var first = conn.RequestAsync("sendMessage", new Obj { ["name"] = "get-positions" }, "", "positions", false, TimeSpan.FromSeconds(2), default);
        var second = conn.RequestAsync("sendMessage", new Obj { ["name"] = "portfolio.get-positions" }, "", "positions", false, TimeSpan.FromSeconds(2), default);
        await Task.Delay(100);
        Assert.Single(socket.Frames);   // second waits for the first answer before sending
        socket.Push("{\"name\":\"positions\",\"msg\":{\"n\":1}}");
        Assert.Equal(1, (int)(await first)["msg"]!["n"]!);
        await Task.Delay(100);
        Assert.Equal(2, socket.Frames.Count);
        socket.Push("{\"name\":\"positions\",\"msg\":{\"n\":2}}");
        Assert.Equal(2, (int)(await second)["msg"]!["n"]!);
    }

    [Fact]
    public async Task RequestAsync_TimesOut()
    {
        var (conn, socket) = Open();
        socket.Push("{\"name\":\"result\",\"request_id\":\"5\",\"msg\":{\"success\":false}}");
        await Assert.ThrowsAsync<TimeoutException>(() =>
            conn.RequestAsync("sendMessage", new Obj(), "5", "option", true, TimeSpan.FromMilliseconds(200), default));
    }

    [Fact]
    public async Task Drop_FailsWaitersWithConnectionLost_AndRaisesClosed()
    {
        var (conn, socket) = Open();
        string? closed = null;
        conn.Closed += r => closed = r;
        var pending = conn.RequestAsync("sendMessage", new Obj(), "3", "option", true, TimeSpan.FromSeconds(5), default);
        socket.Drop();
        var ex = await Assert.ThrowsAsync<IqOptionException>(() => pending);
        Assert.Equal("connection lost", ex.Message);
        await Task.Delay(50);
        Assert.NotNull(closed);
        Assert.False(conn.IsOpen);
    }

    [Fact]
    public async Task WaitUntil_WakesOnMessages()
    {
        var (conn, socket) = Open();
        int seen = 0;
        conn.MessageReceived += _ => Interlocked.Increment(ref seen);
        var wait = conn.WaitUntilAsync(() => Volatile.Read(ref seen) >= 2, TimeSpan.FromSeconds(2), default);
        socket.Push("{\"name\":\"a\"}");
        socket.Push("{\"name\":\"b\"}");
        await wait;
        await Assert.ThrowsAsync<TimeoutException>(() => conn.WaitUntilAsync(() => false, TimeSpan.FromMilliseconds(100), default));
    }

    [Fact]
    public async Task GarbageMessage_IsSkipped()
    {
        var (conn, socket) = Open();
        socket.Push("not json");
        var reply = conn.RequestAsync("sendMessage", new Obj(), "1", "option", true, TimeSpan.FromSeconds(2), default);
        socket.Push("{\"name\":\"option\",\"request_id\":\"1\",\"msg\":{}}");
        await reply;
    }

    [Fact]
    public async Task Login_SendsFormAndReadsSsidCookie()
    {
        var http = new FakeHttpHandler();
        var conn = new Connection(new IqOptionOptions(), () => new FakeSocket(), http, _ => { });
        var (status, _, ssid) = await conn.LoginAsync("a@b.com", "p w", null, default);
        Assert.Equal(200, status);
        Assert.Equal("SSID-TEST", ssid);
        Assert.Equal("https://auth.iqoption.com/api/v2/login", http.Requests[0].Url);
        Assert.Equal("identifier=a%40b.com&password=p+w", http.Requests[0].Body);
        Assert.Contains("Chrome/66.0.3359.139", http.Requests[0].Headers);
        await conn.LogoutAsync(default);
        Assert.Contains("ssid=SSID-TEST", http.Requests[1].Headers);
    }
}
```

Run: `dotnet test IqOptionApi.sln --filter ConnectionTests` — Expected: build FAILS (`Connection` missing).

- [ ] **Step 4: Write `src/IqOptionApi/Connection.cs`**

```csharp
using System.Collections.Concurrent;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IqOptionApi;

/// <summary>Minimal text WebSocket (a fake replaces it in tests).</summary>
internal interface ISocket : IAsyncDisposable
{
    Task ConnectAsync(Uri uri, CancellationToken ct);

    Task SendAsync(string text, CancellationToken ct);

    /// <summary>Next text message, or null once the socket closed.</summary>
    Task<string?> ReceiveAsync(CancellationToken ct);
}

internal sealed class ClientWebSocketAdapter : ISocket
{
    private readonly ClientWebSocket _ws = new();

    public ClientWebSocketAdapter(IqOptionOptions o, string origin)
    {
        _ws.Options.SetRequestHeader("Origin", origin);
        _ws.Options.KeepAliveInterval = TimeSpan.Zero;
        if (o.ProxyHost != null) _ws.Options.Proxy = Connection.BuildProxy(o);
        if (o.IgnoreCertificateErrors) _ws.Options.RemoteCertificateValidationCallback = (_, _, _, _) => true;
    }

    public Task ConnectAsync(Uri uri, CancellationToken ct) => _ws.ConnectAsync(uri, ct);

    public Task SendAsync(string text, CancellationToken ct) =>
        _ws.SendAsync(Encoding.UTF8.GetBytes(text), WebSocketMessageType.Text, true, ct);

    public async Task<string?> ReceiveAsync(CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        using var ms = new MemoryStream();
        while (true)
        {
            WebSocketReceiveResult r;
            try
            {
                r = await _ws.ReceiveAsync(buffer, ct).ConfigureAwait(false);
            }
            catch (WebSocketException)
            {
                return null;
            }
            if (r.MessageType == WebSocketMessageType.Close) return null;
            ms.Write(buffer, 0, r.Count);
            if (r.EndOfMessage) return Encoding.UTF8.GetString(ms.GetBuffer(), 0, (int)ms.Length);
        }
    }

    public async ValueTask DisposeAsync()
    {
        try
        {
            if (_ws.State == WebSocketState.Open)
            {
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                await _ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", cts.Token).ConfigureAwait(false);
            }
        }
        catch (Exception)
        {
            // closing a broken socket is best effort
        }
        _ws.Dispose();
    }
}

/// <summary>Completes <see cref="Next"/> every time something changes, so waiters can re-check their condition.</summary>
internal sealed class AsyncPulse
{
    private TaskCompletionSource _tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task Next => Volatile.Read(ref _tcs).Task;

    public void Pulse() =>
        Interlocked.Exchange(ref _tcs, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).TrySetResult();
}

/// <summary>Waits for the next message with a name (and optionally a request id).</summary>
internal sealed class Waiter : IDisposable
{
    private readonly Connection _owner;

    internal Waiter(Connection owner, string name, string? requestId)
    {
        _owner = owner;
        Name = name;
        RequestId = requestId;
    }

    public string Name { get; }

    public string? RequestId { get; }

    internal TaskCompletionSource<JsonObject> Source { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public Task<JsonObject> Task => Source.Task;

    public void Dispose() => _owner.RemoveWaiter(this);
}

/// <summary>HTTP login + one WebSocket + reply matching. Knows nothing about IQ Option methods.</summary>
internal sealed class Connection : IDisposable
{
    public const string DefaultUserAgent =
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36";

    private readonly IqOptionOptions _o;
    private readonly Func<ISocket> _socketFactory;
    private readonly HttpClient _http;
    private readonly Action<string> _log;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new();
    private readonly object _waitLock = new();
    private readonly List<Waiter> _waiters = new();
    private ISocket? _socket;
    private CancellationTokenSource? _loopCts;
    private int _requestCounter;
    private bool _ready;

    internal Connection(IqOptionOptions o, Func<ISocket>? socketFactory, HttpMessageHandler? httpHandler, Action<string> log)
    {
        _o = o;
        _log = log;
        WebSocketUri = new Uri(o.WebSocketUrl ?? $"wss://{o.Host}/echo/websocket");
        _socketFactory = socketFactory ?? (() => new ClientWebSocketAdapter(o, "http://" + o.Host));
        _http = new HttpClient(httpHandler ?? BuildHandler(o), disposeHandler: true);
    }

    public event Action<JsonObject>? MessageReceived;

    public event Action<string>? Closed;

    public Uri WebSocketUri { get; }

    public AsyncPulse Pulse { get; } = new();

    public bool IsOpen { get; private set; }

    /// <summary>True once the connect sequence finished; normal requests wait for it.</summary>
    public bool Ready
    {
        get => _ready;
        set
        {
            _ready = value;
            Pulse.Pulse();
        }
    }

    public Dictionary<string, string> Headers { get; } = new() { ["User-Agent"] = DefaultUserAgent };

    public Dictionary<string, string> Cookies { get; } = new();

    private string AuthBase => _o.AuthUrl.TrimEnd('/');

    internal static IWebProxy BuildProxy(IqOptionOptions o) =>
        new WebProxy(o.ProxyHost!, o.ProxyPort ?? 80)
        {
            Credentials = o.ProxyUser != null ? new NetworkCredential(o.ProxyUser, o.ProxyPassword) : null,
        };

    private static HttpMessageHandler BuildHandler(IqOptionOptions o)
    {
        var h = new SocketsHttpHandler
        {
            UseCookies = false,
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            UseProxy = o.ProxyHost != null,
        };
        if (o.ProxyHost != null) h.Proxy = BuildProxy(o);
        if (o.IgnoreCertificateErrors)
            h.SslOptions = new SslClientAuthenticationOptions { RemoteCertificateValidationCallback = (_, _, _, _) => true };
        return h;
    }

    // ------------------------------------------------------------------ HTTP

    public async Task<(int Status, string Body, string? Ssid)> LoginAsync(string email, string password, string? token, CancellationToken ct)
    {
        var fields = new List<KeyValuePair<string, string>> { new("identifier", email), new("password", password) };
        if (token != null) fields.Add(new("token", token));
        using var req = new HttpRequestMessage(HttpMethod.Post, AuthBase + "/api/v2/login") { Content = new FormUrlEncodedContent(fields) };
        using var resp = await SendHttpAsync(req, ct).ConfigureAwait(false);
        string body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
        string? ssid = ReadCookie(resp, "ssid");
        if (ssid != null) Cookies["ssid"] = ssid;
        return ((int)resp.StatusCode, body, ssid);
    }

    public async Task<string> TwoFactorAsync(string token, string? method, string? code, CancellationToken ct)
    {
        var content = new StringContent(PyJson.Dumps(new Obj { ["method"] = method, ["token"] = token, ["code"] = code }), Encoding.UTF8);
        content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        using var req = new HttpRequestMessage(HttpMethod.Post, AuthBase + "/api/v2/verify/2fa") { Content = content };
        using var resp = await SendHttpAsync(req, ct).ConfigureAwait(false);
        return await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
    }

    public async Task LogoutAsync(CancellationToken ct)
    {
        using var req = new HttpRequestMessage(HttpMethod.Post, AuthBase + "/api/v1.0/logout");
        using var resp = await SendHttpAsync(req, ct).ConfigureAwait(false);
    }

    private async Task<HttpResponseMessage> SendHttpAsync(HttpRequestMessage req, CancellationToken ct)
    {
        foreach (var (key, value) in Headers) req.Headers.TryAddWithoutValidation(key, value);
        if (Cookies.Count > 0)
            req.Headers.TryAddWithoutValidation("Cookie", string.Join("; ", Cookies.Select(c => c.Key + "=" + c.Value)));
        return await _http.SendAsync(req, ct).ConfigureAwait(false);
    }

    private static string? ReadCookie(HttpResponseMessage resp, string name)
    {
        if (!resp.Headers.TryGetValues("Set-Cookie", out var values)) return null;
        foreach (string v in values)
        {
            string first = v.Split(';')[0];
            int eq = first.IndexOf('=');
            if (eq > 0 && first[..eq].Trim() == name) return first[(eq + 1)..].Trim();
        }
        return null;
    }

    // ------------------------------------------------------------- WebSocket

    public string NextRequestId() => Interlocked.Increment(ref _requestCounter).ToString(CultureInfo.InvariantCulture);

    public async Task OpenAsync(CancellationToken ct)
    {
        await CloseSocketAsync().ConfigureAwait(false);
        var socket = _socketFactory();
        await socket.ConnectAsync(WebSocketUri, ct).ConfigureAwait(false);
        Interlocked.Exchange(ref _requestCounter, 0);
        var cts = new CancellationTokenSource();
        _socket = socket;
        _loopCts = cts;
        IsOpen = true;
        _ = Task.Run(() => ReceiveLoopAsync(socket, cts.Token), CancellationToken.None);
        Pulse.Pulse();
    }

    public async Task CloseSocketAsync()
    {
        var socket = _socket;
        var cts = _loopCts;
        _socket = null;
        _loopCts = null;
        IsOpen = false;
        _ready = false;
        cts?.Cancel();
        if (socket != null)
        {
            try
            {
                await socket.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _log("socket close: " + ex.Message);
            }
        }
        FailWaiters("connection closed");
        Pulse.Pulse();
    }

    private async Task ReceiveLoopAsync(ISocket socket, CancellationToken ct)
    {
        string reason = "connection closed by server";
        try
        {
            while (!ct.IsCancellationRequested)
            {
                string? text = await socket.ReceiveAsync(ct).ConfigureAwait(false);
                if (text == null) break;
                Dispatch(text);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            return;
        }
        catch (Exception ex)
        {
            reason = ex.Message;
        }
        if (ct.IsCancellationRequested || !ReferenceEquals(socket, _socket)) return;
        IsOpen = false;
        _ready = false;
        FailWaiters("connection lost");
        Pulse.Pulse();
        try
        {
            Closed?.Invoke(reason);
        }
        catch (Exception ex)
        {
            _log("Closed handler threw: " + ex.Message);
        }
    }

    internal void Dispatch(string text)
    {
        if (_o.LogFrames) _log("< " + text);
        JsonObject? message;
        try
        {
            message = JsonNode.Parse(text) as JsonObject;
        }
        catch (JsonException ex)
        {
            _log("unreadable message skipped: " + ex.Message);
            return;
        }
        if (message == null) return;
        try
        {
            MessageReceived?.Invoke(message);
        }
        catch (Exception ex)
        {
            _log("message handler threw: " + ex.Message);
        }
        CompleteWaiters(message);
        Pulse.Pulse();
    }

    // --------------------------------------------------------------- waiting

    public Waiter AddWaiter(string name, string? requestId)
    {
        var w = new Waiter(this, name, requestId);
        lock (_waitLock) _waiters.Add(w);
        return w;
    }

    internal void RemoveWaiter(Waiter w)
    {
        lock (_waitLock) _waiters.Remove(w);
    }

    public SemaphoreSlim Gate(string replyName) => _gates.GetOrAdd(replyName, _ => new SemaphoreSlim(1, 1));

    private void CompleteWaiters(JsonObject m)
    {
        string? name = J.Str(m["name"]);
        string rid = J.Str(m["request_id"]) ?? "";
        var hits = new List<Waiter>();
        lock (_waitLock)
        {
            foreach (var w in _waiters)
            {
                if (w.Name == name && (w.RequestId == null || w.RequestId == rid)) hits.Add(w);
            }
            foreach (var w in hits) _waiters.Remove(w);
        }
        foreach (var w in hits) w.Source.TrySetResult(m);
    }

    private void FailWaiters(string reason)
    {
        List<Waiter> all;
        lock (_waitLock)
        {
            all = _waiters.ToList();
            _waiters.Clear();
        }
        foreach (var w in all) w.Source.TrySetException(new IqOptionException(reason));
    }

    public async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout, CancellationToken ct)
    {
        var deadline = timeout is TimeSpan t ? DateTime.UtcNow + t : DateTime.MaxValue;
        while (true)
        {
            var next = Pulse.Next;
            if (condition()) return;
            if (timeout == null)
            {
                await next.WaitAsync(ct).ConfigureAwait(false);
                continue;
            }
            var left = deadline - DateTime.UtcNow;
            if (left <= TimeSpan.Zero) throw new TimeoutException("timed out waiting for the server");
            try
            {
                await next.WaitAsync(left, ct).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                if (condition()) return;
                throw;
            }
        }
    }

    // --------------------------------------------------------------- sending

    public async Task SendAsync(string outer, object? msg, string requestId, CancellationToken ct, bool requireReady = true)
    {
        string frame = PyJson.Frame(outer, msg, requestId);
        if (requireReady) await WaitUntilReadyAsync(ct).ConfigureAwait(false);
        try
        {
            await SendOnceAsync(frame, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (requireReady && ex is WebSocketException or ObjectDisposedException or IqOptionException)
        {
            // Not sent: wait for the reconnect and send once more (Python re-sends after reconnecting).
            await WaitUntilReadyAsync(ct).ConfigureAwait(false);
            await SendOnceAsync(frame, ct).ConfigureAwait(false);
        }
    }

    private async Task SendOnceAsync(string frame, CancellationToken ct)
    {
        await _sendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var socket = _socket ?? throw new IqOptionException("not connected");
            await socket.SendAsync(frame, ct).ConfigureAwait(false);
        }
        finally
        {
            _sendLock.Release();
        }
        if (_o.LogFrames) _log("> " + frame);
    }

    private async Task WaitUntilReadyAsync(CancellationToken ct)
    {
        if (Ready && IsOpen) return;
        try
        {
            await WaitUntilAsync(() => Ready && IsOpen, _o.RequestTimeout, ct).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            throw new IqOptionException("not connected");
        }
    }

    public async Task<JsonObject> RequestAsync(string outer, object? msg, string requestId, string replyName, bool byRequestId,
        TimeSpan timeout, CancellationToken ct, bool requireReady = true)
    {
        SemaphoreSlim? gate = byRequestId ? null : Gate(replyName);
        if (gate != null) await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            using var w = AddWaiter(replyName, byRequestId ? requestId : null);
            await SendAsync(outer, msg, requestId, ct, requireReady).ConfigureAwait(false);
            return await w.Task.WaitAsync(timeout, ct).ConfigureAwait(false);
        }
        finally
        {
            gate?.Release();
        }
    }

    public void Dispose()
    {
        _loopCts?.Cancel();
        _http.Dispose();
    }
}
```

- [ ] **Step 5: Run tests** — `dotnet test IqOptionApi.sln --filter ConnectionTests` — Expected: all PASS. If `Login_SendsForm…` fails on the body, print `http.Requests[0].Body` — `FormUrlEncodedContent` must give `identifier=a%40b.com&password=p+w` (same as `requests`).

- [ ] **Step 6: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add connection core: socket, sending, request matching, HTTP login"
```

---

### Task 6: State, message router and events

**Files:**
- Create: `src/IqOptionApi/MessageRouter.cs`
- Modify: `src/IqOptionApi/Helpers.cs` (add `DictQueueAdd`), `tests/golden/generate_golden.py` (fill `part_router`, add `FIXTURES`)
- Create (generated): `tests/IqOptionApi.Tests/Golden/router.json`
- Test: `tests/IqOptionApi.Tests/RouterTests.cs`, `tests/IqOptionApi.Tests/Support/StateDump.cs`, `tests/IqOptionApi.Tests/Support/JsonAssert.cs`

**Interfaces:**
- Consumes: `J`, `Helpers`, `ActivesTable`, `Candle`, `StrikeQuote`.
- Produces: `internal sealed class State` (fields listed in Step 3, all guarded by `State.Lock`; `static TV GetOrAdd<TK,TV>(Dictionary<TK,TV>, TK)`; `SortedDictionary<long, Candle> CandleBuffer(string asset, int size)`), `internal sealed class LiveDealBuffer { int? MaxLength; void AddNewest(JsonObject); JsonObject? PopOldest(); List<JsonObject> NewestFirst(); void Trim(); }`, `internal sealed class MessageRouter { MessageRouter(State, ActivesTable, Action<string>); void Handle(JsonObject); events MessageReceived(JsonObject), CandleReceived(string,int,Candle), PositionChanged(JsonObject), BalanceChanged(long,double); Action<long>? HeartbeatReceived }`, `Helpers.DictQueueAdd(SortedDictionary<long,Candle>, int maxDict, long key, Candle value)`.
- Test support: `JsonAssert.Equal(JsonNode? expected, JsonNode? actual, string path = "$")` (objects by key, arrays by order, numbers within 1e-9), `JsonAssert.Prune(JsonNode?)`, `StateDump.Of(State) → JsonObject` (same shape as Python `dump_state`).

- [ ] **Step 1: Add fixtures and `part_router` to `generate_golden.py`** — put this block after `write(...)` and replace the `def part_router(): pass` stub:

```python
# ----------------------------------------------------------- fixtures ----
REAL_ID, PRACTICE_ID = 10001, 10004
PROFILE = {"name": "profile", "request_id": "$RID",
           "msg": {"user_id": 777, "name": "Test User", "currency": "USD", "balance_id": PRACTICE_ID}}
BALANCES = {"name": "balances", "request_id": "$RID", "msg": [
    {"id": REAL_ID, "type": 1, "amount": 0, "currency": "USD"},
    {"id": PRACTICE_ID, "type": 4, "amount": 10000.5, "currency": "USD"}]}
TIME_SYNC = {"name": "timeSync", "msg": T * 1000}


def fixtures():
    return {"T": T, "tz": METHOD_TZ, "real_id": REAL_ID, "practice_id": PRACTICE_ID,
            "profile": PROFILE, "balances": BALANCES, "time_sync": TIME_SYNC}


def make_client():
    """An IQ_Option whose api looks exactly like it does right after a successful connect()."""
    from iqoptionapi.stable_api import IQ_Option
    from iqoptionapi.api import IQOptionAPI
    from iqoptionapi.ws.client import WebsocketClient
    import iqoptionapi.global_value as gv
    iq = IQ_Option("user@example.com", "secret")
    api = IQOptionAPI("iqoption.com", "user@example.com", "secret")
    iq.api = api
    api.websocket_client = WebsocketClient(api)
    api.object_id = 1
    api.init_global_value(1)
    gv.get_req_id(1)                    # "1" is used by `authenticate` during a real connect
    gv.balance_id[1] = PRACTICE_ID
    gv._tmp_raw_balance_id.pop(1, None)   # change_balance cache must start empty
    gv.check_websocket_if_connect[1] = 1
    api.timesync.server_timestamp = T * 1000
    api.profile.msg = PROFILE["msg"]
    api.balances_raw = subst(BALANCES, "")
    return iq, api


ROUTER_MAXDICT = {"EURUSD": {"60": 2, "1": 2, "5": 2}}
ROUTER_MESSAGES = [
    {"name": "timeSync", "msg": T * 1000 + 250},
    {"name": "balances", "msg": [{"id": 1, "type": 1, "amount": 5}, {"id": 2, "type": 4, "amount": 9}]},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 100, "to": 160, "id": 1, "open": 1.1, "close": 1.2, "min": 1.0, "max": 1.3, "volume": 0, "at": 5}},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 160, "to": 220, "id": 2, "open": 1.2, "close": 1.25, "min": 1.1, "max": 1.3, "volume": 0, "at": 6}},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 220, "to": 280, "id": 3, "open": 1.25, "close": 1.3, "min": 1.2, "max": 1.4, "volume": 0, "at": 7}},
    {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": 220, "to": 280, "id": 3, "open": 1.25, "close": 1.31, "min": 1.2, "max": 1.4, "volume": 0, "at": 8}},
    {"name": "candles-generated", "msg": {"active_id": 1861, "at": 9, "ask": 1.4, "bid": 1.39, "value": 1.395,
                                           "candles": {"1": {"id": 11, "from": 300, "to": 301, "open": 1.3, "min": 1.3, "max": 1.4, "volume": 1},
                                                       "5": {"id": 12, "from": 300, "to": 305, "open": 1.3, "min": 1.3, "max": 1.4, "volume": 2}}}},
    {"name": "commission-changed", "msg": {"instrument_type": "turbo-option", "active_id": 1861, "commission": {"value": 18}}},
    {"name": "traders-mood-changed", "msg": {"asset_id": 1861, "value": 0.61}},
    {"name": "top-assets-updated", "msg": {"instrument_type": "forex", "data": [{"active_id": 1861}]}},
    {"name": "position-changed", "microserviceName": "portfolio", "msg": {"source": "digital-options", "status": "open", "external_id": 900, "raw_event": {"order_ids": [800]}}},
    {"name": "position-changed", "microserviceName": "portfolio", "msg": {"source": "binary-options", "status": "open", "external_id": 901, "raw_event": {}}},
    {"name": "position-changed", "microserviceName": "portfolio", "msg": {"source": "trading", "status": "closed", "external_id": 902, "raw_event": {"order_ids": [802]}}},
    {"name": "order-changed", "microserviceName": "portfolio", "msg": {"source": "digital-options", "raw_event": {"position_id": 900, "id": 800}}},
    {"name": "option-opened", "msg": {"option_id": 903, "amount": 1}},
    {"name": "option-closed", "msg": {"option_id": 903, "amount": 1, "profit_amount": 1.85}},
    {"name": "client-price-generated", "msg": {"asset_id": 1861, "prices": [
        {"strike": "SPT", "call": {"symbol": "do1861A20261009D123300T1MCSPT", "ask": 52.5, "bid": 47}, "put": {"symbol": "do1861A20261009D123300T1MPSPT", "ask": 48.0, "bid": 51}},
        {"strike": "1170000", "call": {"symbol": "C1"}, "put": {"symbol": "P1", "ask": 40}}]}},
    {"name": "live-deal-binary-option-placed", "msg": {"active_id": 1861, "option_type": "turbo", "amount": 5}},
    {"name": "live-deal-binary-option-placed", "msg": {"active_id": 1861, "option_type": "turbo", "amount": 6}},
    {"name": "live-deal-digital-option", "msg": {"instrument_active_id": 76, "expiration_type": "PT1M", "amount_enrolled": 3}},
    {"name": "live-deal", "msg": {"instrument_active_id": 1861, "instrument_type": "forex", "amount": 7}},
    {"name": "balance-changed", "msg": {"current_balance": {"id": 2, "amount": 9.5}}},
    {"name": "something-unknown", "msg": {"x": 1}},
]


def dump_state(api):
    import iqoptionapi.global_value as gv
    live = {}
    for n, d1 in api.live_deal_data.items():
        for a, d2 in d1.items():
            for t, q in d2.items():
                live["%s|%s|%s" % (n, a, t)] = list(q)
    return prune(to_json({
        "server_timestamp": api.timesync.server_timestamp,
        "balance_id": gv.balance_id[api.object_id],
        "real_time_candles": api.real_time_candles,
        "candle_generated_check": {a: {s: v for s, v in d.items() if v is True} for a, d in api.candle_generated_check.items()},
        "candle_generated_all_size_check": {a: v for a, v in api.candle_generated_all_size_check.items() if v is True},
        "traders_mood": api.traders_mood,
        "commission": api.subscribe_commission_changed_data,
        "top_assets_updated": api.top_assets_updated_data,
        "live_deal": live,
        "order_async": {k: dict(v) for k, v in api.order_async.items()},
        "option_opened": api.socket_option_opened,
        "digital_opened": api.digital_opened,
        "digital_profit": api.digital_profit,
        "quotes": api.instrument_quites_generated_data,
        "quotes_raw": api.instrument_quotes_generated_raw_data,
        "balance": api.balance,
    }))


def part_router():
    import iqoptionapi.global_value as gv
    iq, api = make_client()
    gv.balance_id[1] = None
    for asset, sizes in ROUTER_MAXDICT.items():
        for size, maxdict in sizes.items():
            api.real_time_candles_maxdict_table[asset][int(size)] = maxdict
    for m in ROUTER_MESSAGES:
        api.websocket_client.on_message(None, json.dumps(m))
    write("router.json", {"maxdict": ROUTER_MAXDICT, "messages": ROUTER_MESSAGES, "state": dump_state(api)})
```

Run: `python3 tests/golden/generate_golden.py` — Expected: `wrote router.json`; open it and check `state.real_time_candles.EURUSD.60` has exactly keys `160` and `220`.

- [ ] **Step 2: Write `tests/IqOptionApi.Tests/Support/JsonAssert.cs`**

```csharp
using System.Text.Json;
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

internal static class JsonAssert
{
    public static void Equal(JsonNode? expected, JsonNode? actual, string path = "$")
    {
        if (J.IsNull(expected) || J.IsNull(actual))
        {
            Assert.True(J.IsNull(expected) && J.IsNull(actual), $"{path}: expected {expected?.ToJsonString() ?? "null"} but got {actual?.ToJsonString() ?? "null"}");
            return;
        }
        switch (expected)
        {
            case JsonObject eo:
                var ao = Assert.IsType<JsonObject>(actual);
                Assert.True(eo.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal).SequenceEqual(ao.Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal)),
                    $"{path}: keys differ. expected [{string.Join(",", eo.Select(p => p.Key))}] got [{string.Join(",", ao.Select(p => p.Key))}]");
                foreach (var (k, v) in eo) Equal(v, ao[k], path + "." + k);
                break;
            case JsonArray ea:
                var aa = Assert.IsType<JsonArray>(actual);
                Assert.True(ea.Count == aa.Count, $"{path}: length {ea.Count} vs {aa.Count}");
                for (int i = 0; i < ea.Count; i++) Equal(ea[i], aa[i], $"{path}[{i}]");
                break;
            default:
                var kind = expected.GetValueKind();
                if (kind == JsonValueKind.Number && actual!.GetValueKind() == JsonValueKind.Number)
                {
                    double e = J.Dbl(expected), a = J.Dbl(actual);
                    Assert.True(Math.Abs(e - a) <= 1e-9 * Math.Max(1, Math.Abs(e)), $"{path}: {e} vs {a}");
                }
                else
                {
                    Assert.True(expected.ToJsonString() == actual!.ToJsonString(), $"{path}: {expected.ToJsonString()} vs {actual.ToJsonString()}");
                }
                break;
        }
    }

    public static JsonNode? Prune(JsonNode? n)
    {
        switch (n)
        {
            case JsonObject o:
                var result = new JsonObject();
                foreach (var (k, v) in o)
                {
                    var p = Prune(v);
                    if (p is JsonObject { Count: 0 } || p is JsonArray { Count: 0 }) continue;
                    result[k] = p;
                }
                return result;
            case JsonArray a:
                return new JsonArray(a.Select(Prune).ToArray());
            default:
                return n?.DeepClone();
        }
    }
}
```

- [ ] **Step 3: Write `src/IqOptionApi/MessageRouter.cs`**

```csharp
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
```

- [ ] **Step 4: Add `DictQueueAdd` to `Helpers`**

```csharp
internal static partial class Helpers
{
    /// <summary>Python <c>dict_queue_add</c>: replace a known key, otherwise evict the smallest keys until there is room.</summary>
    internal static void DictQueueAdd(SortedDictionary<long, Candle> dict, int maxDict, long key, Candle value)
    {
        if (dict.ContainsKey(key))
        {
            dict[key] = value;
            return;
        }
        if (maxDict <= 0) throw new InvalidOperationException("candle buffer size must be positive");
        while (dict.Count >= maxDict) dict.Remove(dict.Keys.First());
        dict[key] = value;
    }
}
```

- [ ] **Step 5: Write `tests/IqOptionApi.Tests/Support/StateDump.cs`**

```csharp
using System.Globalization;
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

/// <summary>Turns <see cref="State"/> into the same JSON shape as Python's dump_state().</summary>
internal static class StateDump
{
    private static string K(long v) => v.ToString(CultureInfo.InvariantCulture);

    public static JsonObject Of(State s)
    {
        lock (s.Lock)
        {
            var o = new JsonObject
            {
                ["server_timestamp"] = s.ServerTimestamp,
                ["balance_id"] = s.BalanceId,
                ["real_time_candles"] = Nest(s.Candles, buf => new JsonObject(buf.Select(c => KeyValuePair.Create(K(c.Key), (JsonNode?)c.Value.Raw.DeepClone())))),
                ["candle_generated_check"] = new JsonObject(s.CandleCheck.Select(a => KeyValuePair.Create(a.Key,
                    (JsonNode?)new JsonObject(a.Value.Where(x => x.Value).Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)true)))))),
                ["candle_generated_all_size_check"] = new JsonObject(s.AllSizeCheck.Where(x => x.Value).Select(x => KeyValuePair.Create(x.Key, (JsonNode?)true))),
                ["traders_mood"] = new JsonObject(s.TradersMood.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value))),
                ["commission"] = new JsonObject(s.Commission.Select(t => KeyValuePair.Create(t.Key, (JsonNode?)new JsonObject(t.Value.Select(a =>
                    KeyValuePair.Create(a.Key, (JsonNode?)new JsonObject(a.Value.Select(x => KeyValuePair.Create(PyJson.FloatRepr(x.Key), (JsonNode?)x.Value)))))))))),
                ["top_assets_updated"] = new JsonObject(s.TopAssetsUpdated.Select(x => KeyValuePair.Create(x.Key, x.Value?.DeepClone()))),
                ["live_deal"] = new JsonObject(s.LiveDeals.Select(x => KeyValuePair.Create($"{x.Key.Name}|{x.Key.Asset}|{x.Key.Type}",
                    (JsonNode?)new JsonArray(x.Value.NewestFirst().Select(m => (JsonNode?)m.DeepClone()).ToArray())))),
                ["order_async"] = new JsonObject(s.OrderAsync.Select(x => KeyValuePair.Create(K(x.Key),
                    (JsonNode?)new JsonObject(x.Value.Select(v => KeyValuePair.Create(v.Key, (JsonNode?)v.Value.DeepClone())))))),
                ["option_opened"] = new JsonObject(s.OptionOpened.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value.DeepClone()))),
                ["digital_opened"] = new JsonObject(s.DigitalOpened.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value.DeepClone()))),
                ["digital_profit"] = Nest(s.DigitalProfit, v => v is double d ? JsonValue.Create(d) : null),
                ["quotes"] = Nest(s.Quotes, q => new JsonObject(q.Select(x => KeyValuePair.Create(x.Key, (JsonNode?)new JsonObject
                {
                    ["call"] = new JsonObject { ["id"] = x.Value.CallId, ["profit"] = x.Value.CallProfit },
                    ["put"] = new JsonObject { ["id"] = x.Value.PutId, ["profit"] = x.Value.PutProfit },
                })))),
                ["quotes_raw"] = Nest(s.QuotesRaw, m => m.DeepClone()),
                ["balance"] = new JsonObject(s.Balance.Select(x => KeyValuePair.Create(K(x.Key), (JsonNode?)x.Value))),
            };
            return (JsonObject)JsonAssert.Prune(o)!;
        }
    }

    private static JsonObject Nest<T>(Dictionary<string, Dictionary<int, T>> d, Func<T, JsonNode?> leaf) =>
        new(d.Select(a => KeyValuePair.Create(a.Key, (JsonNode?)new JsonObject(a.Value.Select(x => KeyValuePair.Create(K(x.Key), leaf(x.Value)))))));
}
```

Note: Python keeps `None` leaves (e.g. `digital_profit` with no ask, quote `profit: null`); `Prune` keeps nulls, so both sides stay comparable.

- [ ] **Step 6: Write the failing tests `tests/IqOptionApi.Tests/RouterTests.cs`**

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class RouterTests
{
    private static (MessageRouter Router, State State, List<string> Log) Make()
    {
        var state = new State();
        var log = new List<string>();
        return (new MessageRouter(state, new ActivesTable(), log.Add), state, log);
    }

    [Fact]
    public void StateMatchesPython()
    {
        var golden = Golden.Load("router.json");
        var (router, state, _) = Make();
        foreach (var (asset, sizes) in golden["maxdict"]!.AsObject())
            foreach (var (size, max) in sizes!.AsObject())
                State.GetOrAdd(state.MaxDict, asset)[int.Parse(size)] = (int)max!;
        foreach (var m in golden["messages"]!.AsArray()) router.Handle(m!.DeepClone().AsObject());
        JsonAssert.Equal(golden["state"], StateDump.Of(state));
    }

    [Fact]
    public void BadMessages_AreSkipped_AndLaterMessagesStillWork()
    {
        var (router, state, log) = Make();
        router.Handle(JsonNode.Parse("{\"name\":\"candle-generated\",\"msg\":{\"active_id\":424242,\"size\":60,\"from\":1}}")!.AsObject());
        router.Handle(JsonNode.Parse("{\"name\":\"client-price-generated\",\"msg\":{\"asset_id\":1861,\"prices\":[]}}")!.AsObject());
        router.Handle(JsonNode.Parse("{\"name\":\"position-changed\",\"msg\":5}")!.AsObject());
        router.Handle(JsonNode.Parse("{\"name\":\"traders-mood-changed\",\"msg\":{\"asset_id\":1861,\"value\":0.4}}")!.AsObject());
        Assert.Equal(3, log.Count(l => l.StartsWith("skipped", StringComparison.Ordinal)));
        Assert.Equal(0.4, state.TradersMood[1861]);
    }

    [Fact]
    public void ThrowingHandler_DoesNotStopRouting()
    {
        var (router, state, log) = Make();
        router.MessageReceived += _ => throw new InvalidOperationException("boom");
        router.BalanceChanged += (_, _) => throw new InvalidOperationException("boom2");
        router.Handle(JsonNode.Parse("{\"name\":\"balance-changed\",\"msg\":{\"current_balance\":{\"id\":2,\"amount\":3.5}}}")!.AsObject());
        Assert.Equal(3.5, state.Balance[2]);
        Assert.Equal(2, log.Count(l => l.Contains("threw", StringComparison.Ordinal)));
    }

    [Fact]
    public void LiveDealBuffer_DropsOldestWhenFull()
    {
        var b = new LiveDealBuffer { MaxLength = 2 };
        for (int i = 0; i < 3; i++) b.AddNewest(new JsonObject { ["i"] = i });
        Assert.Equal(new[] { 2, 1 }, b.NewestFirst().Select(x => (int)x["i"]!));
        Assert.Equal(1, (int)b.PopOldest()!["i"]!);
    }
}
```

Run: `dotnet test IqOptionApi.sln --filter RouterTests` — Expected: build FAILS first time if Step 3/4 not yet saved; after saving, all PASS. If `StateMatchesPython` fails, the assertion message names the JSON path that differs — fix the router branch for that path.

- [ ] **Step 7: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add state, message router and router golden test"
```
---

### Task 7: `IqOption` core — connect sequence, heartbeat, session, golden harness

**Files:**
- Create: `src/IqOptionApi/IqOption.cs`
- Modify: `tests/golden/generate_golden.py` (`APPS` list in `FakeWebSocketApp.__init__`, fill `part_connect`, `run_case`, `build_cases`, `part_methods`)
- Create: `tests/IqOptionApi.Tests/Support/Harness.cs`, `tests/IqOptionApi.Tests/Support/Calls.cs`
- Create (generated): `Golden/connect.json`, `Golden/methods.json`
- Test: `tests/IqOptionApi.Tests/ConnectGoldenTests.cs`, `tests/IqOptionApi.Tests/MethodGoldenTests.cs`, `tests/IqOptionApi.Tests/SessionTests.cs`

**Interfaces:**
- Consumes: `Connection`, `MessageRouter`, `State`, `ActivesTable`, `Helpers`, `PyJson`, `Obj`, `J`.
- Produces (public): `IqOption(string email, string password, IqOptionOptions? options = null)`, `Task<(bool Success, string? Reason)> ConnectAsync(CancellationToken ct = default)`, `Task<JsonNode?> TwoFactorAsync(string token, string? method = null, string? code = null, CancellationToken ct = default)`, `void Set2FAToken(string token)`, `void SetSession(IDictionary<string,string> headers, IDictionary<string,string> cookies)`, `Task LogoutAsync(CancellationToken ct = default)`, `Task CloseAsync()`, `ValueTask DisposeAsync()`, `bool IsConnected`, `string? Ssid`, `double GetServerTimestamp()`, `static IReadOnlyList<int> CandleSizes`, events `MessageReceived`, `CandleReceived`, `PositionChanged`, `BalanceChanged`, `Disconnected`, `Reconnected`.
- Produces (internal, used by group tasks): fields `_conn`, `_s`, `_actives`, `_tz`, `_track` (lock) and the tracking lists, `_balanceIds`, `_pendingBinary`, `_pendingDigital`, `_recovered`, `_recoveredDigital`, `_afterSale`; helpers `T` (timeout), `Scaled(double seconds)`, `EnsureConnected()`, `Act(string asset)`, `Bal` (current balance id, throws if none), `Now` (server time), `NextRid()`, `Send(string outer, object msg, string rid, CancellationToken)`, `ById(string reply, object msg, string rid, CancellationToken)`, `ByName(string reply, object msg, string rid, CancellationToken)`, `WaitCondAsync(Func<bool>, TimeSpan, CancellationToken) → Task<bool>`, `Reason(Exception)`, `IsFailure(Exception)`, `Status2000(JsonObject)`; message builders `CandleStreamMsg`, `AllSizesMsg`, `MoodMsg`, `StrikeSubscribeMsg`, `StrikeUnsubscribeMsg`, `TopAssetsMsg`, `CommissionMsg`, `LiveDealMsg`; `ResubscribeStreamsAsync`; `internal double TimeScale`, `internal Connection Conn`, `internal State StateForTests`.
- Test support: `Harness.ConnectedAsync(JsonObject fixtures) → (IqOption, FakeSocket, FakeHttpHandler)`, `Harness.ConnectRules(JsonObject)`, `Harness.SyncAsync(IqOption, FakeSocket)`, `Calls.InvokeAsync(IqOption, FakeSocket, string method, JsonObject args) → Task<JsonNode?>`, `Calls.Setup(IqOption, string? setup)`.

- [ ] **Step 1: Extend `generate_golden.py`** — in `FakeWebSocketApp.__init__` add `APPS.append(self)` (and `APPS = []` next to `FRAMES = []`); replace the `part_connect` and `part_methods` stubs with:

```python
def part_connect():
    global CONNECT_RULES
    from iqoptionapi.stable_api import IQ_Option
    CONNECT_RULES = [
        {"on": "authenticate", "reply": [{"name": "authenticated", "msg": True, "request_id": "$RID"}]},
        {"on": "sendMessage:get-profile", "reply": [PROFILE]},
        {"on": "sendMessage:get-balances", "reply": [BALANCES]},
    ]
    stop = threading.Event()

    def time_pusher():
        while not stop.is_set():
            for app in list(APPS):
                DELIVERY.put((app, json.dumps(TIME_SYNC)))
            _real_sleep(0.02)

    threading.Thread(target=time_pusher, daemon=True).start()
    iq = IQ_Option("user@example.com", "secret")
    ok, reason = iq.connect()
    stop.set()
    login = [h for h in HTTP_LOG if h["url"].endswith("/api/v2/login")][0]
    write("connect.json", {"fixtures": fixtures(), "ok": ok, "frames": list(FRAMES),
                           "login_url": login["url"],
                           "login_form": {k: v for k, v in login["data"].items() if v is not None}})


def amount_of(raw):
    import iqoptionapi.global_value as gv
    return next((b["amount"] for b in raw["msg"] if b["id"] == gv.balance_id[1]), None)


def actives_items():
    import iqoptionapi.constants as OP_code
    return [[k, v] for k, v in OP_code.ACTIVES.items()]


def run_case(case):
    import iqoptionapi.constants as OP_code
    saved = list(OP_code.ACTIVES.items())
    iq, api = make_client()
    frames = []
    rules = case.get("rules", [])
    delay = case.get("delay", False)

    def deliver(reply):
        raw = json.dumps(reply)
        if delay:
            threading.Timer(0.05, api.websocket_client.on_message, args=(None, raw)).start()
        else:
            api.websocket_client.on_message(None, raw)

    def fake_send(name, msg, request_id="", no_force_send=True):
        data = json.dumps(dict(name=name, msg=msg, request_id=request_id))
        frames.append(data)
        for reply in replies_for(json.loads(data), rules):
            deliver(reply)

    api.send_websocket_request = fake_send
    for m in case.get("pre", []):
        api.websocket_client.on_message(None, json.dumps(m))
    if "setup" in case:
        case["setup"](iq, api)
    result = case["call"](iq)
    OP_code.ACTIVES.clear()
    OP_code.ACTIVES.update(saved)
    return {"name": case["name"], "csharp": case["csharp"], "setup_csharp": case.get("setup_csharp"),
            "pre": case.get("pre", []), "rules": rules, "frames": frames,
            "result": to_json(result), "compare_result": case.get("compare_result", True)}


def build_cases():
    cases = []
    cases += [
        {"name": "heartbeat_reply",
         "call": lambda iq: iq.api.websocket_client.on_message(None, json.dumps({"name": "heartbeat", "msg": 1791549020123})),
         "csharp": {"method": "Heartbeat", "args": {"heartbeatTime": 1791549020123}}, "compare_result": False},
    ]
    # @@CASES@@
    return cases


def part_methods():
    write("methods.json", {"fixtures": fixtures(), "cases": [run_case(c) for c in build_cases()]})
```

Run: `python3 tests/golden/generate_golden.py` — Expected: `wrote connect.json`, `wrote methods.json`; `connect.json` has `"ok": true` and 28 frames (authenticate, get-profile, get-balances, get-balances, 12 position-changed, 6 billing, 6 order-changed, setOptions).

- [ ] **Step 2: Write `tests/IqOptionApi.Tests/Support/Harness.cs`**

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

internal static class Harness
{
    public static List<Rule> ConnectRules(JsonObject f) => new()
    {
        new Rule("authenticate", new() { JsonNode.Parse("{\"name\":\"authenticated\",\"msg\":true,\"request_id\":\"$RID\"}")!.AsObject() }),
        new Rule("sendMessage:get-profile", new() { f["profile"]!.DeepClone().AsObject() }),
        new Rule("sendMessage:get-balances", new() { f["balances"]!.DeepClone().AsObject(), f["time_sync"]!.DeepClone().AsObject() }),
    };

    public static IqOptionOptions Options(JsonObject f) => new()
    {
        TimeZone = Golden.Zone((string)f["tz"]!),
        RequestTimeout = TimeSpan.FromSeconds(5),
        AutoReconnect = false,
    };

    public static async Task<(IqOption Api, FakeSocket Socket, FakeHttpHandler Http)> ConnectedAsync(JsonObject fixtures)
    {
        var socket = new FakeSocket { Rules = ConnectRules(fixtures) };
        var http = new FakeHttpHandler();
        var api = new IqOption("user@example.com", "secret", Options(fixtures), () => socket, http);
        var (ok, reason) = await api.ConnectAsync();
        Assert.True(ok, reason);
        socket.TakeFrames();
        return (api, socket, http);
    }

    public static async Task SyncAsync(IqOption api, FakeSocket socket)
    {
        using var w = api.Conn.AddWaiter("__sync__", null);
        socket.Push("{\"name\":\"__sync__\"}");
        await w.Task.WaitAsync(TimeSpan.FromSeconds(5));
    }
}
```

- [ ] **Step 3: Write `tests/IqOptionApi.Tests/Support/Calls.cs`**

```csharp
using System.Globalization;
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

/// <summary>Maps a golden case to a C# call and turns the C# result into the Python result's JSON shape.</summary>
internal static class Calls
{
    public static async Task<JsonNode?> InvokeAsync(IqOption api, FakeSocket socket, string method, JsonObject a)
    {
        switch (method)
        {
            case "Heartbeat":
            {
                socket.Push("{\"name\":\"heartbeat\",\"msg\":" + L(a, "heartbeatTime").ToString(CultureInfo.InvariantCulture) + "}");
                await api.Conn.WaitUntilAsync(() => { lock (socket.Frames) return socket.Frames.Count > 0; }, TimeSpan.FromSeconds(5), default);
                return null;
            }
            // @@CALLS@@
            default:
                throw new ArgumentException("No C# adapter for " + method);
        }
    }

    public static void Setup(IqOption api, string? setup)
    {
        if (setup == null) return;
        var parts = setup.Split(':');
        var s = api.StateForTests;
        lock (s.Lock)
        {
            switch (parts[0])
            {
                case "candle_check": State.GetOrAdd(s.CandleCheck, parts[1])[int.Parse(parts[2], CultureInfo.InvariantCulture)] = true; break;
                case "all_size_check": s.AllSizeCheck[parts[1]] = true; break;
                default: throw new ArgumentException("unknown setup " + setup);
            }
        }
    }

    internal static JsonArray Tuple(params object?[] items) => new(items.Select(ToNode).ToArray());

    internal static JsonNode? ToNode(object? o) => o switch
    {
        null => null,
        JsonNode n => n.DeepClone(),
        bool b => b,
        string s => s,
        int i => i,
        long l => l,
        double d => d,
        _ => throw new ArgumentException("cannot convert " + o.GetType().Name),
    };

    internal static string S(JsonObject a, string k) => (string)a[k]!;

    internal static int I(JsonObject a, string k) => (int)J.Long(a[k]);

    internal static long L(JsonObject a, string k) => J.Long(a[k]);

    internal static double D(JsonObject a, string k) => J.Dbl(a[k]);

    internal static bool B(JsonObject a, string k) => J.IsTrue(a[k]);

    internal static string? SN(JsonObject a, string k) => J.IsNull(a[k]) ? null : (string)a[k]!;

    internal static double? DN(JsonObject a, string k) => J.DblOrNull(a[k]);

    internal static Direction Dir(JsonObject a) => S(a, "direction") == "call" ? Direction.Call : Direction.Put;

    internal static BalanceMode Mode(JsonObject a) => S(a, "mode") == "REAL" ? BalanceMode.Real : BalanceMode.Practice;

    internal static List<string> Strings(JsonObject a, string k) => a[k]!.AsArray().Select(x => (string)x!).ToList();

    internal static List<long> Longs(JsonObject a, string k) => a[k]!.AsArray().Select(x => J.Long(x)).ToList();

    internal static JsonArray Items(IEnumerable<KeyValuePair<string, int>> items) =>
        new(items.Select(p => (JsonNode?)new JsonArray(p.Key, p.Value)).ToArray());
}
```

- [ ] **Step 4: Write the failing tests**

`tests/IqOptionApi.Tests/ConnectGoldenTests.cs`:

```csharp
namespace IqOptionApi.Tests;

public class ConnectGoldenTests
{
    [Fact]
    public async Task ConnectSendsTheSameFramesAsPython()
    {
        var golden = Golden.Load("connect.json");
        var f = golden["fixtures"]!.AsObject();
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        var http = new FakeHttpHandler();
        await using var api = new IqOption("user@example.com", "secret", Harness.Options(f), () => socket, http);

        var (ok, reason) = await api.ConnectAsync();

        Assert.True(ok, reason);
        Assert.Equal(golden["frames"]!.AsArray().Select(x => (string)x!).ToArray(), socket.TakeFrames());
        var login = http.Requests.Single(r => r.Url.EndsWith("/api/v2/login", StringComparison.Ordinal));
        Assert.Equal((string)golden["login_url"]!, login.Url);
        var form = login.Body.Split('&').Select(p => p.Split('=')).ToDictionary(p => p[0], p => Uri.UnescapeDataString(p[1].Replace('+', ' ')));
        Assert.Equal(golden["login_form"]!.AsObject().ToDictionary(p => p.Key, p => (string)p.Value!), form);
        Assert.Equal((long)f["practice_id"]!, api.StateForTests.BalanceId);
        Assert.Equal((double)(long)f["T"]!, api.GetServerTimestamp());
        Assert.True(api.IsConnected);
    }

    [Fact]
    public async Task WrongPassword_ReturnsServerText()
    {
        var f = Golden.Load("connect.json")["fixtures"]!.AsObject();
        var http = new FakeHttpHandler
        {
            Respond = (_, _) => new HttpResponseMessage(System.Net.HttpStatusCode.BadRequest)
            {
                Content = new StringContent("{\"code\":\"invalid_credentials\"}"),
            },
        };
        await using var api = new IqOption("user@example.com", "bad", Harness.Options(f), () => new FakeSocket(), http);
        var (ok, reason) = await api.ConnectAsync();
        Assert.False(ok);
        Assert.Equal("{\"code\":\"invalid_credentials\"}", reason);
    }

    [Fact]
    public async Task CallsBeforeConnect_Throw()
    {
        await using var api = new IqOption("a", "b");
        await Assert.ThrowsAsync<InvalidOperationException>(() => api.GetProfileAsync());
    }
}
```

(`GetProfileAsync` arrives in Task 8; until then comment out `CallsBeforeConnect_Throw` — uncomment it in Task 8 Step 4.)

`tests/IqOptionApi.Tests/MethodGoldenTests.cs`:

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class MethodGoldenTests
{
    private static readonly Lazy<JsonObject> Data = new(() => Golden.Load("methods.json"));

    public static IEnumerable<object[]> Cases() =>
        Data.Value["cases"]!.AsArray().Select(c => new object[] { (string)c!["name"]! });

    [Theory, MemberData(nameof(Cases))]
    public async Task MatchesPython(string name)
    {
        var c = Data.Value["cases"]!.AsArray().Select(x => x!.AsObject()).First(x => (string)x["name"]! == name);
        var (api, socket, _) = await Harness.ConnectedAsync(Data.Value["fixtures"]!.AsObject());
        await using var _ = api;
        socket.Rules = Rule.Parse(c["rules"]);
        foreach (var m in c["pre"]!.AsArray()) socket.Push(m!.ToJsonString());
        await Harness.SyncAsync(api, socket);
        Calls.Setup(api, (string?)c["setup_csharp"]);
        var call = c["csharp"]!.AsObject();

        var result = await Calls.InvokeAsync(api, socket, (string)call["method"]!, call["args"]?.AsObject() ?? new JsonObject());

        Assert.Equal(c["frames"]!.AsArray().Select(f => (string)f!).ToArray(), socket.TakeFrames());
        if ((bool)c["compare_result"]!) JsonAssert.Equal(c["result"], result);
    }
}
```

`tests/IqOptionApi.Tests/SessionTests.cs`:

```csharp
namespace IqOptionApi.Tests;

public class SessionTests
{
    [Fact]
    public async Task TwoFactor_PostsJsonLikePython()
    {
        var http = new FakeHttpHandler();
        await using var api = new IqOption("a", "b", new IqOptionOptions(), () => new FakeSocket(), http);
        await api.TwoFactorAsync("tok", method: "sms");
        var r = http.Requests.Single();
        Assert.Equal("https://auth.iqoption.com/api/v2/verify/2fa", r.Url);
        Assert.Equal("{\"method\": \"sms\", \"token\": \"tok\", \"code\": null}", r.Body);
    }

    [Fact]
    public async Task TwoFactorToken_IsSentWithNextLogin()
    {
        var http = new FakeHttpHandler();
        await using var api = new IqOption("a@b.c", "p", new IqOptionOptions { RequestTimeout = TimeSpan.FromMilliseconds(200), AutoReconnect = false }, () => new FakeSocket(), http);
        api.Set2FAToken("T3");
        await api.ConnectAsync();
        Assert.EndsWith("&token=T3", http.Requests.First(r => r.Url.EndsWith("/login", StringComparison.Ordinal)).Body, StringComparison.Ordinal);
    }

    [Fact]
    public async Task SetSession_ReplacesHeadersAndCookies()
    {
        var http = new FakeHttpHandler();
        await using var api = new IqOption("a", "b", new IqOptionOptions { AutoReconnect = false }, () => new FakeSocket(), http);
        api.SetSession(new Dictionary<string, string> { ["User-Agent"] = "MyBot/1" }, new Dictionary<string, string> { ["lang"] = "en" });
        await api.LogoutAsync();
        Assert.Contains("MyBot/1", http.Requests[0].Headers);
        Assert.Contains("lang=en", http.Requests[0].Headers);
    }
}
```

Run: `dotnet test IqOptionApi.sln --filter "ConnectGoldenTests|MethodGoldenTests|SessionTests"` — Expected: build FAILS (`IqOption` missing).

- [ ] **Step 5: Write `src/IqOptionApi/IqOption.cs`**

```csharp
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
    public event Action? Reconnected;

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
```

Note: `using System.Diagnostics;` is needed from Task 9 on (`Stopwatch`); an unused using is only an IDE hint, not a build warning.

- [ ] **Step 6: Run tests** — `dotnet test IqOptionApi.sln` — Expected: all PASS (including `MatchesPython(heartbeat_reply)`: the frame `{"name": "heartbeat", "msg": {"msg": {"heartbeatTime": 1791549020123, "userTime": 1791549020000}}, "request_id": ""}`).

- [ ] **Step 7: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add IqOption core: connect sequence, heartbeat, session and golden harness"
```
---

## Group tasks (8–14): common procedure

Every group task follows the same five steps; the task lists only the content.

1. **Python cases:** insert the task's block into `generate_golden.py` *above* `    # @@CASES@@` (4-space indent, inside `build_cases()`); then run `python3 tests/golden/generate_golden.py` — Expected: `wrote methods.json` with no traceback.
2. **Adapters:** insert the task's `case` lines into `Calls.cs` *above* `            // @@CALLS@@`.
3. **Run** `dotnet test IqOptionApi.sln --filter MethodGoldenTests` — Expected: build FAILS (the new `IqOption` methods do not exist yet).
4. **Implementation:** insert the task's region into `IqOption.cs` *above* `    // @@GROUPS@@`.
5. **Run** `dotnet test IqOptionApi.sln` — Expected: all tests PASS, 0 warnings. A failing golden case prints the first differing frame or JSON path; fix the C# code, never the golden file. Then commit with the message given in the task.

---

### Task 8: Account & balance

**Interfaces:** Produces `Task<JsonObject> GetProfileAsync(ct)`, `Task<JsonObject> GetBalancesAsync(ct)`, `Task<double?> GetBalanceAsync(ct)`, `long? GetBalanceId()`, `Task<BalanceMode?> GetBalanceModeAsync(ct)`, `Task<long?> GetBalanceIdByModeAsync(BalanceMode, ct)`, `Task ChangeBalanceAsync(BalanceMode, ct)`, `Task<string?> GetCurrencyAsync(ct)`, `Task<bool> ResetPracticeBalanceAsync(ct)`, `Task<JsonObject> GetTransactionsAsync(long from, long to, int limit, int offset, IEnumerable<string> types, ct)`.

**Python cases:**

```python
    BAL_RULE = {"on": "sendMessage:get-balances", "reply": [BALANCES]}
    cases += [
        {"name": "get_profile", "call": lambda iq: iq.get_profile(),
         "rules": [{"on": "sendMessage:get-profile", "reply": [PROFILE]}], "csharp": {"method": "GetProfileAsync", "args": {}}},
        {"name": "get_balances", "call": lambda iq: iq._get_balances(), "rules": [BAL_RULE],
         "csharp": {"method": "GetBalancesAsync", "args": {}}},
        {"name": "get_balance", "call": lambda iq: amount_of(iq._get_balances()), "rules": [BAL_RULE],
         "csharp": {"method": "GetBalanceAsync", "args": {}}},
        {"name": "get_balance_cached",
         "pre": [{"name": "balance-changed", "msg": {"current_balance": {"id": PRACTICE_ID, "amount": 77.5}}}],
         "call": lambda iq: iq.get_balance_v2(), "csharp": {"method": "GetBalanceAsync", "args": {}}},
        {"name": "get_balance_id", "call": lambda iq: iq.get_balance_id(), "csharp": {"method": "GetBalanceId", "args": {}}},
        {"name": "get_balance_mode", "call": lambda iq: iq.get_balance_mode(), "rules": [BAL_RULE],
         "csharp": {"method": "GetBalanceModeAsync", "args": {}}},
        {"name": "get_balance_id_by_mode", "call": lambda iq: iq.get_balance_id_by_mode("REAL"), "rules": [BAL_RULE],
         "csharp": {"method": "GetBalanceIdByModeAsync", "args": {"mode": "REAL"}}},
        {"name": "change_balance", "call": lambda iq: (iq.change_balance("REAL"), iq.get_balance_id())[1], "rules": [BAL_RULE],
         "csharp": {"method": "ChangeBalanceAsync", "args": {"mode": "REAL"}}},
        {"name": "get_currency", "call": lambda iq: iq.get_currency(), "rules": [BAL_RULE],
         "csharp": {"method": "GetCurrencyAsync", "args": {}}},
        {"name": "reset_practice_balance", "call": lambda iq: iq.reset_practice_balance(),
         "rules": [{"on": "sendMessage:reset-training-balance",
                    "reply": [{"name": "training-balance-reset", "request_id": "", "msg": {"isSuccessful": True}}]}],
         "csharp": {"method": "ResetPracticeBalanceAsync", "args": {}}},
        {"name": "get_transactions", "call": lambda iq: iq.get_transactions(1700000000, 1800000000, 10, 0, ["deposit", "order_sell"]),
         "rules": [{"on": "sendMessage:get-transactions",
                    "reply": [{"name": "transactions", "request_id": "$RID", "msg": {"transactions": [{"id": 1}]}}]}],
         "csharp": {"method": "GetTransactionsAsync",
                    "args": {"from": 1700000000, "to": 1800000000, "limit": 10, "offset": 0, "types": ["deposit", "order_sell"]}}},
    ]
```

**Adapters:**

```csharp
            case "GetProfileAsync": return await api.GetProfileAsync();
            case "GetBalancesAsync": return await api.GetBalancesAsync();
            case "GetBalanceAsync": return ToNode(await api.GetBalanceAsync());
            case "GetBalanceId": return ToNode(api.GetBalanceId());
            case "GetBalanceModeAsync":
                return (await api.GetBalanceModeAsync()) switch { BalanceMode.Real => "REAL", BalanceMode.Practice => "PRACTICE", _ => null };
            case "GetBalanceIdByModeAsync": return ToNode(await api.GetBalanceIdByModeAsync(Mode(a)));
            case "ChangeBalanceAsync": await api.ChangeBalanceAsync(Mode(a)); return ToNode(api.GetBalanceId());
            case "GetCurrencyAsync": return await api.GetCurrencyAsync();
            case "ResetPracticeBalanceAsync": return await api.ResetPracticeBalanceAsync();
            case "GetTransactionsAsync":
                return await api.GetTransactionsAsync(L(a, "from"), L(a, "to"), I(a, "limit"), I(a, "offset"), Strings(a, "types"));
```

Also uncomment `CallsBeforeConnect_Throw` in `ConnectGoldenTests.cs`.

**Implementation:**

```csharp
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
```

Commit: `git add IqOptionApi.CSharp && git commit -m "Add account and balance methods"`

---

### Task 9: Assets & market info

**Interfaces:** Produces `IReadOnlyDictionary<string,int> GetAllActives()`, `Task UpdateActivesAsync(ct)`, `Task UpdateDigitalActivesAsync(ct)`, `Task UpdateMarginActivesAsync(string? instrumentType = null, ct)`, `string? OpcodeToName(int)`, `Task<string?> GetNameByActiveIdAsync(int, ct)`, `Task<JsonObject> GetFinancialInformationAsync(int, ct)`, `Task<JsonObject> GetInstrumentsAsync(string, ct)`, `Task<JsonObject?> GetAllInitAsync(ct)`, `Task<Dictionary<string, Dictionary<string,bool>>> GetAllOpenTimeAsync(ct)`, `Task<Dictionary<string, Dictionary<string,JsonObject>>> GetBinaryOptionDetailAsync(ct)`, `Task<Dictionary<string, Dictionary<string,double>>> GetAllProfitAsync(ct)` / `GetAllProfitBlitzAsync(ct)`, `Task<JsonObject?> GetDigitalUnderlyingListAsync(ct)`, `Task<JsonNode?> GetTopAssetAsync(string, ct)`; private `ResendUntilAsync(string reply, object msg, TimeSpan interval, TimeSpan total, CancellationToken)` and `static JsonNode? Path(JsonNode?, params string[])` used by later tasks.

**Python cases** (the constants are reused by later tasks):

```python
    def active(name, ticker, enabled, suspended, exchange, provider, commission):
        return {"name": name, "ticker": ticker, "enabled": enabled, "is_suspended": suspended,
                "exchange": exchange, "provider": provider, "option": {"profit": {"commission": commission}}}

    INIT = {"name": "initialization-data", "request_id": "$RID", "msg": {
        "binary": {"actives": {"1861": active("front.EURUSD", "EURUSD", True, False, "", "", 20),
                               "76": active("front.EURUSD-OTC", "EURUSD-OTC", True, True, "x", "OTC", 15)}},
        "turbo": {"actives": {"1861": active("front.EURUSD", "EURUSD", True, False, "", "", 13),
                              "9999": active("front.NEWASSET", "NEWASSET", False, False, "", "", 30)}},
        "blitz": {"actives": {"76": active("front.EURUSD-OTC", "EURUSD-OTC", True, False, "", "OTC", 43)}}}}
    INIT_RULE = {"on": "sendMessage:get-initialization-data", "reply": [INIT]}
    UNDERLYING = {"name": "underlying-list", "request_id": "$RID", "msg": {"underlying": [
        {"active_id": 1861, "name": "EURUSD", "underlying": "EURUSD", "schedule": [{"open": T - 100, "close": T + 100}]},
        {"active_id": 5555, "name": "NEWDIGI", "underlying": "NEWDIGI", "schedule": [{"open": T + 100, "close": T + 200}]}]}}
    UNDERLYING_RULE = {"on": "sendMessage:digital-option-instruments.get-underlying-list", "reply": [UNDERLYING]}
    INSTRUMENTS = {"name": "instruments", "request_id": "", "msg": {"instruments": [
        {"id": "EURUSD-FX", "active_id": 1, "name": "EUR/USD", "schedule": [{"open": T - 10, "close": T + 10}]},
        {"id": "BTCX", "active_id": 6666, "name": "BTC/X", "schedule": []}]}}
    INSTRUMENTS_RULE = {"on": "sendMessage:get-instruments", "reply": [INSTRUMENTS]}

    def detail_from_v2(iq):
        iq.get_all_init = lambda: {"isSuccessful": True, "result": iq.get_all_init_v2()}
        return iq.get_binary_option_detail()

    cases += [
        {"name": "get_all_init", "call": lambda iq: iq.get_all_init_v2(), "rules": [INIT_RULE],
         "csharp": {"method": "GetAllInitAsync", "args": {}}},
        {"name": "get_all_profit", "call": lambda iq: iq.get_all_profit_v2(), "rules": [INIT_RULE],
         "csharp": {"method": "GetAllProfitAsync", "args": {}}},
        {"name": "get_all_profit_blitz", "call": lambda iq: iq.get_all_profit_blitz_v2(), "rules": [INIT_RULE],
         "csharp": {"method": "GetAllProfitBlitzAsync", "args": {}}},
        {"name": "update_actives", "call": lambda iq: (iq.update_ACTIVES_OPCODE(), actives_items())[1], "rules": [INIT_RULE],
         "csharp": {"method": "UpdateActivesAsync", "args": {}}},
        {"name": "get_binary_option_detail", "call": detail_from_v2, "rules": [INIT_RULE],
         "csharp": {"method": "GetBinaryOptionDetailAsync", "args": {}}},
        {"name": "get_digital_underlying_list", "call": lambda iq: iq.get_digital_underlying_list_data(), "rules": [UNDERLYING_RULE],
         "csharp": {"method": "GetDigitalUnderlyingListAsync", "args": {}}},
        {"name": "update_digital_actives", "call": lambda iq: (iq.get_All_Digital_ACTIVES_OPCODE(), actives_items())[1],
         "rules": [UNDERLYING_RULE], "csharp": {"method": "UpdateDigitalActivesAsync", "args": {}}},
        {"name": "get_instruments", "call": lambda iq: iq.get_instruments("forex"), "rules": [INSTRUMENTS_RULE],
         "csharp": {"method": "GetInstrumentsAsync", "args": {"type": "forex"}}},
        {"name": "update_margin_actives_forex", "call": lambda iq: (iq.instruments_input_to_ACTIVES("forex"), actives_items())[1],
         "rules": [INSTRUMENTS_RULE], "csharp": {"method": "UpdateMarginActivesAsync", "args": {"type": "forex"}}},
        {"name": "update_margin_actives_all", "call": lambda iq: (iq.instruments_input_all_in_ACTIVES(), actives_items())[1],
         "rules": [INSTRUMENTS_RULE], "csharp": {"method": "UpdateMarginActivesAsync", "args": {"type": None}}},
        {"name": "get_all_open_time", "call": lambda iq: iq.get_all_open_time(0),
         "rules": [INIT_RULE, UNDERLYING_RULE, INSTRUMENTS_RULE], "csharp": {"method": "GetAllOpenTimeAsync", "args": {}}},
        {"name": "get_financial_information", "call": lambda iq: iq.get_financial_information(1861),
         "rules": [{"on": "sendMessage:get-financial-information",
                    "reply": [{"name": "financial-information", "request_id": "", "msg": {"data": {"active": {"name": "EUR/USD"}}}}]}],
         "csharp": {"method": "GetFinancialInformationAsync", "args": {"activeId": 1861}}},
        {"name": "get_name_by_active_id", "call": lambda iq: iq.get_name_by_activeId(1861),
         "rules": [{"on": "sendMessage:get-financial-information",
                    "reply": [{"name": "financial-information", "request_id": "", "msg": {"data": {"active": {"name": "EUR/USD"}}}}]}],
         "csharp": {"method": "GetNameByActiveIdAsync", "args": {"activeId": 1861}}},
        {"name": "get_top_asset", "call": lambda iq: iq.get_top_asset("forex"),
         "rules": [{"on": "sendMessage:get-top-assets", "reply": [{"name": "top-assets", "request_id": "$RID", "msg": {"data": [1, 2]}}]}],
         "csharp": {"method": "GetTopAssetAsync", "args": {"type": "forex"}}},
        {"name": "opcode_to_name", "call": lambda iq: iq.opcode_to_name(76), "csharp": {"method": "OpcodeToName", "args": {"activeId": 76}}},
    ]
```

**Adapters:**

```csharp
            case "GetAllInitAsync": return await api.GetAllInitAsync();
            case "GetAllProfitAsync": return Nested(await api.GetAllProfitAsync());
            case "GetAllProfitBlitzAsync": return Nested(await api.GetAllProfitBlitzAsync());
            case "UpdateActivesAsync": await api.UpdateActivesAsync(); return Items(api.GetAllActives());
            case "GetBinaryOptionDetailAsync":
                return new JsonObject((await api.GetBinaryOptionDetailAsync()).Select(x => KeyValuePair.Create(x.Key,
                    (JsonNode?)new JsonObject(x.Value.Select(y => KeyValuePair.Create(y.Key, (JsonNode?)y.Value.DeepClone()))))));
            case "GetDigitalUnderlyingListAsync": return await api.GetDigitalUnderlyingListAsync();
            case "UpdateDigitalActivesAsync": await api.UpdateDigitalActivesAsync(); return Items(api.GetAllActives());
            case "GetInstrumentsAsync": return await api.GetInstrumentsAsync(S(a, "type"));
            case "UpdateMarginActivesAsync": await api.UpdateMarginActivesAsync(SN(a, "type")); return Items(api.GetAllActives());
            case "GetAllOpenTimeAsync":
                return new JsonObject((await api.GetAllOpenTimeAsync()).Select(t => KeyValuePair.Create(t.Key,
                    (JsonNode?)new JsonObject(t.Value.Select(x => KeyValuePair.Create(x.Key, (JsonNode?)new JsonObject { ["open"] = x.Value }))))));
            case "GetFinancialInformationAsync": return await api.GetFinancialInformationAsync(I(a, "activeId"));
            case "GetNameByActiveIdAsync": return await api.GetNameByActiveIdAsync(I(a, "activeId"));
            case "GetTopAssetAsync": return await api.GetTopAssetAsync(S(a, "type"));
            case "OpcodeToName": return api.OpcodeToName(I(a, "activeId"));
```

and add this helper to `Calls`:

```csharp
    internal static JsonObject Nested(Dictionary<string, Dictionary<string, double>> d) =>
        new(d.Select(x => KeyValuePair.Create(x.Key, (JsonNode?)new JsonObject(x.Value.Select(y => KeyValuePair.Create(y.Key, (JsonNode?)y.Value))))));
```

**Implementation:**

```csharp
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
```

Note on `get_all_open_time`: Python stores `enabled` itself when not enabled (always falsy there) — `enabled && !suspended` gives the same booleans.

Commit: `git add IqOptionApi.CSharp && git commit -m "Add asset and market-info methods"`

---

### Task 10: Candles & live prices

**Interfaces:** Produces `Task<List<Candle>> GetCandlesAsync(string asset, int intervalSeconds, int count, double endTime, ct)`, `Task<bool> StartCandlesStreamAsync(string, int size, int maxDict, ct)`, `Task<bool> StartAllCandlesStreamsAsync(string, int maxDict, ct)`, `Task StopCandlesStreamAsync(string, int, ct)`, `Task StopAllCandlesStreamsAsync(string, ct)`, `IReadOnlyDictionary<long,Candle> GetRealtimeCandles(string, int)`, `IReadOnlyDictionary<int, IReadOnlyDictionary<long,Candle>> GetRealtimeCandles(string)`, `IReadOnlyDictionary<string, IReadOnlyDictionary<int, IReadOnlyDictionary<long,Candle>>> GetAllRealtimeCandles()`; private `SubscribeUntilDataAsync(object msg, Func<bool> hasData, CancellationToken)` (also used by Task 14).

**Python cases:**

```python
    CANDLES = {"name": "candles", "request_id": "", "msg": {"candles": [
        {"id": 1, "from": T - 120, "at": 1, "to": T - 60, "open": 1.1, "close": 1.2, "min": 1.0, "max": 1.3, "volume": 0},
        {"id": 2, "from": T - 60, "at": 2, "to": T, "open": 1.2, "close": 1.25, "min": 1.1, "max": 1.3, "volume": 0}]}}
    CANDLES_RULE = {"on": "sendMessage:get-candles", "reply": [CANDLES]}
    LIVE_CANDLE = {"name": "candle-generated", "msg": {"active_id": 1861, "size": 60, "from": T, "to": T + 60, "id": 3,
                                                       "open": 1.25, "close": 1.26, "min": 1.25, "max": 1.27, "volume": 0, "at": 3}}
    ALL_SIZES = {"name": "candles-generated", "msg": {"active_id": 1861, "at": 4, "ask": 1.27, "bid": 1.26, "value": 1.265, "candles": {
        "1": {"id": 21, "from": T, "to": T + 1, "open": 1.26, "min": 1.26, "max": 1.27, "volume": 1},
        "60": {"id": 22, "from": T, "to": T + 60, "open": 1.25, "min": 1.25, "max": 1.27, "volume": 2}}}}
    cases += [
        {"name": "get_candles", "call": lambda iq: iq.get_candles("EURUSD", 60, 2, T + 0.9), "rules": [CANDLES_RULE],
         "csharp": {"method": "GetCandlesAsync", "args": {"asset": "EURUSD", "interval": 60, "count": 2, "endTime": T + 0.9}}},
        {"name": "start_candles_stream",
         "call": lambda iq: (iq.start_candles_stream("EURUSD", 60, 2), iq.get_realtime_candles("EURUSD", 60))[1],
         "rules": [CANDLES_RULE, {"on": "subscribeMessage:candle-generated", "reply": [LIVE_CANDLE]}],
         "csharp": {"method": "StartCandlesStreamAsync", "args": {"asset": "EURUSD", "size": 60, "maxDict": 2}}},
        {"name": "start_all_candles_streams",
         "call": lambda iq: (iq.start_candles_stream("EURUSD", "all", 2), iq.get_realtime_candles("EURUSD", "all"))[1],
         "rules": [CANDLES_RULE, {"on": "subscribeMessage:candles-generated", "reply": [ALL_SIZES]}],
         "csharp": {"method": "StartAllCandlesStreamsAsync", "args": {"asset": "EURUSD", "maxDict": 2}}},
        {"name": "stop_candles_stream", "setup": lambda iq, api: api.candle_generated_check["EURUSD"].__setitem__(60, True),
         "setup_csharp": "candle_check:EURUSD:60", "call": lambda iq: iq.stop_candles_stream("EURUSD", 60),
         "csharp": {"method": "StopCandlesStreamAsync", "args": {"asset": "EURUSD", "size": 60}}},
        {"name": "stop_all_candles_streams",
         "setup": lambda iq, api: api.candle_generated_all_size_check.__setitem__("EURUSD", True),
         "setup_csharp": "all_size_check:EURUSD", "call": lambda iq: iq.stop_candles_stream("EURUSD", "all"),
         "csharp": {"method": "StopAllCandlesStreamsAsync", "args": {"asset": "EURUSD"}}},
    ]
```

**Adapters:**

```csharp
            case "GetCandlesAsync":
                return new JsonArray((await api.GetCandlesAsync(S(a, "asset"), I(a, "interval"), I(a, "count"), D(a, "endTime")))
                    .Select(c => (JsonNode?)c.Raw.DeepClone()).ToArray());
            case "StartCandlesStreamAsync":
                await api.StartCandlesStreamAsync(S(a, "asset"), I(a, "size"), I(a, "maxDict"));
                return CandleMap(api.GetRealtimeCandles(S(a, "asset"), I(a, "size")));
            case "StartAllCandlesStreamsAsync":
                await api.StartAllCandlesStreamsAsync(S(a, "asset"), I(a, "maxDict"));
                return new JsonObject(api.GetRealtimeCandles(S(a, "asset"))
                    .Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)CandleMap(x.Value))));
            case "StopCandlesStreamAsync": await api.StopCandlesStreamAsync(S(a, "asset"), I(a, "size")); return null;
            case "StopAllCandlesStreamsAsync": await api.StopAllCandlesStreamsAsync(S(a, "asset")); return null;
```

and the helper:

```csharp
    internal static JsonObject CandleMap(IReadOnlyDictionary<long, Candle> d) =>
        new(d.Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)x.Value.Raw.DeepClone())));
```

**Implementation:**

```csharp
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
```

Note: the stream-stop cases wait one real second each (Python's `sleep(1)`); that is expected.

Commit: `git add IqOptionApi.CSharp && git commit -m "Add candle and live-candle methods"`
---

### Task 11: Binary / Turbo / Blitz

**Interfaces:** Produces `Task<(bool Success, long Id, string? Error)> BuyAsync(double amount, string asset, Direction, int minutes, ct)`, `Task<List<long?>> BuyMultiAsync(IList<double>, IList<string>, IList<Direction>, IList<int>, ct)`, `Task<(bool Success, JsonObject? Result, string? Error)> BuyByRawExpirationAsync(double, string asset, Direction, string optionType, long expired, ct)` + overload `(double, int activeId, Direction, int optionTypeId, long expired, ct)`, `Task<(bool Success, JsonObject? Result, string? Error)> BuyBlitzAsync(double, string, Direction, int durationSeconds = 30, ct)`, `Task<JsonObject?> SellOptionAsync(IEnumerable<long>, ct)`, `Task<(TradeResult Result, double Profit)> CheckWinAsync(long optionId, ct)`, `Task<JsonObject> GetOptionInfoAsync(int limit, ct)`, `int GetRemainingTime(int minutes, double? timestamp = null)`, `IReadOnlyDictionary<long,JsonObject> GetOptionOpenByOtherPc()`, `void DelOptionOpenByOtherPc(long)`.

**Python cases** (`INIT_RULE` comes from Task 9):

```python
    OPTION_OK = {"on": "sendMessage:binary-options.open-option",
                 "reply": [{"name": "option", "request_id": "$RID", "msg": {"id": 12345}}]}

    def buy_args(amount, asset, direction, minutes):
        return {"method": "BuyAsync", "args": {"amount": amount, "asset": asset, "direction": direction, "minutes": minutes}}

    cases += [
        {"name": "buy_turbo_1m", "call": lambda iq: iq.buy(1, "EURUSD", "call", 1), "rules": [OPTION_OK],
         "csharp": buy_args(1, "EURUSD", "call", 1)},
        {"name": "buy_turbo_5m_put", "call": lambda iq: iq.buy(2.5, "EURUSD-OTC", "put", 5), "rules": [OPTION_OK],
         "csharp": buy_args(2.5, "EURUSD-OTC", "put", 5)},
        {"name": "buy_binary_15m", "call": lambda iq: iq.buy(10, "GBPUSD", "call", 15), "rules": [OPTION_OK],
         "csharp": buy_args(10, "GBPUSD", "call", 15)},
        {"name": "buy_rejected", "call": lambda iq: iq.buy(1, "EURUSD", "call", 1),
         "rules": [{"on": "sendMessage:binary-options.open-option",
                    "reply": [{"name": "option", "request_id": "$RID", "msg": {"message": "Not enough money", "result": False}}]}],
         "csharp": buy_args(1, "EURUSD", "call", 1)},
        {"name": "buy_multi", "call": lambda iq: iq.buy_multi([1, 2], ["EURUSD", "EURUSD-OTC"], ["call", "put"], [1, 2]),
         "rules": [OPTION_OK],
         "csharp": {"method": "BuyMultiAsync", "args": {"amounts": [1, 2], "assets": ["EURUSD", "EURUSD-OTC"],
                                                       "directions": ["call", "put"], "minutes": [1, 2]}}},
        {"name": "buy_by_raw_expiration", "call": lambda iq: iq.buy_by_raw_expirations(1, "EURUSD", "put", "binary", T + 900),
         "rules": [OPTION_OK],
         "csharp": {"method": "BuyByRawExpirationAsync",
                    "args": {"amount": 1, "asset": "EURUSD", "direction": "put", "optionType": "binary", "expired": T + 900}}},
        {"name": "buy_blitz", "call": lambda iq: iq.buy_blitz(1, "EURUSD-OTC", "call", 30), "rules": [INIT_RULE, OPTION_OK],
         "csharp": {"method": "BuyBlitzAsync", "args": {"amount": 1, "asset": "EURUSD-OTC", "direction": "call", "seconds": 30}}},
        {"name": "sell_option", "call": lambda iq: iq.sell_option([12345, 12346]),
         "rules": [{"on": "sendMessage:sell-options", "reply": [{"name": "sold-options", "request_id": "$RID", "msg": {}}]}],
         "csharp": {"method": "SellOptionAsync", "args": {"ids": [12345, 12346]}}, "compare_result": False},
        {"name": "check_win", "pre": [{"name": "option-closed", "msg": {"option_id": 12345, "amount": 1, "profit_amount": 1.85}}],
         "call": lambda iq: iq.check_win_v3(12345, 0.01), "csharp": {"method": "CheckWinAsync", "args": {"id": 12345}}},
        {"name": "get_option_info", "call": lambda iq: iq.get_optioninfo_v2(10),
         "rules": [{"on": "sendMessage:get-options", "reply": [{"name": "options", "request_id": "", "msg": {"options": [{"id": 1}]}}]}],
         "csharp": {"method": "GetOptionInfoAsync", "args": {"limit": 10}}},
        {"name": "get_remaining_1", "call": lambda iq: iq.get_remaning(1), "csharp": {"method": "GetRemainingTime", "args": {"minutes": 1}}},
        {"name": "get_remaining_30", "call": lambda iq: iq.get_remaning(30), "csharp": {"method": "GetRemainingTime", "args": {"minutes": 30}}},
        {"name": "option_open_by_other_pc",
         "pre": [{"name": "position-changed", "microserviceName": "portfolio",
                  "msg": {"source": "binary-options", "status": "open", "external_id": 901, "raw_event": {}}}],
         "call": lambda iq: iq.get_option_open_by_other_pc(), "csharp": {"method": "GetOptionOpenByOtherPc", "args": {}}},
    ]
```

**Adapters:**

```csharp
            case "BuyAsync":
            {
                var (ok, id, err) = await api.BuyAsync(D(a, "amount"), S(a, "asset"), Dir(a), I(a, "minutes"));
                return Tuple(ok, ok ? (object)id : err);
            }
            case "BuyMultiAsync":
                return new JsonArray((await api.BuyMultiAsync(
                        a["amounts"]!.AsArray().Select(x => J.Dbl(x)).ToList(),
                        Strings(a, "assets"),
                        Strings(a, "directions").Select(d => d == "call" ? Direction.Call : Direction.Put).ToList(),
                        a["minutes"]!.AsArray().Select(x => (int)J.Long(x)).ToList()))
                    .Select(x => ToNode(x)).ToArray());
            case "BuyByRawExpirationAsync":
            {
                var (ok, res, err) = await api.BuyByRawExpirationAsync(D(a, "amount"), S(a, "asset"), Dir(a), S(a, "optionType"), L(a, "expired"));
                return Tuple(ok, ok ? (object?)res : err);
            }
            case "BuyBlitzAsync":
            {
                var (ok, res, err) = await api.BuyBlitzAsync(D(a, "amount"), S(a, "asset"), Dir(a), I(a, "seconds"));
                return Tuple(ok, ok ? (object?)res : err);
            }
            case "SellOptionAsync": await api.SellOptionAsync(Longs(a, "ids")); return null;
            case "CheckWinAsync": return (await api.CheckWinAsync(L(a, "id"))).Profit;
            case "GetOptionInfoAsync": return await api.GetOptionInfoAsync(I(a, "limit"));
            case "GetRemainingTime": return api.GetRemainingTime(I(a, "minutes"));
            case "GetOptionOpenByOtherPc":
                return new JsonObject(api.GetOptionOpenByOtherPc().Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)x.Value.DeepClone())));
```


**Extra tests** — create `tests/IqOptionApi.Tests/BinaryTests.cs`:

```csharp
namespace IqOptionApi.Tests;

public class BinaryTests
{
    [Fact]
    public async Task Buy_UnknownAsset_SendsNothing()
    {
        var (api, socket, _) = await Harness.ConnectedAsync(Golden.Load("methods.json")["fixtures"]!.AsObject());
        await using var _ = api;
        await Assert.ThrowsAsync<KeyNotFoundException>(() => api.BuyAsync(1, "EURUSDD", Direction.Call, 1));
        Assert.Empty(socket.TakeFrames());
    }

    [Fact]
    public async Task Buy_TimesOutWithFailureTuple()
    {
        var f = Golden.Load("methods.json")["fixtures"]!.AsObject();
        var socket = new FakeSocket { Rules = Harness.ConnectRules(f) };
        var options = Harness.Options(f);
        options.RequestTimeout = TimeSpan.FromMilliseconds(300);
        await using var api = new IqOption("user@example.com", "secret", options, () => socket, new FakeHttpHandler());
        Assert.True((await api.ConnectAsync()).Success);
        socket.Rules = new() { new Rule("sendMessage:binary-options.open-option", new() { System.Text.Json.Nodes.JsonNode.Parse("{\"name\":\"result\",\"request_id\":\"$RID\",\"msg\":{\"success\":false}}")!.AsObject() }) };
        Assert.Equal((false, 0L, "timeout"), await api.BuyAsync(1, "EURUSD", Direction.Call, 1));
    }

    [Fact]
    public async Task Blitz_InvalidDuration_Throws()
    {
        var (api, _, _) = await Harness.ConnectedAsync(Golden.Load("methods.json")["fixtures"]!.AsObject());
        await using var _ = api;
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => api.BuyBlitzAsync(1, "EURUSD", Direction.Call, 30));
    }
}
```

**Implementation:**

```csharp
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
```

Commit: `git add IqOptionApi.CSharp && git commit -m "Add binary, turbo and blitz methods"`

---

### Task 12: Digital options

**Interfaces:** Produces `Task<(bool Success, long Id, string? Error)> BuyDigitalSpotAsync(string asset, double amount, Direction, int minutes, ct)`, `Task<(bool Success, JsonObject? OrderChanged, string? Error)> BuyDigitalAsync(double amount, string instrumentId, ct)`, `Task<bool> CloseDigitalOptionAsync(long, ct)`, `Task<bool> CloseDigitalOptionBatchAsync(IEnumerable<long>, ct)`, `Task<(TradeResult Result, double Profit)> CheckWinDigitalAsync(long orderId, ct)`, `Task<(JsonObject Raw, Dictionary<string,(string Call, string Put)>? Strikes)> GetStrikeListAsync(string, int, ct)`, `Task SubscribeStrikeListAsync(string, int, ct)`, `Task UnsubscribeStrikeListAsync(string, int, ct)`, `Task<Dictionary<string,StrikeQuote>> GetRealtimeStrikeListAsync(string, int, ct)`, `Task<JsonObject> GetInstrumentQuotesGeneratedDataAsync(string, int, ct)`, `double? GetDigitalCurrentProfit(string, int)`, `Task<double?> GetDigitalSpotProfitAfterSaleAsync(long, ct)`, `Task<JsonObject> GetDigitalPositionAsync(long orderId, ct)`, `Task<JsonObject> GetDigitalPositionByPositionIdAsync(long, ct)`, `IReadOnlyDictionary<long,JsonObject> GetDigitalOpenByOtherPc()`, `void DelDigitalOpenByOtherPc(long)`; private `DigitalCloseProfit(long orderId)` used by Task 15.

**Python cases:**

```python
    PLACED = {"on": "sendMessage:digital-options.place-digital-option", "reply": [
        {"name": "digital-option-placed", "request_id": "$RID", "msg": {"id": 555}},
        {"name": "position-changed", "microserviceName": "portfolio",
         "msg": {"source": "digital-options", "status": "open", "external_id": 777, "raw_event": {"order_ids": [555]}}}]}
    QUOTES = {"name": "client-price-generated", "msg": {"asset_id": 1861, "prices": [
        {"strike": "SPT", "call": {"symbol": "do1861A20261009D123300T1MCSPT", "ask": 52.5, "bid": 47.5},
         "put": {"symbol": "do1861A20261009D123300T1MPSPT", "ask": 48.0, "bid": 51.0}},
        {"strike": "1170000", "call": {"symbol": "C1", "ask": 60, "bid": 47}, "put": {"symbol": "P1", "ask": 41, "bid": 50}},
        {"strike": "1171000", "call": {"symbol": "C2", "ask": 55, "bid": 40}, "put": {"symbol": "P2", "bid": 55}}]}}
    OPEN_777 = {"name": "position-changed", "microserviceName": "portfolio", "msg": {
        "source": "digital-options", "status": "open", "external_id": 777,
        "raw_event": {"order_ids": [555], "instrument_id": "do1861A20261009D123300T1MCSPT", "instrument_underlying": "EURUSD",
                      "buy_amount": 1, "sell_amount": 0, "instrument_period": 60, "count": 1.9,
                      "instrument_strike_value": 1170500, "currency_rate": 1,
                      "extra_data": {"lower_instrument_strike": 1170000, "upper_instrument_strike": 1171000,
                                     "lower_instrument_id": "C1", "upper_instrument_id": "C2"}}}}
    CLOSED_555 = {"name": "position-changed", "microserviceName": "portfolio", "msg": {
        "source": "digital-options", "status": "closed", "close_reason": "expired", "close_profit": 1.9, "invest": 1,
        "pnl_realized": 0.9, "external_id": 777, "raw_event": {"order_ids": [555]}}}
    RESULT_OK = {"name": "result", "request_id": "$RID", "msg": {"success": True}}
    POSITION_REPLY = {"on": "sendMessage:digital-options.get-position",
                      "reply": [{"name": "position", "request_id": "", "msg": {"position": {"id": 777, "status": "open"}}}]}
    cases += [
        {"name": "buy_digital_spot_1m", "call": lambda iq: iq.buy_digital_spot("EURUSD", 1, "call", 1), "rules": [PLACED],
         "csharp": {"method": "BuyDigitalSpotAsync", "args": {"asset": "EURUSD", "amount": 1, "direction": "call", "minutes": 1}}},
        {"name": "buy_digital_spot_5m", "call": lambda iq: iq.buy_digital_spot("EURUSD-OTC", 1.5, "put", 5), "rules": [PLACED],
         "csharp": {"method": "BuyDigitalSpotAsync", "args": {"asset": "EURUSD-OTC", "amount": 1.5, "direction": "put", "minutes": 5}}},
        {"name": "buy_digital_spot_rejected", "call": lambda iq: iq.buy_digital_spot("EURUSD", 1, "call", 1),
         "rules": [{"on": "sendMessage:digital-options.place-digital-option",
                    "reply": [{"name": "digital-option-placed", "request_id": "$RID", "msg": {"message": "fail"}}]}],
         "csharp": {"method": "BuyDigitalSpotAsync", "args": {"asset": "EURUSD", "amount": 1, "direction": "call", "minutes": 1}},
         "compare_result": False},
        {"name": "buy_digital", "call": lambda iq: iq.buy_digital(1, "do1861A20261009D123400T1MCSPT"),
         "rules": [{"on": "sendMessage:digital-options.place-digital-option", "reply": [
             {"name": "digital-option-placed", "request_id": "$RID", "msg": {"id": 556}},
             {"name": "order-changed", "microserviceName": "portfolio",
              "msg": {"source": "digital-options", "raw_event": {"position_id": 778, "id": 556}}}]}],
         "csharp": {"method": "BuyDigitalAsync", "args": {"amount": 1, "instrumentId": "do1861A20261009D123400T1MCSPT"}}},
        {"name": "close_digital_option", "call": lambda iq: iq.close_digital_option(777),
         "rules": [{"on": "sendMessage:digital-options.close-position", "reply": [RESULT_OK]}],
         "csharp": {"method": "CloseDigitalOptionAsync", "args": {"id": 777}}},
        {"name": "close_digital_option_batch", "call": lambda iq: iq.close_digital_option_batch([777, 778]),
         "rules": [{"on": "sendMessage:digital-options.close-position-batch", "reply": [RESULT_OK]}],
         "csharp": {"method": "CloseDigitalOptionBatchAsync", "args": {"ids": [777, 778]}}},
        {"name": "check_win_digital", "pre": [CLOSED_555], "call": lambda iq: iq.check_win_digital_v2(555),
         "csharp": {"method": "CheckWinDigitalAsync", "args": {"id": 555}}},
        {"name": "get_strike_list", "call": lambda iq: iq.get_strike_list("EURUSD", 1),
         "rules": [{"on": "sendMessage:get-strike-list", "reply": [{"name": "strike-list", "request_id": "", "msg": {"strike": [
             {"value": "1170000", "call": {"id": "C1"}, "put": {"id": "P1"}},
             {"value": 1171000, "call": {"id": "C2"}, "put": {"id": "P2"}}]}}]}],
         "csharp": {"method": "GetStrikeListAsync", "args": {"asset": "EURUSD", "minutes": 1}}},
        {"name": "subscribe_strike_list", "call": lambda iq: iq.subscribe_strike_list("EURUSD", 1),
         "csharp": {"method": "SubscribeStrikeListAsync", "args": {"asset": "EURUSD", "minutes": 1}}},
        {"name": "unsubscribe_strike_list", "call": lambda iq: iq.unsubscribe_strike_list("EURUSD", 1),
         "csharp": {"method": "UnsubscribeStrikeListAsync", "args": {"asset": "EURUSD", "minutes": 1}}},
        {"name": "get_realtime_strike_list", "pre": [QUOTES], "call": lambda iq: iq.get_realtime_strike_list("EURUSD", 1),
         "csharp": {"method": "GetRealtimeStrikeListAsync", "args": {"asset": "EURUSD", "minutes": 1}}},
        {"name": "get_instrument_quotes", "pre": [QUOTES], "call": lambda iq: iq.get_instrument_quites_generated_data("EURUSD", 1),
         "csharp": {"method": "GetInstrumentQuotesGeneratedDataAsync", "args": {"asset": "EURUSD", "minutes": 1}}},
        {"name": "get_digital_current_profit", "pre": [QUOTES], "call": lambda iq: iq.get_digital_current_profit("EURUSD", 1),
         "csharp": {"method": "GetDigitalCurrentProfit", "args": {"asset": "EURUSD", "minutes": 1}}},
        {"name": "get_digital_spot_profit_after_sale", "pre": [OPEN_777, QUOTES],
         "call": lambda iq: iq.get_digital_spot_profit_after_sale(777),
         "csharp": {"method": "GetDigitalSpotProfitAfterSaleAsync", "args": {"id": 777}}},
        {"name": "get_digital_position_by_position_id", "call": lambda iq: iq.get_digital_position_by_position_id(777),
         "rules": [POSITION_REPLY], "csharp": {"method": "GetDigitalPositionByPositionIdAsync", "args": {"id": 777}}},
        {"name": "get_digital_position", "pre": [OPEN_777], "call": lambda iq: iq.get_digital_position(555),
         "rules": [POSITION_REPLY], "csharp": {"method": "GetDigitalPositionAsync", "args": {"id": 555}}},
        {"name": "digital_open_by_other_pc", "pre": [OPEN_777], "call": lambda iq: iq.get_digital_open_by_other_pc(),
         "csharp": {"method": "GetDigitalOpenByOtherPc", "args": {}}},
    ]
```

**Adapters:**

```csharp
            case "BuyDigitalSpotAsync":
            {
                var (ok, id, err) = await api.BuyDigitalSpotAsync(S(a, "asset"), D(a, "amount"), Dir(a), I(a, "minutes"));
                return Tuple(ok, ok ? (object)id : err);
            }
            case "BuyDigitalAsync":
            {
                var (ok, changed, err) = await api.BuyDigitalAsync(D(a, "amount"), S(a, "instrumentId"));
                return Tuple(ok, ok ? (object?)changed : err);
            }
            case "CloseDigitalOptionAsync": return await api.CloseDigitalOptionAsync(L(a, "id"));
            case "CloseDigitalOptionBatchAsync": return await api.CloseDigitalOptionBatchAsync(Longs(a, "ids"));
            case "CheckWinDigitalAsync": return Tuple(true, (await api.CheckWinDigitalAsync(L(a, "id"))).Profit);
            case "GetStrikeListAsync":
            {
                var (raw, strikes) = await api.GetStrikeListAsync(S(a, "asset"), I(a, "minutes"));
                return Tuple(raw, strikes == null ? null : new JsonObject(strikes.Select(x => KeyValuePair.Create(x.Key,
                    (JsonNode?)new JsonObject { ["call"] = x.Value.Call, ["put"] = x.Value.Put }))));
            }
            case "SubscribeStrikeListAsync": await api.SubscribeStrikeListAsync(S(a, "asset"), I(a, "minutes")); return null;
            case "UnsubscribeStrikeListAsync": await api.UnsubscribeStrikeListAsync(S(a, "asset"), I(a, "minutes")); return null;
            case "GetRealtimeStrikeListAsync":
                return new JsonObject((await api.GetRealtimeStrikeListAsync(S(a, "asset"), I(a, "minutes"))).Select(x => KeyValuePair.Create(x.Key,
                    (JsonNode?)new JsonObject
                    {
                        ["call"] = new JsonObject { ["id"] = x.Value.CallId, ["profit"] = x.Value.CallProfit },
                        ["put"] = new JsonObject { ["id"] = x.Value.PutId, ["profit"] = x.Value.PutProfit },
                    })));
            case "GetInstrumentQuotesGeneratedDataAsync": return await api.GetInstrumentQuotesGeneratedDataAsync(S(a, "asset"), I(a, "minutes"));
            case "GetDigitalCurrentProfit": return ToNode(api.GetDigitalCurrentProfit(S(a, "asset"), I(a, "minutes")));
            case "GetDigitalSpotProfitAfterSaleAsync": return ToNode(await api.GetDigitalSpotProfitAfterSaleAsync(L(a, "id")));
            case "GetDigitalPositionByPositionIdAsync": return await api.GetDigitalPositionByPositionIdAsync(L(a, "id"));
            case "GetDigitalPositionAsync": return await api.GetDigitalPositionAsync(L(a, "id"));
            case "GetDigitalOpenByOtherPc":
                return new JsonObject(api.GetDigitalOpenByOtherPc().Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)x.Value.DeepClone())));
```

**Implementation:**

```csharp
    #region Digital

    private Obj DigitalPlaceFrame(string instrumentId, double amount)
    {
        if (instrumentId.StartsWith("do", StringComparison.Ordinal))
        {
            string assetId = instrumentId.Split("do")[1].Split('A')[0];
            return new Obj
            {
                ["name"] = "digital-options.place-digital-option",
                ["version"] = "2.0",
                ["body"] = new Obj
                {
                    ["user_balance_id"] = Bal,
                    ["instrument_id"] = instrumentId,
                    ["instrument_index"] = 0,
                    ["amount"] = PyJson.Str(amount),
                    ["asset_id"] = int.Parse(assetId, CultureInfo.InvariantCulture),
                },
            };
        }
        return new Obj
        {
            ["name"] = "digital-options.place-digital-option",
            ["version"] = "1.0",
            ["body"] = new Obj { ["user_balance_id"] = Bal, ["instrument_id"] = instrumentId, ["amount"] = PyJson.Str(amount) },
        };
    }

    private bool HasAsync(long id, string name)
    {
        lock (_s.Lock) return _s.HasAsync(id, name);
    }

    private JsonObject AsyncMessage(long id, string name)
    {
        lock (_s.Lock) return _s.OrderAsync[id][name];
    }

    /// <summary>Buys a digital spot option (Python <c>buy_digital_spot</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="amount">Stake.</param>
    /// <param name="direction">Call or put.</param>
    /// <param name="minutes">Expiry in minutes (1, 5, 15).</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns><c>(true, positionId, null)</c> or <c>(false, 0, error)</c>.</returns>
    public async Task<(bool Success, long Id, string? Error)> BuyDigitalSpotAsync(string asset, double amount, Direction direction, int minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        double now = Now;
        var (exp, _) = Helpers.DigitalExp((long)now, minutes, now, _tz);
        string instrumentId = Helpers.DigitalSpotInstrumentId(Act(asset), exp, minutes, direction);
        JsonObject reply;
        try
        {
            reply = await ById("digital-option-placed", DigitalPlaceFrame(instrumentId, amount), rid, ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, 0, Reason(ex));
        }
        if (reply["msg"] is not JsonObject m || !m.ContainsKey("id"))
            return (false, 0, J.Str(Path(reply, "msg", "message")) ?? reply.ToJsonString());
        long orderId = J.Long(m["id"]);
        if (!await WaitCondAsync(() => HasAsync(orderId, "position-changed"), T, ct).ConfigureAwait(false)) return (false, 0, "timeout");
        var external = Path(AsyncMessage(orderId, "position-changed"), "msg", "external_id");
        return J.IsNull(external) ? (false, 0, null) : (true, J.Long(external), null);
    }

    /// <summary>Buys any digital instrument id, e.g. a strike from <see cref="GetRealtimeStrikeListAsync"/> (Python <c>buy_digital</c>).</summary>
    /// <param name="amount">Stake.</param>
    /// <param name="instrumentId">Instrument id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonObject? OrderChanged, string? Error)> BuyDigitalAsync(double amount, string instrumentId, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        try
        {
            var reply = await ById("digital-option-placed", DigitalPlaceFrame(instrumentId, amount), rid, ct).ConfigureAwait(false);
            if (reply["msg"] is not JsonObject m || !m.ContainsKey("id"))
                return (false, null, J.Str(Path(reply, "msg", "message")) ?? reply.ToJsonString());
            long orderId = J.Long(m["id"]);
            if (!await WaitCondAsync(() => HasAsync(orderId, "order-changed"), T, ct).ConfigureAwait(false)) return (false, null, "timeout");
            return (true, AsyncMessage(orderId, "order-changed"), null);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null, Reason(ex));
        }
    }

    /// <summary>Sells a digital position early (Python <c>close_digital_option</c>).</summary>
    /// <param name="positionId">Position id.</param>
    /// <param name="ct">Cancellation.</param>
    public Task<bool> CloseDigitalOptionAsync(long positionId, CancellationToken ct = default) =>
        DigitalResultAsync(new Obj { ["name"] = "digital-options.close-position", ["version"] = "1.0", ["body"] = new Obj { ["position_id"] = positionId } }, ct);

    /// <summary>Sells several digital positions early (Python <c>close_digital_option_batch</c>).</summary>
    /// <param name="positionIds">Position ids.</param>
    /// <param name="ct">Cancellation.</param>
    public Task<bool> CloseDigitalOptionBatchAsync(IEnumerable<long> positionIds, CancellationToken ct = default) =>
        DigitalResultAsync(new Obj { ["name"] = "digital-options.close-position-batch", ["version"] = "1.0", ["body"] = new Obj { ["position_ids"] = positionIds.ToList() } }, ct);

    private async Task<bool> DigitalResultAsync(Obj msg, CancellationToken ct)
    {
        EnsureConnected();
        string rid = NextRid();
        try
        {
            var r = await ById("result", msg, rid, ct).ConfigureAwait(false);
            return J.IsTrue(Path(r, "msg", "success"));
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return false;
        }
    }

    /// <summary>Waits until a digital trade closes (Python <c>check_win_digital_v2</c>, waiting like <c>check_win_digital</c>). No timeout.</summary>
    /// <param name="orderId">Order id (or position id) of the trade.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(TradeResult Result, double Profit)> CheckWinDigitalAsync(long orderId, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track) _pendingDigital.Add(orderId);
        try
        {
            double? profit = null;
            await _conn.WaitUntilAsync(() => (profit = DigitalCloseProfit(orderId)) != null, null, ct).ConfigureAwait(false);
            return (Helpers.ResultOf(profit!.Value), profit.Value);
        }
        finally
        {
            lock (_track) _pendingDigital.Remove(orderId);
        }
    }

    private double? DigitalCloseProfit(long orderId)
    {
        lock (_s.Lock)
        {
            if (_s.OrderAsync.TryGetValue(orderId, out var d) && d.TryGetValue("position-changed", out var pc)
                && pc["msg"] is JsonObject m && J.Str(m["status"]) == "closed")
            {
                if (J.Str(m["close_reason"]) == "expired") return J.Dbl(m["close_profit"]) - J.Dbl(m["invest"]);
                if (J.Str(m["close_reason"]) != "default") Log($"digital {orderId} closed with reason '{J.Str(m["close_reason"])}', using pnl_realized");
                return J.Dbl(m["pnl_realized"]);
            }
        }
        lock (_track) return _recoveredDigital.TryGetValue(orderId, out double p) ? p : null;
    }

    /// <summary>Strike ids for an expiry (Python <c>get_strike_list</c>): raw message plus <c>{"%.6f" price: (call, put)}</c>, or null strikes if unreadable.</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="minutes">Expiry in minutes.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(JsonObject Raw, Dictionary<string, (string Call, string Put)>? Strikes)> GetStrikeListAsync(string asset, int minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        long exp = Helpers.StrikeListExpiration((long)Now, minutes, _tz);
        var raw = await ByName("strike-list", new Obj
        {
            ["name"] = "get-strike-list",
            ["body"] = new Obj { ["type"] = "digital-option", ["underlying"] = asset, ["expiration"] = exp * 1000, ["period"] = minutes * 60 },
            ["version"] = "4.0",
        }, "", ct).ConfigureAwait(false);
        try
        {
            var strikes = new Dictionary<string, (string, string)>();
            foreach (var s in J.Arr(Path(raw, "msg", "strike")))
                strikes[Helpers.StrikeKey(J.Dbl(s!["value"]))] = (J.Str(Path(s, "call", "id"))!, J.Str(Path(s, "put", "id"))!);
            return (raw, strikes);
        }
        catch (FormatException ex)
        {
            Log("get-strike-list: " + ex.Message);
            return (raw, null);
        }
    }

    /// <summary>Streams digital quotes and payouts for an expiry (Python <c>subscribe_strike_list</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="minutes">Expiry in minutes.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task SubscribeStrikeListAsync(string asset, int minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        var msg = StrikeSubscribeMsg(asset, minutes);
        lock (_track)
        {
            if (!_strikeSubscriptions.Contains((asset, minutes))) _strikeSubscriptions.Add((asset, minutes));
        }
        await Send("subscribeMessage", msg, "", ct).ConfigureAwait(false);
    }

    /// <summary>Stops <see cref="SubscribeStrikeListAsync"/> (Python <c>unsubscribe_strike_list</c>; the server filter has no period).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="minutes">Expiry in minutes.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task UnsubscribeStrikeListAsync(string asset, int minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track) _strikeSubscriptions.Remove((asset, minutes));
        await Send("unsubscribeMessage", StrikeUnsubscribeMsg(asset), "", ct).ConfigureAwait(false);
    }

    /// <summary>Live strikes with ids and payout % (Python <c>get_realtime_strike_list</c>); waits for the first quote. Needs <see cref="SubscribeStrikeListAsync"/>.</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="minutes">Expiry in minutes.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<Dictionary<string, StrikeQuote>> GetRealtimeStrikeListAsync(string asset, int minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        int period = minutes * 60;
        await _conn.WaitUntilAsync(() => { lock (_s.Lock) return _s.Quotes.TryGetValue(asset, out var q) && q.TryGetValue(period, out var d) && d.Count > 0; }, T, ct)
            .ConfigureAwait(false);
        lock (_s.Lock) return new Dictionary<string, StrikeQuote>(_s.Quotes[asset][period]);
    }

    /// <summary>The last raw quote message for an expiry (Python <c>get_instrument_quites_generated_data</c>); waits for it.</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="minutes">Expiry in minutes.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetInstrumentQuotesGeneratedDataAsync(string asset, int minutes, CancellationToken ct = default)
    {
        EnsureConnected();
        int period = minutes * 60;
        await _conn.WaitUntilAsync(() => { lock (_s.Lock) return _s.QuotesRaw.TryGetValue(asset, out var q) && q.ContainsKey(period); }, T, ct)
            .ConfigureAwait(false);
        lock (_s.Lock) return _s.QuotesRaw[asset][period];
    }

    /// <summary>Current spot payout % (Python <c>get_digital_current_profit</c>); null if not streamed yet.</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="minutes">Expiry in minutes.</param>
    public double? GetDigitalCurrentProfit(string asset, int minutes)
    {
        lock (_s.Lock) return _s.DigitalProfit.TryGetValue(asset, out var d) && d.TryGetValue(minutes * 60, out var p) ? p : null;
    }

    /// <summary>Estimated profit if the spot position were sold now (Python <c>get_digital_spot_profit_after_sale</c>, Python's formula kept as-is).</summary>
    /// <param name="positionId">Position id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<double?> GetDigitalSpotProfitAfterSaleAsync(long positionId, CancellationToken ct = default)
    {
        EnsureConnected();
        await _conn.WaitUntilAsync(() => HasAsync(positionId, "position-changed"), T, ct).ConfigureAwait(false);
        var p = J.Obj(Path(AsyncMessage(positionId, "position-changed"), "msg", "raw_event"));
        var extra = J.Obj(p["extra_data"]);
        int duration = (int)(J.Dbl(p["instrument_period"]) / 60);
        var quotes = await GetInstrumentQuotesGeneratedDataAsync(J.Str(p["instrument_underlying"])!, duration, ct).ConfigureAwait(false);
        double? Bid(string? id)
        {
            foreach (var row in J.Arr(Path(quotes, "msg", "prices")))
            {
                if (J.Str(Path(row, "call", "symbol")) == id) return J.DblOrNull(Path(row, "call", "bid"));
                if (J.Str(Path(row, "put", "symbol")) == id) return J.DblOrNull(Path(row, "put", "bid"));
            }
            return null;
        }
        double? fNow = Bid(J.Str(extra["lower_instrument_id"])), f2Now = Bid(J.Str(extra["upper_instrument_id"]));
        double f, f2;
        lock (_track)
        {
            var cached = _afterSale.TryGetValue(positionId, out var c) ? c : (0, 0);
            f = fNow ?? cached.F;
            f2 = f2Now ?? cached.F2;
            _afterSale[positionId] = (f, f2);
        }
        return Helpers.SpotProfitAfterSale(
            J.Dbl(p["instrument_strike_value"]) / 1000000.0,
            J.Dbl(extra["lower_instrument_strike"]) / 1000000.0,
            J.Dbl(extra["upper_instrument_strike"]) / 1000000.0,
            f, f2, isCall: false, J.Dbl(p["currency_rate"]), J.Dbl(p["count"]),
            Math.Max(J.Dbl(p["buy_amount"]), J.Dbl(p["sell_amount"])));
    }

    /// <summary>Digital position for an order (Python <c>get_digital_position</c>): waits for its position id, then asks the server.</summary>
    /// <param name="orderId">Order id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetDigitalPositionAsync(long orderId, CancellationToken ct = default)
    {
        EnsureConnected();
        await _conn.WaitUntilAsync(() => HasAsync(orderId, "position-changed"), T, ct).ConfigureAwait(false);
        return await GetDigitalPositionByPositionIdAsync(J.Long(Path(AsyncMessage(orderId, "position-changed"), "msg", "external_id")), ct).ConfigureAwait(false);
    }

    /// <summary>Digital position by position id (Python <c>get_digital_position_by_position_id</c>); the whole message.</summary>
    /// <param name="positionId">Position id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetDigitalPositionByPositionIdAsync(long positionId, CancellationToken ct = default)
    {
        EnsureConnected();
        return await ByName("position", new Obj { ["name"] = "digital-options.get-position", ["body"] = new Obj { ["position_id"] = positionId } }, "", ct)
            .ConfigureAwait(false);
    }

    /// <summary>Digital options opened on any device, by position id (Python <c>get_digital_open_by_other_pc</c>).</summary>
    public IReadOnlyDictionary<long, JsonObject> GetDigitalOpenByOtherPc()
    {
        lock (_s.Lock) return new Dictionary<long, JsonObject>(_s.DigitalOpened);
    }

    /// <summary>Forgets one entry of <see cref="GetDigitalOpenByOtherPc"/>.</summary>
    /// <param name="id">Position id.</param>
    public void DelDigitalOpenByOtherPc(long id)
    {
        lock (_s.Lock) _s.DigitalOpened.Remove(id);
    }

    #endregion
```

Notes: Python `get_digital_spot_profit_after_sale` caches bids with default `0`, so the "missing bid" branch never returns None; the C# cache does the same. Python's get_strike_list catches KeyError/TypeError → `J.Arr`/`J.Dbl` throw `FormatException` in those cases.

Commit: `git add IqOptionApi.CSharp && git commit -m "Add digital option methods"`
---

### Task 13: Margin (Forex / CFD / Crypto)

**Interfaces:** Produces `Task<(bool Success, long? OrderId, string? Error)> BuyOrderAsync(...)` (parameters as spec §5.8), `Task<(bool Success, JsonNode? Result)> ChangeOrderAsync(string idName, long id, string? stopLoseKind, double? stopLoseValue, string? takeProfitKind, double? takeProfitValue, bool useTrailStop, bool autoMarginCall, ct)`, `Task<(bool Success, JsonObject? Message)> ChangeAutoMarginCallAsync(string idName, long id, bool, ct)`, `Task<(bool Success, JsonObject? Order)> GetOrderAsync(long, ct)`, `JsonObject GetAsyncOrder(long)`, `Task<(bool Success, JsonObject? Message)> GetPendingAsync(string, ct)` / `GetPositionsAsync(string, ct)` / `GetPositionAsync(long orderId, ct)` / `GetPositionHistoryAsync(long userBalanceId, long start, long end, int offset, int limit, IEnumerable<string>, ct)` / `GetAvailableLeveragesAsync(string, string asset = "", ct)` / `GetOvernightFeeAsync(string, string, ct)`, `Task<JsonObject> GetPortfolioPositionsAsync(int limit = 1, int offset = 0, ct)`, `Task<JsonObject> GetHistoryPositionsAsync(long, ct)`, `Task<bool> CancelOrderAsync(long, ct)`, `Task<bool> ClosePositionAsync(long orderId, ct)`, `Task<double?> CheckWinMarginAsync(long id, double pollingSeconds = 0.5, ct)`; private `RawCheckWinAsync(long, CancellationToken)` used by Task 15.

**Python cases** (`CLOSED_555` comes from Task 12):

```python
    ORDER_FILLED = {"on": "sendMessage:get-order", "reply": [{"name": "order", "request_id": "$RID", "status": 2000,
                                                              "msg": {"id": 4242, "status": "filled", "position_id": 5151}}]}
    PLACED_TEMP = {"on": "sendMessage:place-order-temp",
                   "reply": [{"name": "order-placed-temp", "request_id": "$RID", "msg": {"id": 4242}}]}

    def ok_reply(on, name, msg=None):
        return {"on": "sendMessage:" + on,
                "reply": [{"name": name, "request_id": "$RID", "status": 2000, "msg": msg if msg is not None else {"ok": True}}]}

    BUY_ORDER_ARGS = {"instrumentType": "forex", "instrumentId": "EURUSD", "side": "buy", "amount": 10, "leverage": 100,
                      "type": "market", "limitPrice": None, "stopPrice": None, "stopLoseKind": "percent", "stopLoseValue": 50,
                      "takeProfitKind": None, "takeProfitValue": None, "useTrailStop": False, "autoMarginCall": False,
                      "useTokenForCommission": False}
    HISTORY_5151 = ok_reply("get-history-positions", "history-positions", {"positions": [{"external_id": 5151, "pnl_realized": 2.675}]})
    buy_order = lambda iq: iq.buy_order("forex", "EURUSD", "buy", 10, 100, "market", stop_lose_kind="percent", stop_lose_value=50)
    cases += [
        {"name": "buy_order", "call": buy_order, "rules": [PLACED_TEMP, ORDER_FILLED],
         "csharp": {"method": "BuyOrderAsync", "args": BUY_ORDER_ARGS}},
        {"name": "buy_order_rejected", "call": buy_order,
         "rules": [PLACED_TEMP, {"on": "sendMessage:get-order", "reply": [{"name": "order", "request_id": "$RID", "status": 2000,
                   "msg": {"id": 4242, "status": "rejected", "reject_status": "insufficient_funds", "position_id": None}}]}],
         "csharp": {"method": "BuyOrderAsync", "args": BUY_ORDER_ARGS}},
        {"name": "get_order", "call": lambda iq: iq.get_order(4242), "rules": [ORDER_FILLED],
         "csharp": {"method": "GetOrderAsync", "args": {"id": 4242}}},
        {"name": "change_auto_margin_call", "call": lambda iq: iq.change_auto_margin_call("order_id", 4242, True),
         "rules": [ok_reply("change-auto-margin-call", "auto-margin-call-changed")],
         "csharp": {"method": "ChangeAutoMarginCallAsync", "args": {"idName": "order_id", "id": 4242, "autoMarginCall": True}}},
        {"name": "change_order_by_position",
         "call": lambda iq: iq.change_order("position_id", 4242, "percent", 30, "percent", 60, False, False),
         "rules": [ORDER_FILLED, ok_reply("change-tpsl", "tpsl-changed", {"changed": True}),
                   ok_reply("change-auto-margin-call", "auto-margin-call-changed")],
         "csharp": {"method": "ChangeOrderAsync", "args": {"idName": "position_id", "id": 4242, "stopLoseKind": "percent",
                                                          "stopLoseValue": 30, "takeProfitKind": "percent", "takeProfitValue": 60,
                                                          "useTrailStop": False, "autoMarginCall": False}}},
        {"name": "get_async_order", "pre": [CLOSED_555], "call": lambda iq: dict(iq.get_async_order(555)),
         "csharp": {"method": "GetAsyncOrder", "args": {"id": 555}}},
        {"name": "get_pending", "call": lambda iq: iq.get_pending("forex"),
         "rules": [ok_reply("get-deferred-orders", "deferred-orders", {"deferred_orders": []})],
         "csharp": {"method": "GetPendingAsync", "args": {"type": "forex"}}},
        {"name": "get_positions_forex", "call": lambda iq: iq.get_positions("forex"),
         "rules": [ok_reply("get-positions", "positions", {"positions": [], "total": 0})],
         "csharp": {"method": "GetPositionsAsync", "args": {"type": "forex"}}},
        {"name": "get_positions_digital", "call": lambda iq: iq.get_positions("digital-option"),
         "rules": [ok_reply("digital-options.get-positions", "positions", {"positions": []})],
         "csharp": {"method": "GetPositionsAsync", "args": {"type": "digital-option"}}},
        {"name": "portfolio_get_positions", "call": lambda iq: iq.portfolio_get_positions(),
         "rules": [ok_reply("portfolio.get-positions", "positions", {"positions": []})],
         "csharp": {"method": "GetPortfolioPositionsAsync", "args": {}}},
        {"name": "get_position", "call": lambda iq: iq.get_position(4242),
         "rules": [ORDER_FILLED, ok_reply("get-position", "position", {"id": 5151})],
         "csharp": {"method": "GetPositionAsync", "args": {"id": 4242}}},
        {"name": "get_position_history", "call": lambda iq: iq.get_position_history_v2(PRACTICE_ID, 0, T, 0, 10, ["forex", "crypto"]),
         "rules": [ok_reply("get-history", "history", {"positions": []})],
         "csharp": {"method": "GetPositionHistoryAsync", "args": {"balanceId": PRACTICE_ID, "start": 0, "end": T, "offset": 0,
                                                                 "limit": 10, "types": ["forex", "crypto"]}}},
        {"name": "get_history_positions", "call": lambda iq: iq.get_history_positions(5151), "rules": [HISTORY_5151],
         "csharp": {"method": "GetHistoryPositionsAsync", "args": {"id": 5151}}},
        {"name": "get_available_leverages", "call": lambda iq: iq.get_available_leverages("forex", "EURUSD"),
         "rules": [ok_reply("get-available-leverages", "available-leverages", {"leverages": [1, 100]})],
         "csharp": {"method": "GetAvailableLeveragesAsync", "args": {"type": "forex", "asset": "EURUSD"}}},
        {"name": "get_available_leverages_all", "call": lambda iq: iq.get_available_leverages("forex"),
         "rules": [ok_reply("get-available-leverages", "available-leverages", {"leverages": [1, 100]})],
         "csharp": {"method": "GetAvailableLeveragesAsync", "args": {"type": "forex", "asset": ""}}},
        {"name": "cancel_order", "call": lambda iq: iq.cancel_order(4242), "rules": [ok_reply("cancel-order", "order-canceled")],
         "csharp": {"method": "CancelOrderAsync", "args": {"id": 4242}}},
        {"name": "close_position", "call": lambda iq: iq.close_position(4242),
         "rules": [ORDER_FILLED, ok_reply("close-position", "position-closed")],
         "csharp": {"method": "ClosePositionAsync", "args": {"id": 4242}}},
        {"name": "get_overnight_fee", "call": lambda iq: iq.get_overnight_fee("forex", "EURUSD"),
         "rules": [ok_reply("get-overnight-fee", "overnight-fee", {"fee": 0.1})],
         "csharp": {"method": "GetOvernightFeeAsync", "args": {"type": "forex", "asset": "EURUSD"}}},
        {"name": "check_win_margin", "call": lambda iq: iq.check_win(4242, 0.01), "rules": [ORDER_FILLED, HISTORY_5151],
         "csharp": {"method": "CheckWinMarginAsync", "args": {"id": 4242, "polling": 0.01}}},
    ]
```

**Adapters:**

```csharp
            case "BuyOrderAsync":
            {
                var (ok, id, err) = await api.BuyOrderAsync(S(a, "instrumentType"), S(a, "instrumentId"), S(a, "side"), D(a, "amount"), I(a, "leverage"),
                    S(a, "type"), DN(a, "limitPrice"), DN(a, "stopPrice"), SN(a, "stopLoseKind"), DN(a, "stopLoseValue"), SN(a, "takeProfitKind"),
                    DN(a, "takeProfitValue"), B(a, "useTrailStop"), B(a, "autoMarginCall"), B(a, "useTokenForCommission"));
                return Tuple(ok, ok ? (object?)id : err);
            }
            case "GetOrderAsync": { var (ok, o) = await api.GetOrderAsync(L(a, "id")); return Tuple(ok, o); }
            case "ChangeAutoMarginCallAsync":
            {
                var (ok, m) = await api.ChangeAutoMarginCallAsync(S(a, "idName"), L(a, "id"), B(a, "autoMarginCall"));
                return Tuple(ok, m);
            }
            case "ChangeOrderAsync":
            {
                var (ok, r) = await api.ChangeOrderAsync(S(a, "idName"), L(a, "id"), SN(a, "stopLoseKind"), DN(a, "stopLoseValue"),
                    SN(a, "takeProfitKind"), DN(a, "takeProfitValue"), B(a, "useTrailStop"), B(a, "autoMarginCall"));
                return Tuple(ok, r);
            }
            case "GetAsyncOrder": return api.GetAsyncOrder(L(a, "id"));
            case "GetPendingAsync": { var (ok, m) = await api.GetPendingAsync(S(a, "type")); return Tuple(ok, m); }
            case "GetPositionsAsync": { var (ok, m) = await api.GetPositionsAsync(S(a, "type")); return Tuple(ok, m); }
            case "GetPortfolioPositionsAsync": return await api.GetPortfolioPositionsAsync();
            case "GetPositionAsync": { var (ok, m) = await api.GetPositionAsync(L(a, "id")); return Tuple(ok, m); }
            case "GetPositionHistoryAsync":
            {
                var (ok, m) = await api.GetPositionHistoryAsync(L(a, "balanceId"), L(a, "start"), L(a, "end"), I(a, "offset"), I(a, "limit"), Strings(a, "types"));
                return Tuple(ok, m);
            }
            case "GetHistoryPositionsAsync": return await api.GetHistoryPositionsAsync(L(a, "id"));
            case "GetAvailableLeveragesAsync": { var (ok, m) = await api.GetAvailableLeveragesAsync(S(a, "type"), S(a, "asset")); return Tuple(ok, m); }
            case "CancelOrderAsync": return await api.CancelOrderAsync(L(a, "id"));
            case "ClosePositionAsync": return await api.ClosePositionAsync(L(a, "id"));
            case "GetOvernightFeeAsync": { var (ok, m) = await api.GetOvernightFeeAsync(S(a, "type"), S(a, "asset")); return Tuple(ok, m); }
            case "CheckWinMarginAsync": return ToNode(await api.CheckWinMarginAsync(L(a, "id"), D(a, "polling")));
```

**Implementation:**

```csharp
    #region Margin (Forex / CFD / Crypto)

    private static void CheckIdName(string idName)
    {
        if (idName != "order_id" && idName != "position_id")
            throw new ArgumentException("idName must be \"order_id\" or \"position_id\"", nameof(idName));
    }

    private async Task<(bool Success, JsonObject? Message)> StatusReplyAsync(string reply, Obj msg, CancellationToken ct)
    {
        EnsureConnected();
        try
        {
            var r = await ByName(reply, msg, "", ct).ConfigureAwait(false);
            return Status2000(r) ? (true, r["msg"] as JsonObject) : (false, null);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null);
        }
    }

    /// <summary>Opens a forex/CFD/crypto order (Python <c>buy_order</c>).</summary>
    /// <param name="instrumentType">"forex", "cfd" or "crypto".</param>
    /// <param name="instrumentId">Instrument id, e.g. "EURUSD".</param>
    /// <param name="side">"buy" or "sell".</param>
    /// <param name="amount">Investment.</param>
    /// <param name="leverage">Leverage, e.g. 100.</param>
    /// <param name="type">"market", "limit" or "stop".</param>
    /// <param name="limitPrice">Limit price (type "limit").</param>
    /// <param name="stopPrice">Stop price (type "stop").</param>
    /// <param name="stopLoseKind">"percent", "price" or "diff".</param>
    /// <param name="stopLoseValue">Stop-loss value.</param>
    /// <param name="takeProfitKind">"percent", "price" or "diff".</param>
    /// <param name="takeProfitValue">Take-profit value.</param>
    /// <param name="useTrailStop">Trailing stop.</param>
    /// <param name="autoMarginCall">Use balance to keep the position open.</param>
    /// <param name="useTokenForCommission">Pay commission with tokens.</param>
    /// <param name="ct">Cancellation.</param>
    /// <returns><c>(true, orderId, null)</c>, or <c>(false, null, rejectStatus)</c>.</returns>
    public async Task<(bool Success, long? OrderId, string? Error)> BuyOrderAsync(string instrumentType, string instrumentId, string side, double amount,
        int leverage, string type, double? limitPrice = null, double? stopPrice = null, string? stopLoseKind = null, double? stopLoseValue = null,
        string? takeProfitKind = null, double? takeProfitValue = null, bool useTrailStop = false, bool autoMarginCall = false,
        bool useTokenForCommission = false, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        long orderId;
        try
        {
            var r = await ById("order-placed-temp", new Obj
            {
                ["name"] = "place-order-temp",
                ["version"] = "4.0",
                ["body"] = new Obj
                {
                    ["instrument_type"] = instrumentType,
                    ["instrument_id"] = instrumentId,
                    ["side"] = side,
                    ["amount"] = new PyFloat(amount),
                    ["leverage"] = leverage,
                    ["type"] = type,
                    ["limit_price"] = limitPrice,
                    ["stop_price"] = stopPrice,
                    ["stop_lose_kind"] = stopLoseKind,
                    ["stop_lose_value"] = stopLoseValue,
                    ["take_profit_kind"] = takeProfitKind,
                    ["take_profit_value"] = takeProfitValue,
                    ["use_trail_stop"] = useTrailStop,
                    ["auto_margin_call"] = autoMarginCall,
                    ["use_token_for_commission"] = useTokenForCommission,
                    ["user_balance_id"] = Bal,
                    ["client_platform_id"] = "9",
                },
            }, rid, ct).ConfigureAwait(false);
            orderId = J.Long(Path(r, "msg", "id"));
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null, Reason(ex));
        }
        var clock = Stopwatch.StartNew();
        var (ok, data) = await GetOrderAsync(orderId, ct).ConfigureAwait(false);
        while (data != null && J.Str(data["status"]) == "pending_new")
        {
            if (clock.Elapsed > T) return (false, null, "timeout");
            (ok, data) = await GetOrderAsync(orderId, ct).ConfigureAwait(false);
            await Task.Delay(Scaled(1), ct).ConfigureAwait(false);
        }
        if (!ok || data == null) return (false, null, null);
        return J.Str(data["status"]) != "rejected" ? (true, orderId, null) : (false, null, J.Str(data["reject_status"]));
    }

    /// <summary>Changes stop-loss / take-profit (Python <c>change_order</c>); <paramref name="idName"/> is "order_id" or "position_id" (then <paramref name="id"/> is still an order id).</summary>
    /// <param name="idName">"order_id" or "position_id".</param>
    /// <param name="id">Order id.</param>
    /// <param name="stopLoseKind">Stop-loss kind.</param>
    /// <param name="stopLoseValue">Stop-loss value.</param>
    /// <param name="takeProfitKind">Take-profit kind.</param>
    /// <param name="takeProfitValue">Take-profit value.</param>
    /// <param name="useTrailStop">Trailing stop.</param>
    /// <param name="autoMarginCall">Auto margin call.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonNode? Result)> ChangeOrderAsync(string idName, long id, string? stopLoseKind, double? stopLoseValue,
        string? takeProfitKind, double? takeProfitValue, bool useTrailStop, bool autoMarginCall, CancellationToken ct = default)
    {
        CheckIdName(idName);
        EnsureConnected();
        long target = id;
        if (idName == "position_id")
        {
            var (ok, order) = await GetOrderAsync(id, ct).ConfigureAwait(false);
            if (!ok || order == null) return (false, null);
            target = J.Long(order["position_id"]);
        }
        var gate = _conn.Gate("tpsl-changed");
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            using var w = _conn.AddWaiter("tpsl-changed", null);
            await Send("sendMessage", new Obj
            {
                ["name"] = "change-tpsl",
                ["version"] = "2.0",
                ["body"] = new Obj
                {
                    [idName] = target,
                    ["stop_lose_kind"] = stopLoseKind,
                    ["stop_lose_value"] = stopLoseValue,
                    ["take_profit_kind"] = takeProfitKind,
                    ["take_profit_value"] = takeProfitValue,
                    ["use_trail_stop"] = useTrailStop,
                    ["extra"] = new Obj { ["stop_lose_kind"] = stopLoseKind, ["take_profit_kind"] = takeProfitKind },
                },
            }, "", ct).ConfigureAwait(false);
            await ChangeAutoMarginCallAsync(idName, target, autoMarginCall, ct).ConfigureAwait(false);
            var r = await w.Task.WaitAsync(T, ct).ConfigureAwait(false);
            return Status2000(r) ? (true, r["msg"]) : (false, r);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Turns "use balance to keep position open" on/off (Python <c>change_auto_margin_call</c>).</summary>
    /// <param name="idName">"order_id" or "position_id".</param>
    /// <param name="id">The id.</param>
    /// <param name="autoMarginCall">On or off.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonObject? Message)> ChangeAutoMarginCallAsync(string idName, long id, bool autoMarginCall, CancellationToken ct = default)
    {
        CheckIdName(idName);
        EnsureConnected();
        try
        {
            var r = await ByName("auto-margin-call-changed", new Obj
            {
                ["name"] = "change-auto-margin-call", ["version"] = "2.0", ["body"] = new Obj { [idName] = id, ["auto_margin_call"] = autoMarginCall },
            }, "", ct).ConfigureAwait(false);
            return (Status2000(r), r);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null);
        }
    }

    /// <summary>Order details (Python <c>get_order</c>): status pending_new / new / filled / rejected / canceled, position_id, …</summary>
    /// <param name="orderId">Order id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonObject? Order)> GetOrderAsync(long orderId, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        try
        {
            var r = await ById("order", new Obj { ["name"] = "get-order", ["body"] = new Obj { ["order_id"] = orderId } }, rid, ct).ConfigureAwait(false);
            return Status2000(r) ? (true, r["msg"] as JsonObject) : (false, null);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null);
        }
    }

    /// <summary>Pushed messages for an order/option/position id: "position-changed", "order-changed", "option-opened", "option-closed" (Python <c>get_async_order</c>).</summary>
    /// <param name="id">Order, option or position id.</param>
    public JsonObject GetAsyncOrder(long id)
    {
        lock (_s.Lock)
        {
            return _s.OrderAsync.TryGetValue(id, out var d)
                ? new JsonObject(d.Select(x => KeyValuePair.Create(x.Key, (JsonNode?)x.Value.DeepClone())))
                : new JsonObject();
        }
    }

    /// <summary>Pending (limit/stop) orders (Python <c>get_pending</c>).</summary>
    /// <param name="instrumentType">Instrument type.</param>
    /// <param name="ct">Cancellation.</param>
    public Task<(bool Success, JsonObject? Message)> GetPendingAsync(string instrumentType, CancellationToken ct = default) =>
        StatusReplyAsync("deferred-orders", new Obj
        {
            ["name"] = "get-deferred-orders", ["version"] = "1.0", ["body"] = new Obj { ["user_balance_id"] = Bal, ["instrument_type"] = instrumentType },
        }, ct);

    /// <summary>Open positions of one type (Python <c>get_positions</c>).</summary>
    /// <param name="instrumentType">e.g. "forex", "digital-option", "fx-option".</param>
    /// <param name="ct">Cancellation.</param>
    public Task<(bool Success, JsonObject? Message)> GetPositionsAsync(string instrumentType, CancellationToken ct = default)
    {
        string name = instrumentType switch
        {
            "digital-option" => "digital-options.get-positions",
            "fx-option" => "trading-fx-option.get-positions",
            _ => "get-positions",
        };
        return StatusReplyAsync("positions", new Obj
        {
            ["name"] = name, ["body"] = new Obj { ["instrument_type"] = instrumentType, ["user_balance_id"] = Bal },
        }, ct);
    }

    /// <summary>Open positions of every type (Python <c>portfolio_get_positions</c>); the whole message.</summary>
    /// <param name="limit">Page size.</param>
    /// <param name="offset">Page offset.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetPortfolioPositionsAsync(int limit = 1, int offset = 0, CancellationToken ct = default)
    {
        EnsureConnected();
        return await ByName("positions", new Obj
        {
            ["name"] = "portfolio.get-positions",
            ["version"] = "3.0",
            ["body"] = new Obj
            {
                // Python sends str(list) here; kept byte-for-byte.
                ["instrument_type"] = "['cfd', 'forex', 'crypto', 'digital-option', 'turbo-option', 'binary-option']",
                ["limit"] = limit,
                ["offset"] = offset,
            },
        }, "", ct).ConfigureAwait(false);
    }

    /// <summary>Position of an order (Python <c>get_position</c>).</summary>
    /// <param name="orderId">Order id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonObject? Message)> GetPositionAsync(long orderId, CancellationToken ct = default)
    {
        var (_, order) = await GetOrderAsync(orderId, ct).ConfigureAwait(false);
        if (order == null) return (false, null);
        return await StatusReplyAsync("position", new Obj
        {
            ["name"] = "get-position", ["body"] = new Obj { ["position_id"] = order["position_id"]?.DeepClone() },
        }, ct).ConfigureAwait(false);
    }

    /// <summary>Closed positions, paged (Python <c>get_position_history_v2</c>).</summary>
    /// <param name="userBalanceId">Balance id.</param>
    /// <param name="start">Start (Unix seconds).</param>
    /// <param name="end">End (Unix seconds).</param>
    /// <param name="offset">Page offset.</param>
    /// <param name="limit">Page size.</param>
    /// <param name="instrumentTypes">e.g. "forex", "crypto", "digital-option".</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<(bool Success, JsonObject? Message)> GetPositionHistoryAsync(long userBalanceId, long start, long end, int offset, int limit,
        IEnumerable<string> instrumentTypes, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        try
        {
            var r = await ById("history", new Obj
            {
                ["name"] = "get-history",
                ["body"] = new Obj
                {
                    ["instrument_types"] = instrumentTypes.ToList(),
                    ["user_balance_id"] = userBalanceId,
                    ["start"] = start,
                    ["end"] = end,
                    ["offset"] = offset,
                    ["limit"] = limit,
                },
                ["version"] = "2.0",
            }, rid, ct).ConfigureAwait(false);
            return Status2000(r) ? (true, r["msg"] as JsonObject) : (false, null);
        }
        catch (Exception ex) when (IsFailure(ex))
        {
            return (false, null);
        }
    }

    /// <summary>History rows for one external id (Python <c>get_history_positions</c>); the whole message.</summary>
    /// <param name="externalId">Position / option id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetHistoryPositionsAsync(long externalId, CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        return await ById("history-positions", new Obj
        {
            ["name"] = "get-history-positions", ["body"] = new Obj { ["external_id"] = externalId },
        }, rid, ct).ConfigureAwait(false);
    }

    /// <summary>Available leverages (Python <c>get_available_leverages</c>); empty asset = all.</summary>
    /// <param name="instrumentType">Instrument type.</param>
    /// <param name="asset">Asset name or "".</param>
    /// <param name="ct">Cancellation.</param>
    public Task<(bool Success, JsonObject? Message)> GetAvailableLeveragesAsync(string instrumentType, string asset = "", CancellationToken ct = default) =>
        StatusReplyAsync("available-leverages", new Obj
        {
            ["name"] = "get-available-leverages",
            ["version"] = "2.0",
            ["body"] = new Obj { ["instrument_type"] = instrumentType, ["actives"] = new object[] { asset == "" ? "" : Act(asset) } },
        }, ct);

    /// <summary>Cancels a pending order (Python <c>cancel_order</c>).</summary>
    /// <param name="orderId">Order id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<bool> CancelOrderAsync(long orderId, CancellationToken ct = default) =>
        (await StatusReplyAsync("order-canceled", new Obj
        {
            ["name"] = "cancel-order", ["version"] = "1.0", ["body"] = new Obj { ["order_id"] = orderId },
        }, ct).ConfigureAwait(false)).Success;

    /// <summary>Closes the position opened by an order (Python <c>close_position</c>; takes the ORDER id).</summary>
    /// <param name="orderId">Order id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<bool> ClosePositionAsync(long orderId, CancellationToken ct = default)
    {
        var (_, order) = await GetOrderAsync(orderId, ct).ConfigureAwait(false);
        if (order == null || J.IsNull(order["position_id"])) return false;
        return (await StatusReplyAsync("position-closed", new Obj
        {
            ["name"] = "close-position", ["version"] = "1.0", ["body"] = new Obj { ["position_id"] = order["position_id"]!.DeepClone() },
        }, ct).ConfigureAwait(false)).Success;
    }

    /// <summary>Overnight fee (Python <c>get_overnight_fee</c>).</summary>
    /// <param name="instrumentType">Instrument type.</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="ct">Cancellation.</param>
    public Task<(bool Success, JsonObject? Message)> GetOvernightFeeAsync(string instrumentType, string asset, CancellationToken ct = default) =>
        StatusReplyAsync("overnight-fee", new Obj
        {
            ["name"] = "get-overnight-fee", ["version"] = "1.0", ["body"] = new Obj { ["user_group_id"] = 1, ["instrument_type"] = instrumentType, ["active_id"] = Act(asset) },
        }, ct);

    /// <summary>Waits for the realised profit of an order or option from the trade history (Python <c>check_win</c>); null if the order was cancelled. No timeout.</summary>
    /// <param name="id">Order id (margin) or option id.</param>
    /// <param name="pollingSeconds">How often to ask.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<double?> CheckWinMarginAsync(long id, double pollingSeconds = 0.5, CancellationToken ct = default)
    {
        EnsureConnected();
        var (check, data) = await GetOrderAsync(id, ct).ConfigureAwait(false);
        long? positionId = null;
        while (true)
        {
            if (positionId == null)
            {
                if (check && data != null)
                {
                    var pid = data["position_id"];
                    while (J.IsNull(pid))
                    {
                        var (_, again) = await GetOrderAsync(id, ct).ConfigureAwait(false);
                        if (again != null)
                        {
                            pid = again["position_id"];
                            if (J.Str(again["status"]) == "canceled") return null;
                        }
                        await Task.Delay(Scaled(pollingSeconds), ct).ConfigureAwait(false);
                    }
                    positionId = J.Long(pid);
                }
                else
                {
                    positionId = id;
                }
            }
            var pnl = await RawCheckWinAsync(positionId.Value, ct).ConfigureAwait(false);
            if (pnl != null) return pnl;
            await Task.Delay(Scaled(pollingSeconds), ct).ConfigureAwait(false);
        }
    }

    private async Task<double?> RawCheckWinAsync(long externalId, CancellationToken ct)
    {
        var data = await GetHistoryPositionsAsync(externalId, ct).ConfigureAwait(false);
        JsonNode? row = null;
        foreach (var r in J.Arr(Path(data, "msg", "positions")))
        {
            if (J.Long(r!["external_id"]) == externalId) row = r;
        }
        return row == null ? null : Helpers.PyRound2(J.Dbl(row["pnl_realized"]));
    }

    #endregion
```

Commit: `git add IqOptionApi.CSharp && git commit -m "Add forex, CFD and crypto order methods"`

---

### Task 14: Extras

**Interfaces:** Produces `Task<JsonObject> GetLeaderBoardAsync(string country, int fromPosition, int toPosition, int nearTradersCount, int userCountryId = 0, int nearTradersCountryCount = 0, int topCountryCount = 0, int topCount = 0, int topType = 2, ct)`, `Task<JsonObject> GetActiveExposureAsync(string, string, int durationMinutes, string currency = "USD", ct)`, `Task<bool> StartMoodStreamAsync(string, ct)`, `Task StopMoodStreamAsync(string, ct)`, `double? GetTradersMood(string)`, `IReadOnlyDictionary<int,double> GetAllTradersMood()`, `Task Subscribe/UnsubscribeTopAssetsUpdatedAsync(string, ct)`, `JsonNode? GetTopAssetsUpdated(string)`, `Task Subscribe/UnsubscribeCommissionChangedAsync(string, ct)`, `IReadOnlyDictionary<string, IReadOnlyDictionary<double,int>> GetCommissionChange(string)`, `Task SubscribeLiveDealAsync(string name, string asset, string type, int bufferSize, ct)`, `Task UnsubscribeLiveDealAsync(string, string, string, ct)`, `IReadOnlyList<JsonObject> GetLiveDeal(...)`, `JsonObject? PopLiveDeal(...)`, `void ClearLiveDeal(..., int bufferSize)`, `Task<JsonNode?> GetUserProfileClientAsync(long, ct)`, `Task<JsonObject> RequestLeaderboardUserinfoDealsClientAsync(long, int, ct)`, `Task<JsonNode?> GetUsersAvailabilityAsync(long, ct)`.

**Python cases:**

```python
    EXPOSURE = {"on": "sendMessage:get-active-exposure",
                "reply": [{"name": "active-exposure", "request_id": "$RID", "msg": {"call": 0.6, "put": 0.4}}]}
    MOOD = {"name": "traders-mood-changed", "msg": {"asset_id": 1861, "value": 0.55}}
    DEAL5 = {"name": "live-deal-binary-option-placed", "msg": {"active_id": 1861, "option_type": "turbo", "amount": 5}}
    DEAL6 = {"name": "live-deal-binary-option-placed", "msg": {"active_id": 1861, "option_type": "turbo", "amount": 6}}

    def plain(name, call, method, args):
        return {"name": name, "call": call, "csharp": {"method": method, "args": args}}

    cases += [
        {"name": "get_leader_board", "call": lambda iq: iq.get_leader_board("Worldwide", 1, 10, 5),
         "rules": [{"on": "sendMessage:request-leaderboard-deals-client",
                    "reply": [{"name": "leaderboard-deals-client", "request_id": "", "msg": {"result": {"positional": {}}}}]}],
         "csharp": {"method": "GetLeaderBoardAsync", "args": {"country": "Worldwide", "from": 1, "to": 10, "near": 5}}},
        {"name": "get_active_exposure_turbo", "delay": True, "rules": [EXPOSURE],
         "call": lambda iq: iq.get_active_exposure("turbo-option", "EURUSD", 1),
         "csharp": {"method": "GetActiveExposureAsync", "args": {"type": "turbo-option", "asset": "EURUSD", "minutes": 1}}},
        {"name": "get_active_exposure_digital_5", "delay": True, "rules": [EXPOSURE],
         "call": lambda iq: iq.get_active_exposure("digital-option", "EURUSD", 5),
         "csharp": {"method": "GetActiveExposureAsync", "args": {"type": "digital-option", "asset": "EURUSD", "minutes": 5}}},
        {"name": "start_mood_stream", "call": lambda iq: (iq.start_mood_stream("EURUSD"), iq.get_traders_mood("EURUSD"))[1],
         "rules": [{"on": "subscribeMessage:traders-mood-changed", "reply": [MOOD]}],
         "csharp": {"method": "StartMoodStreamAsync", "args": {"asset": "EURUSD"}}},
        plain("stop_mood_stream", lambda iq: iq.stop_mood_stream("EURUSD"), "StopMoodStreamAsync", {"asset": "EURUSD"}),
        {"name": "get_all_traders_mood", "pre": [MOOD, {"name": "traders-mood-changed", "msg": {"asset_id": 76, "value": 0.3}}],
         "call": lambda iq: iq.get_all_traders_mood(), "csharp": {"method": "GetAllTradersMood", "args": {}}},
        plain("subscribe_top_assets_updated", lambda iq: iq.subscribe_top_assets_updated("forex"), "SubscribeTopAssetsUpdatedAsync", {"type": "forex"}),
        plain("unsubscribe_top_assets_updated", lambda iq: iq.unsubscribe_top_assets_updated("forex"), "UnsubscribeTopAssetsUpdatedAsync", {"type": "forex"}),
        {"name": "get_top_assets_updated", "pre": [{"name": "top-assets-updated", "msg": {"instrument_type": "forex", "data": [{"active_id": 1861}]}}],
         "call": lambda iq: iq.get_top_assets_updated("forex"), "csharp": {"method": "GetTopAssetsUpdated", "args": {"type": "forex"}}},
        plain("subscribe_commission_changed", lambda iq: iq.subscribe_commission_changed("digital-option"), "SubscribeCommissionChangedAsync", {"type": "digital-option"}),
        plain("unsubscribe_commission_changed", lambda iq: iq.unsubscribe_commission_changed("digital-option"), "UnsubscribeCommissionChangedAsync", {"type": "digital-option"}),
        {"name": "get_commission_change",
         "pre": [{"name": "commission-changed", "msg": {"instrument_type": "turbo-option", "active_id": 1861, "commission": {"value": 18}}}],
         "call": lambda iq: iq.get_commission_change("turbo-option"), "csharp": {"method": "GetCommissionChange", "args": {"type": "turbo-option"}}},
        plain("subscribe_live_deal", lambda iq: iq.subscribe_live_deal("live-deal-binary-option-placed", "EURUSD", "turbo", 10),
              "SubscribeLiveDealAsync", {"name": "live-deal-binary-option-placed", "asset": "EURUSD", "type": "turbo", "size": 10}),
        plain("subscribe_live_deal_digital", lambda iq: iq.subscribe_live_deal("live-deal-digital-option", "EURUSD", "PT1M", 10),
              "SubscribeLiveDealAsync", {"name": "live-deal-digital-option", "asset": "EURUSD", "type": "PT1M", "size": 10}),
        plain("unsubscribe_live_deal", lambda iq: iq.unscribe_live_deal("live-deal", "EURUSD", "forex"),
              "UnsubscribeLiveDealAsync", {"name": "live-deal", "asset": "EURUSD", "type": "forex"}),
        {"name": "get_live_deal", "pre": [DEAL5, DEAL6],
         "call": lambda iq: list(iq.get_live_deal("live-deal-binary-option-placed", "EURUSD", "turbo")),
         "csharp": {"method": "GetLiveDeal", "args": {"name": "live-deal-binary-option-placed", "asset": "EURUSD", "type": "turbo"}}},
        {"name": "pop_live_deal", "pre": [DEAL5, DEAL6],
         "call": lambda iq: iq.pop_live_deal("live-deal-binary-option-placed", "EURUSD", "turbo"),
         "csharp": {"method": "PopLiveDeal", "args": {"name": "live-deal-binary-option-placed", "asset": "EURUSD", "type": "turbo"}}},
        {"name": "get_user_profile_client", "call": lambda iq: iq.get_user_profile_client(777),
         "rules": [{"on": "sendMessage:get-user-profile-client",
                    "reply": [{"name": "user-profile-client", "request_id": "", "msg": {"user_id": 777, "user_name": "x"}}]}],
         "csharp": {"method": "GetUserProfileClientAsync", "args": {"id": 777}}},
        {"name": "request_leaderboard_userinfo_deals_client", "call": lambda iq: iq.request_leaderboard_userinfo_deals_client(777, 0),
         "rules": [{"on": "sendMessage:request-leaderboard-userinfo-deals-client",
                    "reply": [{"name": "leaderboard-userinfo-deals-client", "request_id": "", "msg": {"isSuccessful": True, "result": {}}}]}],
         "csharp": {"method": "RequestLeaderboardUserinfoDealsClientAsync", "args": {"id": 777, "country": 0}}},
        {"name": "get_users_availability", "call": lambda iq: iq.get_users_availability(777),
         "rules": [{"on": "sendMessage:get-users-availability",
                    "reply": [{"name": "users-availability", "request_id": "", "msg": {"statuses": []}}]}],
         "csharp": {"method": "GetUsersAvailabilityAsync", "args": {"id": 777}}},
    ]
```

**Adapters:**

```csharp
            case "GetLeaderBoardAsync": return await api.GetLeaderBoardAsync(S(a, "country"), I(a, "from"), I(a, "to"), I(a, "near"));
            case "GetActiveExposureAsync": return await api.GetActiveExposureAsync(S(a, "type"), S(a, "asset"), I(a, "minutes"));
            case "StartMoodStreamAsync": await api.StartMoodStreamAsync(S(a, "asset")); return ToNode(api.GetTradersMood(S(a, "asset")));
            case "StopMoodStreamAsync": await api.StopMoodStreamAsync(S(a, "asset")); return null;
            case "GetAllTradersMood":
                return new JsonObject(api.GetAllTradersMood().Select(x => KeyValuePair.Create(x.Key.ToString(CultureInfo.InvariantCulture), (JsonNode?)x.Value)));
            case "SubscribeTopAssetsUpdatedAsync": await api.SubscribeTopAssetsUpdatedAsync(S(a, "type")); return null;
            case "UnsubscribeTopAssetsUpdatedAsync": await api.UnsubscribeTopAssetsUpdatedAsync(S(a, "type")); return null;
            case "GetTopAssetsUpdated": return api.GetTopAssetsUpdated(S(a, "type"))?.DeepClone();
            case "SubscribeCommissionChangedAsync": await api.SubscribeCommissionChangedAsync(S(a, "type")); return null;
            case "UnsubscribeCommissionChangedAsync": await api.UnsubscribeCommissionChangedAsync(S(a, "type")); return null;
            case "GetCommissionChange":
                return new JsonObject(api.GetCommissionChange(S(a, "type")).Select(x => KeyValuePair.Create(x.Key,
                    (JsonNode?)new JsonObject(x.Value.Select(y => KeyValuePair.Create(PyJson.FloatRepr(y.Key), (JsonNode?)y.Value))))));
            case "SubscribeLiveDealAsync": await api.SubscribeLiveDealAsync(S(a, "name"), S(a, "asset"), S(a, "type"), I(a, "size")); return null;
            case "UnsubscribeLiveDealAsync": await api.UnsubscribeLiveDealAsync(S(a, "name"), S(a, "asset"), S(a, "type")); return null;
            case "GetLiveDeal":
                return new JsonArray(api.GetLiveDeal(S(a, "name"), S(a, "asset"), S(a, "type")).Select(x => (JsonNode?)x.DeepClone()).ToArray());
            case "PopLiveDeal": return api.PopLiveDeal(S(a, "name"), S(a, "asset"), S(a, "type"))?.DeepClone();
            case "GetUserProfileClientAsync": return await api.GetUserProfileClientAsync(L(a, "id"));
            case "RequestLeaderboardUserinfoDealsClientAsync": return await api.RequestLeaderboardUserinfoDealsClientAsync(L(a, "id"), I(a, "country"));
            case "GetUsersAvailabilityAsync": return await api.GetUsersAvailabilityAsync(L(a, "id"));
```

**Implementation:**

```csharp
    #region Extras

    /// <summary>Leaderboard (Python <c>get_leader_board</c>); <paramref name="country"/> is "Worldwide" or a two-letter code.</summary>
    /// <param name="country">"Worldwide" or e.g. "PK".</param>
    /// <param name="fromPosition">First rank.</param>
    /// <param name="toPosition">Last rank.</param>
    /// <param name="nearTradersCount">Traders near you.</param>
    /// <param name="userCountryId">Your country id.</param>
    /// <param name="nearTradersCountryCount">Near traders in your country.</param>
    /// <param name="topCountryCount">Top in country.</param>
    /// <param name="topCount">Top overall.</param>
    /// <param name="topType">Ranking type (2 by default).</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetLeaderBoardAsync(string country, int fromPosition, int toPosition, int nearTradersCount, int userCountryId = 0,
        int nearTradersCountryCount = 0, int topCountryCount = 0, int topCount = 0, int topType = 2, CancellationToken ct = default)
    {
        EnsureConnected();
        if (!Countries.Ids.TryGetValue(country, out int countryId))
            throw new KeyNotFoundException($"Unknown country '{country}'. Use \"Worldwide\" or a two-letter code such as \"PK\".");
        var r = await ByName("leaderboard-deals-client", new Obj
        {
            ["name"] = "request-leaderboard-deals-client",
            ["version"] = "1.0",
            ["body"] = new Obj
            {
                ["country_id"] = countryId,
                ["user_country_id"] = userCountryId,
                ["from_position"] = fromPosition,
                ["to_position"] = toPosition,
                ["near_traders_country_count"] = nearTradersCountryCount,
                ["near_traders_count"] = nearTradersCount,
                ["top_country_count"] = topCountryCount,
                ["top_count"] = topCount,
                ["top_type"] = topType,
            },
        }, "", ct).ConfigureAwait(false);
        return J.Obj(r["msg"]);
    }

    /// <summary>Share of call vs put money for an asset and expiry (Python <c>get_active_exposure</c>): <c>{call, put}</c>.</summary>
    /// <param name="instrumentType">"turbo-option", "binary-option" or "digital-option".</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="durationMinutes">Expiry in minutes.</param>
    /// <param name="currency">Currency.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> GetActiveExposureAsync(string instrumentType, string asset, int durationMinutes, string currency = "USD", CancellationToken ct = default)
    {
        EnsureConnected();
        string rid = NextRid();
        int activeId = Act(asset);
        double now = Now;
        object time;
        if (instrumentType == "digital-option")
        {
            var (value, isFloat) = Helpers.DigitalExp((long)now, durationMinutes, now, _tz);
            time = isFloat ? new PyFloat(value) : (long)value;
        }
        else
        {
            time = Helpers.GetExpirationTime((long)now, durationMinutes, now, _tz).Exp;
        }
        var r = await ById("active-exposure", new Obj
        {
            ["name"] = "get-active-exposure",
            ["version"] = "1.0",
            ["body"] = new Obj { ["instrument_type"] = instrumentType, ["active_id"] = activeId, ["time"] = time, ["currency"] = currency },
        }, rid, ct).ConfigureAwait(false);
        var ans = new JsonObject();
        if (r["msg"] is JsonObject m && m.ContainsKey("call") && m.ContainsKey("put"))
        {
            ans["call"] = m["call"]?.DeepClone();
            ans["put"] = m["put"]?.DeepClone();
        }
        return ans;
    }

    /// <summary>Streams traders' mood for an asset (Python <c>start_mood_stream</c>); true once the first value arrived.</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<bool> StartMoodStreamAsync(string asset, CancellationToken ct = default)
    {
        EnsureConnected();
        int id = Act(asset);
        lock (_track)
        {
            if (!_moodStreams.Contains(asset)) _moodStreams.Add(asset);
        }
        var clock = Stopwatch.StartNew();
        while (true)
        {
            await Send("subscribeMessage", MoodMsg(asset), "", ct).ConfigureAwait(false);
            if (await WaitCondAsync(() => { lock (_s.Lock) return _s.TradersMood.ContainsKey(id); }, Scaled(5), ct).ConfigureAwait(false)) return true;
            if (clock.Elapsed > T) return false;
        }
    }

    /// <summary>Stops the mood stream (Python <c>stop_mood_stream</c>).</summary>
    /// <param name="asset">Asset name.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task StopMoodStreamAsync(string asset, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track) _moodStreams.Remove(asset);
        await Send("unsubscribeMessage", MoodMsg(asset), "", ct).ConfigureAwait(false);
    }

    /// <summary>Share of traders buying "call" (higher), 0–1 (Python <c>get_traders_mood</c>); null if not streamed yet.</summary>
    /// <param name="asset">Asset name.</param>
    public double? GetTradersMood(string asset)
    {
        int id = Act(asset);
        lock (_s.Lock) return _s.TradersMood.TryGetValue(id, out double v) ? v : null;
    }

    /// <summary>Every streamed mood by asset id (Python <c>get_all_traders_mood</c>).</summary>
    public IReadOnlyDictionary<int, double> GetAllTradersMood()
    {
        lock (_s.Lock) return new Dictionary<int, double>(_s.TradersMood);
    }

    /// <summary>Streams top-asset updates (Python <c>subscribe_top_assets_updated</c>).</summary>
    /// <param name="instrumentType">Instrument type.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task SubscribeTopAssetsUpdatedAsync(string instrumentType, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track)
        {
            if (!_topAssetsSubscriptions.Contains(instrumentType)) _topAssetsSubscriptions.Add(instrumentType);
        }
        await Send("subscribeMessage", TopAssetsMsg(instrumentType), "", ct).ConfigureAwait(false);
    }

    /// <summary>Stops <see cref="SubscribeTopAssetsUpdatedAsync"/>.</summary>
    /// <param name="instrumentType">Instrument type.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task UnsubscribeTopAssetsUpdatedAsync(string instrumentType, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track) _topAssetsSubscriptions.Remove(instrumentType);
        await Send("unsubscribeMessage", TopAssetsMsg(instrumentType), "", ct).ConfigureAwait(false);
    }

    /// <summary>Last top-assets update (Python <c>get_top_assets_updated</c>); null if none.</summary>
    /// <param name="instrumentType">Instrument type.</param>
    public JsonNode? GetTopAssetsUpdated(string instrumentType)
    {
        lock (_s.Lock) return _s.TopAssetsUpdated.TryGetValue(instrumentType, out var v) ? v : null;
    }

    /// <summary>Streams commission changes (Python <c>subscribe_commission_changed</c>).</summary>
    /// <param name="instrumentType">Instrument type.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task SubscribeCommissionChangedAsync(string instrumentType, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track)
        {
            if (!_commissionSubscriptions.Contains(instrumentType)) _commissionSubscriptions.Add(instrumentType);
        }
        await Send("subscribeMessage", CommissionMsg(instrumentType), "", ct).ConfigureAwait(false);
    }

    /// <summary>Stops <see cref="SubscribeCommissionChangedAsync"/>.</summary>
    /// <param name="instrumentType">Instrument type.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task UnsubscribeCommissionChangedAsync(string instrumentType, CancellationToken ct = default)
    {
        EnsureConnected();
        lock (_track) _commissionSubscriptions.Remove(instrumentType);
        await Send("unsubscribeMessage", CommissionMsg(instrumentType), "", ct).ConfigureAwait(false);
    }

    /// <summary>Commission changes: asset → (server time → commission) (Python <c>get_commission_change</c>).</summary>
    /// <param name="instrumentType">Instrument type.</param>
    public IReadOnlyDictionary<string, IReadOnlyDictionary<double, int>> GetCommissionChange(string instrumentType)
    {
        lock (_s.Lock)
        {
            return _s.Commission.TryGetValue(instrumentType, out var byAsset)
                ? byAsset.ToDictionary(x => x.Key, x => (IReadOnlyDictionary<double, int>)new Dictionary<double, int>(x.Value))
                : new Dictionary<string, IReadOnlyDictionary<double, int>>();
        }
    }

    /// <summary>Streams other users' trades (Python <c>subscribe_live_deal</c>); keeps the newest <paramref name="bufferSize"/>.</summary>
    /// <param name="name">"live-deal-binary-option-placed", "live-deal-digital-option" or "live-deal".</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="type">"turbo"/"binary", "PT1M"/"PT5M"/"PT15M", or an instrument type.</param>
    /// <param name="bufferSize">How many deals to keep.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task SubscribeLiveDealAsync(string name, string asset, string type, int bufferSize, CancellationToken ct = default)
    {
        EnsureConnected();
        var msg = LiveDealMsg(name, asset, type);
        lock (_s.Lock)
        {
            var key = (name, asset, type);
            if (!_s.LiveDeals.TryGetValue(key, out var buffer)) _s.LiveDeals[key] = buffer = new LiveDealBuffer();
            buffer.MaxLength = bufferSize;
            buffer.Trim();
        }
        lock (_track)
        {
            if (!_liveDealSubscriptions.Contains((name, asset, type))) _liveDealSubscriptions.Add((name, asset, type));
        }
        await Send("subscribeMessage", msg, "", ct).ConfigureAwait(false);
    }

    /// <summary>Stops <see cref="SubscribeLiveDealAsync"/> (Python <c>unscribe_live_deal</c>).</summary>
    /// <param name="name">Stream name.</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="type">Type filter.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task UnsubscribeLiveDealAsync(string name, string asset, string type, CancellationToken ct = default)
    {
        EnsureConnected();
        var msg = LiveDealMsg(name, asset, type);
        lock (_track) _liveDealSubscriptions.Remove((name, asset, type));
        await Send("unsubscribeMessage", msg, "", ct).ConfigureAwait(false);
    }

    /// <summary>Stored live deals, newest first (Python <c>get_live_deal</c>).</summary>
    /// <param name="name">Stream name.</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="type">Type filter.</param>
    public IReadOnlyList<JsonObject> GetLiveDeal(string name, string asset, string type)
    {
        lock (_s.Lock) return _s.LiveDeals.TryGetValue((name, asset, type), out var b) ? b.NewestFirst() : new List<JsonObject>();
    }

    /// <summary>Removes and returns the oldest stored deal (Python <c>pop_live_deal</c>); null if empty.</summary>
    /// <param name="name">Stream name.</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="type">Type filter.</param>
    public JsonObject? PopLiveDeal(string name, string asset, string type)
    {
        lock (_s.Lock) return _s.LiveDeals.TryGetValue((name, asset, type), out var b) ? b.PopOldest() : null;
    }

    /// <summary>Empties the stored deals and sets a new size (Python <c>clear_live_deal</c>).</summary>
    /// <param name="name">Stream name.</param>
    /// <param name="asset">Asset name.</param>
    /// <param name="type">Type filter.</param>
    /// <param name="bufferSize">How many deals to keep.</param>
    public void ClearLiveDeal(string name, string asset, string type, int bufferSize)
    {
        lock (_s.Lock) _s.LiveDeals[(name, asset, type)] = new LiveDealBuffer { MaxLength = bufferSize };
    }

    /// <summary>Public profile of another user (Python <c>get_user_profile_client</c>).</summary>
    /// <param name="userId">User id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonNode?> GetUserProfileClientAsync(long userId, CancellationToken ct = default)
    {
        EnsureConnected();
        var r = await ByName("user-profile-client", new Obj
        {
            ["name"] = "get-user-profile-client", ["body"] = new Obj { ["user_id"] = userId }, ["version"] = "1.0",
        }, "", ct).ConfigureAwait(false);
        return r["msg"];
    }

    /// <summary>Leaderboard info of a user (Python <c>request_leaderboard_userinfo_deals_client</c>; asks again every 0.2 s until successful).</summary>
    /// <param name="userId">User id.</param>
    /// <param name="countryId">Country id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonObject> RequestLeaderboardUserinfoDealsClientAsync(long userId, int countryId, CancellationToken ct = default)
    {
        EnsureConnected();
        const string reply = "leaderboard-userinfo-deals-client";
        var msg = new Obj
        {
            ["name"] = "request-leaderboard-userinfo-deals-client",
            ["body"] = new Obj { ["country_ids"] = new[] { countryId }, ["requested_user_id"] = userId },
            ["version"] = "1.0",
        };
        var gate = _conn.Gate(reply);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var clock = Stopwatch.StartNew();
            while (true)
            {
                using var w = _conn.AddWaiter(reply, null);
                await Send("sendMessage", msg, "", ct).ConfigureAwait(false);
                try
                {
                    var r = await w.Task.WaitAsync(Scaled(0.2), ct).ConfigureAwait(false);
                    if (J.IsTrue(Path(r, "msg", "isSuccessful"))) return J.Obj(r["msg"]);
                }
                catch (TimeoutException)
                {
                    // not answered yet: ask again, like Python
                }
                if (clock.Elapsed > T) throw new TimeoutException("no successful " + reply + " reply");
            }
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>Online status of a user (Python <c>get_users_availability</c>).</summary>
    /// <param name="userId">User id.</param>
    /// <param name="ct">Cancellation.</param>
    public async Task<JsonNode?> GetUsersAvailabilityAsync(long userId, CancellationToken ct = default)
    {
        EnsureConnected();
        var r = await ResendUntilAsync("users-availability", new Obj
        {
            ["name"] = "get-users-availability", ["body"] = new Obj { ["user_ids"] = new[] { userId } }, ["version"] = "1.0",
        }, Scaled(0.2), T, ct).ConfigureAwait(false);
        return r["msg"];
    }

    #endregion
```

Commit: `git add IqOptionApi.CSharp && git commit -m "Add leaderboard, mood, live deals and other extras"`
---

### Task 15: Auto-reconnect, result recovery and real-socket tests

**Files:**
- Modify: `src/IqOptionApi/IqOption.cs` (replace `OnSocketClosed`; add `ReconnectLoopAsync`, `RecoverResultsAsync`)
- Create: `tests/IqOptionApi.Tests/Support/FakeIqServer.cs`
- Test: `tests/IqOptionApi.Tests/ReconnectTests.cs`

**Interfaces:**
- Consumes: `ConnectCoreAsync(bool resetBalance, CancellationToken)`, `ClosedProfit`, `DigitalCloseProfit`, `RawCheckWinAsync`, `GetDigitalPositionByPositionIdAsync`, `Harness.ConnectRules`, `Rule`.
- Produces: automatic reconnect behaviour (spec §4.8); `FakeIqServer` (`StartAsync(JsonObject fixtures)`, `BaseUrl`, `AddRule(string on, params string[] replies)`, `ReplaceRule(...)`, `Frames`, `FramesNamed(string key)`, `Logins`, `Connections`, `RejectLogin`, `Drop()`, `PushAsync(string)`).

- [ ] **Step 1: Write `tests/IqOptionApi.Tests/Support/FakeIqServer.cs`**

```csharp
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Hosting.Server;
using Microsoft.AspNetCore.Hosting.Server.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace IqOptionApi.Tests;

/// <summary>Small in-process IQ Option look-alike: HTTP login/logout and a WebSocket that answers by <see cref="Rule"/>s.</summary>
internal sealed class FakeIqServer : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly List<Rule> _rules = new();
    private WebSocket? _current;
    private int _logins;
    private int _connections;

    private FakeIqServer(WebApplication app) => _app = app;

    public string BaseUrl { get; private set; } = "";

    public ConcurrentQueue<string> Frames { get; } = new();

    public int Logins => Volatile.Read(ref _logins);

    public int Connections => Volatile.Read(ref _connections);

    public bool RejectLogin { get; set; }

    public static async Task<FakeIqServer> StartAsync(JsonObject fixtures)
    {
        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls("http://127.0.0.1:0");
        builder.Logging.ClearProviders();
        var app = builder.Build();
        var server = new FakeIqServer(app);
        server._rules.AddRange(Harness.ConnectRules(fixtures));
        app.UseWebSockets();
        app.Run(server.HandleAsync);
        await app.StartAsync();
        server.BaseUrl = app.Services.GetRequiredService<IServer>().Features.Get<IServerAddressesFeature>()!.Addresses.First();
        return server;
    }

    public void AddRule(string on, params string[] replies)
    {
        lock (_rules) _rules.Add(new Rule(on, replies.Select(r => JsonNode.Parse(r)!.AsObject()).ToList()));
    }

    public void ReplaceRule(string on, params string[] replies)
    {
        lock (_rules) _rules.RemoveAll(r => r.On == on);
        AddRule(on, replies);
    }

    public string[] FramesNamed(string key) => Frames.Where(f => Rule.KeyOf(JsonNode.Parse(f)!.AsObject()) == key).ToArray();

    public void Drop() => _current?.Abort();

    public async Task PushAsync(string json)
    {
        var ws = _current;
        if (ws != null) await SendAsync(ws, json);
    }

    private async Task HandleAsync(HttpContext ctx)
    {
        string path = ctx.Request.Path.Value ?? "";
        if (path == "/api/v2/login")
        {
            int n = Interlocked.Increment(ref _logins);
            if (RejectLogin)
            {
                ctx.Response.StatusCode = 400;
                await ctx.Response.WriteAsync("{\"code\":\"invalid_credentials\"}");
                return;
            }
            ctx.Response.Headers.Append("Set-Cookie", $"ssid=SSID-{n}; Path=/");
            await ctx.Response.WriteAsync("{\"code\":\"success\"}");
        }
        else if (path == "/api/v1.0/logout")
        {
            await ctx.Response.WriteAsync("{}");
        }
        else if (path == "/echo/websocket" && ctx.WebSockets.IsWebSocketRequest)
        {
            using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
            _current = ws;
            Interlocked.Increment(ref _connections);
            await ServeAsync(ws, ctx.RequestAborted);
        }
        else
        {
            ctx.Response.StatusCode = 404;
        }
    }

    private async Task ServeAsync(WebSocket ws, CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        try
        {
            while (ws.State == WebSocketState.Open)
            {
                using var ms = new MemoryStream();
                WebSocketReceiveResult r;
                do
                {
                    r = await ws.ReceiveAsync(buffer, ct);
                    if (r.MessageType == WebSocketMessageType.Close) return;
                    ms.Write(buffer, 0, r.Count);
                }
                while (!r.EndOfMessage);
                string text = Encoding.UTF8.GetString(ms.ToArray());
                Frames.Enqueue(text);
                List<Rule> rules;
                lock (_rules) rules = _rules.ToList();
                foreach (string reply in Rule.RepliesFor(JsonNode.Parse(text)!.AsObject(), rules)) await SendAsync(ws, reply);
            }
        }
        catch (Exception ex) when (ex is WebSocketException or OperationCanceledException or ObjectDisposedException)
        {
            // the client went away or the test aborted the socket
        }
    }

    private async Task SendAsync(WebSocket ws, string text)
    {
        await _sendLock.WaitAsync();
        try
        {
            await ws.SendAsync(Encoding.UTF8.GetBytes(text), WebSocketMessageType.Text, true, CancellationToken.None);
        }
        catch (Exception ex) when (ex is WebSocketException or ObjectDisposedException)
        {
            // socket already gone
        }
        finally
        {
            _sendLock.Release();
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
```

- [ ] **Step 2: Write the failing tests `tests/IqOptionApi.Tests/ReconnectTests.cs`**

```csharp
using System.Text.Json.Nodes;

namespace IqOptionApi.Tests;

public class ReconnectTests
{
    private static JsonObject Fixtures => Golden.Load("methods.json")["fixtures"]!.AsObject();

    private static IqOption Client(FakeIqServer server) => new("user@example.com", "secret", new IqOptionOptions
    {
        WebSocketUrl = server.BaseUrl.Replace("http://", "ws://", StringComparison.Ordinal) + "/echo/websocket",
        AuthUrl = server.BaseUrl,
        Host = "127.0.0.1",
        RequestTimeout = TimeSpan.FromSeconds(3),
        ReconnectDelays = new[] { TimeSpan.FromMilliseconds(50) },
        TimeZone = Golden.Zone("Asia/Karachi"),
    });

    private static async Task WaitForAsync(Func<bool> condition)
    {
        for (int i = 0; i < 250 && !condition(); i++) await Task.Delay(20);
        Assert.True(condition(), "condition not reached in 5 s");
    }

    [Fact]
    public async Task RealSocket_ConnectBuyAndCheckWin()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        server.AddRule("sendMessage:binary-options.open-option",
            "{\"name\":\"option\",\"request_id\":\"$RID\",\"msg\":{\"id\":42}}",
            "{\"name\":\"option-closed\",\"msg\":{\"option_id\":42,\"amount\":1,\"profit_amount\":1.8}}");
        await using var api = Client(server);
        Assert.True((await api.ConnectAsync()).Success);

        var (ok, id, _) = await api.BuyAsync(1, "EURUSD", Direction.Call, 1);
        var (result, profit) = await api.CheckWinAsync(id).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(ok);
        Assert.Equal(TradeResult.Win, result);
        Assert.Equal(0.8, profit, 9);
    }

    [Fact]
    public async Task Drop_DuringBuy_FailsAndDoesNotResend()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        await using var api = Client(server);
        var reconnected = new TaskCompletionSource();
        api.Reconnected += () => reconnected.TrySetResult();
        Assert.True((await api.ConnectAsync()).Success);

        var buy = api.BuyAsync(1, "EURUSD", Direction.Call, 1);
        await WaitForAsync(() => server.FramesNamed("sendMessage:binary-options.open-option").Length == 1);
        server.Drop();

        Assert.Equal((false, 0L, "connection lost"), await buy.WaitAsync(TimeSpan.FromSeconds(5)));
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(10));
        await Task.Delay(200);
        Assert.Single(server.FramesNamed("sendMessage:binary-options.open-option"));
        Assert.True(api.IsConnected);
    }

    [Fact]
    public async Task Reconnect_ResubscribesStreams_AndKeepsSelectedBalance()
    {
        var f = Fixtures;
        await using var server = await FakeIqServer.StartAsync(f);
        server.AddRule("subscribeMessage:traders-mood-changed", "{\"name\":\"traders-mood-changed\",\"msg\":{\"asset_id\":1861,\"value\":0.5}}");
        await using var api = Client(server);
        var reconnected = new TaskCompletionSource();
        api.Reconnected += () => reconnected.TrySetResult();
        Assert.True((await api.ConnectAsync()).Success);
        Assert.True(await api.StartMoodStreamAsync("EURUSD"));
        await api.ChangeBalanceAsync(BalanceMode.Real);

        server.Drop();
        await reconnected.Task.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(2, server.FramesNamed("subscribeMessage:traders-mood-changed").Length);
        Assert.Equal(2, server.FramesNamed("setOptions").Length);
        Assert.Equal((long)f["real_id"]!, api.GetBalanceId());
    }

    [Fact]
    public async Task Reconnect_RecoversResultPushedWhileOffline()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        server.AddRule("sendMessage:get-history-positions",
            "{\"name\":\"history-positions\",\"request_id\":\"$RID\",\"msg\":{\"positions\":[{\"external_id\":42,\"pnl_realized\":0.8}]}}");
        await using var api = Client(server);
        Assert.True((await api.ConnectAsync()).Success);

        var check = api.CheckWinAsync(42);
        await Task.Delay(100);
        server.Drop();

        var (result, profit) = await check.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(TradeResult.Win, result);
        Assert.Equal(0.8, profit, 9);
    }

    [Fact]
    public async Task Close_StopsReconnecting()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        await using var api = Client(server);
        Assert.True((await api.ConnectAsync()).Success);

        await api.CloseAsync();
        server.Drop();
        await Task.Delay(500);

        Assert.Equal(1, server.Logins);
        Assert.Equal(1, server.Connections);
        Assert.False(api.IsConnected);
    }

    [Fact]
    public async Task LoginRejectedOnReconnect_GivesUp()
    {
        await using var server = await FakeIqServer.StartAsync(Fixtures);
        await using var api = Client(server);
        var reasons = new List<string>();
        api.Disconnected += r => { lock (reasons) reasons.Add(r); };
        Assert.True((await api.ConnectAsync()).Success);

        server.RejectLogin = true;
        server.ReplaceRule("authenticate", "{\"name\":\"authenticated\",\"msg\":false,\"request_id\":\"$RID\"}");
        server.Drop();

        await WaitForAsync(() => { lock (reasons) return reasons.Any(r => r.StartsWith("login failed", StringComparison.Ordinal)); });
        Assert.False(api.IsConnected);
    }
}
```

Run: `dotnet test IqOptionApi.sln --filter ReconnectTests` — Expected: `RealSocket_ConnectBuyAndCheckWin` and `Close_StopsReconnecting` PASS; the four reconnect tests FAIL (no reconnect yet).

- [ ] **Step 3: Implement reconnect in `IqOption.cs`** — replace the whole `OnSocketClosed` method with:

```csharp
    private void OnSocketClosed(string reason)
    {
        if (_closing || !_connected) return;
        Log("connection lost: " + reason);
        RaiseDisconnected(reason);
        if (_o.AutoReconnect) _ = Task.Run(ReconnectLoopAsync);
        else _connected = false;
    }

    private async Task ReconnectLoopAsync()
    {
        if (!await _reconnecting.WaitAsync(0).ConfigureAwait(false)) return;
        try
        {
            IReadOnlyList<TimeSpan> delays = _o.ReconnectDelays is { Count: > 0 } custom ? custom : DefaultReconnectDelays;
            for (int attempt = 0; !_closing; attempt++)
            {
                await Task.Delay(delays[Math.Min(attempt, delays.Count - 1)]).ConfigureAwait(false);
                if (_closing) return;
                (bool Success, string? Reason, bool LoginRejected) r;
                try
                {
                    r = await ConnectCoreAsync(resetBalance: false, CancellationToken.None).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    r = (false, ex.Message, false);
                }
                if (_closing)
                {
                    await _conn.CloseSocketAsync().ConfigureAwait(false);
                    return;
                }
                if (r.Success)
                {
                    Log("reconnected");
                    await RecoverResultsAsync().ConfigureAwait(false);
                    try
                    {
                        Reconnected?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        Log("Reconnected handler threw: " + ex.Message);
                    }
                    return;
                }
                if (r.LoginRejected)
                {
                    _connected = false;
                    RaiseDisconnected("login failed: " + r.Reason);
                    return;
                }
                Log($"reconnect attempt {attempt + 1} failed: {r.Reason}");
            }
        }
        finally
        {
            _reconnecting.Release();
        }
    }

    /// <summary>Trade results pushed while offline are lost; ask the history once for every pending CheckWin call.</summary>
    private async Task RecoverResultsAsync()
    {
        long[] binary, digital;
        lock (_track)
        {
            binary = _pendingBinary.ToArray();
            digital = _pendingDigital.ToArray();
        }
        foreach (long id in binary)
        {
            if (ClosedProfit(id) != null) continue;
            try
            {
                if (await RawCheckWinAsync(id, CancellationToken.None).ConfigureAwait(false) is double pnl)
                {
                    lock (_track) _recovered[id] = pnl;
                }
            }
            catch (Exception ex) when (IsFailure(ex) || ex is FormatException)
            {
                Log($"could not recover the result of option {id}: {ex.Message}");
            }
        }
        foreach (long orderId in digital)
        {
            if (DigitalCloseProfit(orderId) != null) continue;
            JsonNode? external;
            lock (_s.Lock)
            {
                external = _s.OrderAsync.TryGetValue(orderId, out var d) && d.TryGetValue("position-changed", out var pc)
                    ? Path(pc, "msg", "external_id")
                    : null;
            }
            if (J.IsNull(external)) continue;
            try
            {
                var reply = await GetDigitalPositionByPositionIdAsync(J.Long(external), CancellationToken.None).ConfigureAwait(false);
                if (Path(reply, "msg", "position") is not JsonObject pos || J.Str(pos["status"]) != "closed") continue;
                double profit = J.Str(pos["close_reason"]) == "expired"
                    ? J.Dbl(pos["pnl_realized"]) - J.Dbl(pos["buy_amount"])
                    : J.Dbl(pos["pnl_realized"]);
                lock (_track) _recoveredDigital[orderId] = profit;
            }
            catch (Exception ex) when (IsFailure(ex) || ex is FormatException)
            {
                Log($"could not recover the result of digital order {orderId}: {ex.Message}");
            }
        }
        _conn.Pulse.Pulse();
    }
```

- [ ] **Step 4: Run all tests** — `dotnet test IqOptionApi.sln` — Expected: all PASS. If `Drop_DuringBuy…` returns `"connection closed"` instead of `"connection lost"`, the receive loop treated the drop as a local close — check that `ClientWebSocketAdapter.ReceiveAsync` returns `null` (not an `OperationCanceledException`) when the server aborts.

- [ ] **Step 5: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add automatic reconnect with re-subscribe and result recovery"
```

---

### Task 16: Example bot, README and final verification

**Files:**
- Create: `examples/SimpleBot/SimpleBot.csproj`, `examples/SimpleBot/Program.cs`, `README.md`

**Interfaces:** Consumes the public API only.

- [ ] **Step 1: Create the project**

```bash
dotnet new console -o examples/SimpleBot -f net9.0
dotnet sln add examples/SimpleBot/SimpleBot.csproj
dotnet add examples/SimpleBot reference src/IqOptionApi
```

Set in `SimpleBot.csproj` `<PropertyGroup>`: `<Nullable>enable</Nullable>`, `<ImplicitUsings>enable</ImplicitUsings>`, `<TreatWarningsAsErrors>true</TreatWarningsAsErrors>`.

- [ ] **Step 2: Write `examples/SimpleBot/Program.cs`**

```csharp
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
```

Run: `dotnet build IqOptionApi.sln` — Expected: Build succeeded, 0 warnings.

- [ ] **Step 3: Write `README.md`** (sections, in this order):
  1. **What this is** — two paragraphs: C# port of the Python `iqoptionapi` v9.4; a library (no UI, no strategy); unofficial, protocol can change; automated trading may conflict with IQ Option's terms.
  2. **Requirements** — .NET 9 SDK.
  3. **Add it to your bot** — the three options from spec §3 with exact commands.
  4. **Quick start** — the 15-line connect/candles/buy/check-win example (same calls as SimpleBot).
  5. **Running the example** — `export IQ_EMAIL=…`, `export IQ_PASSWORD=…`, `dotnet run --project examples/SimpleBot -- --asset EURUSD-OTC`; PRACTICE safety lock and `--allow-real`.
  6. **Python → C# method table** — copy the tables of spec §5.2–§5.9 (C# signature | Python name), plus §5.10 "Not ported".
  7. **Differences from Python** — the fix list of spec §6.2 and the kept quirk §6.3, each one line.
  8. **Tests** — `dotnet test IqOptionApi.sln`; how golden data is produced (`python3 tests/golden/generate_golden.py` and `python3 tests/golden/gen_actives_cs.py`), and that golden files are committed.

- [ ] **Step 4: Full verification**

```bash
dotnet build IqOptionApi.sln -warnaserror
dotnet test IqOptionApi.sln
python3 -c "import json;d=json.load(open('tests/IqOptionApi.Tests/Golden/methods.json'));print(len(d['cases']),'golden method cases')"
grep -c "public " src/IqOptionApi/IqOption.cs
git status --short
```

Expected: build OK with 0 warnings; all tests PASS (none skipped); about 115 golden method cases; `git status` shows only the README/example changes about to be committed.

- [ ] **Step 5: Commit**

```bash
git add IqOptionApi.CSharp && git commit -m "Add example bot, README and finish the C# port"
```

- [ ] **Step 6: Hand over the live check** — ask the user to run SimpleBot on their PRACTICE account (they type the commands; their password never passes through the assistant) and paste the output. Fix anything the live server does differently, adding a regression test for each fix.
