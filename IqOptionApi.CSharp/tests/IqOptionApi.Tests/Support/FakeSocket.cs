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

    /// <summary>When it returns true for a frame, SendAsync throws like a broken socket.</summary>
    public Func<string, bool>? FailSend { get; set; }

    public Task SendAsync(string text, CancellationToken ct)
    {
        if (FailSend?.Invoke(text) == true) throw new System.Net.WebSockets.WebSocketException("send failed (test)");
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
