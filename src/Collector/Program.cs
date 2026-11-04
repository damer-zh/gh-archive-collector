using System.IO.Compression;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

var settings = Settings.Load();
var mode = args.FirstOrDefault() ?? Environment.GetEnvironmentVariable("COLLECTOR_MODE") ?? "record";

using var http = new HttpClient { Timeout = TimeSpan.FromMinutes(10) };
http.DefaultRequestHeaders.UserAgent.ParseAdd(settings.UserAgent);
var logger = new JsonLogger();
var metrics = new Metrics(settings.MetricsPort, mode);
using var metricsCts = new CancellationTokenSource();
_ = metrics.RunAsync(metricsCts.Token);

try
{
    await logger.InfoAsync("collector_started", new { mode });
    switch (mode.ToLowerInvariant())
    {
        case "record": await RunRecordAsync(settings, http, logger, metrics); break;
        case "backfill": await RunBackfillAsync(settings, http, logger, metrics); break;
        default: throw new ArgumentException($"Unsupported mode: {mode}. Use record or backfill.");
    }
}
catch (OperationCanceledException) when (Console.IsInputRedirected == false)
{
    await logger.InfoAsync("collector_stopped");
}
catch (Exception ex)
{
    await logger.ErrorAsync("collector_failed", new { error = ex.Message });
    Environment.ExitCode = 1;
}
finally { metricsCts.Cancel(); }

static async Task RunBackfillAsync(Settings s, HttpClient http, JsonLogger log, Metrics metrics)
{
    var from = s.HistoryFrom ?? DateTimeOffset.UtcNow.AddDays(-7).Date;
    var to = s.HistoryTo ?? DateTimeOffset.UtcNow.Date;
    for (var hour = from.ToUniversalTime(); hour < to.ToUniversalTime(); hour = hour.AddHours(1))
        await FetchAndStoreAsync(s, http, log, metrics, hour, "history");
}

static async Task RunRecordAsync(Settings s, HttpClient http, JsonLogger log, Metrics metrics)
{
    var cursorPath = Path.Combine(s.OutputRoot, "stream", "cursor.txt");
    Directory.CreateDirectory(Path.GetDirectoryName(cursorPath)!);
    var next = File.Exists(cursorPath) && DateTime.TryParse(File.ReadAllText(cursorPath), out var saved)
        ? saved.ToUniversalTime().AddHours(1)
        : DateTime.UtcNow.AddHours(-1).Date.AddHours(DateTime.UtcNow.Hour - 1);

    while (true)
    {
        var currentClosedHour = DateTime.UtcNow.AddHours(-1).AddMinutes(-s.PublicationDelayMinutes);
        if (next <= currentClosedHour)
        {
            await FetchAndStoreAsync(s, http, log, metrics, next, "stream");
            File.WriteAllText(cursorPath, next.ToString("O"));
            next = next.AddHours(1);
        }
        else await Task.Delay(TimeSpan.FromSeconds(s.PollSeconds));
    }
}

static async Task FetchAndStoreAsync(Settings s, HttpClient http, JsonLogger log, Metrics metrics, DateTime hour, string partition)
{
    var url = $"{s.BaseUrl}/{hour:yyyy-MM-dd}-{hour:HH}.json.gz";
    var targetDir = Path.Combine(s.OutputRoot, partition, $"dt={hour:yyyy-MM-dd}", $"hour={hour:HH}");
    Directory.CreateDirectory(targetDir);
    var target = Path.Combine(targetDir, $"events-{hour:yyyyMMddHH}.jsonl.gz");
    var temp = target + ".tmp";
    if (File.Exists(target)) return;

    for (var attempt = 1; ; attempt++)
    {
        try
        {
            using var response = await http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead);
            if (response.StatusCode == HttpStatusCode.NotFound || response.StatusCode == HttpStatusCode.TooManyRequests || (int)response.StatusCode >= 500)
                throw new HttpRequestException($"HTTP {(int)response.StatusCode}");
            response.EnsureSuccessStatusCode();
            await using var source = await response.Content.ReadAsStreamAsync();
            await using var gzip = new GZipStream(source, CompressionMode.Decompress);
            await using var output = File.Create(temp);
            await using var writer = new StreamWriter(output);
            using var reader = new StreamReader(gzip);
            string? line;
            var accepted = 0;
            while ((line = await reader.ReadLineAsync()) is not null)
            {
                using var doc = JsonDocument.Parse(line);
                if (!doc.RootElement.TryGetProperty("type", out var type) || !s.Filter.Contains(type.GetString() ?? "")) continue;
                if (!doc.RootElement.TryGetProperty("id", out var id) || !doc.RootElement.TryGetProperty("created_at", out var created)) continue;
                using var record = JsonDocument.Parse($"{{\"event_key\":{JsonSerializer.Serialize(id.GetString() ?? id.ToString())},\"event_time\":{JsonSerializer.Serialize(created.GetString())},\"payload\":{line}}}");
                await writer.WriteLineAsync(record.RootElement.GetRawText());
                accepted++; metrics.Events++;
            }
            await writer.FlushAsync();
            File.Move(temp, target, true);
            metrics.LastSuccess = DateTimeOffset.UtcNow;
            await log.InfoAsync("archive_stored", new { url, partition, hour, accepted });
            return;
        }
        catch (Exception ex) when (attempt < s.MaxRetries)
        {
            metrics.Errors++;
            await log.ErrorAsync("archive_retry", new { url, attempt, error = ex.Message });
            if (File.Exists(temp)) File.Delete(temp);
            await Task.Delay(TimeSpan.FromSeconds(Math.Min(s.MaxBackoffSeconds, Math.Pow(2, attempt))));
        }
    }
}

sealed record Settings(string BaseUrl, string OutputRoot, HashSet<string> Filter, string UserAgent, int PollSeconds, int PublicationDelayMinutes, int MaxRetries, int MaxBackoffSeconds, int MetricsPort, DateTime? HistoryFrom, DateTime? HistoryTo)
{
    public static Settings Load() => new(
        Environment.GetEnvironmentVariable("COLLECTOR_BASE_URL") ?? "https://data.gharchive.org",
        Environment.GetEnvironmentVariable("COLLECTOR_RAW_DIR") ?? Path.Combine("data", "raw"),
        (Environment.GetEnvironmentVariable("SOURCE_FILTER") ?? "PullRequestEvent,IssuesEvent,IssueCommentEvent").Split(',', StringSplitOptions.RemoveEmptyEntries).ToHashSet(StringComparer.Ordinal),
        Environment.GetEnvironmentVariable("COLLECTOR_USER_AGENT") ?? "hybrid-systems-collector/1.0",
        Int("COLLECTOR_POLL_SECONDS", 300), Int("COLLECTOR_PUBLICATION_DELAY_MINUTES", 10), Int("COLLECTOR_MAX_RETRIES", 6), Int("COLLECTOR_MAX_BACKOFF_SECONDS", 300), Int("COLLECTOR_METRICS_PORT", 9101), Date("COLLECTOR_HISTORY_FROM"), Date("COLLECTOR_HISTORY_TO"));
    static int Int(string name, int fallback) => int.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v : fallback;
    static DateTime? Date(string name) => DateTime.TryParse(Environment.GetEnvironmentVariable(name), out var v) ? v.ToUniversalTime() : null;
}

sealed class JsonLogger
{
    public Task InfoAsync(string msg, object? data = null) => WriteAsync("info", msg, data);
    public Task ErrorAsync(string msg, object? data = null) => WriteAsync("error", msg, data);
    static Task WriteAsync(string level, string msg, object? data) { Console.WriteLine(JsonSerializer.Serialize(new { ts = DateTimeOffset.UtcNow, level, msg, data })); return Task.CompletedTask; }
}

sealed class Metrics(int port, string mode)
{
    public long Events, Errors; public DateTimeOffset? LastSuccess;
    public async Task RunAsync(CancellationToken token)
    {
        using var listener = new HttpListener(); listener.Prefixes.Add($"http://*:{port}/"); listener.Start();
        while (!token.IsCancellationRequested)
        {
            var context = await listener.GetContextAsync();
            var body = $"collector_events_total{{mode=\"{mode}\"}} {Events}\ncollector_errors_total{{mode=\"{mode}\"}} {Errors}\ncollector_last_success_timestamp_seconds {(LastSuccess?.ToUnixTimeSeconds() ?? 0)}\n";
            var bytes = System.Text.Encoding.UTF8.GetBytes(body); context.Response.ContentType = "text/plain; version=0.0.4"; context.Response.ContentLength64 = bytes.Length; await context.Response.OutputStream.WriteAsync(bytes); context.Response.Close();
        }
    }
}
