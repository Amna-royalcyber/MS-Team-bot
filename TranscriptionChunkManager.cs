using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptItem
{
    public required DateTime Timestamp { get; init; }

    /// <summary>Entra object id (GUID) when resolved; otherwise synthetic e.g. <c>msi-pending-{sourceId}</c>.</summary>
    public required string EntraObjectId { get; set; }

    public required string ParticipantName { get; set; }
    public required string Text { get; init; }
    public uint? SourceStreamId { get; init; }
}

public sealed class TranscriptionChunk
{
    public required DateTime StartTime { get; init; }
    public required DateTime EndTime { get; init; }
    public required List<TranscriptItem> Items { get; init; }
}

/// <summary>
/// MIM schedule: first POST at 1 min (flag 0), then every 3 min (flag 1, always), flag 2 when no participants.
/// </summary>
public sealed class TranscriptionChunkManager : BackgroundService, IChunkManager
{
    private const int MimFlagFirstWindow = 0;
    private const int MimFlagSubsequentWindow = 1;
    private const int MimFlagNoParticipants = 2;

    private readonly BotSettings _settings;
    private readonly MeetingContextStore _meetingContext;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<TranscriptionChunkManager> _logger;

    private readonly object _lock = new();
    private int _anchorOnce;
    private DateTime _meetingStartTimeUtc;
    private DateTime _windowStartUtc;
    private bool _hasAnchor;
    private int _completedPosts;
    private readonly List<TranscriptItem> _accumulator = new();
    private readonly HashSet<string> _dedupeKeys = new(StringComparer.Ordinal);

    public TranscriptionChunkManager(
        BotSettings settings,
        MeetingContextStore meetingContext,
        IHttpClientFactory httpClientFactory,
        ILogger<TranscriptionChunkManager> logger)
    {
        _settings = settings;
        _meetingContext = meetingContext;
        _httpClientFactory = httpClientFactory;
        _logger = logger;
    }

    public void ResetForNewJoin()
    {
        Interlocked.Exchange(ref _anchorOnce, 0);
        lock (_lock)
        {
            _hasAnchor = false;
            _accumulator.Clear();
            _dedupeKeys.Clear();
            _completedPosts = 0;
        }
    }

    public void BeginMeeting(DateTime anchorUtc)
    {
        if (Interlocked.Exchange(ref _anchorOnce, 1) != 0)
        {
            return;
        }

        lock (_lock)
        {
            _meetingStartTimeUtc = anchorUtc.Kind == DateTimeKind.Utc ? anchorUtc : anchorUtc.ToUniversalTime();
            _windowStartUtc = _meetingStartTimeUtc;
            _hasAnchor = true;
            _completedPosts = 0;
            _accumulator.Clear();
            _dedupeKeys.Clear();
            _logger.LogInformation(
                "MIM schedule started. First post at +{FirstMin} min (flag 0), then every {SubsequentMin} min (flag 1). Anchor={AnchorUtc} (UTC).",
                FirstPostMinutes(),
                SubsequentPostMinutes(),
                _meetingStartTimeUtc);
        }
    }

    public void EndMeeting() => _ = EndMeetingAsync();

    public async Task EndMeetingAsync(CancellationToken cancellationToken = default)
    {
        List<TranscriptItem>? remaining;
        int completedPosts;
        lock (_lock)
        {
            remaining = _accumulator.Count > 0 ? _accumulator.ToList() : null;
            completedPosts = _completedPosts;
            _accumulator.Clear();
            _dedupeKeys.Clear();
            _hasAnchor = false;
            Interlocked.Exchange(ref _anchorOnce, 0);
        }

        if (remaining is not null && remaining.Count > 0)
        {
            var flag = completedPosts == 0 ? MimFlagFirstWindow : MimFlagSubsequentWindow;
            await PostPayloadAsync(remaining, _windowStartUtc, DateTime.UtcNow, flag, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task FlushNoParticipantsAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
        {
            return;
        }

        List<TranscriptItem> snapshot;
        var windowStart = _windowStartUtc;
        lock (_lock)
        {
            if (!_hasAnchor)
            {
                return;
            }

            snapshot = _accumulator.ToList();
            _accumulator.Clear();
            _dedupeKeys.Clear();
            _hasAnchor = false;
            Interlocked.Exchange(ref _anchorOnce, 0);
        }

        await PostPayloadAsync(snapshot, windowStart, DateTime.UtcNow, MimFlagNoParticipants, cancellationToken)
            .ConfigureAwait(false);
    }

    public Task RecordFinalAsync(
        DateTime utteranceUtc,
        string participantId,
        string speakerName,
        string text,
        string dedupeKey,
        uint? sourceStreamId = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint) || string.IsNullOrWhiteSpace(text))
        {
            return Task.CompletedTask;
        }

        var utc = utteranceUtc.Kind == DateTimeKind.Utc ? utteranceUtc : utteranceUtc.ToUniversalTime();
        lock (_lock)
        {
            if (!_hasAnchor)
            {
                return Task.CompletedTask;
            }

            if (utc < _meetingStartTimeUtc)
            {
                utc = _meetingStartTimeUtc;
            }

            if (!_dedupeKeys.Add(dedupeKey))
            {
                return Task.CompletedTask;
            }

            _accumulator.Add(new TranscriptItem
            {
                Timestamp = utc,
                EntraObjectId = participantId.Trim(),
                ParticipantName = speakerName.Trim(),
                Text = text.Trim(),
                SourceStreamId = sourceStreamId
            });
        }

        return Task.CompletedTask;
    }

    public Task<int> ReconcileRecentIdentityAsync(
        uint sourceId,
        string participantId,
        string displayName,
        TimeSpan lookback,
        CancellationToken cancellationToken = default)
    {
        var sinceUtc = DateTime.UtcNow - lookback;
        var updated = 0;
        lock (_lock)
        {
            foreach (var item in _accumulator)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (item.SourceStreamId != sourceId || item.Timestamp < sinceUtc)
                {
                    continue;
                }

                var pendingPrefix = $"msi-pending-{sourceId}";
                var isPendingId = item.EntraObjectId.StartsWith(pendingPrefix, StringComparison.OrdinalIgnoreCase);
                var isPendingName = item.ParticipantName.StartsWith("Unknown", StringComparison.OrdinalIgnoreCase) ||
                                    item.ParticipantName.StartsWith("msi-pending-", StringComparison.OrdinalIgnoreCase);
                if (!isPendingId && !isPendingName &&
                    !string.Equals(item.EntraObjectId, participantId, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                item.EntraObjectId = participantId;
                item.ParticipantName = displayName;
                updated++;
            }
        }

        if (updated > 0)
        {
            _logger.LogInformation(
                "CHUNK[RECONCILE] Updated {Count} transcript lines for sourceId={SourceId} -> {DisplayName}.",
                updated,
                sourceId,
                displayName);
        }

        return Task.FromResult(updated);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(5));
        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
            {
                continue;
            }

            List<TranscriptItem>? toPost = null;
            DateTime windowStart;
            DateTime windowEnd;
            int flag;
            lock (_lock)
            {
                if (!_hasAnchor)
                {
                    continue;
                }

                var now = DateTime.UtcNow;
                var nextPostUtc = _meetingStartTimeUtc.AddMinutes(
                    FirstPostMinutes() + (_completedPosts * SubsequentPostMinutes()));
                if (now < nextPostUtc)
                {
                    continue;
                }

                windowStart = _windowStartUtc;
                windowEnd = now;
                flag = _completedPosts == 0 ? MimFlagFirstWindow : MimFlagSubsequentWindow;
                toPost = _accumulator.ToList();
                _accumulator.Clear();
                _dedupeKeys.Clear();
                _completedPosts++;
                _windowStartUtc = now;
            }

            if (toPost is not null)
            {
                await PostPayloadAsync(toPost, windowStart, windowEnd, flag, stoppingToken).ConfigureAwait(false);
            }
        }
    }

    private int FirstPostMinutes() =>
        Math.Clamp(_settings.TranscriptFirstPostMinutes, 1, 60);

    private int SubsequentPostMinutes() =>
        Math.Clamp(_settings.TranscriptSubsequentPostMinutes, 1, 60);

    private async Task PostPayloadAsync(
        IReadOnlyList<TranscriptItem> items,
        DateTime windowStart,
        DateTime windowEnd,
        int flag,
        CancellationToken cancellationToken)
    {
        var endpoint = _settings.TranscriptAlbEndpoint;
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return;
        }

        var transcript = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var fragment in items.OrderBy(i => i.Timestamp))
        {
            if (string.IsNullOrWhiteSpace(fragment.Text))
            {
                continue;
            }

            var speaker = string.IsNullOrWhiteSpace(fragment.ParticipantName) ? "Unknown" : fragment.ParticipantName.Trim();
            var line = fragment.Text.Trim();
            if (transcript.TryGetValue(speaker, out var existing))
            {
                transcript[speaker] = $"{existing} {line}";
            }
            else
            {
                transcript[speaker] = line;
            }
        }

        var snowTicket = _meetingContext.CurrentSnowTicketId;
        if (string.IsNullOrWhiteSpace(snowTicket))
        {
            snowTicket = MeetingJoinParser.ExtractSnowTicketIdFromTitle(_meetingContext.CurrentMeetingTitle);
        }

        var payload = new MimChunkPayload
        {
            MeetingId = NormalizeMeetingIdForOutbound(_meetingContext.CurrentMeetingId),
            Transcript = transcript,
            Flag = flag,
            BridgeLeadId = _meetingContext.CurrentBridgeLeadId,
            SnowTicketId = snowTicket
        };

        try
        {
            var client = _httpClientFactory.CreateClient("AlbTranscriptSender");
            var jsonOptions = new JsonSerializerOptions
            {
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            };

            using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
            {
                Content = JsonContent.Create(payload, options: jsonOptions)
            };

            using var response = await client.SendAsync(request, cancellationToken);
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning(
                    "MIM post failed. Status={Status}, MeetingId={MeetingId}, Flag={Flag}, Speakers={Count}, Window={Start}–{End}.",
                    (int)response.StatusCode,
                    payload.MeetingId,
                    payload.Flag,
                    transcript.Count,
                    windowStart,
                    windowEnd);
                return;
            }

            _logger.LogInformation(
                "MIM post OK. MeetingId={MeetingId}, Flag={Flag}, SnowTicketId={SnowTicketId}, Speakers={Count}, Window={Start}–{End}.",
                payload.MeetingId,
                payload.Flag,
                payload.SnowTicketId ?? "(none)",
                transcript.Count,
                windowStart,
                windowEnd);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MIM post error for window {Start}–{End}, flag={Flag}.", windowStart, windowEnd, flag);
        }
    }

    private static string NormalizeMeetingIdForOutbound(string? meetingId)
    {
        if (string.IsNullOrWhiteSpace(meetingId))
        {
            return "unknown";
        }

        var value = meetingId.Trim();
        if (Guid.TryParse(value, out var g))
        {
            return g.ToString();
        }

        const string prefix = "19:meeting_";
        const string suffix = "@thread.v2";
        if (value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) &&
            value.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
        {
            var encoded = value[prefix.Length..^suffix.Length];
            if (!string.IsNullOrWhiteSpace(encoded))
            {
                var b64 = encoded.Replace('-', '+').Replace('_', '/');
                var pad = b64.Length % 4;
                if (pad != 0)
                {
                    b64 = b64.PadRight(b64.Length + (4 - pad), '=');
                }

                try
                {
                    var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(b64)).Trim();
                    if (Guid.TryParse(decoded, out var decodedGuid))
                    {
                        return decodedGuid.ToString();
                    }
                }
                catch
                {
                    // fall through
                }
            }
        }

        foreach (var token in value.Split([':', '_', '@', '/', '\\', '?', '&', '='], StringSplitOptions.RemoveEmptyEntries))
        {
            if (Guid.TryParse(token, out var extracted))
            {
                return extracted.ToString();
            }
        }

        return value;
    }

    private sealed class MimChunkPayload
    {
        [JsonPropertyName("meeting_id")]
        public string MeetingId { get; set; } = string.Empty;

        [JsonPropertyName("transcript")]
        public Dictionary<string, string> Transcript { get; set; } = new(StringComparer.Ordinal);

        [JsonPropertyName("bridge_lead_id")]
        public string BridgeLeadId { get; set; } = string.Empty;

        [JsonPropertyName("snow_ticket_id")]
        public string? SnowTicketId { get; set; }

        [JsonPropertyName("flag")]
        public int Flag { get; set; }
    }
}
