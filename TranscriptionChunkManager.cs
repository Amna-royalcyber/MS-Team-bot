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

public sealed class TimeWindowChunk
{
    public required DateTime StartTime { get; init; }
    public required DateTime EndTime { get; init; }
    public required List<TranscriptItem> Fragments { get; init; }
}

/// <summary>
/// Wall-clock windows from call anchor (see <see cref="BotSettings.TranscriptPostIntervalSeconds"/>). Posts JSON to the MIM API Gateway endpoint.
/// </summary>
public sealed class TranscriptionChunkManager : BackgroundService, IChunkManager
{
    private TimeSpan ChunkDuration =>
        TimeSpan.FromSeconds(Math.Clamp(_settings.TranscriptPostIntervalSeconds, 10, 300));

    private const int MimFlagHasTranscript = 0;
    private const int MimFlagSilence = 1;
    private const int MimFlagNoParticipants = 2;

    private readonly BotSettings _settings;
    private readonly MeetingContextStore _meetingContext;
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ILogger<TranscriptionChunkManager> _logger;

    private readonly object _lock = new();
    private int _anchorOnce;
    private DateTime _meetingStartTimeUtc;
    private bool _hasAnchor;
    private TimeWindowChunk? _currentWindow;
    private readonly HashSet<string> _dedupeKeys = new(StringComparer.Ordinal);
    private readonly object _debounceLock = new();
    private CancellationTokenSource? _debounceCts;

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

    /// <summary>Reset chunk state when starting a new join attempt (before call id exists).</summary>
    public void ResetForNewJoin()
    {
        Interlocked.Exchange(ref _anchorOnce, 0);
        lock (_lock)
        {
            _hasAnchor = false;
            _currentWindow = null;
            _dedupeKeys.Clear();
        }
    }

    /// <summary>Set wall-clock anchor once when the call is established (starts [0–3), [3–6), … windows).</summary>
    public void BeginMeeting(DateTime anchorUtc)
    {
        if (Interlocked.Exchange(ref _anchorOnce, 1) != 0)
        {
            return;
        }

        lock (_lock)
        {
            _meetingStartTimeUtc = anchorUtc.Kind == DateTimeKind.Utc ? anchorUtc : anchorUtc.ToUniversalTime();
            _hasAnchor = true;
            _currentWindow = CreateNewWindow(_meetingStartTimeUtc);
            _dedupeKeys.Clear();
            _logger.LogInformation(
                "Transcription chunk anchor set to {AnchorUtc} (UTC). MIM post interval={IntervalSeconds}s.",
                _meetingStartTimeUtc,
                ChunkDuration.TotalSeconds);
        }
    }

    public void EndMeeting() => _ = EndMeetingAsync();

    public async Task EndMeetingAsync(CancellationToken cancellationToken = default)
    {
        TimeWindowChunk? remaining;
        lock (_lock)
        {
            remaining = _currentWindow;
            _currentWindow = null;
            _hasAnchor = false;
            _dedupeKeys.Clear();
            Interlocked.Exchange(ref _anchorOnce, 0);
        }

        if (remaining is not null)
        {
            await FlushWindowAsync(remaining, flag: null, cancellationToken).ConfigureAwait(false);
        }
    }

    public async Task FlushNoParticipantsAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
        {
            return;
        }

        TimeWindowChunk? window;
        lock (_lock)
        {
            if (!_hasAnchor || _currentWindow is null)
            {
                return;
            }

            window = _currentWindow;
            _currentWindow = CreateNewWindow(_currentWindow.EndTime);
            _dedupeKeys.Clear();
        }

        await FlushWindowAsync(window, MimFlagNoParticipants, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Record a final transcript line into the current window (may flush prior windows when interval elapses).</summary>
    public async Task RecordFinalAsync(
        DateTime utteranceUtc,
        string participantId,
        string speakerName,
        string text,
        string dedupeKey,
        uint? sourceStreamId = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        var utc = utteranceUtc.Kind == DateTimeKind.Utc ? utteranceUtc : utteranceUtc.ToUniversalTime();
        _logger.LogDebug(
            "CHUNK[RECORD] Final transcript received: participantId={ParticipantId}, speaker={Speaker}, sourceId={SourceId}, chars={Chars}.",
            participantId,
            speakerName,
            sourceStreamId,
            text.Length);

        List<TimeWindowChunk>? windowsToFlush = null;
        lock (_lock)
        {
            if (!_hasAnchor || _currentWindow is null)
            {
                return;
            }

            if (utc < _meetingStartTimeUtc)
            {
                utc = _meetingStartTimeUtc;
            }

            while (utc >= _currentWindow.EndTime)
            {
                windowsToFlush ??= new List<TimeWindowChunk>();
                windowsToFlush.Add(_currentWindow);
                _currentWindow = CreateNewWindow(_currentWindow.EndTime);
                _dedupeKeys.Clear();
            }

            if (!_dedupeKeys.Add(dedupeKey))
            {
                _logger.LogDebug("CHUNK[RECORD] Duplicate transcript dropped by dedupe key.");
                return;
            }

            _currentWindow.Fragments.Add(new TranscriptItem
            {
                Timestamp = utc,
                EntraObjectId = participantId.Trim(),
                ParticipantName = speakerName.Trim(),
                Text = text.Trim(),
                SourceStreamId = sourceStreamId
            });
        }

        if (windowsToFlush is not null)
        {
            foreach (var window in windowsToFlush)
            {
                await FlushWindowAsync(window, flag: null, cancellationToken);
            }
        }

        ScheduleDebouncedMimPost();
    }

    private void ScheduleDebouncedMimPost()
    {
        if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
        {
            return;
        }

        var delaySeconds = Math.Clamp(_settings.TranscriptPostDebounceSeconds, 1, 30);
        CancellationTokenSource cts;
        lock (_debounceLock)
        {
            _debounceCts?.Cancel();
            _debounceCts?.Dispose();
            cts = new CancellationTokenSource();
            _debounceCts = cts;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(delaySeconds), cts.Token).ConfigureAwait(false);
                await FlushDebouncedCurrentWindowAsync(cts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // superseded by a newer final transcript
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Debounced MIM post failed.");
            }
        });
    }

    private async Task FlushDebouncedCurrentWindowAsync(CancellationToken cancellationToken)
    {
        TimeWindowChunk? window;
        lock (_lock)
        {
            if (!_hasAnchor || _currentWindow is null || _currentWindow.Fragments.Count == 0)
            {
                return;
            }

            window = _currentWindow;
            _currentWindow = CreateNewWindow(DateTime.UtcNow);
            _dedupeKeys.Clear();
        }

        await FlushWindowAsync(window, flag: null, cancellationToken).ConfigureAwait(false);
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
            if (_currentWindow is null)
            {
                return Task.FromResult(0);
            }

            foreach (var item in _currentWindow.Fragments)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (item.SourceStreamId != sourceId)
                {
                    continue;
                }

                if (item.Timestamp < sinceUtc)
                {
                    continue;
                }

                // Late-binding patch: replace temporary speaker placeholders once identity resolves.
                var pendingPrefix = $"msi-pending-{sourceId}";
                var isPendingId = item.EntraObjectId.StartsWith(pendingPrefix, StringComparison.OrdinalIgnoreCase);
                var isPendingName = item.ParticipantName.StartsWith("Unknown", StringComparison.OrdinalIgnoreCase) ||
                                    item.ParticipantName.StartsWith("msi-pending-", StringComparison.OrdinalIgnoreCase);
                if (!isPendingId && !isPendingName && !string.Equals(item.EntraObjectId, participantId, StringComparison.OrdinalIgnoreCase))
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
                "CHUNK[RECONCILE] Updated {Count} recent transcript fragments for sourceId={SourceId} -> {DisplayName} ({ParticipantId}).",
                updated,
                sourceId,
                displayName,
                participantId);
        }

        return Task.FromResult(updated);
    }

    /// <summary>Timer-driven: close chunks when wall clock passes chunk end (handles silence with empty payloads).</summary>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var tickSeconds = Math.Max(1, Math.Min(5, _settings.TranscriptPostIntervalSeconds / 6));
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(tickSeconds));
        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            if (string.IsNullOrWhiteSpace(_settings.TranscriptAlbEndpoint))
            {
                continue;
            }

            while (true)
            {
                TimeWindowChunk? windowToFlush = null;
                lock (_lock)
                {
                    if (!_hasAnchor || _currentWindow is null)
                    {
                        break;
                    }

                    var now = DateTime.UtcNow;
                    if (now < _currentWindow.EndTime)
                    {
                        break;
                    }

                    windowToFlush = _currentWindow;
                    _currentWindow = CreateNewWindow(_currentWindow.EndTime);
                    _dedupeKeys.Clear();
                }

                if (windowToFlush is null)
                {
                    break;
                }

                await FlushWindowAsync(windowToFlush, flag: null, stoppingToken);
            }
        }
    }

    private TimeWindowChunk CreateNewWindow(DateTime startUtc)
    {
        return new TimeWindowChunk
        {
            StartTime = startUtc,
            EndTime = startUtc.Add(ChunkDuration),
            Fragments = new List<TranscriptItem>()
        };
    }

    private async Task FlushWindowAsync(TimeWindowChunk window, int? flag, CancellationToken cancellationToken)
    {
        var endpoint = _settings.TranscriptAlbEndpoint;
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return;
        }

        var ordered = window.Fragments.OrderBy(i => i.Timestamp).ToList();
        _logger.LogDebug(
            "CHUNK[FLUSH] Preparing window flush Start={Start}, End={End}, RawLines={RawLines}.",
            window.StartTime,
            window.EndTime,
            ordered.Count);

        var transcript = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var fragment in ordered)
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

        var resolvedFlag = flag ?? ResolveMimFlag(transcript.Count);
        var snowTicket = _meetingContext.CurrentSnowTicketId;
        if (string.IsNullOrWhiteSpace(snowTicket))
        {
            snowTicket = MeetingJoinParser.ExtractSnowTicketIdFromTitle(_meetingContext.CurrentMeetingTitle);
        }

        var payload = new MimChunkPayload
        {
            MeetingId = NormalizeMeetingIdForOutbound(_meetingContext.CurrentMeetingId),
            Transcript = transcript,
            Flag = resolvedFlag,
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
                    "MIM chunk post failed. Status={Status}, MeetingId={MeetingId}, Flag={Flag}, SnowTicketId={SnowTicketId}, Start={Start}, End={End}, Speakers={Count}.",
                    (int)response.StatusCode,
                    payload.MeetingId,
                    payload.Flag,
                    payload.SnowTicketId ?? "(none)",
                    window.StartTime,
                    window.EndTime,
                    transcript.Count);
                return;
            }

            _logger.LogInformation(
                "Posted transcript chunk to MIM API. MeetingId={MeetingId}, Flag={Flag}, SnowTicketId={SnowTicketId}, Start={Start}, End={End}, Speakers={Count}.",
                payload.MeetingId,
                payload.Flag,
                payload.SnowTicketId ?? "(none)",
                window.StartTime,
                window.EndTime,
                transcript.Count);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MIM chunk post error for window {Start}–{End}.", window.StartTime, window.EndTime);
        }
        finally
        {
            window.Fragments.Clear();
        }
    }

    private static int ResolveMimFlag(int transcriptSpeakerCount) =>
        transcriptSpeakerCount == 0 ? MimFlagSilence : MimFlagHasTranscript;

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
                    // keep searching for any guid-looking token below
                }
            }
        }

        // Last fallback: extract the first GUID-looking token from the value.
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
