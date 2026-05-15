using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading;
using Microsoft.CognitiveServices.Speech;
using Microsoft.CognitiveServices.Speech.Audio;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// One Azure Speech recognizer per media stream id. Identity is supplied by the caller (Graph + SSRC map); never inferred from audio.
/// </summary>
public sealed class AzureSpeechTranscriptionService : IAsyncDisposable
{
    private static readonly AudioStreamFormat Pcm16kMono = AudioStreamFormat.GetWaveFormatPCM(16000, 16, 1);
    /// <summary>20 ms of silence at 16 kHz mono PCM16 — keeps push-stream recognizers alive when Teams sends no frames.</summary>
    private static readonly byte[] SilencePcm20Ms = new byte[640];

    /// <summary>Restart recognizer before Azure/Teams long-idle cutoff (~5 min reported in the field).</summary>
    private static readonly TimeSpan MaxIdleBeforeRestart = TimeSpan.FromMinutes(3);

    private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(15);

    private readonly BotSettings _settings;
    private readonly TranscriptBroadcaster _broadcaster;
    private readonly IChunkManager _chunkManager;
    private readonly ILogger<AzureSpeechTranscriptionService> _logger;
    private readonly ConcurrentDictionary<uint, StreamSession> _sessions = new();
    private readonly Timer _keepAliveTimer;
    private volatile bool _disposed;
    private int _loggedMissingAzureConfig;

    private sealed class StreamSession
    {
        public readonly SemaphoreSlim Serialize = new(1, 1);
        public readonly object Gate = new();
        public PushAudioInputStream? Push;
        public SpeechRecognizer? Recognizer;
        public bool Started;
        public bool NeedsRestart;
        public TranscriptionParticipant? Participant;
        /// <summary>Last time real (non-keepalive) PCM was written.</summary>
        public DateTime LastRealAudioUtc = DateTime.MinValue;
    }

    public AzureSpeechTranscriptionService(
        BotSettings settings,
        TranscriptBroadcaster broadcaster,
        IChunkManager chunkManager,
        ILogger<AzureSpeechTranscriptionService> logger)
    {
        _settings = settings;
        _broadcaster = broadcaster;
        _chunkManager = chunkManager;
        _logger = logger;
        _keepAliveTimer = new Timer(KeepAliveActiveSessions, null, KeepAliveInterval, KeepAliveInterval);
    }

    /// <summary>Process PCM for a stream with identity already resolved. Unknown SSRC must be dropped by the caller.</summary>
    public async Task ProcessAudioAsync(uint ssrc, TranscriptionParticipant participant, byte[] pcm16kMono, long timestampHns)
    {
        if (_disposed || pcm16kMono.Length == 0)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(_settings.AzureSpeechKey) ||
            string.IsNullOrWhiteSpace(_settings.AzureSpeechRegion))
        {
            if (Interlocked.Exchange(ref _loggedMissingAzureConfig, 1) == 0)
            {
                _logger.LogWarning(
                    "Azure Speech is not configured. Set Bot:AzureSpeechKey and Bot:AzureSpeechRegion (or env BOT_AZURE_SPEECH_KEY / BOT_AZURE_SPEECH_REGION).");
            }

            return;
        }

        var session = _sessions.GetOrAdd(ssrc, _ => new StreamSession());
        await session.Serialize.WaitAsync().ConfigureAwait(false);
        try
        {
            var now = DateTime.UtcNow;
            var shouldStart = false;
            var shouldRestart = false;
            lock (session.Gate)
            {
                session.Participant = participant;
                if (!session.Started)
                {
                    shouldStart = true;
                }
                else if (session.NeedsRestart)
                {
                    shouldRestart = true;
                }
                else if (session.LastRealAudioUtc != DateTime.MinValue &&
                         (now - session.LastRealAudioUtc) > MaxIdleBeforeRestart)
                {
                    shouldRestart = true;
                }
            }

            if (shouldRestart)
            {
                var idleSeconds = session.LastRealAudioUtc == DateTime.MinValue
                    ? 0
                    : (now - session.LastRealAudioUtc).TotalSeconds;
                _logger.LogInformation(
                    "SPEECH[RESTART] Restarting recognizer after idle/cancel for SSRC/sourceId {Ssrc} (idle {IdleSeconds:F0}s).",
                    ssrc,
                    idleSeconds);
                await StopRecognizerCoreAsync(session).ConfigureAwait(false);
                shouldStart = true;
            }

            if (shouldStart)
            {
                _logger.LogInformation(
                    "SPEECH[START] Creating recognizer for SSRC/sourceId {Ssrc}, participant={DisplayName} ({ParticipantId}).",
                    ssrc,
                    participant.DisplayName,
                    participant.ParticipantId);
                await StartRecognizerAsync(session, ssrc, participant).ConfigureAwait(false);
            }

            lock (session.Gate)
            {
                if (session.Push is not null)
                {
                    session.Push.Write(pcm16kMono);
                    session.LastRealAudioUtc = now;
                    session.NeedsRestart = false;
                }
            }
        }
        finally
        {
            session.Serialize.Release();
        }
    }

    /// <summary>
    /// Fallback path when Graph never provides mediaStreams/sourceId attribution for a live SSRC.
    /// Transcription continues with an explicit unattributed identity.
    /// </summary>
    public async Task ProcessAudioFallbackAsync(uint ssrc, byte[] pcm16kMono, long timestampHns)
    {
        var fallbackParticipant = new TranscriptionParticipant(
            ParticipantId: $"unmapped:{ssrc}",
            DisplayName: $"Unattributed-{ssrc}",
            IntraId: $"unmapped:{ssrc}");
        await ProcessAudioAsync(ssrc, fallbackParticipant, pcm16kMono, timestampHns).ConfigureAwait(false);
    }

    /// <summary>Late-binding identity reconciliation for an already active SSRC recognizer session.</summary>
    public async Task UpdateIdentityAsync(uint ssrc, TranscriptionParticipant identity)
    {
        if (!_sessions.TryGetValue(ssrc, out var session))
        {
            return;
        }

        await session.Serialize.WaitAsync().ConfigureAwait(false);
        try
        {
            lock (session.Gate)
            {
                session.Participant = identity;
            }
        }
        finally
        {
            session.Serialize.Release();
        }
    }

    private async Task StartRecognizerAsync(StreamSession session, uint ssrc, TranscriptionParticipant participant)
    {
        lock (session.Gate)
        {
            if (session.Started)
            {
                return;
            }
        }

        try
        {
            var speechConfig = SpeechConfig.FromSubscription(_settings.AzureSpeechKey!, _settings.AzureSpeechRegion!);
            speechConfig.SpeechRecognitionLanguage = "en-US";
            // Long meeting gaps: avoid service ending the session after ~minutes of no speech on the push stream.
            speechConfig.SetProperty(PropertyId.SpeechServiceConnection_EndSilenceTimeoutMs, "3600000");
            speechConfig.SetProperty(PropertyId.SpeechServiceConnection_InitialSilenceTimeoutMs, "3600000");
            speechConfig.SetProperty(PropertyId.Speech_SegmentationSilenceTimeoutMs, "1000");

            var push = AudioInputStream.CreatePushStream(Pcm16kMono);
            var audioConfig = AudioConfig.FromStreamInput(push);
            var recognizer = new SpeechRecognizer(speechConfig, audioConfig);

            recognizer.Recognizing += (_, e) =>
            {
                var text = e.Result.Text;
                if (string.IsNullOrWhiteSpace(text))
                {
                    return;
                }

                TranscriptionParticipant currentIdentity;
                lock (session.Gate)
                {
                    currentIdentity = session.Participant ?? participant;
                }

                _ = _broadcaster.BroadcastStructuredTranscriptAsync(
                    currentIdentity.IntraId,
                    currentIdentity.ParticipantId,
                    currentIdentity.DisplayName,
                    ssrc,
                    text,
                    confidence: null,
                    utteranceUtc: DateTime.UtcNow,
                    isFinal: false);
            };

            recognizer.Recognized += (_, e) =>
            {
                if (e.Result.Reason != ResultReason.RecognizedSpeech)
                {
                    return;
                }

                var text = e.Result.Text;
                if (string.IsNullOrWhiteSpace(text))
                {
                    return;
                }

                TranscriptionParticipant currentIdentity;
                lock (session.Gate)
                {
                    currentIdentity = session.Participant ?? participant;
                }

                _logger.LogInformation("TRANSCRIPT [{DisplayName}]: {Text}", currentIdentity.DisplayName, text);
                var conf = TryParseConfidence(e.Result);
                _ = EmitTranscriptAsync(ssrc, currentIdentity, text, conf, isFinal: true);
            };

            recognizer.Canceled += (_, e) =>
            {
                lock (session.Gate)
                {
                    session.NeedsRestart = true;
                }

                if (e.Reason == CancellationReason.Error)
                {
                    _logger.LogWarning(
                        "SPEECH[CANCEL] Azure Speech error on stream {SourceId}: {Details}",
                        ssrc,
                        e.ErrorDetails);
                }
                else
                {
                    _logger.LogInformation(
                        "SPEECH[CANCEL] Azure Speech session ended on stream {SourceId}. Reason={Reason}. Will restart on next audio.",
                        ssrc,
                        e.Reason);
                }
            };

            recognizer.SessionStopped += (_, _) =>
            {
                lock (session.Gate)
                {
                    session.NeedsRestart = true;
                }

                _logger.LogInformation(
                    "SPEECH[SESSION] Azure Speech session stopped on stream {SourceId}. Will restart on next audio.",
                    ssrc);
            };

            await recognizer.StartContinuousRecognitionAsync().ConfigureAwait(false);
            _logger.LogInformation("SPEECH[START] Recognizer started for SSRC/sourceId {Ssrc}.", ssrc);

            lock (session.Gate)
            {
                session.Push = push;
                session.Recognizer = recognizer;
                session.Started = true;
                session.NeedsRestart = false;
                session.Participant = participant;
                session.LastRealAudioUtc = DateTime.UtcNow;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Azure Speech recognizer failed for stream {SourceId}.", ssrc);
        }
    }

    private void KeepAliveActiveSessions(object? _)
    {
        if (_disposed)
        {
            return;
        }

        foreach (var kv in _sessions)
        {
            var ssrc = kv.Key;
            var session = kv.Value;
            if (!session.Serialize.Wait(0))
            {
                continue;
            }

            try
            {
                var now = DateTime.UtcNow;
                lock (session.Gate)
                {
                    if (!session.Started || session.Push is null || session.NeedsRestart)
                    {
                        continue;
                    }

                    if (session.LastRealAudioUtc == DateTime.MinValue)
                    {
                        continue;
                    }

                    var idle = now - session.LastRealAudioUtc;
                    if (idle < KeepAliveInterval)
                    {
                        continue;
                    }

                    session.Push.Write(SilencePcm20Ms);
                }
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "SPEECH[KEEPALIVE] Failed for SSRC/sourceId {Ssrc}.", ssrc);
            }
            finally
            {
                session.Serialize.Release();
            }
        }
    }

    private static async Task StopRecognizerCoreAsync(StreamSession session)
    {
        SpeechRecognizer? rec;
        lock (session.Gate)
        {
            rec = session.Recognizer;
            session.Recognizer = null;
            session.Push?.Close();
            session.Push = null;
            session.Started = false;
        }

        if (rec is not null)
        {
            try
            {
                await rec.StopContinuousRecognitionAsync().ConfigureAwait(false);
            }
            catch
            {
                // ignore
            }

            rec.Dispose();
        }
    }

    private async Task EmitTranscriptAsync(uint ssrc, TranscriptionParticipant participant, string text, double? confidence, bool isFinal)
    {
        try
        {
            var utc = DateTime.UtcNow;
            await _broadcaster.BroadcastStructuredTranscriptAsync(
                participant.IntraId,
                participant.ParticipantId,
                participant.DisplayName,
                ssrc,
                text,
                confidence,
                utc,
                isFinal).ConfigureAwait(false);

            if (isFinal)
            {
                var dedupeKey = $"{ssrc}|{utc.Ticks}|{text}";
                await _chunkManager.RecordFinalAsync(
                    utc,
                    participant.ParticipantId,
                    participant.DisplayName,
                    text,
                    dedupeKey,
                    ssrc).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Emit transcript failed for stream {SourceId}.", ssrc);
        }
    }

    public async ValueTask DisposeAsync()
    {
        _logger.LogInformation("SPEECH[DISPOSE] Disposing Azure speech service sessions: {SessionCount}.", _sessions.Count);
        _disposed = true;
        await _keepAliveTimer.DisposeAsync().ConfigureAwait(false);
        foreach (var kv in _sessions.ToArray())
        {
            await DisposeSessionAsync(kv.Value).ConfigureAwait(false);
        }

        _sessions.Clear();
    }

    private static async Task DisposeSessionAsync(StreamSession session)
    {
        await StopRecognizerCoreAsync(session).ConfigureAwait(false);
        session.Serialize.Dispose();
    }

    private static double? TryParseConfidence(SpeechRecognitionResult result)
    {
        try
        {
            var json = result.Properties.GetProperty(PropertyId.SpeechServiceResponse_JsonResult);
            if (string.IsNullOrWhiteSpace(json))
            {
                return null;
            }

            using var doc = JsonDocument.Parse(json);
            if (!doc.RootElement.TryGetProperty("NBest", out var nBest) || nBest.GetArrayLength() == 0)
            {
                return null;
            }

            var first = nBest[0];
            return first.TryGetProperty("Confidence", out var c) ? c.GetDouble() : null;
        }
        catch
        {
            return null;
        }
    }
}
