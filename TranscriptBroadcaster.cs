using Microsoft.AspNetCore.SignalR;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public sealed class TranscriptBroadcaster
{
    private readonly IHubContext<TranscriptHub> _hubContext;
    private readonly ILogger<TranscriptBroadcaster> _logger;

    public TranscriptBroadcaster(
        IHubContext<TranscriptHub> hubContext,
        ILogger<TranscriptBroadcaster> logger)
    {
        _hubContext = hubContext;
        _logger = logger;
    }

    /// <summary>Updates transcript page header with meeting title and/or correlation meeting id (SignalR event <c>meeting-title</c>).</summary>
    public async Task<bool> BroadcastMeetingHeaderAsync(string? title, string? meetingId)
    {
        var mid = string.IsNullOrWhiteSpace(meetingId)
            ? null
            : meetingId.Trim();
        if (string.Equals(mid, "unknown", StringComparison.OrdinalIgnoreCase))
        {
            mid = null;
        }

        var tit = string.IsNullOrWhiteSpace(title) ? null : title.Trim();
        if (tit is null && mid is null)
        {
            _logger.LogDebug("MEETING[UI] Skip SignalR meeting-title: no title and no usable meeting id.");
            return false;
        }

        try
        {
            await _hubContext.Clients.All.SendAsync(
                "meeting-title",
                new
                {
                    title = tit,
                    meetingId = mid
                });
            _logger.LogInformation(
                "MEETING[UI] SignalR meeting-title sent. HasTitle={HasTitle}, MeetingId={MeetingId}, TitleChars={TitleChars}",
                tit is not null,
                mid ?? "(none)",
                tit?.Length ?? 0);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "MEETING[UI] SignalR meeting-title broadcast failed.");
            return false;
        }
    }

    /// <summary>Forward final transcript as produced by the speech layer (identity already set upstream).</summary>
    public async Task BroadcastStructuredTranscriptAsync(
        string intraId,
        string participantId,
        string displayName,
        uint ssrc,
        string text,
        double? confidence,
        DateTime utteranceUtc,
        bool isFinal)
    {
        try
        {
            _logger.LogDebug(
                "BROADCAST[TRANSCRIPT] Emitting transcript: ssrc={Ssrc}, participant={ParticipantId}, intra={IntraId}, chars={Chars}.",
                ssrc,
                participantId,
                intraId,
                text.Length);
            await _hubContext.Clients.All.SendAsync(
                "transcript",
                new
                {
                    kind = isFinal ? "Final" : "Intermediate",
                    intraId,
                    participantId,
                    displayName,
                    speakerLabel = displayName,
                    sourceId = ssrc,
                    ssrc,
                    text,
                    confidence,
                    timestamp = new DateTimeOffset(utteranceUtc, TimeSpan.Zero),
                    azureAdObjectId = participantId,
                    tempLabel = false
                });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR structured transcript broadcast failed for ssrc={Ssrc}.", ssrc);
        }
    }

    /// <summary>Optional UI hint when roster display name / Entra id updates for a stream. Does not change transcript identity (that is SSRC-bound before speech).</summary>
    public async Task BroadcastTranscriptIdentityUpdateAsync(uint sourceId, string? displayName, string? entraOid)
    {
        try
        {
            _logger.LogDebug(
                "BROADCAST[IDENTITY] Updating transcript identity: sourceId={SourceId}, displayName={DisplayName}, participantId={ParticipantId}.",
                sourceId,
                displayName,
                entraOid);
            await _hubContext.Clients.All.SendAsync("transcript-update", new
            {
                type = "transcript-update",
                sourceId,
                displayName,
                azureAdObjectId = entraOid,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR transcript-update failed for sourceId={SourceId}.", sourceId);
        }
    }

    public async Task BroadcastIdentityResolved(uint sourceId, string displayName, string entraOid)
    {
        try
        {
            await _hubContext.Clients.All.SendAsync("identity-resolved", new
            {
                sourceId,
                displayName,
                entraOid,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR identity-resolved broadcast failed for sourceId={SourceId}.", sourceId);
        }
    }

    /// <summary>
    /// UI hint that recent already-processed transcript rows for this sourceId should be renamed.
    /// </summary>
    public async Task BroadcastTranscriptRetroactiveUpdateAsync(uint sourceId, string displayName, string participantId, int updatedCount)
    {
        try
        {
            await _hubContext.Clients.All.SendAsync("transcript-retroactive-update", new
            {
                type = "transcript-retroactive-update",
                sourceId,
                displayName,
                participantId,
                updatedCount,
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR transcript-retroactive-update failed for sourceId={SourceId}.", sourceId);
        }
    }

    public async Task BroadcastRosterAsync(IReadOnlyList<RosterParticipantDto> participants)
    {
        try
        {
            _logger.LogDebug("BROADCAST[ROSTER] Emitting roster with {Count} participants.", participants.Count);
            await _hubContext.Clients.All.SendAsync("roster", new
            {
                participants = participants.Select(p => new
                {
                    id = p.CallParticipantId,
                    displayName = p.DisplayName,
                    azureAdObjectId = p.AzureAdObjectId,
                    userPrincipalName = p.UserPrincipalName
                }).ToList(),
                timestamp = DateTimeOffset.UtcNow
            });
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SignalR roster broadcast failed.");
        }
    }
}
