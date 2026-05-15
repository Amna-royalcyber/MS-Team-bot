namespace TeamsMediaBot;

public sealed class MeetingContextStore
{
    private readonly object _lock = new();
    private string _meetingId = "unknown";
    private string _bridgeLeadId = string.Empty;
    private string _meetingTitle = string.Empty;
    private string _snowTicketId = string.Empty;
    private DateTime? _callEstablishedUtc;

    public string CurrentMeetingId
    {
        get
        {
            lock (_lock)
            {
                return _meetingId;
            }
        }
    }

    /// <summary>Meeting organizer Entra object id used as bridge_lead_id in outbound payloads.</summary>
    public string CurrentBridgeLeadId
    {
        get
        {
            lock (_lock)
            {
                return _bridgeLeadId;
            }
        }
    }

    /// <summary>Human-readable meeting title for SignalR UI (optional).</summary>
    public string CurrentMeetingTitle
    {
        get
        {
            lock (_lock)
            {
                return _meetingTitle;
            }
        }
    }

    /// <summary>ServiceNow ticket id parsed from meeting title (prefix before first underscore).</summary>
    public string CurrentSnowTicketId
    {
        get
        {
            lock (_lock)
            {
                return _snowTicketId;
            }
        }
    }

    /// <summary>Wall-clock time when Graph reports the call established (used for transcript windows).</summary>
    public DateTime? CallEstablishedUtc
    {
        get
        {
            lock (_lock)
            {
                return _callEstablishedUtc;
            }
        }
    }

    public void SetMeetingId(string? meetingId)
    {
        if (string.IsNullOrWhiteSpace(meetingId))
        {
            return;
        }

        lock (_lock)
        {
            _meetingId = meetingId.Trim();
        }
    }

    public void SetBridgeLeadId(string? bridgeLeadId)
    {
        if (string.IsNullOrWhiteSpace(bridgeLeadId))
        {
            return;
        }

        lock (_lock)
        {
            _bridgeLeadId = bridgeLeadId.Trim();
        }
    }

    public void SetMeetingTitle(string? title)
    {
        if (string.IsNullOrWhiteSpace(title))
        {
            return;
        }

        lock (_lock)
        {
            var trimmed = title.Trim();
            _meetingTitle = trimmed;
            _snowTicketId = MeetingJoinParser.ExtractSnowTicketIdFromTitle(trimmed) ?? string.Empty;
        }
    }

    public void SetCallEstablishedUtc(DateTime utc)
    {
        var normalized = utc.Kind == DateTimeKind.Utc ? utc : utc.ToUniversalTime();
        lock (_lock)
        {
            _callEstablishedUtc = normalized;
        }
    }

    public void ResetMeetingContext()
    {
        lock (_lock)
        {
            _meetingId = "unknown";
            _bridgeLeadId = string.Empty;
            _meetingTitle = string.Empty;
            _snowTicketId = string.Empty;
            _callEstablishedUtc = null;
        }
    }
}
