namespace TeamsMediaBot;

public sealed class MeetingContextStore
{
    private readonly object _lock = new();
    private string _meetingId = "unknown";
    private string _bridgeLeadId = string.Empty;
    private string _meetingTitle = string.Empty;
    private string _snowTicketId = string.Empty;
    private DateTime? _callEstablishedUtc;
    /// <summary>When the meeting became empty (0 humans): enables Dynamo poll 2+ min after this even if the call ended and context was reset.</summary>
    private DateTime? _dynamoPostEmptyAnchorUtc;
    private string _dynamoPostEmptyMeetingId = string.Empty;
    private string _dynamoPostEmptyBridgeLeadId = string.Empty;

    public void BeginDynamoPostEmptyPollRetention(string? meetingId, string? bridgeLeadId)
    {
        if (string.IsNullOrWhiteSpace(meetingId) ||
            string.Equals(meetingId.Trim(), "unknown", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        lock (_lock)
        {
            _dynamoPostEmptyMeetingId = meetingId.Trim();
            _dynamoPostEmptyBridgeLeadId = string.IsNullOrWhiteSpace(bridgeLeadId) ? string.Empty : bridgeLeadId.Trim();
            _dynamoPostEmptyAnchorUtc = DateTime.UtcNow;
        }
    }

    /// <summary>Clears post-empty Dynamo retention (e.g. new join).</summary>
    public void ClearDynamoPostEmptyPollRetention()
    {
        lock (_lock)
        {
            _dynamoPostEmptyAnchorUtc = null;
            _dynamoPostEmptyMeetingId = string.Empty;
            _dynamoPostEmptyBridgeLeadId = string.Empty;
        }
    }

    public bool TryGetDynamoPostEmptyPoll(out string meetingId, out DateTime anchorUtc)
    {
        lock (_lock)
        {
            if (_dynamoPostEmptyAnchorUtc is null || string.IsNullOrWhiteSpace(_dynamoPostEmptyMeetingId))
            {
                meetingId = string.Empty;
                anchorUtc = default;
                return false;
            }

            meetingId = _dynamoPostEmptyMeetingId;
            anchorUtc = _dynamoPostEmptyAnchorUtc.Value;
            return true;
        }
    }

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

    /// <summary>Clears live meeting fields when the Graph call ends. Post-empty Dynamo retention is kept until the next join.</summary>
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

    /// <summary>
    /// Full wipe before a new join: live meeting state <em>and</em> post-empty Dynamo polling retention,
    /// so the next meeting does not inherit titles, ids, or stale Dynamo anchors.
    /// </summary>
    public void PrepareForNewMeetingJoin()
    {
        lock (_lock)
        {
            _meetingId = "unknown";
            _bridgeLeadId = string.Empty;
            _meetingTitle = string.Empty;
            _snowTicketId = string.Empty;
            _callEstablishedUtc = null;
            _dynamoPostEmptyAnchorUtc = null;
            _dynamoPostEmptyMeetingId = string.Empty;
            _dynamoPostEmptyBridgeLeadId = string.Empty;
        }
    }
}
