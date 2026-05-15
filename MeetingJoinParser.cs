using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace TeamsMediaBot;

/// <summary>
/// Lightweight join URL parsing aligned with transcriber-style APIs (thread id + optional passcode from query).
/// Full Graph join coordinates still come from <see cref="CallHandler"/> when using <c>MeetingJoinUrl</c>.
/// </summary>
public static class MeetingJoinParser
{
    private static readonly Regex MeetupJoinPath = new(
        @"meetup-join/([^/?#]+)/([^/?#]+)",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Extracts meeting thread id from a meetup-join path and optional meeting passcode (<c>p</c> / <c>pwd</c> query).
    /// </summary>
    public static MeetingJoinUrlParts ParseJoinUrl(string? joinUrl)
    {
        if (string.IsNullOrWhiteSpace(joinUrl))
        {
            return new MeetingJoinUrlParts(null, null);
        }

        var trimmed = joinUrl.Trim();
        string? threadId = null;
        var pathMatch = MeetupJoinPath.Match(trimmed);
        if (pathMatch.Success)
        {
            threadId = FullyUnescape(pathMatch.Groups[1].Value);
        }

        string? passcode = null;
        if (Uri.TryCreate(trimmed, UriKind.Absolute, out var uri))
        {
            passcode = GetQueryParameter(uri.Query, "p")
                ?? GetQueryParameter(uri.Query, "pwd")
                ?? GetQueryParameter(uri.Query, "password");
        }

        return new MeetingJoinUrlParts(threadId, passcode);
    }

    /// <summary>
    /// Reads <c>?context=</c> JSON from a meetup-join URL (Tid, Oid) for Graph calls such as online meeting lookup.
    /// </summary>
    public static bool TryParseTeamsJoinContext(string? joinUrl, out string? tenantId, out string? organizerObjectId)
    {
        tenantId = null;
        organizerObjectId = null;
        if (string.IsNullOrWhiteSpace(joinUrl) || !Uri.TryCreate(joinUrl.Trim(), UriKind.Absolute, out var uri))
        {
            return false;
        }

        var raw = GetQueryParameter(uri.Query, "context");
        if (string.IsNullOrEmpty(raw))
        {
            return false;
        }

        var current = raw;
        for (var i = 0; i < 3; i++)
        {
            string decoded;
            try
            {
                decoded = i == 0 ? current : Uri.UnescapeDataString(current);
            }
            catch
            {
                return false;
            }

            try
            {
                using var doc = JsonDocument.Parse(decoded);
                var root = doc.RootElement;
                if (root.TryGetProperty("Tid", out var tid) && tid.ValueKind == JsonValueKind.String)
                {
                    tenantId = tid.GetString();
                }
                else if (root.TryGetProperty("tid", out var tidLower) && tidLower.ValueKind == JsonValueKind.String)
                {
                    tenantId = tidLower.GetString();
                }

                if (root.TryGetProperty("Oid", out var oid) && oid.ValueKind == JsonValueKind.String)
                {
                    organizerObjectId = oid.GetString();
                }
                else if (root.TryGetProperty("oid", out var oidLower) && oidLower.ValueKind == JsonValueKind.String)
                {
                    organizerObjectId = oidLower.GetString();
                }

                return !string.IsNullOrWhiteSpace(organizerObjectId);
            }
            catch (JsonException)
            {
                current = decoded;
            }
        }

        return false;
    }

    private static string? GetQueryParameter(string query, string key)
    {
        if (string.IsNullOrEmpty(query))
        {
            return null;
        }

        query = query.TrimStart('?');
        foreach (var part in query.Split('&'))
        {
            var eq = part.IndexOf('=');
            if (eq <= 0)
            {
                continue;
            }

            var name = part[..eq];
            if (!name.Equals(key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            try
            {
                return Uri.UnescapeDataString(part[(eq + 1)..]);
            }
            catch
            {
                return part[(eq + 1)..];
            }
        }

        return null;
    }

    private static string FullyUnescape(string value)
    {
        var current = value;
        for (var i = 0; i < 3; i++)
        {
            var decoded = Uri.UnescapeDataString(current);
            if (decoded == current)
            {
                break;
            }

            current = decoded;
        }

        return current;
    }

    /// <summary>
    /// ServiceNow ticket id from meeting title prefix before first underscore (e.g. <c>INC139282_Incident call</c> → <c>INC139282</c>).
    /// </summary>
    public static string? ExtractSnowTicketIdFromTitle(string? meetingTitle)
    {
        if (string.IsNullOrWhiteSpace(meetingTitle))
        {
            return null;
        }

        var title = meetingTitle.Trim();
        var underscore = title.IndexOf('_');
        var ticket = underscore >= 0 ? title[..underscore] : title;
        return string.IsNullOrWhiteSpace(ticket) ? null : ticket.Trim();
    }

    /// <summary>
    /// Stable meeting key for storage / UI correlation (decodes <c>19:meeting_…@thread.v2</c> embedded GUID when present).
    /// </summary>
    public static string NormalizeMeetingIdForStorage(string meetingId)
    {
        if (string.IsNullOrWhiteSpace(meetingId))
        {
            return meetingId;
        }

        var value = meetingId.Trim();
        if (Guid.TryParse(value, out var parsedGuid))
        {
            return parsedGuid.ToString();
        }

        const string prefix = "19:meeting_";
        const string suffix = "@thread.v2";
        if (!value.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) ||
            !value.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
        {
            return value;
        }

        var encoded = value[prefix.Length..^suffix.Length];
        if (string.IsNullOrWhiteSpace(encoded))
        {
            return value;
        }

        var base64 = encoded.Replace('-', '+').Replace('_', '/');
        var pad = base64.Length % 4;
        if (pad != 0)
        {
            base64 = base64.PadRight(base64.Length + (4 - pad), '=');
        }

        try
        {
            var bytes = Convert.FromBase64String(base64);
            var decoded = Encoding.UTF8.GetString(bytes).Trim();
            return Guid.TryParse(decoded, out var embeddedGuid) ? embeddedGuid.ToString() : value;
        }
        catch
        {
            return value;
        }
    }

    /// <summary>
    /// Substrings that may appear in Graph/user <c>joinWebUrl</c> for the same meeting (thread id, GUID, URL-encoded).
    /// </summary>
    public static IReadOnlyList<string> EnumerateMeetingJoinUrlMatchTokens(string? meetingThreadOrCorrelationId)
    {
        var set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        void addCore(string? s)
        {
            if (string.IsNullOrWhiteSpace(s))
            {
                return;
            }

            var t = s.Trim();
            set.Add(t);
            set.Add(Uri.EscapeDataString(t));
            set.Add(t.Replace(":", "%3A", StringComparison.OrdinalIgnoreCase));
            set.Add(t.Replace("@", "%40", StringComparison.OrdinalIgnoreCase));
        }

        addCore(meetingThreadOrCorrelationId);
        var v = meetingThreadOrCorrelationId?.Trim();
        if (string.IsNullOrEmpty(v))
        {
            return set.Where(x => !string.IsNullOrEmpty(x)).ToList();
        }

        if (Guid.TryParse(v, out var g))
        {
            addCore(g.ToString("D"));
            addCore(g.ToString("N"));
            addCore(g.ToString("B"));
        }

        const string prefix = "19:meeting_";
        const string suffix = "@thread.v2";
        if (v.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) &&
            v.EndsWith(suffix, StringComparison.OrdinalIgnoreCase))
        {
            var encoded = v[prefix.Length..^suffix.Length];
            if (!string.IsNullOrWhiteSpace(encoded))
            {
                var base64 = encoded.Replace('-', '+').Replace('_', '/');
                var pad = base64.Length % 4;
                if (pad != 0)
                {
                    base64 = base64.PadRight(base64.Length + (4 - pad), '=');
                }

                try
                {
                    var bytes = Convert.FromBase64String(base64);
                    var decoded = Encoding.UTF8.GetString(bytes).Trim();
                    if (Guid.TryParse(decoded, out var embedded))
                    {
                        addCore(embedded.ToString("D"));
                        addCore(embedded.ToString("N"));
                        addCore(embedded.ToString("B"));
                    }
                }
                catch
                {
                    // ignore
                }
            }
        }

        var stable = NormalizeMeetingIdForStorage(v);
        if (!string.Equals(stable, v, StringComparison.Ordinal))
        {
            addCore(stable);
        }

        return set.Where(x => !string.IsNullOrEmpty(x)).ToList();
    }
}

public readonly record struct MeetingJoinUrlParts(string? JoinMeetingId, string? Passcode);
