using Azure.Identity;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

/// <summary>
/// Resolves Teams meeting title (<see cref="OnlineMeeting.Subject"/>) using Graph when
/// <c>OnlineMeetings.Read.All</c> is granted. Matches organizer's online meetings to the join URL / thread id.
/// </summary>
public sealed class OnlineMeetingTitleService
{
    private readonly BotSettings _settings;
    private readonly ILogger<OnlineMeetingTitleService> _logger;

    public OnlineMeetingTitleService(BotSettings settings, ILogger<OnlineMeetingTitleService> logger)
    {
        _settings = settings;
        _logger = logger;
    }

    public async Task<string?> TryGetSubjectForJoinUrlAsync(
        string organizerObjectId,
        string joinUrl,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(organizerObjectId) || string.IsNullOrWhiteSpace(joinUrl))
        {
            return null;
        }

        var threadId = MeetingJoinParser.ParseJoinUrl(joinUrl).JoinMeetingId;
        if (string.IsNullOrWhiteSpace(threadId))
        {
            _logger.LogInformation("MEETING[UI] Title lookup skipped: join URL has no meetup-join thread segment.");
            return null;
        }

        try
        {
            var credential = new ClientSecretCredential(
                _settings.TenantId.Trim(),
                _settings.ClientId.Trim(),
                _settings.ClientSecret.Trim());
            var graph = new GraphServiceClient(credential, new[] { "https://graph.microsoft.com/.default" });

            var response = await graph.Users[organizerObjectId.Trim()].OnlineMeetings.GetAsync(
                requestConfiguration =>
                {
                    requestConfiguration.QueryParameters.Top = 100;
                    requestConfiguration.QueryParameters.Orderby = new[] { "startDateTime desc" };
                },
                cancellationToken).ConfigureAwait(false);

            if (response?.Value is null)
            {
                _logger.LogInformation(
                    "MEETING[UI] Graph onlineMeetings returned no page for organizer {OrganizerObjectId}.",
                    organizerObjectId);
                return null;
            }

            var scanned = 0;
            foreach (var om in response.Value)
            {
                if (om is null || string.IsNullOrWhiteSpace(om.JoinWebUrl))
                {
                    continue;
                }

                scanned++;
                if (!JoinUrlsReferToSameMeeting(om.JoinWebUrl, joinUrl, threadId))
                {
                    continue;
                }

                var subject = string.IsNullOrWhiteSpace(om.Subject) ? null : om.Subject.Trim();
                _logger.LogInformation(
                    "MEETING[UI] Graph matched online meeting by join URL. OrganizerOid={OrganizerObjectId}, Subject={Subject}, ScannedJoinWebUrls={Scanned}",
                    organizerObjectId,
                    subject ?? "(empty)",
                    scanned);
                return subject;
            }

            _logger.LogInformation(
                "MEETING[UI] No Graph online meeting matched join URL after scanning {Scanned} joinWebUrl entries. OrganizerOid={OrganizerObjectId}",
                scanned,
                organizerObjectId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "MEETING[UI] Graph online meeting title lookup failed for organizer {OrganizerObjectId}. Check OnlineMeetings.Read.All and admin consent.",
                organizerObjectId);
        }

        return null;
    }

    /// <summary>Match by meeting chat thread id when join URL is not available (coordinate join).</summary>
    public async Task<string?> TryGetSubjectForThreadAsync(
        string organizerObjectId,
        string chatThreadId,
        CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(organizerObjectId) || string.IsNullOrWhiteSpace(chatThreadId))
        {
            return null;
        }

        var thread = chatThreadId.Trim();
        try
        {
            var credential = new ClientSecretCredential(
                _settings.TenantId.Trim(),
                _settings.ClientId.Trim(),
                _settings.ClientSecret.Trim());
            var graph = new GraphServiceClient(credential, new[] { "https://graph.microsoft.com/.default" });

            var response = await graph.Users[organizerObjectId.Trim()].OnlineMeetings.GetAsync(
                requestConfiguration =>
                {
                    requestConfiguration.QueryParameters.Top = 100;
                    requestConfiguration.QueryParameters.Orderby = new[] { "startDateTime desc" };
                },
                cancellationToken).ConfigureAwait(false);

            if (response?.Value is null)
            {
                _logger.LogInformation(
                    "MEETING[UI] Graph onlineMeetings returned no page for organizer {OrganizerObjectId} (thread match).",
                    organizerObjectId);
                return null;
            }

            var scanned = 0;
            foreach (var om in response.Value)
            {
                if (om is null || string.IsNullOrWhiteSpace(om.JoinWebUrl))
                {
                    continue;
                }

                scanned++;
                if (!JoinUrlContainsThread(om.JoinWebUrl, thread))
                {
                    continue;
                }

                var subject = string.IsNullOrWhiteSpace(om.Subject) ? null : om.Subject.Trim();
                _logger.LogInformation(
                    "MEETING[UI] Graph matched online meeting by thread id. OrganizerOid={OrganizerObjectId}, Subject={Subject}, ScannedJoinWebUrls={Scanned}",
                    organizerObjectId,
                    subject ?? "(empty)",
                    scanned);
                return subject;
            }

            _logger.LogInformation(
                "MEETING[UI] No Graph online meeting matched thread id after scanning {Scanned} joinWebUrl entries. OrganizerOid={OrganizerObjectId}",
                scanned,
                organizerObjectId);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "MEETING[UI] Graph online meeting title lookup (thread) failed for organizer {OrganizerObjectId}. Check OnlineMeetings.Read.All and admin consent.",
                organizerObjectId);
        }

        return null;
    }

    private static bool JoinUrlContainsThread(string joinWebUrl, string threadId)
    {
        if (string.IsNullOrWhiteSpace(joinWebUrl) || string.IsNullOrWhiteSpace(threadId))
        {
            return false;
        }

        var variants = new[]
        {
            threadId,
            Uri.EscapeDataString(threadId),
            threadId.Replace(":", "%3A", StringComparison.OrdinalIgnoreCase),
            threadId.Replace("@", "%40", StringComparison.OrdinalIgnoreCase)
        };

        foreach (var v in variants)
        {
            if (!string.IsNullOrEmpty(v) && joinWebUrl.Contains(v, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static bool JoinUrlsReferToSameMeeting(string graphJoinWebUrl, string userJoinUrl, string decodedThreadId)
    {
        var graphNorm = NormalizeMeetingJoinUrl(graphJoinWebUrl);
        var userNorm = NormalizeMeetingJoinUrl(userJoinUrl);
        if (string.Equals(graphNorm, userNorm, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        var variants = new[]
        {
            decodedThreadId,
            Uri.EscapeDataString(decodedThreadId),
            decodedThreadId.Replace(":", "%3A", StringComparison.OrdinalIgnoreCase),
            decodedThreadId.Replace("@", "%40", StringComparison.OrdinalIgnoreCase)
        };

        foreach (var v in variants)
        {
            if (string.IsNullOrEmpty(v))
            {
                continue;
            }

            if (graphJoinWebUrl.Contains(v, StringComparison.OrdinalIgnoreCase) &&
                userJoinUrl.Contains(v, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string NormalizeMeetingJoinUrl(string url)
    {
        var trimmed = url.Trim();
        if (!Uri.TryCreate(trimmed, UriKind.Absolute, out var uri))
        {
            return trimmed.ToLowerInvariant();
        }

        return uri.GetLeftPart(UriPartial.Path).TrimEnd('/').ToLowerInvariant();
    }
}
