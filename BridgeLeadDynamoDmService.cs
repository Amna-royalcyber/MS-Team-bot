using System.Collections.Concurrent;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Text;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
using Microsoft.Graph.Models.ODataErrors;
using Microsoft.Graph.Models;

namespace TeamsMediaBot;

/// <summary>
/// Polls DynamoDB meeting records and sends generated responses to bridge leads in Teams personal chat.
/// </summary>
public sealed class BridgeLeadDynamoDmService : BackgroundService
{
    private readonly BotSettings _settings;
    private readonly ILogger<BridgeLeadDynamoDmService> _logger;
    private readonly IAmazonDynamoDB? _dynamo;
    private GraphServiceClient _graph;
    private readonly ClientSecretCredential _credential;
    private readonly HttpClient _http = new();
    private readonly ConcurrentDictionary<string, byte> _sentKeys = new(StringComparer.OrdinalIgnoreCase);
    private static readonly string[] GraphScopes = { "https://graph.microsoft.com/.default" };

    public BridgeLeadDynamoDmService(BotSettings settings, ILogger<BridgeLeadDynamoDmService> logger)
    {
        _settings = settings;
        _logger = logger;

        if (!string.IsNullOrWhiteSpace(_settings.DynamoRegion))
        {
            var region = RegionEndpoint.GetBySystemName(_settings.DynamoRegion.Trim());
            _dynamo = new AmazonDynamoDBClient(region);
        }

        _credential = new ClientSecretCredential(_settings.TenantId, _settings.ClientId, _settings.ClientSecret);
        _graph = CreateGraphClient();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (_dynamo is null || string.IsNullOrWhiteSpace(_settings.DynamoMeetingRecordsTableName))
        {
            _logger.LogInformation("BridgeLeadDynamoDmService disabled: Dynamo config missing (table or region).");
            return;
        }

        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(_settings.DynamoPollIntervalSeconds));
        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                await PollAndNotifyAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "BridgeLeadDynamoDmService polling cycle failed.");
            }
        }
    }

    private async Task PollAndNotifyAsync(CancellationToken cancellationToken)
    {
        var tableName = _settings.DynamoMeetingRecordsTableName!.Trim();
        var response = await _dynamo!.ScanAsync(new ScanRequest
        {
            TableName = tableName
        }, cancellationToken).ConfigureAwait(false);

        foreach (var item in response.Items)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var meetingId = ReadString(item, "meeting_id");
            var bridgeLeadId = ReadString(item, "bridge_lead_id");
            var generatedResponse = ReadString(item, "generated_responce") ?? ReadString(item, "response_generated");
            if (string.IsNullOrWhiteSpace(meetingId) ||
                string.IsNullOrWhiteSpace(bridgeLeadId) ||
                string.IsNullOrWhiteSpace(generatedResponse))
            {
                continue;
            }

            var dedupeKey = $"{meetingId}|{ComputeHash(generatedResponse)}";
            if (!_sentKeys.TryAdd(dedupeKey, 0))
            {
                continue;
            }

            try
            {
                // Preferred bot path: Teams activity notification (works without separate sender user id).
                if (string.IsNullOrWhiteSpace(_settings.BotDmSenderUserObjectId))
                {
                    var sent = await SendFallbackActivityNotificationAsync(bridgeLeadId.Trim(), generatedResponse.Trim(), cancellationToken)
                        .ConfigureAwait(false);
                    if (!sent)
                    {
                        throw new InvalidOperationException(
                            "Bot-originated activity notification failed. Grant TeamsActivity.Send/TeamsActivity.Send.User application permission and admin consent.");
                    }

                    _logger.LogInformation(
                        "Bridge-lead bot notification sent from Dynamo record: meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                        meetingId,
                        bridgeLeadId);
                    continue;
                }

                var dmSent = await SendMessageToLeadAsync(bridgeLeadId.Trim(), generatedResponse.Trim(), cancellationToken).ConfigureAwait(false);
                if (!dmSent)
                {
                    var fallbackSent = await SendFallbackActivityNotificationAsync(bridgeLeadId.Trim(), generatedResponse.Trim(), cancellationToken)
                        .ConfigureAwait(false);
                    if (fallbackSent)
                    {
                        _logger.LogInformation(
                            "Bridge-lead fallback activity notification sent after DM failure: meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                            meetingId,
                            bridgeLeadId);
                        continue;
                    }

                    _sentKeys.TryRemove(dedupeKey, out _);
                    _logger.LogError(
                        "Bridge-lead DM failed and fallback activity notification failed: meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                        meetingId,
                        bridgeLeadId);
                    continue;
                }
                _logger.LogInformation(
                    "Bridge-lead chat DM sent from Dynamo record: meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                    meetingId,
                    bridgeLeadId);
            }
            catch (Exception ex)
            {
                if (IsNonRetryableAppOnlyChatPostError(ex))
                {
                    var fallbackSent = await SendFallbackActivityNotificationAsync(bridgeLeadId.Trim(), generatedResponse.Trim(), cancellationToken)
                        .ConfigureAwait(false);
                    if (fallbackSent)
                    {
                        _logger.LogInformation(
                            "Bridge-lead fallback activity notification sent for meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                            meetingId,
                            bridgeLeadId);
                        continue;
                    }

                    _logger.LogError(
                        ex,
                        "Bridge-lead DM cannot be sent via Graph app-only chat message POST for meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}. " +
                        "Fallback activity notification also failed. This is non-retryable with current permissions/model. " +
                        "Use delegated auth/proactive bot message or grant Teams activity notification permissions.",
                        meetingId,
                        bridgeLeadId);
                    continue;
                }

                _sentKeys.TryRemove(dedupeKey, out _);
                _logger.LogError(
                    ex,
                    "Failed sending bridge-lead DM for meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                    meetingId,
                    bridgeLeadId);
            }
        }
    }

    private async Task<bool> SendMessageToLeadAsync(string bridgeLeadEntraId, string message, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(bridgeLeadEntraId))
        {
            return false;
        }

        try
        {
            await EnsureGraphClientAuthenticatedAsync(cancellationToken).ConfigureAwait(false);

            if (!string.IsNullOrWhiteSpace(_settings.TeamsAppId))
            {
                var userAppInstallation = new UserScopeTeamsAppInstallation
                {
                    AdditionalData = new Dictionary<string, object>
                    {
                        ["teamsApp@odata.bind"] = $"https://graph.microsoft.com/v1.0/appCatalogs/teamsApps/{_settings.TeamsAppId.Trim()}"
                    }
                };

                try
                {
                    await ExecuteGraphWithReauthAsync(
                        client => client.Users[bridgeLeadEntraId].Teamwork.InstalledApps.PostAsync(
                            userAppInstallation,
                            cancellationToken: cancellationToken)).ConfigureAwait(false);
                    _logger.LogInformation("Proactively installed app for user {Id}", bridgeLeadEntraId);
                }
                catch (ODataError ex) when (string.Equals(ex.Error?.Code, "Conflict", StringComparison.OrdinalIgnoreCase))
                {
                    // App already installed for this user.
                }
                catch (ODataError ex)
                {
                    _logger.LogWarning(
                        "App install attempt failed for user {Id}. TeamsAppId={TeamsAppId}, GraphCode={Code}, GraphMessage={Message}",
                        bridgeLeadEntraId,
                        _settings.TeamsAppId,
                        ex.Error?.Code,
                        ex.Error?.Message);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        ex,
                        "Unexpected exception during app install attempt for user {Id}. TeamsAppId={TeamsAppId}",
                        bridgeLeadEntraId,
                        _settings.TeamsAppId);
                }
            }
            else
            {
                _logger.LogWarning(
                    "TeamsAppId is not configured. Skipping proactive app installation for user {Id}; fallback activity notifications may fail with 403.",
                    bridgeLeadEntraId);
            }

            // App-only proactive pattern: use activity notification as primary delivery mechanism.
            var sent = await SendFallbackActivityNotificationAsync(bridgeLeadEntraId, message, cancellationToken).ConfigureAwait(false);
            if (sent)
            {
                _logger.LogInformation("Successfully sent proactive bot notification to Bridge Lead {Id}", bridgeLeadEntraId);
                return true;
            }

            _logger.LogWarning("Proactive bot notification failed for Bridge Lead {Id}.", bridgeLeadEntraId);
            return false;
        }
        catch (ODataError odataError)
        {
            _logger.LogError(
                "Graph API Error: {Code} - {Msg}",
                odataError.Error?.Code,
                odataError.Error?.Message);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to send DM to Lead {Id}", bridgeLeadEntraId);
            return false;
        }
    }

    private GraphServiceClient CreateGraphClient()
    {
        return new GraphServiceClient(_credential, GraphScopes);
    }

    private async Task EnsureGraphClientAuthenticatedAsync(CancellationToken cancellationToken)
    {
        var token = await _credential.GetTokenAsync(new TokenRequestContext(GraphScopes), cancellationToken).ConfigureAwait(false);
        if (token.ExpiresOn <= DateTimeOffset.UtcNow.AddMinutes(2))
        {
            _graph = CreateGraphClient();
            _logger.LogInformation("Graph client re-created because cached token is near expiry.");
        }
    }

    private async Task<T> ExecuteGraphWithReauthAsync<T>(Func<GraphServiceClient, Task<T>> operation)
    {
        try
        {
            return await operation(_graph).ConfigureAwait(false);
        }
        catch (ODataError ex) when (IsAuthTokenFailure(ex))
        {
            _graph = CreateGraphClient();
            _logger.LogWarning("Graph auth token failure detected; re-created Graph client and retrying once.");
            return await operation(_graph).ConfigureAwait(false);
        }
    }

    private static bool IsAuthTokenFailure(ODataError ex)
    {
        var code = ex.Error?.Code ?? string.Empty;
        var message = ex.Error?.Message ?? string.Empty;
        return code.Contains("InvalidAuthenticationToken", StringComparison.OrdinalIgnoreCase) ||
               code.Contains("AuthenticationFailed", StringComparison.OrdinalIgnoreCase) ||
               code.Contains("Unauthorized", StringComparison.OrdinalIgnoreCase) ||
               message.Contains("expired", StringComparison.OrdinalIgnoreCase);
    }

    private static string? ReadString(IDictionary<string, AttributeValue> item, string key)
    {
        if (!item.TryGetValue(key, out var value) || value is null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(value.S))
        {
            return value.S;
        }

        return !string.IsNullOrWhiteSpace(value.N) ? value.N : null;
    }

    private static string ComputeHash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input));
        return Convert.ToHexString(bytes);
    }

    private static bool IsNonRetryableAppOnlyChatPostError(Exception ex)
    {
        var text = ex.ToString();
        if (ex is ODataError odata && !string.IsNullOrWhiteSpace(odata.Error?.Message))
        {
            text = odata.Error.Message;
        }

        return text.Contains("application-only context only for import purposes", StringComparison.OrdinalIgnoreCase) ||
               text.Contains("requires one of 'Teamwork.Migrate.All'", StringComparison.OrdinalIgnoreCase) ||
               text.Contains("Missing role permissions on the request", StringComparison.OrdinalIgnoreCase) ||
               text.Contains("Message POST is allowed in application-only context only for import purposes", StringComparison.OrdinalIgnoreCase) ||
               text.Contains("Cannot create one-on-one chat with duplicate members", StringComparison.OrdinalIgnoreCase) ||
               text.Contains("Duplicate chat members is specified", StringComparison.OrdinalIgnoreCase);
    }

    private async Task<bool> SendFallbackActivityNotificationAsync(string bridgeLeadEntraId, string message, CancellationToken cancellationToken)
    {
        try
        {
            var token = await _credential.GetTokenAsync(new TokenRequestContext(GraphScopes), cancellationToken).ConfigureAwait(false);
            using var request = new HttpRequestMessage(
                HttpMethod.Post,
                $"https://graph.microsoft.com/v1.0/users/{bridgeLeadEntraId}/teamwork/sendActivityNotification");
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token.Token);

            // Requires TeamsActivity.Send (or TeamsActivity.Send.User) and a valid activityType in the Teams app manifest.
            var payload = new
            {
                topic = new
                {
                    source = "text",
                    value = "Bridge Lead Update",
                    webUrl = "https://teams.microsoft.com/l/chat/0/0"
                },
                activityType = "taskCreated",
                previewText = new
                {
                    content = message
                },
                templateParameters = new[]
                {
                    new { name = "content", value = message }
                }
            };

            request.Content = JsonContent.Create(payload);
            using var response = await _http.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (response.IsSuccessStatusCode)
            {
                return true;
            }

            var body = await response.Content.ReadAsStringAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogWarning(
                "Fallback activity notification failed for bridgeLeadId={BridgeLeadId}. Status={Status}, Body={Body}",
                bridgeLeadEntraId,
                (int)response.StatusCode,
                body);
            return false;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Fallback activity notification exception for bridgeLeadId={BridgeLeadId}.", bridgeLeadEntraId);
            return false;
        }
    }
}
