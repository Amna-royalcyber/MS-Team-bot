using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Azure.Identity;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Graph;
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
    private readonly GraphServiceClient _graph;
    private readonly ConcurrentDictionary<string, byte> _sentKeys = new(StringComparer.OrdinalIgnoreCase);

    public BridgeLeadDynamoDmService(BotSettings settings, ILogger<BridgeLeadDynamoDmService> logger)
    {
        _settings = settings;
        _logger = logger;

        if (!string.IsNullOrWhiteSpace(_settings.DynamoRegion))
        {
            var region = RegionEndpoint.GetBySystemName(_settings.DynamoRegion.Trim());
            _dynamo = new AmazonDynamoDBClient(region);
        }

        var credential = new ClientSecretCredential(_settings.TenantId, _settings.ClientId, _settings.ClientSecret);
        _graph = new GraphServiceClient(credential, new[] { "https://graph.microsoft.com/.default" });
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
                await SendDirectMessageAsync(bridgeLeadId.Trim(), generatedResponse.Trim(), cancellationToken).ConfigureAwait(false);
                _logger.LogInformation(
                    "Bridge-lead DM sent from Dynamo record: meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                    meetingId,
                    bridgeLeadId);
            }
            catch (Exception ex)
            {
                _sentKeys.TryRemove(dedupeKey, out _);
                _logger.LogError(
                    ex,
                    "Failed sending bridge-lead DM for meetingId={MeetingId}, bridgeLeadId={BridgeLeadId}.",
                    meetingId,
                    bridgeLeadId);
            }
        }
    }

    private async Task SendDirectMessageAsync(string bridgeLeadEntraId, string message, CancellationToken cancellationToken)
    {
        // If sender user id is not configured, fall back to bridge_lead_id per table-driven requirement.
        // In tenants where app-only chat posting requires an explicit sender user, set BotDmSenderUserObjectId.
        var senderId = string.IsNullOrWhiteSpace(_settings.BotDmSenderUserObjectId)
            ? bridgeLeadEntraId
            : _settings.BotDmSenderUserObjectId.Trim();

        if (string.Equals(senderId, bridgeLeadEntraId, StringComparison.OrdinalIgnoreCase))
        {
            var existingChatId = await FindExistingOneOnOneChatIdAsync(bridgeLeadEntraId, cancellationToken).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(existingChatId))
            {
                throw new InvalidOperationException(
                    "Cannot create one-on-one chat with duplicate members. Set BotDmSenderUserObjectId to a different Entra user id, or ensure an existing personal chat is available.");
            }

            await PostMessageAsync(existingChatId, message, cancellationToken).ConfigureAwait(false);
            return;
        }

        var chat = new Chat
        {
            ChatType = ChatType.OneOnOne,
            Members = new List<ConversationMember>
            {
                BuildMember(senderId),
                BuildMember(bridgeLeadEntraId)
            }
        };

        var createdChat = await _graph.Chats.PostAsync(chat, cancellationToken: cancellationToken).ConfigureAwait(false);
        if (string.IsNullOrWhiteSpace(createdChat?.Id))
        {
            throw new InvalidOperationException("Graph returned empty chat id while creating personal chat.");
        }

        await PostMessageAsync(createdChat.Id, message, cancellationToken).ConfigureAwait(false);
    }

    private async Task PostMessageAsync(string chatId, string message, CancellationToken cancellationToken)
    {
        await _graph.Chats[chatId].Messages.PostAsync(
            new ChatMessage
            {
                Body = new ItemBody
                {
                    ContentType = BodyType.Text,
                    Content = message
                }
            },
            cancellationToken: cancellationToken).ConfigureAwait(false);
    }

    private async Task<string?> FindExistingOneOnOneChatIdAsync(string userObjectId, CancellationToken cancellationToken)
    {
        var chats = await _graph.Users[userObjectId].Chats.GetAsync(
            requestConfiguration =>
            {
                requestConfiguration.QueryParameters.Filter = "chatType eq 'oneOnOne'";
                requestConfiguration.QueryParameters.Top = 1;
            },
            cancellationToken).ConfigureAwait(false);

        return chats?.Value?.FirstOrDefault()?.Id;
    }

    private static AadUserConversationMember BuildMember(string userObjectId)
    {
        return new AadUserConversationMember
        {
            Roles = new List<string> { "owner" },
            AdditionalData = new Dictionary<string, object>
            {
                ["user@odata.bind"] = $"https://graph.microsoft.com/v1.0/users('{userObjectId}')"
            }
        };
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
}
