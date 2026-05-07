using Microsoft.Bot.Builder;
using Microsoft.Bot.Builder.Integration.AspNet.Core;
using Microsoft.Bot.Connector;
using Microsoft.Bot.Schema;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Sends proactive 1:1 Teams bot messages: uses a stored <see cref="ConversationReference"/> when available,
/// otherwise creates a personal conversation via Bot Connector <c>CreateConversationAsync</c> using the user's Entra object id.
/// </summary>
public sealed class TeamsProactiveMessagingService
{
    /// <summary>Default Teams SMBA endpoint; override with <see cref="BotSettings.TeamsConnectorServiceUrl"/> if proactive create fails.</summary>
    internal const string DefaultTeamsConnectorServiceUrl = "https://smba.trafficmanager.net/teams/";

    private readonly BotSettings _settings;
    private readonly CloudAdapter _adapter;
    private readonly TeamsConversationReferenceStore _references;
    private readonly ILogger<TeamsProactiveMessagingService> _logger;

    public TeamsProactiveMessagingService(
        BotSettings settings,
        CloudAdapter adapter,
        TeamsConversationReferenceStore references,
        ILogger<TeamsProactiveMessagingService> logger)
    {
        _settings = settings;
        _adapter = adapter;
        _references = references;
        _logger = logger;
    }

    /// <summary>Returns true if a chat message was sent to the user.</summary>
    public async Task<bool> TrySendPersonalChatAsync(string entraUserObjectId, string text, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(entraUserObjectId) || string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var oid = entraUserObjectId.Trim();

        if (_references.TryGet(oid, out var reference) && reference is not null)
        {
            try
            {
                await _adapter.ContinueConversationAsync(
                    _settings.ClientId,
                    reference,
                    async (turnContext, ct) =>
                    {
                        await turnContext.SendActivityAsync(MessageFactory.Text(text), ct).ConfigureAwait(false);
                    },
                    cancellationToken).ConfigureAwait(false);

                _logger.LogInformation("Proactive Teams chat message sent (existing conversation) to Entra user {Oid}.", oid);
                return true;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "ContinueConversation failed for Entra user {Oid}; attempting CreateConversation.", oid);
            }
        }

        return await TryCreatePersonalConversationAndSendAsync(oid, text, cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> TryCreatePersonalConversationAndSendAsync(
        string entraUserObjectId,
        string text,
        CancellationToken cancellationToken)
    {
        var serviceUrl = string.IsNullOrWhiteSpace(_settings.TeamsConnectorServiceUrl)
            ? DefaultTeamsConnectorServiceUrl
            : _settings.TeamsConnectorServiceUrl.Trim().TrimEnd('/') + "/";

        var parameters = new ConversationParameters
        {
            IsGroup = false,
            Bot = new ChannelAccount(_settings.ClientId),
            Members = new List<ChannelAccount> { new ChannelAccount(aadObjectId: entraUserObjectId) },
            TenantId = _settings.TenantId
        };

        try
        {
            await _adapter.CreateConversationAsync(
                _settings.ClientId,
                Channels.Msteams,
                serviceUrl,
                audience: null,
                parameters,
                async (turnContext, ct) =>
                {
                    await turnContext.SendActivityAsync(MessageFactory.Text(text), ct).ConfigureAwait(false);
                    var captured = turnContext.Activity.GetConversationReference();
                    _references.Upsert(entraUserObjectId, captured);
                    _logger.LogInformation(
                        "Created Teams 1:1 conversation and stored reference for Entra user {Oid}. ServiceUrl={ServiceUrl}",
                        entraUserObjectId,
                        serviceUrl);
                },
                cancellationToken).ConfigureAwait(false);

            return true;
        }
        catch (Exception ex)
        {
            _logger.LogWarning(
                ex,
                "CreateConversation (1:1) failed for Entra user {Oid}. Ensure the Teams app is installed for this user " +
                "(org-wide or Graph install with consent), Azure Bot messaging endpoint is https://<host>/api/messages, " +
                "and Bot:TeamsConnectorServiceUrl matches your region if needed (amer/emea/apac).",
                entraUserObjectId);
            return false;
        }
    }
}
