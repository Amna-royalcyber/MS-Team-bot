using Microsoft.Bot.Builder;
using Microsoft.Bot.Builder.Integration.AspNet.Core;
using Microsoft.Bot.Schema;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Sends proactive bot chat messages using a stored <see cref="ConversationReference"/>.
/// </summary>
public sealed class TeamsProactiveMessagingService
{
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
        if (!_references.TryGet(entraUserObjectId, out var reference) || reference is null)
        {
            _logger.LogWarning(
                "No stored Bot Framework conversation for Entra user {Oid}. " +
                "Open the bot in Teams (personal chat), send any message once, or reinstall the app so the bot can store a conversation reference. " +
                "Ensure Azure Bot messaging endpoint points to https://<your-host>/api/messages and matches this deployment.",
                entraUserObjectId);
            return false;
        }

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

            _logger.LogInformation("Proactive Teams chat message sent to Entra user {Oid}.", entraUserObjectId);
            return true;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Proactive Teams chat message failed for Entra user {Oid}.", entraUserObjectId);
            return false;
        }
    }
}
