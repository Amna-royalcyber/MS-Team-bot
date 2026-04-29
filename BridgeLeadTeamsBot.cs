using Microsoft.Bot.Builder;
using Microsoft.Bot.Schema;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Receives Teams user messages to the bot and stores <see cref="ConversationReference"/> for proactive bridge-lead chat delivery.
/// </summary>
public sealed class BridgeLeadTeamsBot : ActivityHandler
{
    private readonly TeamsConversationReferenceStore _references;
    private readonly ILogger<BridgeLeadTeamsBot> _logger;

    public BridgeLeadTeamsBot(TeamsConversationReferenceStore references, ILogger<BridgeLeadTeamsBot> logger)
    {
        _references = references;
        _logger = logger;
    }

    protected override async Task OnMessageActivityAsync(ITurnContext<IMessageActivity> turnContext, CancellationToken cancellationToken)
    {
        var oid = turnContext.Activity.From?.AadObjectId;
        if (!string.IsNullOrWhiteSpace(oid))
        {
            var reference = TurnContext.GetConversationReference(turnContext.Activity);
            _references.Upsert(oid, reference);
            _logger.LogInformation("Stored Teams conversation reference for Entra user {Oid}.", oid);
        }

        await turnContext.SendActivityAsync(
            MessageFactory.Text("You're connected for bridge-lead alerts. You can close this chat; Dynamo updates will appear here when available."),
            cancellationToken).ConfigureAwait(false);
    }

    protected override async Task OnMembersAddedAsync(
        IList<ChannelAccount> membersAdded,
        ITurnContext<IConversationUpdateActivity> turnContext,
        CancellationToken cancellationToken)
    {
        foreach (var member in membersAdded)
        {
            if (member.Id == turnContext.Activity.Recipient?.Id)
            {
                continue;
            }

            var oid = member.AadObjectId;
            if (!string.IsNullOrWhiteSpace(oid))
            {
                var reference = TurnContext.GetConversationReference(turnContext.Activity);
                _references.Upsert(oid, reference);
                _logger.LogInformation("Stored Teams conversation reference from membersAdded for Entra user {Oid}.", oid);
            }
        }

        await base.OnMembersAddedAsync(membersAdded, turnContext, cancellationToken).ConfigureAwait(false);
    }
}
