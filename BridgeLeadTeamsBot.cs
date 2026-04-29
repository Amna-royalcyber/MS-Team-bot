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

    public override async Task OnTurnAsync(ITurnContext turnContext, CancellationToken cancellationToken)
    {
        TryCaptureConversationReference(turnContext);
        await base.OnTurnAsync(turnContext, cancellationToken).ConfigureAwait(false);
    }

    private void TryCaptureConversationReference(ITurnContext turnContext)
    {
        var activity = turnContext.Activity;
        if (activity?.Conversation is null)
        {
            return;
        }

        var oid = activity.From?.AadObjectId;
        if (string.IsNullOrWhiteSpace(oid))
        {
            return;
        }

        if (string.Equals(activity.From?.Id, activity.Recipient?.Id, StringComparison.Ordinal))
        {
            return;
        }

        var reference = activity.GetConversationReference();
        _references.Upsert(oid.Trim(), reference);
        _logger.LogInformation(
            "Captured Teams conversation reference from activity Type={Type} ChannelId={ChannelId} for Entra user {Oid}.",
            activity.Type,
            activity.ChannelId,
            oid);
    }

    protected override async Task OnMessageActivityAsync(ITurnContext<IMessageActivity> turnContext, CancellationToken cancellationToken)
    {
        await turnContext.SendActivityAsync(
            MessageFactory.Text("You're connected for bridge-lead alerts. You can close this chat; Dynamo updates will appear here when available."),
            cancellationToken).ConfigureAwait(false);
    }

    protected override async Task OnInstallationUpdateAddAsync(
        ITurnContext<IInstallationUpdateActivity> turnContext,
        CancellationToken cancellationToken)
    {
        await turnContext.SendActivityAsync(
            MessageFactory.Text("Teams Meeting Transcription is installed. Bridge-lead alerts will appear in this chat when DynamoDB posts updates."),
            cancellationToken).ConfigureAwait(false);
        await base.OnInstallationUpdateAddAsync(turnContext, cancellationToken).ConfigureAwait(false);
    }

    protected override async Task OnMembersAddedAsync(
        IList<ChannelAccount> membersAdded,
        ITurnContext<IConversationUpdateActivity> turnContext,
        CancellationToken cancellationToken)
    {
        _ = membersAdded;
        await base.OnMembersAddedAsync(membersAdded, turnContext, cancellationToken).ConfigureAwait(false);
    }
}
