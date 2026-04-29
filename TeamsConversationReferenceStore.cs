using System.Collections.Concurrent;
using Microsoft.Bot.Schema;

namespace TeamsMediaBot;

/// <summary>
/// Stores Bot Framework <see cref="ConversationReference"/> per Entra user object id so proactive messages can be sent in personal chat.
/// </summary>
public sealed class TeamsConversationReferenceStore
{
    private readonly ConcurrentDictionary<string, ConversationReference> _byEntraUserId =
        new(StringComparer.OrdinalIgnoreCase);

    public void Upsert(string entraUserObjectId, ConversationReference reference)
    {
        if (string.IsNullOrWhiteSpace(entraUserObjectId))
        {
            return;
        }

        _byEntraUserId[entraUserObjectId.Trim()] = reference;
    }

    public bool TryGet(string entraUserObjectId, out ConversationReference? reference)
    {
        reference = null;
        if (string.IsNullOrWhiteSpace(entraUserObjectId))
        {
            return false;
        }

        return _byEntraUserId.TryGetValue(entraUserObjectId.Trim(), out reference);
    }
}
