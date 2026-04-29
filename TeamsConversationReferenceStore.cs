using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Bot.Schema;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

/// <summary>
/// Stores Bot Framework <see cref="ConversationReference"/> per Entra user object id so proactive messages can be sent in personal chat.
/// Persists to disk so references survive process restarts.
/// </summary>
public sealed class TeamsConversationReferenceStore
{
    private readonly ConcurrentDictionary<string, ConversationReference> _byEntraUserId =
        new(StringComparer.OrdinalIgnoreCase);

    private readonly string _persistPath;
    private readonly ILogger<TeamsConversationReferenceStore> _logger;
    private readonly SemaphoreSlim _saveGate = new(1, 1);

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public TeamsConversationReferenceStore(IHostEnvironment hostEnvironment, ILogger<TeamsConversationReferenceStore> logger)
    {
        _logger = logger;
        _persistPath = Path.Combine(hostEnvironment.ContentRootPath, "bridge_lead_conversations.json");
        LoadFromDisk();
    }

    public int Count => _byEntraUserId.Count;

    public void Upsert(string entraUserObjectId, ConversationReference reference)
    {
        if (string.IsNullOrWhiteSpace(entraUserObjectId))
        {
            return;
        }

        _byEntraUserId[entraUserObjectId.Trim()] = reference;
        _ = SaveToDiskAsync();
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

    private void LoadFromDisk()
    {
        try
        {
            if (!File.Exists(_persistPath))
            {
                return;
            }

            var json = File.ReadAllText(_persistPath);
            var dto = JsonSerializer.Deserialize<PersistedConversationsDto>(json, JsonOptions);
            if (dto?.Conversations is null)
            {
                return;
            }

            foreach (var kv in dto.Conversations)
            {
                if (!string.IsNullOrWhiteSpace(kv.Key) && kv.Value is not null)
                {
                    _byEntraUserId[kv.Key.Trim()] = kv.Value;
                }
            }

            _logger.LogInformation("Loaded {Count} persisted Teams conversation reference(s) from {Path}.", _byEntraUserId.Count, _persistPath);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not load persisted conversation references from {Path}.", _persistPath);
        }
    }

    private async Task SaveToDiskAsync()
    {
        await _saveGate.WaitAsync().ConfigureAwait(false);
        try
        {
            var dto = new PersistedConversationsDto
            {
                Conversations = new Dictionary<string, ConversationReference>(_byEntraUserId, StringComparer.OrdinalIgnoreCase)
            };
            var json = JsonSerializer.Serialize(dto, JsonOptions);
            await File.WriteAllTextAsync(_persistPath, json).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Could not persist conversation references to {Path}.", _persistPath);
        }
        finally
        {
            _saveGate.Release();
        }
    }

    private sealed class PersistedConversationsDto
    {
        public Dictionary<string, ConversationReference> Conversations { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    }
}
