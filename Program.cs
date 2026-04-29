using Microsoft.AspNetCore.Builder;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Bot.Builder;
using Microsoft.Bot.Builder.BotFramework;
using Microsoft.Bot.Builder.Integration.AspNet.Core;
using Microsoft.Bot.Schema;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TeamsMediaBot;

public static class Program
{
    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Kestrel URL must differ from the Media Platform's internal HTTP listener (see BotService + Media:HttpControlPort).
        // nginx should proxy to this port (default 5080). Override with BOT_HTTP_LISTEN_URL or Bot:ListenUrl.
        var appListenUrl = Environment.GetEnvironmentVariable("BOT_HTTP_LISTEN_URL");
        if (string.IsNullOrWhiteSpace(appListenUrl))
        {
            appListenUrl = builder.Configuration["Bot:ListenUrl"];
        }
        if (string.IsNullOrWhiteSpace(appListenUrl))
        {
            appListenUrl = "http://127.0.0.1:5080";
        }
        builder.WebHost.UseUrls(appListenUrl.Trim());

        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();

        builder.Services.Configure<ForwardedHeadersOptions>(options =>
        {
            options.ForwardedHeaders = ForwardedHeaders.XForwardedFor | ForwardedHeaders.XForwardedProto;
            options.KnownNetworks.Clear();
            options.KnownProxies.Clear();
        });

        builder.Services.AddSignalR().AddJsonProtocol(options =>
        {
            // Ensure browser clients receive camelCase (kind, text, speakerLabel, azureAdObjectId).
            options.PayloadSerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
            options.PayloadSerializerOptions.DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull;
        });
        builder.Services.AddSingleton(new BotSettings
        {
            TenantId = GetConfig(builder.Configuration, "BOT_TENANT_ID", "AzureAd:TenantId"),
            ClientId = GetConfig(builder.Configuration, "BOT_CLIENT_ID", "AzureAd:ClientId"),
            ClientSecret = GetConfig(builder.Configuration, "BOT_CLIENT_SECRET", "AzureAd:ClientSecret"),
            ServiceBaseUrl = GetConfig(builder.Configuration, "BOT_SERVICE_BASE_URL", "Bot:CallbackUrl"),
            AzureSpeechKey = ReadOptional(builder.Configuration, "BOT_AZURE_SPEECH_KEY", "Bot:AzureSpeechKey"),
            AzureSpeechRegion = ReadOptional(builder.Configuration, "BOT_AZURE_SPEECH_REGION", "Bot:AzureSpeechRegion"),
            MediaCertificateThumbprint = GetConfig(builder.Configuration, "BOT_MEDIA_CERT_THUMBPRINT", "Media:CertificateThumbprint"),
            MediaPublicIp = GetConfig(builder.Configuration, "BOT_MEDIA_PUBLIC_IP", "Media:PublicIp"),
            MediaInstanceInternalPort = ReadInt(builder.Configuration, "BOT_MEDIA_INSTANCE_INTERNAL_PORT", "Media:InstanceInternalPort", 8445),
            MediaInstancePublicPort = ReadInt(builder.Configuration, "BOT_MEDIA_INSTANCE_PUBLIC_PORT", "Media:InstancePublicPort", 8445),
            MediaHttpControlPort = ReadInt(builder.Configuration, "BOT_MEDIA_HTTP_CONTROL_PORT", "Media:HttpControlPort", 5000),
            MediaUdpPortMin = ReadOptionalUInt(builder.Configuration, "BOT_MEDIA_UDP_PORT_MIN", "Media:UdpPortMin"),
            MediaUdpPortMax = ReadOptionalUInt(builder.Configuration, "BOT_MEDIA_UDP_PORT_MAX", "Media:UdpPortMax"),
            MediaServiceFqdn = ReadOptional(builder.Configuration, "BOT_MEDIA_SERVICE_FQDN", "Media:ServiceFqdn"),
            JoinMeetingSubject = ReadOptional(builder.Configuration, "BOT_JOIN_MEETING_SUBJECT", "Bot:JoinMeetingSubject"),
            TranscriptBroadcastPartials = ReadBool(builder.Configuration, "BOT_TRANSCRIPT_BROADCAST_PARTIALS", "Bot:TranscriptBroadcastPartials", defaultValue: false),
            TranscribeAudioChunkMilliseconds = ReadInt(builder.Configuration, "BOT_TRANSCRIBE_CHUNK_MS", "Bot:TranscribeAudioChunkMilliseconds", 100),
            TranscribePartialMinIntervalMilliseconds = ReadInt(builder.Configuration, "BOT_TRANSCRIBE_PARTIAL_MS", "Bot:TranscribePartialMinIntervalMilliseconds", 90),
            TranscriptTimelineMergeMilliseconds = ReadInt(builder.Configuration, "BOT_TRANSCRIPT_TIMELINE_MS", "Bot:TranscriptTimelineMergeMilliseconds", 20),
            TranscriptAlbEndpoint = ReadOptional(builder.Configuration, "BOT_TRANSCRIPT_ALB_ENDPOINT", "Bot:TranscriptAlbEndpoint"),
            DynamoMeetingRecordsTableName = ReadOptional(builder.Configuration, "BOT_DYNAMO_TABLE_NAME", "Bot:DynamoMeetingRecordsTableName"),
            DynamoRegion = ReadOptional(builder.Configuration, "BOT_DYNAMO_REGION", "Bot:DynamoRegion"),
            DynamoPollIntervalSeconds = Math.Clamp(
                ReadInt(builder.Configuration, "BOT_DYNAMO_POLL_SECONDS", "Bot:DynamoPollIntervalSeconds", 60),
                30,
                600),
            BotDmSenderUserObjectId = ReadOptional(builder.Configuration, "BOT_DM_SENDER_USER_ID", "Bot:BotDmSenderUserObjectId"),
            TeamsAppId = ReadOptional(builder.Configuration, "BOT_TEAMS_APP_ID", "Bot:TeamsAppId"),
            TeamsManifestAppId = ReadOptional(builder.Configuration, "BOT_TEAMS_MANIFEST_APP_ID", "Bot:TeamsManifestAppId"),
            MicrosoftAppType = ReadOptional(builder.Configuration, "BOT_MICROSOFT_APP_TYPE", "Bot:MicrosoftAppType") ?? "SingleTenant",
            IdentityAudioBufferMilliseconds = Math.Clamp(
                ReadInt(builder.Configuration, "BOT_IDENTITY_AUDIO_BUFFER_MS", "Bot:IdentityAudioBufferMilliseconds", 7000),
                5000,
                10000),
            IdentityResolutionRetrySeconds = Math.Clamp(
                ReadInt(builder.Configuration, "BOT_IDENTITY_RETRY_SEC", "Bot:IdentityResolutionRetrySeconds", 2),
                1,
                30)
        });

        builder.Services.AddHttpClient("AlbTranscriptSender", client =>
        {
            client.Timeout = TimeSpan.FromSeconds(15);
        });
        builder.Services.AddSingleton<MeetingContextStore>();
        builder.Services.AddSingleton<ParticipantManager>();
        builder.Services.AddSingleton<IParticipantManager>(sp => sp.GetRequiredService<ParticipantManager>());
        builder.Services.AddSingleton<TranscriptionChunkManager>();
        builder.Services.AddSingleton<IChunkManager>(sp => sp.GetRequiredService<TranscriptionChunkManager>());
        builder.Services.AddHostedService(sp => sp.GetRequiredService<TranscriptionChunkManager>());
        builder.Services.AddSingleton<TranscriptBroadcaster>();
        builder.Services.AddSingleton<SsrcParticipantMapper>();
        builder.Services.AddSingleton<EntraUserResolver>();
        builder.Services.AddSingleton<MeetingParticipantService>();
        builder.Services.AddSingleton<AzureSpeechTranscriptionService>();
        builder.Services.AddSingleton<ParticipantAudioRouter>();
        builder.Services.AddSingleton<AudioProcessor>();
        builder.Services.AddSingleton<MediaHandler>();
        builder.Services.AddSingleton<CallHandler>();
        builder.Services.AddSingleton<BotService>();
        builder.Services.AddSingleton<TeamsConversationReferenceStore>();
        builder.Services.AddSingleton<CloudAdapter>(sp =>
        {
            var settings = sp.GetRequiredService<BotSettings>();
            var appType = string.IsNullOrWhiteSpace(settings.MicrosoftAppType)
                ? "SingleTenant"
                : settings.MicrosoftAppType.Trim();
            var inMemory = new Dictionary<string, string?>
            {
                ["MicrosoftAppId"] = settings.ClientId,
                ["MicrosoftAppPassword"] = settings.ClientSecret,
                ["MicrosoftAppType"] = appType
            };
            if (string.Equals(appType, "SingleTenant", StringComparison.OrdinalIgnoreCase))
            {
                inMemory["MicrosoftAppTenantId"] = settings.TenantId;
            }

            var configuration = new ConfigurationBuilder().AddInMemoryCollection(inMemory!).Build();
            var authentication = new ConfigurationBotFrameworkAuthentication(configuration);
            var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
            var adapter = new CloudAdapter(authentication, loggerFactory.CreateLogger<CloudAdapter>());
            adapter.OnTurnError = async (turnContext, exception) =>
            {
                loggerFactory.CreateLogger("BotFramework").LogError(exception, "Bot OnTurnError");
                if (turnContext?.Activity?.ChannelId != null)
                {
                    try
                    {
                        await turnContext.SendActivityAsync(
                            MessageFactory.Text("The bot hit an error processing your message. Please try again."),
                            CancellationToken.None).ConfigureAwait(false);
                    }
                    catch
                    {
                        // ignore secondary failures
                    }
                }
            };
            return adapter;
        });
        builder.Services.AddSingleton<BridgeLeadTeamsBot>();
        builder.Services.AddSingleton<IBot>(sp => sp.GetRequiredService<BridgeLeadTeamsBot>());
        builder.Services.AddSingleton<TeamsProactiveMessagingService>();
        builder.Services.AddHostedService<BridgeLeadDynamoDmService>();

        var app = builder.Build();

        var botSettings = app.Services.GetRequiredService<BotSettings>();
        try
        {
            var callback = new Uri(botSettings.ServiceBaseUrl.Trim(), UriKind.Absolute);
            var messagingBase = $"{callback.Scheme}://{callback.Authority}";
            app.Logger.LogInformation(
                "Teams Bot Framework: set Azure Bot messaging endpoint to {MessagingUrl} (same host as CallbackUrl). " +
                "Reverse proxy must forward POST /api/messages to this app. Conversation refs persist to ContentRoot/bridge_lead_conversations.json",
                $"{messagingBase}/api/messages");
        }
        catch (Exception ex)
        {
            app.Logger.LogWarning(ex, "Could not derive messaging URL from Bot:CallbackUrl.");
        }

        // Must run first so SignalR and HTTPS URLs see correct scheme/host behind nginx.
        app.UseForwardedHeaders();

        app.UseDefaultFiles();
        app.UseStaticFiles();

        app.MapHub<TranscriptHub>("/hubs/transcripts");

        static async Task<IResult> HandleGraphCallback(HttpContext ctx, BotService botService, ILogger log)
        {
            // Convert ASP.NET request into HttpRequestMessage for the comms SDK.
            var req = ctx.Request;
            var uri = new Uri($"{req.Scheme}://{req.Host}{req.Path}{req.QueryString}");
            var msg = new HttpRequestMessage(new HttpMethod(req.Method), uri);

            foreach (var header in req.Headers)
            {
                if (!msg.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()))
                {
                    msg.Content ??= new StreamContent(req.Body);
                    msg.Content.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
                }
            }

            if (msg.Content is null)
            {
                msg.Content = new StreamContent(req.Body);
            }

            var sdkResponse = await botService.ProcessNotificationAsync(msg);

            ctx.Response.StatusCode = (int)sdkResponse.StatusCode;
            foreach (var header in sdkResponse.Headers)
            {
                ctx.Response.Headers[header.Key] = header.Value.ToArray();
            }
            if (sdkResponse.Content is not null)
            {
                foreach (var header in sdkResponse.Content.Headers)
                {
                    ctx.Response.Headers[header.Key] = header.Value.ToArray();
                }
                // ASP.NET Core sets these automatically for some responses
                ctx.Response.Headers.Remove("transfer-encoding");
                await sdkResponse.Content.CopyToAsync(ctx.Response.Body);
            }

            log.LogInformation("Processed Graph comms callback. Status={Status}", (int)sdkResponse.StatusCode);
            return Results.Empty;
        }

        // Graph Communications notifications endpoint(s).
        app.MapPost("/communications/calls", (HttpContext ctx, BotService botService, ILogger<BotService> log) =>
            HandleGraphCallback(ctx, botService, log));

        app.MapPost("/callback", (HttpContext ctx, BotService botService, ILogger<BotService> log) =>
            HandleGraphCallback(ctx, botService, log));

        app.MapPost("/api/messages", async (HttpContext ctx, CloudAdapter adapter, IBot bot, ILoggerFactory loggerFactory, CancellationToken ct) =>
        {
            loggerFactory.CreateLogger("BotMessages").LogInformation(
                "Incoming POST /api/messages ContentLength={Len} UserAgent={Agent}",
                ctx.Request.ContentLength ?? 0,
                ctx.Request.Headers.UserAgent.ToString());
            await adapter.ProcessAsync(ctx.Request, ctx.Response, bot, ct).ConfigureAwait(false);
        });

        app.MapGet("/api/bridge-lead/diag", (TeamsConversationReferenceStore store) =>
            Results.Ok(new
            {
                storedConversationReferences = store.Count,
                note = "If storedConversationReferences is 0, Teams is not hitting /api/messages or no user activity has been captured yet."
            }));

        static async Task<IResult> HandleMeetingsApiJoin(
            HttpContext ctx,
            JoinMeetingRequest request,
            BotService botService,
            ILogger log)
        {
            if (string.IsNullOrWhiteSpace(request.MeetingId) &&
                string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new { message = "Provide MeetingId, MeetingJoinUrl, or ChatThreadId (+ OrganizerObjectId)." });
            }

            if (string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new
                {
                    message = "This bot needs MeetingJoinUrl, or ChatThreadId with OrganizerObjectId, to join via Graph Communications.",
                    meetingId = request.MeetingId
                });
            }

            if (!string.IsNullOrWhiteSpace(request.ChatThreadId) &&
                string.IsNullOrWhiteSpace(request.OrganizerObjectId))
            {
                return Results.BadRequest(new { message = "When using ChatThreadId, OrganizerObjectId is required (and MeetingTenantId if not the bot home tenant)." });
            }

            var parsed = MeetingJoinParser.ParseJoinUrl(request.MeetingJoinUrl);
            var transcriptKey = !string.IsNullOrWhiteSpace(request.MeetingId)
                ? request.MeetingId!.Trim()
                : (!string.IsNullOrWhiteSpace(parsed.JoinMeetingId)
                    ? parsed.JoinMeetingId
                    : (!string.IsNullOrWhiteSpace(request.ChatThreadId)
                        ? request.ChatThreadId.Trim()
                        : request.MeetingJoinUrl?.Trim())) ?? "unknown";

            try
            {
                await botService.JoinMeetingAsync(request);
                return Results.Accepted(
                    uri: null,
                    value: new
                    {
                        transcriptKey,
                        joinMeetingId = parsed.JoinMeetingId,
                        chatThreadId = request.ChatThreadId,
                        message = "Join request submitted."
                    });
            }
            catch (ArgumentException ex)
            {
                log.LogWarning(ex, "Invalid join request.");
                return Results.BadRequest(new { message = ex.Message });
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("already active", StringComparison.OrdinalIgnoreCase))
            {
                log.LogWarning(ex, "Join rejected: a call is already in progress.");
                return Results.Conflict(new { message = ex.Message, transcriptKey });
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Join meeting failed.");
                return Results.Json(
                    new
                    {
                        message = "Join meeting failed.",
                        error = ex.Message,
                        inner = ex.InnerException?.Message,
                        type = ex.GetType().FullName,
                        traceId = ctx.TraceIdentifier
                    },
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        }

        app.MapPost("/api/meetings/join", async (HttpContext ctx, JoinMeetingRequest request, BotService botService, ILogger<CallHandler> log) =>
            await HandleMeetingsApiJoin(ctx, request, botService, log));

        app.MapPost("/api/bot/join", async (HttpContext ctx, JoinMeetingRequest request, BotService botService, ILogger<CallHandler> log) =>
        {
            if (string.IsNullOrWhiteSpace(request.MeetingId) &&
                string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new { message = "Provide MeetingJoinUrl, or MeetingId with MeetingJoinUrl, or ChatThreadId + OrganizerObjectId." });
            }

            if (string.IsNullOrWhiteSpace(request.MeetingJoinUrl) &&
                string.IsNullOrWhiteSpace(request.ChatThreadId))
            {
                return Results.BadRequest(new { message = "MeetingJoinUrl (or ChatThreadId + OrganizerObjectId) is required." });
            }

            if (!string.IsNullOrWhiteSpace(request.ChatThreadId) &&
                string.IsNullOrWhiteSpace(request.OrganizerObjectId))
            {
                return Results.BadRequest(new { message = "When using ChatThreadId, OrganizerObjectId is required." });
            }

            try
            {
                await botService.JoinMeetingAsync(request);
                return Results.Ok(new { message = "Join request submitted." });
            }
            catch (ArgumentException ex)
            {
                log.LogWarning(ex, "Invalid join request.");
                return Results.BadRequest(new { message = ex.Message });
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("already active", StringComparison.OrdinalIgnoreCase))
            {
                log.LogWarning(ex, "Join rejected: a call is already in progress.");
                return Results.Conflict(new { message = ex.Message });
            }
            catch (Exception ex)
            {
                log.LogError(ex, "Join meeting failed.");
                return Results.Json(
                    new
                    {
                        message = "Join meeting failed.",
                        error = ex.Message,
                        inner = ex.InnerException?.Message,
                        type = ex.GetType().FullName,
                        traceId = ctx.TraceIdentifier
                    },
                    statusCode: StatusCodes.Status500InternalServerError);
            }
        });

        await app.RunAsync();
    }

    private static string GetConfig(IConfiguration configuration, string envKey, string configKey)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim();
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig))
        {
            return fromConfig.Trim();
        }

        throw new InvalidOperationException($"Missing configuration: {envKey} or {configKey}");
    }

    private static string? ReadOptional(IConfiguration configuration, string envKey, string configKey)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim();
        }

        var fromConfig = configuration[configKey];
        return string.IsNullOrWhiteSpace(fromConfig) ? null : fromConfig.Trim();
    }

    private static int ReadInt(IConfiguration configuration, string envKey, string configKey, int defaultValue)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env) && int.TryParse(env.Trim(), out var fromEnv))
        {
            return fromEnv;
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig) && int.TryParse(fromConfig.Trim(), out var fromFile))
        {
            return fromFile;
        }

        return defaultValue;
    }

    private static bool ReadBool(IConfiguration configuration, string envKey, string configKey, bool defaultValue)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env))
        {
            return env.Trim() is "1" or "true" or "True" or "yes" or "Yes";
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig))
        {
            return fromConfig.Trim() is "1" or "true" or "True" or "yes" or "Yes";
        }

        return defaultValue;
    }

    private static uint? ReadOptionalUInt(IConfiguration configuration, string envKey, string configKey)
    {
        var env = Environment.GetEnvironmentVariable(envKey);
        if (!string.IsNullOrWhiteSpace(env) && uint.TryParse(env.Trim(), out var fromEnv))
        {
            return fromEnv;
        }

        var fromConfig = configuration[configKey];
        if (!string.IsNullOrWhiteSpace(fromConfig) && uint.TryParse(fromConfig.Trim(), out var fromFile))
        {
            return fromFile;
        }

        return null;
    }
}
