# Run from the project folder (e.g. C:\Mediabot-hosting) after copying the Azure Speech refactor.
# Removes obsolete AWS Transcribe sources that break the build when AWSSDK packages are not referenced.

$ErrorActionPreference = 'Stop'
$here = $PSScriptRoot
$root = Split-Path $here -Parent
Set-Location $root

$legacy = @(
    'AwsTranscribeService.cs',
    'TranscribeStreamService.cs',
    'TranscriptionManager.cs',
    'ParticipantAudioStreamHandler.cs',
    'MixedDominantAudioDelayGate.cs',
    'UnmixedAudioDelayGate.cs',
    'TranscriptAggregator.cs',
    'TranscriptBuffer.cs',
    'TranscriptDeduplicator.cs',
    'TranscriptIdentityResolver.cs'
)

foreach ($name in $legacy) {
    $path = Join-Path $root $name
    if (Test-Path -LiteralPath $path) {
        Remove-Item -LiteralPath $path -Force
        Write-Host "Removed: $name"
    }
}

Write-Host "Done. Run: dotnet restore; dotnet build"
