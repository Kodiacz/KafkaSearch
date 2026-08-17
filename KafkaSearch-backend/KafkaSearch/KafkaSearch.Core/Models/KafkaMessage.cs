using System.Text.Json;

namespace KafkaSearch.Core.Models;

public sealed record KafkaMessage(
    string Topic,
    int Partition,
    long Offset,
    string? Key,
    JsonElement Value,
    IReadOnlyDictionary<string, byte[]> Headers,
    DateTime Timestamp);