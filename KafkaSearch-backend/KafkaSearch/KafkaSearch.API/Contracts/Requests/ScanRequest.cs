namespace KafkaSearch.API.Contracts.Requests;

using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Models;

public sealed record ScanRequest(
    string ClusterProfileName,
    string Topic,
    FilterNode? Filter,
    int? partition,
    string? key,
    KeyFormat KeyFormat = KeyFormat.Auto,
    SeekMode seekMode = SeekMode.Newest,
    long? offsetValue = null,
    DateTime? fromTimestamp = null,
    int MaxMessagesPerPartition = 50_000);