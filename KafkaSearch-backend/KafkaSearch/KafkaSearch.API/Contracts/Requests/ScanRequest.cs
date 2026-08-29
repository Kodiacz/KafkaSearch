namespace KafkaSearch.API.Contracts.Requests;

using Confluent.Kafka;
using KafkaSearch.Core.Filtering;

public sealed record ScanRequest(
    string ClusterProfileName,
    string Topic,
    FilterNode? Filter,
    int? partition,
    Offset? offset = null,
    DateTime? fromTimestamp = null,
    int MaxMessagesPerPartition = 50_000);