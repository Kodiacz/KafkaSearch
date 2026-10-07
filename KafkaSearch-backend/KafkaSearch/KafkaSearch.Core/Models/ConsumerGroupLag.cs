namespace KafkaSearch.Core.Models;

public sealed record ConsumerGroupLag(
    string GroupId,
    string State,
    long TotalLag,
    IReadOnlyDictionary<int, long> LagByPartition);
