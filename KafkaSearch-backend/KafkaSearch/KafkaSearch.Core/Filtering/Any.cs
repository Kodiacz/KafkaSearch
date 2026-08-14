namespace KafkaSearch.Core.Filtering;

public sealed record Any(string Path, FilterNode Where) : FilterNode;
