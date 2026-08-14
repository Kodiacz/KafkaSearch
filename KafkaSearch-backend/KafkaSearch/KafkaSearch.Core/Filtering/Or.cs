namespace KafkaSearch.Core.Filtering;

public sealed record Or(FilterNode[] Nodes) : FilterNode;
