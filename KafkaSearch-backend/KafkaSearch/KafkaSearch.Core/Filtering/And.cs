namespace KafkaSearch.Core.Filtering;

public sealed record And(FilterNode[] Nodes) : FilterNode;
