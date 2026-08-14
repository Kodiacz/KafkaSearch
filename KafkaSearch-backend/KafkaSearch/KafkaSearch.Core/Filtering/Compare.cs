namespace KafkaSearch.Core.Filtering;

using KafkaSearch.Core.Enums;
using System.Text.Json;

public sealed record Compare(string Path, CompareOp Op, JsonElement Value) : FilterNode;