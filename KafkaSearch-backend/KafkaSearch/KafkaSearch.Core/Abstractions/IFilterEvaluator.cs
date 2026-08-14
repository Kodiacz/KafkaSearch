namespace KafkaSearch.Core.Abstractions;

using KafkaSearch.Core.Filtering;
using System.Text.Json;

internal interface IFilterEvaluator
{
    public bool Evaluate(FilterNode node, JsonElement root);
}
