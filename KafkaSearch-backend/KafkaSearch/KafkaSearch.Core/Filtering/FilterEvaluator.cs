namespace KafkaSearch.Core.Filtering;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Enums;
using System.Text.Json;

public class FilterEvaluator : IFilterEvaluator
{
    public bool Evaluate(FilterNode node, JsonElement scope) => node switch
    {
        Compare c => TryResolve(scope, c.Path, out var el) && Matches(el, c.Op, c.Value),
        And a => a.Nodes.All(n => Evaluate(n, scope)),
        Or o => o.Nodes.Any(n => Evaluate(n, scope)),
        Any any => TryResolve(scope, any.Path, out var arr)
                     && arr.ValueKind == JsonValueKind.Array
                     && arr.EnumerateArray().Any(el => Evaluate(any.Where, el)),
        _ => false
    };

    private static bool TryResolve(JsonElement scope, string path, out JsonElement value)
    {
        value = scope;

        var inspectValue = value.ToString();

        if (string.IsNullOrEmpty(path))
            return true;

        foreach (var segment in path.Split('.'))
        {
            var isDifferentThenObject = value.ValueKind != JsonValueKind.Object;
            var isNotParsed = !value.TryGetProperty(segment, out var property);
            var resultInspection = property.ToString();

            if (isDifferentThenObject || isNotParsed)
            {
                value = default;
                return false;
            }

            //value = property;
        }

        return true;
    }

    private static bool Matches(JsonElement actual, CompareOp op, JsonElement expected) => op switch
    {
        CompareOp.Exists => true,
        CompareOp.Equal => AreEqual(actual, expected),
        CompareOp.NotEqual => !AreEqual(actual, expected),
        CompareOp.Contains => AsString(actual)?.Contains(AsString(expected) ?? "", StringComparison.OrdinalIgnoreCase) == true,
        CompareOp.StartsWith => AsString(actual)?.StartsWith(AsString(expected) ?? "", StringComparison.OrdinalIgnoreCase) == true,
        CompareOp.EndsWith => AsString(actual)?.EndsWith(AsString(expected) ?? "", StringComparison.OrdinalIgnoreCase) == true,
        CompareOp.GreaterThan => TryCompareNumeric(actual, expected, out var c) && c > 0,
        CompareOp.GreaterThanOrEqual => TryCompareNumeric(actual, expected, out var c) && c >= 0,
        CompareOp.LessThan => TryCompareNumeric(actual, expected, out var c) && c < 0,
        CompareOp.LessThanOrEqual => TryCompareNumeric(actual, expected, out var c) && c <= 0,
        _ => false
    };

    private static bool AreEqual(JsonElement actual, JsonElement expected)
    {
        if (actual.ValueKind == JsonValueKind.Number && expected.ValueKind == JsonValueKind.Number)
            return actual.GetDouble().Equals(expected.GetDouble());

        if (actual.ValueKind is JsonValueKind.True or JsonValueKind.False &&
            expected.ValueKind is JsonValueKind.True or JsonValueKind.False)
            return actual.GetBoolean() == expected.GetBoolean();

        if (actual.ValueKind == JsonValueKind.Null)
            return expected.ValueKind == JsonValueKind.Null;

        return string.Equals(AsString(actual), AsString(expected), StringComparison.OrdinalIgnoreCase);
    }

    private static string? AsString(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.String => e.GetString(),
        JsonValueKind.Number => e.GetRawText(),
        JsonValueKind.True or JsonValueKind.False => e.GetRawText(),
        _ => null
    };

    private static bool TryCompareNumeric(JsonElement actual, JsonElement expected, out int comparison)
    {
        comparison = 0;

        if (actual.ValueKind == JsonValueKind.Number && expected.ValueKind == JsonValueKind.Number)
        {
            comparison = actual.GetDouble().CompareTo(expected.GetDouble());
            return true;
        }

        var a = AsString(actual);
        var b = AsString(expected);

        if (double.TryParse(a, out var da) && double.TryParse(b, out var db))
        {
            comparison = da.CompareTo(db);
            return true;
        }

        if (DateTimeOffset.TryParse(a, out var ta) && DateTimeOffset.TryParse(b, out var tb))
        {
            comparison = ta.CompareTo(tb);
            return true;
        }

        return false;
    }
}