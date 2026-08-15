namespace Kafka.Core.Test;

using KafkaSearch.Core.Enums;
using KafkaSearch.Core.Filtering;

public class FilterEvaluatorTests : IDisposable
{
    private readonly FilterEvaluator _filterEvaluator;

    public FilterEvaluatorTests()
    {
        _filterEvaluator = new FilterEvaluator();
    }

    [Fact]
    public void Evaluate_MessageWithActiveMarket_ReturnsTrue()
    {
        var json = LoadJson("oddschange-mixed-states");
        using var doc = JsonDocument.Parse(json);

        var filter = new Any("markets",
            new Compare("marketStatus", CompareOp.Equal, ToElement("Active")));

        var result = _filterEvaluator.Evaluate(filter, doc.RootElement);

        Assert.True(result);
    }

    [Fact]
    public void Evaluate_ConditionsMustMatchSameMarket_ReturnsFalse()
    {
        var json = LoadJson("oddschange-mixed-states");
        using var doc = JsonDocument.Parse(json);

        var filter = new Any(
           "markets",
           new And([
                    new Compare("marketStatus", CompareOp.Equal, ToElement("Deactivated")),
                    new Compare("isFavourite",  CompareOp.Equal, ToElement(false))]
            )
        );

        Assert.False(_filterEvaluator.Evaluate(filter, doc.RootElement));
    }

    [Fact]
    public void Evaluate_ConditionsMatchSameMarket_ReturnsTrue()
    {
        var json = LoadJson("oddschange-mixed-states");
        using var doc = JsonDocument.Parse(json);

        // Market 1 is both Active AND isFavourite=false
        var filter = new Any(
            "markets",
            new And([
                new Compare("marketStatus", CompareOp.Equal, ToElement("Active")),
            new Compare("isFavourite",  CompareOp.Equal, ToElement(false))]
            )
        );

        Assert.True(_filterEvaluator.Evaluate(filter, doc.RootElement));
    }

    public void Dispose()
    {
    }

    private static string LoadJson(string fileName) =>
        File.ReadAllText(Path.Combine(
            AppContext.BaseDirectory,
            "FilterEvaluatorTestSamples",
            $"{fileName}.json"));

    private static JsonElement ToElement(object value)
    {
        using var doc = JsonDocument.Parse(JsonSerializer.Serialize(value));
        return doc.RootElement.Clone();
    }
}