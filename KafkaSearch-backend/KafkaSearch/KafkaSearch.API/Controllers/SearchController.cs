namespace KafkaSearch.API.Controllers;

using KafkaSearch.Core.Enums;
using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

[ApiController]
[Route("api/search")]
public class SearchController : ControllerBase
{
    private readonly IMessageScanService _messageScanService;
    private readonly JsonSerializerOptions _jsonOptions;

    public SearchController(IMessageScanService messageScanService, JsonSerializerOptions jsonOptions)
    {
        _messageScanService = messageScanService;
        _jsonOptions = jsonOptions;
    }

    [HttpGet("scan")]
    public IResult Scan(
        [FromQuery] string clusterProfileName,
        [FromQuery] string topic,
        [FromQuery] string? incomingFilter = null,
        [FromQuery] int maxMessagesPerPartition = 50_000,
        CancellationToken cancellationToken = default)
    {
        FilterNode filter;

        if (string.IsNullOrWhiteSpace(incomingFilter))
        {
            filter = new Compare(string.Empty, CompareOp.Exists, default);
        }
        else
        {
            try
            {
                var parsed = JsonSerializer.Deserialize<FilterNode>(incomingFilter, _jsonOptions);
                if (parsed is null)
                    return TypedResults.BadRequest("Filter cannot be null.");
                filter = parsed;
            }
            catch (JsonException ex)
            {
                return TypedResults.BadRequest($"Invalid filter: {ex.Message}");
            }
        }

        return TypedResults.ServerSentEvents(
            _messageScanService.Scan(clusterProfileName, topic, filter, maxMessagesPerPartition, cancellationToken),
            eventType: "message");
    }
}