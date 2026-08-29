namespace KafkaSearch.API.Controllers;

using KafkaSearch.API.Contracts.Requests;
using KafkaSearch.Core.Enums;
using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using System.Diagnostics.Tracing;
using System.Text.Json;

[ApiController]
[Route("api/search")]
public class SearchController : ControllerBase
{
    private readonly IMessageScanService _messageScanService;
    private readonly JsonSerializerOptions _jsonOptions;

    public SearchController(IMessageScanService messageScanService, IOptions<JsonOptions> jsonOptions)
    {
        _messageScanService = messageScanService;
        _jsonOptions = jsonOptions.Value.JsonSerializerOptions;
    }

    [HttpPost("scan")]
    public IResult Scan([FromBody] ScanRequest request, CancellationToken cancellationToken = default)
    {
        var filter = request.Filter ?? new Compare(string.Empty, CompareOp.Exists, default);

        return TypedResults.ServerSentEvents(
            _messageScanService.Scan(
                request.ClusterProfileName,
                request.Topic,
                filter,
                request.MaxMessagesPerPartition,
                request.partition,
                request.offset,
                request.fromTimestamp,
                cancellationToken),
            eventType: "message");
    }
}