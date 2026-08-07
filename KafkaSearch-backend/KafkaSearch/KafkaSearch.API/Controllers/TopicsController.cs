namespace KafkaSearch.API.Controllers;

using KafkaSearch.Core.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/topics")]
public class TopicsController : ControllerBase
{
    private readonly IKafkaConnectionService _kafkaConnectionService;
    private readonly IClusterProfileService _clusterProfileService;

    public TopicsController(IKafkaConnectionService kafkaConnectionService, IClusterProfileService clusterProfileService)
    {
        _kafkaConnectionService = kafkaConnectionService;
        _clusterProfileService = clusterProfileService;
    }

    [HttpGet("/names")]
    public IActionResult GetTopicsNames(string clusterProfile)
    {
        var profileResult = _clusterProfileService.GetByName(clusterProfile);

        if (profileResult.IsFailure)
            return BadRequest(profileResult.Failure.Message);

        var clientResult = _kafkaConnectionService.GetAdminClientMetadata(profileResult.Value.ClusterName!);

        if (clientResult.IsFailure) 
            return BadRequest(clientResult.Failure.Message);

        var adminClient = clientResult.Value!;

        var topics = adminClient.Topics.Select(x => x.Topic);

        return Ok(topics);
    }
}
