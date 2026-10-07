namespace KafkaSearch.API.Controllers;

using KafkaSearch.API.Extensions;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("api/topics")]
public class TopicsController : ControllerBase
{
    private ITopicService _topicService;

    public TopicsController(ITopicService topicService)
    {
        _topicService = topicService;   
    }

    [HttpGet("names")]
    public async Task<IActionResult> GetTopicsNames(string clusterProfile)
        => _topicService.GetTopicsNames(clusterProfile).ToActionResult(this);

    [HttpGet("describe/{topic}")]
    public async Task<IActionResult> GetTopicInformation(string clusterProfile, string topic)
    {
        var topicDescription = await _topicService.DescribeTopic(clusterProfile, topic);
        return topicDescription.ToActionResult(this);
    }
}
