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
    public IActionResult GetTopicsNames(string clusterProfile)
        => _topicService.GetTopicsNames(clusterProfile).ToActionResult(this);
}
