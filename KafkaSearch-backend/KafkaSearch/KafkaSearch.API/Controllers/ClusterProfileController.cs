using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace KafkaSearch.API.Controllers;

[Route("api/[controller]")]
[ApiController]
public class ClusterProfileController : ControllerBase
{
    private readonly IClusterProfileService _clusterProfileService;

    public ClusterProfileController(IClusterProfileService clusterProfileService)
    {
        _clusterProfileService = clusterProfileService;
    }

    [HttpPost]
    public IActionResult Create([FromBody] ClusterProfile clusterProfile)
    {
        var result = _clusterProfileService.Create(clusterProfile);

        if (result.IsFailure)
            return BadRequest(result.Failure.Message);
        
        return Created();
    }
}
