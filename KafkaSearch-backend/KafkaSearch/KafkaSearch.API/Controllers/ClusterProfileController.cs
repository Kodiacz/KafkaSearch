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
    [Route("api/KafkaSearch/CreateProfile")]
    public IActionResult Create([FromBody] ClusterProfile clusterProfile)
    {
        var result = _clusterProfileService.Create(clusterProfile);

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return Created();
    }

    [HttpPost]
    [Route("api/KafkaSearch/UpdateProfile/{existingClusterName}")]
    public IActionResult Update([FromRoute] string existingClusterName, [FromBody] ClusterProfile newClusterProfile)
    {
        var result = _clusterProfileService.Update(existingClusterName, newClusterProfile);

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return Ok();
    }
}
