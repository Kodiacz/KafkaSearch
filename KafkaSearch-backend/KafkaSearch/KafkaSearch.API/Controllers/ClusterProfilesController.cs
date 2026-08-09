using KafkaSearch.API.Contracts.Requests;
using KafkaSearch.API.Extensions;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace KafkaSearch.API.Controllers;

[ApiController]
[Route("api/cluster-profiles")]
public class ClusterProfilesController : ControllerBase
{
    private readonly IClusterProfileService _clusterProfileService;
    private readonly IKafkaConnectionService _kafkaConnectionService;

    public ClusterProfilesController(
        IClusterProfileService clusterProfileService, 
        IKafkaConnectionService kafkaConnectionService)
    {
        _clusterProfileService = clusterProfileService;
        _kafkaConnectionService = kafkaConnectionService;
    }

    [HttpGet]
    [Route("get-profiles")]
    public IActionResult GetAll()
        => _clusterProfileService.GetAll().ToActionResult(this);

    [HttpGet("{clusterName}")]
    public IActionResult GetByName([FromRoute] string clusterName)
        => _clusterProfileService.GetByName(clusterName).ToActionResult(this);

    [HttpPost("create")]
    public IActionResult Create([FromBody] ClusterProfile clusterProfile)
    {
        var result = _clusterProfileService.Create(clusterProfile);

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return Created();
    }

    [HttpPut("{existingClusterName}")]
    public IActionResult Update([FromRoute] string existingClusterName, [FromBody] UpdateClusterProfileRequest updateClusterProfileRequest)
    {
        var result = _clusterProfileService
            .Update(existingClusterName, updateClusterProfileRequest.ToClusterProfile(existingClusterName));

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return Ok();
    }

    [HttpDelete("{clusterName}")]
    public IActionResult Delete([FromRoute] string clusterName)
        => _clusterProfileService.Delete(clusterName).ToActionResult(this);
    

    [HttpPost("{clusterName}/test-connection")]
    public IActionResult TestConnection([FromRoute] string clusterName)
        => _kafkaConnectionService.GetAdminClientMetadata(clusterName).ToActionResult(this);
    
}