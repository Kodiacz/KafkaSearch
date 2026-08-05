using KafkaSearch.API.Contracts.Requests;
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
    [Route("api/KafkaSearch/GetProfiles")]
    public IActionResult GetAll()
    {
        var result = _clusterProfileService.GetAll();

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return Ok(result.Value);
    }

    [HttpGet("{clusterName}")]
    public IActionResult GetByName([FromRoute] string clusterName)
    {
        var result = _clusterProfileService.GetByName(clusterName);

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return Ok(result.Value);
    }

    [HttpPost]
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
    {
        var result = _clusterProfileService.Delete(clusterName);

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return NoContent();
    }

    [HttpPost("{clusterName}/test-connection")]
    public IActionResult TestConnection([FromRoute] string clusterName)
    {
        var result = _kafkaConnectionService.GetAdminClientMetadata(clusterName);

        if (result.IsFailure)
            return result.Failure.IsValidation
                ? BadRequest(result.Failure.Message)
                : StatusCode(500, result.Failure.Message);

        return Ok();
    }
}