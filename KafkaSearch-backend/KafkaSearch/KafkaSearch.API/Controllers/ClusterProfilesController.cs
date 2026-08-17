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
    private readonly IClusterClientProvider _clusterClientProvider;

    public ClusterProfilesController(
        IClusterProfileService clusterProfileService, 
        IClusterClientProvider clusterClientProvider)
    {
        _clusterProfileService = clusterProfileService;
        _clusterClientProvider = clusterClientProvider;
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
        => _clusterProfileService.Create(clusterProfile).ToActionResult(this);

    [HttpPut("{existingClusterName}")]
    public IActionResult Update([FromRoute] string existingClusterName, [FromBody] UpdateClusterProfileRequest updateClusterProfileRequest)
        => _clusterProfileService
                .Update(existingClusterName, updateClusterProfileRequest.ToClusterProfile(existingClusterName))
                .ToActionResult(this);
    
    [HttpDelete("{clusterName}")]
    public IActionResult Delete([FromRoute] string clusterName)
        => _clusterProfileService.Delete(clusterName).ToActionResult(this);
    

    [HttpPost("{clusterName}/test-connection")]
    public IActionResult TestConnection([FromRoute] string clusterName)
        => _clusterClientProvider.MetadataFor(clusterName).ToActionResult(this);

}