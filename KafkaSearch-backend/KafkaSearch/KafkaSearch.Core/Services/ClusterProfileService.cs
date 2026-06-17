namespace KafkaSearch.Core.Services;

using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Options;

public class ClusterProfileService(IOptions<KafkaOptions> kafkaOptions) : IClusterProfileService
{
	public OperationResult<bool> CreateClusterProfile(ClusterProfile clusterProfile)
	{
		 
		var result = OperationResult.Ok(true);
		return result;
	}
}
