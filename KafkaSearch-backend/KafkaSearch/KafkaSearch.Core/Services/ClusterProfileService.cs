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
		if (!ValidateClusterProfile(clusterProfile))
		{
			return OperationResult.Fail<bool>(Failure.Validation("Invalid cluster profile."));
		}



		var result = OperationResult.Ok(true);
		return result;
	}

	private bool ValidateClusterProfile(ClusterProfile clusterProfile)
	{
		if (clusterProfile == null) return false;

		if (string.IsNullOrEmpty(clusterProfile.ClusterName)) return false;

		if (string.IsNullOrEmpty(clusterProfile.BootstrapServers)) return false;

		return true;
	}
}
