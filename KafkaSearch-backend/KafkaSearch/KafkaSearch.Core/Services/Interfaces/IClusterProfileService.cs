using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

namespace KafkaSearch.Core.Services.Interfaces;

public interface IClusterProfileService
{
	OperationResult<bool> CreateClusterProfile(ClusterProfile clusterProfile);
}
