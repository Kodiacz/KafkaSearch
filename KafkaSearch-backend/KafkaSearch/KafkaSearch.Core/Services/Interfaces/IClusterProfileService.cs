using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

namespace KafkaSearch.Core.Services.Interfaces;

public interface IClusterProfileService
{
	OperationResult<bool> CreateClusterProfile(ClusterProfile clusterProfile);
	OperationResult<bool> Delete(ClusterProfile clusterProfile);
	OperationResult<bool> Update(string clusterName);
	OperationResult<ClusterProfile> GetByName(string clusterName);
	OperationResult<ClusterProfile[]> GetByAll(string[] clusterNames);
}
