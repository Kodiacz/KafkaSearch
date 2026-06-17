using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

namespace KafkaSearch.Core.Services.Interfaces;

public interface IClusterProfileService
{
	OperationResult<bool> Create(ClusterProfile clusterProfile);
	OperationResult<bool> Delete(ClusterProfile clusterProfile);
	OperationResult<bool> Update(string clusterName);
	OperationResult<ClusterProfile> GetByName(string clusterName);
	OperationResult<ClusterProfile[]> GetAll(string[] clusterNames);
}
