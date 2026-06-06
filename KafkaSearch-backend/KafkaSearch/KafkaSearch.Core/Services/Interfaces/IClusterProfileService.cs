using KafkaSearch.Core.Models;

namespace KafkaSearch.Core.Services.Interfaces;

public interface IClusterProfileService
{
	bool CreateClusterProfile(ClusterProfile clusterProfile);
}
