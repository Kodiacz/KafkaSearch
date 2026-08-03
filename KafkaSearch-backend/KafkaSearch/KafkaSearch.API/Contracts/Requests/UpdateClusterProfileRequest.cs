namespace KafkaSearch.API.Contracts.Requests;

using KafkaSearch.Core.Models;

public sealed record UpdateClusterProfileRequest(
    string? BootstrapServers,
    string? KafkaClusterVersion,
    ZookeeperSettings? ZookeeperSettings)
{
    public ClusterProfile ToClusterProfile(string clusterName)
    {
        return new ClusterProfile()
        {
            ClusterName = clusterName,
            BootstrapServers = BootstrapServers!,
            KafkaClusterVersion = KafkaClusterVersion ?? null,
            ZookeeperSettings = ZookeeperSettings ?? null
        };
    }
}
