namespace KafkaSearch.Core.Models;

public class ClusterProfile
{
	public required string ClusterName { get; init; }

	public required string BootstrapServers { get; init; }

	public string? KafkaClusterVersion { get; init; }

	public ZookeeperSettings? ZookeeperSettings { get; init; }
}
