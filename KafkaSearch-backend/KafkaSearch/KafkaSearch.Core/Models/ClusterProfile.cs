namespace KafkaSearch.Core.Models;

public class ClusterProfile
{
	public required string ClusterName { get; set; }

	public required string BootstrapServers { get; set; }

	public string? KafkaClusterVersion { get; set; }

	public ZookeeperSettings? ZookeeperSettings { get; set; }
}
