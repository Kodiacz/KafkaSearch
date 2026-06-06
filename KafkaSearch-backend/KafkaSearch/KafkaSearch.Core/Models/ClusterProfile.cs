using System.Security.Principal;

namespace KafkaSearch.Core.Models;

public class ClusterProfile
{
	public string ClusterName { get; set; }

	public string BootstrapServerIP { get; set; }

	public string KafkaClusterVersion { get; set; }

	public ZookeeperSettings ZookeeperSettings { get; set; }
}
