namespace KafkaSearch.Core.Models;

public class ZookeeperSettings
{
	public bool EnableZookeeperAccess { get; set; }

	public string Host { get; set; }

	public string Port { get; set; }

	public string ChrootPath { get; set; }
}
