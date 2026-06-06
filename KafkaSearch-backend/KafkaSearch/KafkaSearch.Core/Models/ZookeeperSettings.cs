namespace KafkaSearch.Core.Models;

public class ZookeeperSettings
{
	public bool EnableZookeeperAccess { get; set; }

	public required string Host { get; set; }

	public required string Port { get; set; }

	public string? ChrootPath { get; set; }
}
