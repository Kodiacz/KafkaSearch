namespace KafkaSearch.Core.Options;

using System.ComponentModel.DataAnnotations;

public class KafkaOptions
{
	[Required]
	public string ClusterProfileDataPath { get; init; } = null!;
}
