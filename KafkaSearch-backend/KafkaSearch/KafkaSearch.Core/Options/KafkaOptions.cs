namespace KafkaSearch.Core.Options;

using System.ComponentModel.DataAnnotations;

public class KafkaOptions
{
	[Required]
	public string ClusterProfileDataPath { get; set; } = null!;
}
