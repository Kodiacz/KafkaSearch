namespace KafkaSearch.Core.Services;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Options;
using System.Text.Json;

public class ClusterProfileService(
    IOptions<KafkaOptions> kafkaOptions,
    IFileSystem fileSystem) : IClusterProfileService
{
    public OperationResult<bool> Create(ClusterProfile clusterProfile)
    {
        if (!ValidateClusterProfile(clusterProfile))
        {
            return OperationResult.Fail<bool>(Failure.Validation("Invalid cluster profile."));
        }
        var directory = kafkaOptions.Value.ClusterProfileDataPath;

		var path = Path.Combine(kafkaOptions.Value.ClusterProfileDataPath, $"{clusterProfile.ClusterName}-ClusterProfile.json");

        if (fileSystem.FileExists(path))
        {
            return OperationResult.Fail<bool>(Failure.Validation("Cluster profile already exists."));
        }

		var json = JsonSerializer.Serialize(clusterProfile, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        var result = OperationResult.Try(() => {
            fileSystem.WriteAllText(path, json);
            return true;
		});

        return result;
    }

	public OperationResult<bool> Delete(string clusterName)
	{
		throw new NotImplementedException();
	}

	public OperationResult<ClusterProfile[]> GetAll()
	{
		throw new NotImplementedException();
	}

	public OperationResult<ClusterProfile> GetByName(string clusterName)
	{
		throw new NotImplementedException();
	}

	public OperationResult<bool> Update(string clusterName, ClusterProfile clusterProfile)
	{
		throw new NotImplementedException();
	}

	private bool ValidateClusterProfile(ClusterProfile clusterProfile)
    {
        if (clusterProfile == null) return false;

        if (string.IsNullOrEmpty(clusterProfile.ClusterName)) return false;

        if (string.IsNullOrEmpty(clusterProfile.BootstrapServers)) return false;

        return true;
    }
}