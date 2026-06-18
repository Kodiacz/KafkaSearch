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
    public static class ClusterProfileServiceErrorMessages
    {
        public const string InvalidClusterProfile = "Invalid cluster profile.";
        public const string AlreadyExists = "Cluster profile already exists.";
        public const string ClusterProfileDataPathDoesNotExist = "Cluster profile data path does not exist.";
    }

    public const string ClusterProfileFilePattern = "{0}-ClusterProfile.json";

    public OperationResult<bool> Create(ClusterProfile clusterProfile)
    {
        if (!ValidateClusterProfile(clusterProfile))
        {
            return OperationResult.Fail<bool>(Failure.Validation("Invalid cluster profile."));
        }

        var directory = kafkaOptions.Value.ClusterProfileDataPath;

		var path = Path.Combine(directory, string.Format(ClusterProfileFilePattern, clusterProfile.ClusterName));

        if (!fileSystem.FileExists(path))
        {
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.ClusterProfileDataPathDoesNotExist));
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

        if (string.IsNullOrEmpty(clusterProfile.ClusterName) || string.IsNullOrWhiteSpace(clusterProfile.ClusterName)) return false;

        if (string.IsNullOrEmpty(clusterProfile.BootstrapServers) || string.IsNullOrWhiteSpace(clusterProfile.BootstrapServers)) return false;

        return true;
    }
}