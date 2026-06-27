namespace KafkaSearch.Core.Services;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Options;
using System.Text.Json;

public class ClusterProfileService : IClusterProfileService
{
    public static class ClusterProfileServiceErrorMessages
    {
        public const string InvalidClusterProfile = "Invalid cluster profile.";
        public const string AlreadyExists = "Cluster profile already exists.";
        public const string InvalidClusterName = "Invalid cluster name.";
        public const string ClusterNameNotFound = "Cluster name not found.";
        public const string InvalidDirectory = "Invalid data directory.";
    }

    public const string ClusterProfileFilePattern = "{0}-ClusterProfile.json";

    private readonly IFileSystem _fileSystem;
        private readonly IOptions<KafkaOptions> _kafkaOptions;

    public ClusterProfileService(
        IOptions<KafkaOptions> kafkaOptions,
        IFileSystem fileSystem) 
    {
           _fileSystem = fileSystem;
           _kafkaOptions = kafkaOptions;
    }

    public OperationResult<bool> Create(ClusterProfile clusterProfile)
    {
        if (!ValidateClusterProfile(clusterProfile))
        {
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidClusterProfile));
        }

		var path = CreatePath(clusterProfile.ClusterName);

        if (_fileSystem.FileExists(path))
        {
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.AlreadyExists));
        }

		var json = JsonSerializer.Serialize(clusterProfile, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        var result = OperationResult.Try(() => {
            _fileSystem.WriteAllText(path, json);
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

        if (string.IsNullOrWhiteSpace(clusterProfile.ClusterName)) return false;

        if (string.IsNullOrWhiteSpace(clusterProfile.BootstrapServers)) return false;

        return true;
    }

    private string CreatePath(string clusterName)
    {
        var directory = _kafkaOptions.Value.ClusterProfileDataPath;
        return Path.Combine(directory, string.Format(ClusterProfileFilePattern, clusterName));
    }
}