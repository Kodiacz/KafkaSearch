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

		var pathResult = CreatePath(clusterProfile.ClusterName);

        if (pathResult.IsFailure)
            return OperationResult.Fail<bool>(pathResult.Failure);

        if (_fileSystem.FileExists(pathResult.Value!))
        {
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.AlreadyExists));
        }

		var json = JsonSerializer.Serialize(clusterProfile, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        var result = OperationResult.Try(() => {
            _fileSystem.WriteAllText(pathResult.Value!, json);
            return true;
		});

        return result;
    }

	public OperationResult<bool> Delete(string clusterName)
	{
        if (string.IsNullOrWhiteSpace(clusterName))
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidClusterName));
        
        var pathResult = CreatePath(clusterName);

        if (pathResult.IsFailure)
            return OperationResult.Fail<bool>(pathResult.Failure);

        if (!_fileSystem.FileExists(pathResult.Value!))
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.ClusterNameNotFound, 404));

        var result = OperationResult.Try(() =>
        {
            _fileSystem.DeleteFile(pathResult.Value!);
            return true;
        });

        return result;
    }

	public OperationResult<ClusterProfile[]> GetAll()
	{
		throw new NotImplementedException();
	}

	public OperationResult<ClusterProfile> GetByName(string clusterName)
	{
		throw new NotImplementedException();
	}

	public OperationResult<bool> Update(string existingClusterName, ClusterProfile NewClusterProfile)
	{
        if (string.IsNullOrWhiteSpace(existingClusterName))
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidClusterName));

        if (!ValidateClusterProfile(NewClusterProfile))
        {
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidClusterProfile));
        }

        var pathResult = CreatePath(existingClusterName);

        if (pathResult.IsFailure)
            return OperationResult.Fail<bool>(pathResult.Failure);

        if (!_fileSystem.FileExists(pathResult.Value!))
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.ClusterNameNotFound, 404));

        var json = JsonSerializer.Serialize(NewClusterProfile, new JsonSerializerOptions()
        {
            WriteIndented = true,
        });

        var result = OperationResult.Try(() =>
        {
            _fileSystem.WriteAllText(pathResult.Value!, json);
            return true;
        });

        return result;
    }

	private bool ValidateClusterProfile(ClusterProfile clusterProfile)
    {
        if (clusterProfile == null) return false;

        if (string.IsNullOrWhiteSpace(clusterProfile.ClusterName)) return false;

        if (string.IsNullOrWhiteSpace(clusterProfile.BootstrapServers)) return false;

        return true;
    }

    private OperationResult<string> CreatePath(string clusterName)
    {
        var directory = _kafkaOptions.Value.ClusterProfileDataPath;

        if (_fileSystem.DirectoryExists(directory) == false)
            return OperationResult.Fail<string>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidDirectory, 404));

        return OperationResult.Ok(Path.Combine(directory, string.Format(ClusterProfileFilePattern, clusterName)));
    }
}