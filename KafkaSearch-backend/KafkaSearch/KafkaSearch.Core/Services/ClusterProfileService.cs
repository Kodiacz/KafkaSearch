namespace KafkaSearch.Core.Services;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Extensions;
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
        var directory = _kafkaOptions.Value.ClusterProfileDataPath;

        if (!_fileSystem.DirectoryExists(directory))
            return OperationResult.Fail<ClusterProfile[]>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidDirectory));

        return OperationResult.Try(() =>
        {
            var files = _fileSystem.GetFiles(directory, "*.json");

            return files
                .Select(file => JsonSerializer.Deserialize<ClusterProfile>(_fileSystem.ReadAllText(file))!)
                .ToArray();
        });
    }

	public OperationResult<ClusterProfile> GetByName(string clusterName)
	{
        var directory = _kafkaOptions.Value.ClusterProfileDataPath;

        if (!_fileSystem.DirectoryExists(directory))
            return OperationResult.Fail<ClusterProfile>(Failure.Validation(ClusterProfileServiceErrorMessages.ClusterNameNotFound, 404));

        var filePath = Path.Combine(directory, clusterName);

        return OperationResult.Try(() =>
        {
            var json = _fileSystem.ReadAllText(filePath);
            return JsonSerializer.Deserialize<ClusterProfile>(json)!;
        });
    }

	public OperationResult<bool> Update(string existingClusterName, ClusterProfile NewClusterProfile)
	{
        if (string.IsNullOrWhiteSpace(existingClusterName))
            return OperationResult.Fail<bool>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidClusterName));

        var (isValid, message) = ValidateClusterProfile(NewClusterProfile);
        if (!isValid)
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

	private (bool, string) ValidateClusterProfile(ClusterProfile clusterProfile)
    {
        if (clusterProfile == null) return (false, "Cluster profile cannot be null.");

        if (string.IsNullOrWhiteSpace(clusterProfile.BootstrapServers)) return (false, "Bootstrap servers cannot be null or whitespace.");
        var (isValid, message) = ClusterProfileExtensions.IsClusterProfileNameValid(clusterProfile.ClusterName);
        if (!isValid) return (false, message);

        return (true, string.Empty);
    }

    private static bool IsValidClusterName(string clusterName)
    {
        if (string.IsNullOrWhiteSpace(clusterName)) return false;
        if (clusterName.Length > 64) return false;

        foreach (var c in clusterName)
            if (!char.IsLetterOrDigit(c) && c != '-' && c != '_')
                return false;

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