namespace KafkaSearch.Core.Services;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Models.Rules;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Options;
using System.Text.Json;

public class ClusterProfileService : IClusterProfileService
{
    public static class ClusterProfileServiceErrorMessages
    {
        public const string InvalidClusterProfile = "Cluster profile cannot be null.";
        public const string InvalidClusterProfileBootStrapServers = "Bootstrap servers cannot be null or whitespace.";
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
        var validationResult = Validate(clusterProfile);
        if (validationResult.IsFailure)
            return OperationResult.Fail<bool>(validationResult.Failure);
        
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
        var clusterNameValidation = ValidateClusterName(clusterName);
        if (clusterNameValidation.IsFailure)
            return OperationResult.Fail<bool>(clusterNameValidation.Failure);

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
        var clusterNameValidation = ValidateClusterName(clusterName);

        if (clusterNameValidation.IsFailure)
            return OperationResult.Fail<ClusterProfile>(clusterNameValidation.Failure);

        var pathResult = CreatePath(clusterName);

        if (pathResult.IsFailure)
            return OperationResult.Fail<ClusterProfile>(pathResult.Failure);

        var filePath = pathResult.Value!;

        return OperationResult.Try(() =>
        {
            var json = _fileSystem.ReadAllText(filePath);
            return JsonSerializer.Deserialize<ClusterProfile>(json)!;
        });
    }

	public OperationResult<bool> Update(string existingClusterName, ClusterProfile NewClusterProfile)
	{
        var clusterNameValidation = ValidateClusterName(existingClusterName);
        if (clusterNameValidation.IsFailure)
            return OperationResult.Fail<bool>(clusterNameValidation.Failure);

        var validationResult = Validate(NewClusterProfile);

        if (validationResult.IsFailure)
            return OperationResult.Fail<bool>(validationResult.Failure);

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

    private OperationResult Validate(ClusterProfile profile)
    {
        if (profile is null)
            return OperationResult.Fail(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidClusterProfile));

        var failures = new[]
        {
            ClusterProfileRules.ClusterName(profile.ClusterName),
            ClusterProfileRules.BootstrapServers(profile.BootstrapServers),
            ClusterProfileRules.KafkaClusterVersion(profile.KafkaClusterVersion),
            ClusterProfileRules.Zookeeper(profile.ZookeeperSettings)
        }
        .Where(f => f != Failure.NoFailure)
        .ToArray();

        return failures.Length == 0
            ? OperationResult.Ok()
            : OperationResult.Fail(Failure.Merge(failures));
    }

    private OperationResult ValidateClusterName(string clusterName)
    {
            var clusterNameValidation = ClusterProfileRules.ClusterName(clusterName);
        if (clusterNameValidation.Type != FailureType.None)
            return OperationResult.Fail(clusterNameValidation);

        return OperationResult.Ok();
    }

    private OperationResult<string> CreatePath(string clusterName)
    {
        var directory = _kafkaOptions.Value.ClusterProfileDataPath;

        if (_fileSystem.DirectoryExists(directory) == false)
            return OperationResult.Fail<string>(Failure.Validation(ClusterProfileServiceErrorMessages.InvalidDirectory, 404));

        return OperationResult.Ok(Path.Combine(directory, string.Format(ClusterProfileFilePattern, clusterName)));
    }
}