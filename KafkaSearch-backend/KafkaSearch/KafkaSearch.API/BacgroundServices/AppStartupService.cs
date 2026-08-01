namespace KafkaSearch.API.BacgroundServices;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services.Interfaces;
using Microsoft.Extensions.Options;

public class AppStartupService(
    IFileSystem fileSystem,
    IOptions<KafkaOptions> kafkaOptions,
    IClusterProfileService clusterProfileService,
    IKafkaConnectionService kafkaConnectionService,
    ILogger logger) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        CreateDefaultDirectoy();
        LoadAdminCache();
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        // runs on shutdown
        return Task.CompletedTask;
    }

    private Task CreateDefaultDirectoy()
    {
        fileSystem.CreateDirectory(kafkaOptions.Value.ClusterProfileDataPath);
        return Task.CompletedTask;
    }

    private Task LoadAdminCache()
    {
        var profilesResult = clusterProfileService.GetAll();

        if (profilesResult.IsFailure)
        {
            logger.LogError("Failed to load cluster profiles: {Error}", profilesResult.Failure.Message);
            return Task.CompletedTask;
        }

        foreach (var profile in profilesResult.Value)
            kafkaConnectionService.GetOrCreateAdminClient(profile.ClusterName);

        return Task.CompletedTask;
    }
}
