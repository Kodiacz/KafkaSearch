namespace KafkaSearch.API.BacgroundServices;

using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Options;
using Microsoft.Extensions.Options;

public class AppStartupService(
    IFileSystem fileSystem,
    IOptions<KafkaOptions> kafkaOptions) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        fileSystem.CreateDirectory(kafkaOptions.Value.ClusterProfileDataPath);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        // runs on shutdown
        return Task.CompletedTask;
    }
}
