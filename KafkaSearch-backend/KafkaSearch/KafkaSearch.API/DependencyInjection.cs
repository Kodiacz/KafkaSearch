namespace KafkaSearch.API;

using KafkaSearch.API.Infrastructure;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Services;
using KafkaSearch.Core.Services.Interfaces;

public static class DependencyInjection
{
    public static IServiceCollection AddKafkaSearchServices(this IServiceCollection services)
    {
        // Register your services here
        services.AddSingleton<IClusterProfileService, ClusterProfileService>();
        services.AddSingleton<IKafkaConnectionService, KafkaConnectionService>();
        services.AddSingleton<IFileSystem, FileSystem>();
        return services;
    }
}
