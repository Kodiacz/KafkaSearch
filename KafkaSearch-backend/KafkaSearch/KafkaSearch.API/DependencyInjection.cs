namespace KafkaSearch.API;

using KafkaSearch.API.Infrastructure;
using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Factories;
using KafkaSearch.Core.Filtering;
using KafkaSearch.Core.Services;
using KafkaSearch.Core.Services.Interfaces;

public static class DependencyInjection
{
    public static IServiceCollection AddKafkaSearchServices(this IServiceCollection services)
    {
        services.AddSingleton<IClusterProfileService, ClusterProfileService>();
        services.AddSingleton<IKafkaConnectionService, KafkaConnectionService>();
        services.AddSingleton<IMessageScanService, MessageScanService>();
        services.AddSingleton<ITopicService, TopicService>();
        services.AddSingleton<IKafkaClientFactory, KafkaClientFactory>();
        services.AddSingleton<IClusterClientProvider, ClusterClientProvider>();
        services.AddSingleton<IFilterEvaluator, FilterEvaluator>();
        services.AddSingleton<IFileSystem, FileSystem>();
        return services;
    }
}
