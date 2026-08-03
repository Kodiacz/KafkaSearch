namespace KafkaSearch.Core.Models.Rules;

using KafkaSearch.Core.Common;

public static class ClusterProfileRules
{
    public const string BootstrapServersRequired = "Bootstrap servers cannot be null or whitespace.";
    public const string BootstrapServersWhitespace = "Bootstrap servers cannot contain whitespace.";

    public const string ClusterNameRequired = "Cluster name cannot be null or whitespace.";
    public const string ClusterNameInvalidCharacters = "Cluster name can only contain letters, digits, hyphens and underscores.";
    public const string ClusterNameCannotEndWithClusterProfile = "Cluster name cannot end with '-ClusterProfile'.";

    public static Failure ClusterName(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Failure.Validation(ClusterNameRequired);

        if (value.Length > 64)
            return Failure.Validation("Cluster name cannot exceed 64 characters.");

        if (!value.All(c => char.IsLetterOrDigit(c) || c == '-' || c == '_'))
            return Failure.Validation(ClusterNameInvalidCharacters);

        if (value.EndsWith("-ClusterProfile", StringComparison.OrdinalIgnoreCase) ||
            value.EndsWith("ClusterProfile", StringComparison.OrdinalIgnoreCase))
            return Failure.Validation(ClusterNameCannotEndWithClusterProfile);

        return Failure.NoFailure;
    }

    public static Failure BootstrapServers(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return Failure.Validation(BootstrapServersRequired);

        if (value.Any(char.IsWhiteSpace))
            return Failure.Validation(BootstrapServersWhitespace);

        return Failure.NoFailure;
    }

    public static Failure KafkaClusterVersion(string? value)
        => value is not null && string.IsNullOrWhiteSpace(value)
            ? Failure.Validation("Kafka cluster version cannot be empty when provided.")
            : Failure.NoFailure;

    public static Failure Zookeeper(ZookeeperSettings? value)
    {
        if (value is null || !value.EnableZookeeperAccess)
            return Failure.NoFailure;

        if (string.IsNullOrWhiteSpace(value.Host))
            return Failure.Validation("Zookeeper host is required when Zookeeper access is enabled.");

        if (string.IsNullOrWhiteSpace(value.Port))
            return Failure.Validation("Zookeeper port is required when Zookeeper access is enabled.");

        return Failure.NoFailure;
    }
}
