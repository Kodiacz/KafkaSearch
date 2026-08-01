namespace KafkaSearch.Core.Extensions;

using System.Text.RegularExpressions;

public class ClusterProfileExtensions
{
    public static string GetClusterProfileFilePath(string clusterProfileDataPath, string clusterName)
    {
        return Path.Combine(clusterProfileDataPath, $"{clusterName}.json");
    }   

    public static (bool, string) IsClusterProfileNameValid(string clusterName)
    {
        if (string.IsNullOrWhiteSpace(clusterName))
            return (false, "Cluster name cannot be null or whitespace.");
        if (clusterName.Length < 3 || clusterName.Length > 50)
            return (false, "Cluster name must be between 3 and 50 characters long.");
        if (!Regex.IsMatch(clusterName, @"^[a-zA-Z0-9_-]+$"))
            return (false, "Cluster name can only contain alphanumeric characters, underscores, and hyphens.");
        if (clusterName.EndsWith("ClusterProfile", StringComparison.OrdinalIgnoreCase))
            return (false, "Cluster name cannot end with 'ClusterProfile'.");
        return (true, string.Empty);
    }
}
