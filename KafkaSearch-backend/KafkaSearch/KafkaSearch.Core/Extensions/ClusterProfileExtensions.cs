namespace KafkaSearch.Core.Extensions;

using KafkaSearch.Core.Models;
using System.Text.RegularExpressions;

public static class ClusterProfileExtensions
{
    public static string GetClusterProfileFilePath(string clusterProfileDataPath, string clusterName)
    {
        return Path.Combine(clusterProfileDataPath, $"{clusterName}.json");
    }   

    public static (bool, string) IsClusterProfileNameValid(this ClusterProfile clusterProfile)
    {
        if (string.IsNullOrWhiteSpace(clusterProfile.ClusterName))
            return (false, "Cluster name cannot be null or whitespace.");
        if (clusterProfile.ClusterName.Length < 3 || clusterProfile.ClusterName.Length > 50)
            return (false, "Cluster name must be between 3 and 50 characters long.");
        if (!Regex.IsMatch(clusterProfile.ClusterName, @"^[a-zA-Z0-9_-]+$"))
            return (false, "Cluster name can only contain alphanumeric characters, underscores, and hyphens.");
        if (clusterProfile.ClusterName.EndsWith("ClusterProfile", StringComparison.OrdinalIgnoreCase))
            return (false, "Cluster name cannot end with 'ClusterProfile'.");
        return (true, string.Empty);
    }
}
