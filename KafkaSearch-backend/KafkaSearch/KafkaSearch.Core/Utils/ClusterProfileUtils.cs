using KafkaSearch.Core.Common;
using KafkaSearch.Core.Models;

namespace KafkaSearch.Core.Utils;

public class ClusterProfileUtils
{
    public Func<ClusterProfile, Failure> ValidationRules => (profile) =>
    {
        if (string.IsNullOrWhiteSpace(profile.ClusterName))
            return Failure.Validation("ClusterName cannot be null or empty.");
        if (string.IsNullOrWhiteSpace(profile.BootstrapServers))
            return Failure.Validation("BootstrapServers cannot be null or empty.");
        return null; // No validation errors
    };
}
