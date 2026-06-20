using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services;
using Microsoft.Extensions.Options;
using NSubstitute;
using System.Text.Json;
using static KafkaSearch.Core.Services.ClusterProfileService;

namespace Kafka.Core.Test;

public class ClusterProfileServiceTests : IDisposable
{
    private readonly IFileSystem _fileSystem;
    private readonly IOptions<KafkaOptions> _kafkaOptions;
    private readonly ClusterProfileService _clusterProfileService;

    public ClusterProfileServiceTests()
    {
        _fileSystem = Substitute.For<IFileSystem>();
        _kafkaOptions = Substitute.For<IOptions<KafkaOptions>>();
        _clusterProfileService = new ClusterProfileService(_kafkaOptions, _fileSystem);
    }

    #region Create

    [Fact]
    public void Create_WithNullProfile_ReturnsValidationFailure()
    {
        // Arrange

        ClusterProfile clusterProfile = null;

        // Act

        var result = _clusterProfileService.Create(clusterProfile);

        // Assert

        Assert.False(result.IsSuccess);
        Assert.False(result.Value);
        Assert.True(result.IsFailure);
        Assert.Equal(ClusterProfileServiceErrorMessages.InvalidClusterProfile, result.Failure.Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Create_WithInvalidClusterName_ReturnsValidationFailure(string clusterName)
    {
        // Arrange
        var clusterProfile = new ClusterProfile
        {
            ClusterName = clusterName,
            BootstrapServers = "localhost:9092"
        };

        // Act
        var result = _clusterProfileService.Create(clusterProfile);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.False(result.Value);
        Assert.True(result.IsFailure);
        Assert.Equal(ClusterProfileServiceErrorMessages.InvalidClusterProfile, result.Failure.Message);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    public void Create_WithInvalidClusterBootstrapServers_ReturnsValidationFailure(string bootstrapServers)
    {
        // Arrange
        var clusterProfile = new ClusterProfile
        {
            ClusterName = "TestCluster",
            BootstrapServers = bootstrapServers
        };

        // Act
        var result = _clusterProfileService.Create(clusterProfile);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.False(result.Value);
        Assert.True(result.IsFailure);
        Assert.Equal(ClusterProfileServiceErrorMessages.InvalidClusterProfile, result.Failure.Message);
    }

    [Fact]
    public void Create_IfPathExists_ReturnsValidationFailure()
    {
        // Arrange

        ClusterProfile clusterProfile = new ClusterProfile
        {
            ClusterName = "TestCluster",
            BootstrapServers = "localhost:9092"
        };

        var directory = "C:\\NonExistentPath";
        _kafkaOptions.Value.Returns(new KafkaOptions { ClusterProfileDataPath = directory });
        var path = Path.Combine(directory, string.Format(ClusterProfileFilePattern, clusterProfile.ClusterName));
        _fileSystem.FileExists(path).Returns(true);

        // Act

        var result = _clusterProfileService.Create(clusterProfile);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.False(result.Value);
        Assert.True(result.IsFailure);
        Assert.Equal(ClusterProfileServiceErrorMessages.AlreadyExists, result.Failure.Message);
    }

    [Fact]
    public void Create_CreatesJson_ReturnsSuccess()
    {
        // Arrange
        ClusterProfile clusterProfile = new ClusterProfile
        {
            ClusterName = "TestCluster",
            BootstrapServers = "localhost:9092"
        };

        var directory = "C:\\NonExistentPath";
        _kafkaOptions.Value.Returns(new KafkaOptions { ClusterProfileDataPath = directory });
        var expectedPath = Path.Combine(directory, string.Format(ClusterProfileFilePattern, clusterProfile.ClusterName));
        _fileSystem.FileExists(expectedPath).Returns(false);

        // Act
        var result = _clusterProfileService.Create(clusterProfile);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.True(result.Value);
        Assert.False(result.IsFailure);
        _fileSystem.Received(1).WriteAllText(
             expectedPath,
             Arg.Do<string>(json =>
             {
                 var deserialized = JsonSerializer.Deserialize<ClusterProfile>(json);
                 Assert.Equal(clusterProfile.ClusterName, deserialized.ClusterName);
                 Assert.Equal(clusterProfile.BootstrapServers, deserialized.BootstrapServers);
             }));
    }

    #endregion

    #region Delete

    #endregion

    #region Update

    #endregion

    #region GetByName

    #endregion

    #region GetAll

    #endregion
    public void Dispose()
    {

    }
}
