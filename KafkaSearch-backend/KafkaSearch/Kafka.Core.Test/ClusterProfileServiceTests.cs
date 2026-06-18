using KafkaSearch.Core.Abstractions;
using KafkaSearch.Core.Models;
using KafkaSearch.Core.Options;
using KafkaSearch.Core.Services;
using Microsoft.Extensions.Options;
using NSubstitute;
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

    [Fact] 
    public void Create_WithEmptyClusterName_ReturnsValidationFailure() 
    {
        // Arrange

        ClusterProfile clusterProfile = new ClusterProfile
        {
            ClusterName = "",
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

    [Fact]
    public void Create_WithWhiteSpaceClusterName_ReturnsValidationFailure()
    {
        // Arrange

        ClusterProfile clusterProfile = new ClusterProfile
        {
            ClusterName = "   ",
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

    [Fact] 
    public void Create_WithEmptyBootstrapServers_ReturnsValidationFailure() 
    {
        // Arrange

        ClusterProfile clusterProfile = new ClusterProfile
        {
            ClusterName = "TestCluster",
            BootstrapServers = ""
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
    public void Create_WithWhiteSpaceBootstrapServers_ReturnsValidationFailure()
    {
        // Arrange

        ClusterProfile clusterProfile = new ClusterProfile
        {
            ClusterName = "TestCluster",
            BootstrapServers = "   "
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
    public void Create_IfPathDoesNotExist_ReturnsValidationFailure()
    {
        // Arrange

        ClusterProfile clusterProfile = new ClusterProfile
        {
            ClusterName = "TestCluster",
            BootstrapServers = "localhost:9092"
        };

        _kafkaOptions.Value.Returns(new KafkaOptions { ClusterProfileDataPath = "C:\\NonExistentPath" });

        // Act

        var result = _clusterProfileService.Create(clusterProfile);

        // Assert
        Assert.False(result.IsSuccess);
        Assert.False(result.Value);
        Assert.True(result.IsFailure);
        Assert.Equal(ClusterProfileServiceErrorMessages.ClusterProfileDataPathDoesNotExist, result.Failure.Message);
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

        _kafkaOptions.Value.Returns(new KafkaOptions { ClusterProfileDataPath = "C:\\ClusterProfiles" });
        _fileSystem.FileExists(Arg.Any<string>()).Returns(true);

        // Act
        var result = _clusterProfileService.Create(clusterProfile);

        // Assert
        Assert.True(result.IsSuccess);
        Assert.True(result.Value);
        Assert.False(result.IsFailure);
        var expectedPath = Path.Combine("C:\\ClusterProfiles", string.Format(ClusterProfileService.ClusterProfileFilePattern, clusterProfile.ClusterName));
        _fileSystem.Received(1).WriteAllText(expectedPath, Arg.Any<string>());
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
