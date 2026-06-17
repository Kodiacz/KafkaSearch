namespace KafkaSearch.API.Infrastructure;

using KafkaSearch.Core.Abstractions;

public class FileSystem : IFileSystem
{
    public void CreateDirectory(string path)
        => Directory.CreateDirectory(path);
    
    public void DeleteFile(string path)
        => File.Delete(path);

    public bool DirectoryExists(string path)
        => Directory.Exists(path);

    public bool FileExists(string path)
     => File.Exists(path);

    public string[] GetFiles(string path, string searchPattern)
        => Directory.GetFiles(path, searchPattern);

    public string ReadAllText(string path)
        => File.ReadAllText(path);

    public void WriteAllText(string path, string content)
        => File.WriteAllText(path, content);
}