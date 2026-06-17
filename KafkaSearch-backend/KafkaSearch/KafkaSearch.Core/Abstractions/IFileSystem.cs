namespace KafkaSearch.Core.Abstractions;

public interface IFileSystem
{
    bool FileExists(string path);
    void WriteAllText(string path, string content);
    string ReadAllText(string path);
    void DeleteFile(string path);
    bool DirectoryExists(string path);
    void CreateDirectory(string path);
    string[] GetFiles(string path, string searchPattern);
}
