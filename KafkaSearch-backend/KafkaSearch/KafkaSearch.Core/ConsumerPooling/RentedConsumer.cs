namespace KafkaSearch.Core.ConsumerPooling;

using Confluent.Kafka;

public sealed class RentedConsumer : IDisposable
{
    private readonly ClusterConsumerPool _pool;
    private readonly PooledConsumerEntry _entry;
    private bool _healthy = true;
    private bool _disposed;

    internal RentedConsumer(ClusterConsumerPool pool, PooledConsumerEntry entry)
    {
        _pool = pool;
        _entry = entry;
    }

    public IConsumer<byte[], byte[]> Consumer => _entry.Consumer;

    public void MarkUnhealthy() => _healthy = false;

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;
        _pool.Return(_entry, _healthy);
    }
}
