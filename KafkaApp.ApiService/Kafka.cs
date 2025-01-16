using Confluent.Kafka;
using System.Text.Json;

namespace KafkaApp.ApiService;

public class KafkaProducer<TKey, TValue>
{
    private readonly IProducer<TKey, TValue> _producer;

    public KafkaProducer(string bootstrapServers)
    {
        var config = new ProducerConfig
        {
            AllowAutoCreateTopics = true,
            BootstrapServers = bootstrapServers,
            Acks = Acks.All, // Ensure message delivery
            LingerMs = 5,    // Batch messages
            EnableIdempotence = true // Ensure exactly-once delivery,

        };

        _producer = new ProducerBuilder<TKey, TValue>(config)
              .Build();
    }

    public async Task ProduceAsync(string topic, TKey key, TValue value)
    {
        try
        {
            var deliveryReport = await _producer.ProduceAsync(topic, new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            });

            Console.WriteLine($"Delivered to: {deliveryReport.TopicPartitionOffset}");
        }
        catch (ProduceException<TKey, TValue> ex)
        {
            Console.WriteLine($"Error producing message: {ex.Error.Reason}");
        }
    }

    public void Dispose()
    {
        _producer?.Dispose();
    }
}


public class KafkaConsumer<TKey, TValue>
{
    private readonly IConsumer<TKey, TValue> _consumer;

    public KafkaConsumer(string bootstrapServers, string groupId)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false, // Manual offset commits
            EnablePartitionEof = true,
            AllowAutoCreateTopics = true
        };

        _consumer = new ConsumerBuilder<TKey, TValue>(config)
            .Build();
    }

    public void Consume(string topic, CancellationToken cancellationToken)
    {
        _consumer.Subscribe(topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = _consumer.Consume(cancellationToken);

                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine($"Reached end of partition: {consumeResult.Partition}");
                        continue;
                    }

                    Console.WriteLine($"Consumed message: {consumeResult.Message.Value}");

                    // Process the message (e.g., save to DB, trigger events)

                    // Manually commit the offset after processing
                    _consumer.Commit(consumeResult);
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"Consume error: {ex.Error.Reason}");
                }
            }
        }
        finally
        {
            _consumer.Close(); // Ensure the consumer is closed gracefully
        }
    }
}


public class KafkaConsumerService : BackgroundService
{
    private readonly KafkaConsumer<string, string> _consumer;

    public KafkaConsumerService(KafkaConsumer<string, string> consumer)
    {
        _consumer = consumer;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return Task.Run(() => _consumer.Consume("my-topic", stoppingToken), stoppingToken);
    }
}


public class JsonSerializer<T> : ISerializer<T>, IDeserializer<T>
{
    public byte[] Serialize(T data, SerializationContext context)
        => JsonSerializer.SerializeToUtf8Bytes(data);

    public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => JsonSerializer.Deserialize<T>(data);
}


public class KafkaSettings
{
    public string BootstrapServers { get; set; }
    public string GroupId { get; set; }
    public string Topic { get; set; }
}
