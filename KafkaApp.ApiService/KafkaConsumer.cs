using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaApp.ApiService;

public class KafkaConsumer : IKafkaConsumer
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IConfiguration _configuration;

    public KafkaConsumer(IConsumer<string, string> consumer,
        IConfiguration configuration)
    {
        _consumer = consumer;
        _configuration = configuration;
    }

    static async Task CreateTopicAsync(string bootstrapServers, string topicName)
    {
        using var adminClient =
            new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        try
        {
            await adminClient.CreateTopicsAsync([
                new TopicSpecification
                {
                    Name = topicName, ReplicationFactor = 1, NumPartitions = 1
                }
            ]);
        }
        catch (CreateTopicsException e)
        {
            Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
        }
    }

    public void Consume<TMessage>(string topic, Func<ConsumeResult<string, TMessage>, Task> onMessage,
        CancellationToken cancellationToken = default)
    {
        var kafka = _configuration.GetConnectionString("Kafka");

        CreateTopicAsync(kafka, topic).GetAwaiter().GetResult();

        _consumer.Subscribe(topic);

        Task.Run(() =>
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = _consumer.Consume(cancellationToken);

                    if (result != null && result.Message != null)
                    {
                        // Deserialize message to TMessage
                        var message = JsonSerializer.Deserialize<TMessage>(result.Message.Value.ToString());

                        var consumeResult = new ConsumeResult<string, TMessage>
                        {
                            Topic = result.Topic,
                            Partition = result.Partition,
                            Offset = result.Offset,
                            Message = new Message<string, TMessage>
                            {
                                Key = result.Message.Key,
                                Value = message,
                                Timestamp = result.Message.Timestamp
                            },
                        };

                        onMessage?.Invoke(consumeResult);
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"Consumption canceled for topic {topic}");
            }
        }, cancellationToken);
    }
}